"""
Reporting view builders for published hierarchies.

This module rebuilds derived Spark views from the published base hierarchy
tables. The resulting views are intended for downstream reporting and should be
treated as derived artifacts, not as the source of truth for hierarchy data.

Design notes
------------
This builder stays generic to the hierarchy engine. It produces structural
artifacts only:

- recursive path view
- flattened all-node view
- leaf-level dimension/reporting views
- all-node dimension/reporting views

Project-specific semantic enrichment, such as Power BI display metadata,
residual-row behavior, sign conventions, or special rollup rules, should be
implemented downstream in project-owned views by joining to the published
all-node reporting view.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


class HierarchyViewBuilder:
    """
    Build reporting-oriented hierarchy views from published base tables.

    Notes
    -----
    Table and view identifiers are expected to be trusted, fully qualified
    Spark object names supplied by the caller.
    """

    def __init__(self, spark: SparkSession, target_max_depth: int = 10):
        """Initialize the builder."""
        self.spark = spark
        self.target_max_depth = target_max_depth

    def rebuild_paths_view(
        self,
        node_table: str,
        paths_view: str,
    ) -> str:
        """
        Rebuild the recursive hierarchy path view.
        """
        logger.info("Rebuilding hierarchy paths view: %s", paths_view)
        sql_stmt = f"""
        CREATE OR REPLACE VIEW {paths_view} AS
        WITH RECURSIVE hierarchy_cte AS (
            SELECT
                n.hierarchy_id,
                n.version_id,
                n.account_key,
                n.account_name,
                n.parent_account_key,
                CAST(array(n.account_key) AS array<string>) AS path_keys,
                CAST(array(n.account_name) AS array<string>) AS path_names,
                1 AS depth,
                n.account_key AS root_account_key,
                n.account_name AS root_account_name
            FROM {node_table} n
            WHERE n.parent_account_key IS NULL

            UNION ALL

            SELECT
                c.hierarchy_id,
                c.version_id,
                c.account_key,
                c.account_name,
                c.parent_account_key,
                concat(p.path_keys, array(c.account_key)) AS path_keys,
                concat(p.path_names, array(c.account_name)) AS path_names,
                p.depth + 1 AS depth,
                p.root_account_key,
                p.root_account_name
            FROM {node_table} c
            JOIN hierarchy_cte p
              ON c.hierarchy_id = p.hierarchy_id
             AND c.version_id = p.version_id
             AND c.parent_account_key = p.account_key
            WHERE NOT array_contains(p.path_keys, c.account_key)
        )
        SELECT
            hierarchy_id,
            version_id,
            account_key,
            account_name,
            parent_account_key,
            path_keys,
            path_names,
            depth,
            root_account_key,
            root_account_name
        FROM hierarchy_cte
        """
        self.spark.sql(sql_stmt)
        return paths_view

    def rebuild_flat_view(
        self,
        node_table: str,
        paths_view: str,
        flat_view: str,
    ) -> str:
        """
        Rebuild the flattened all-node hierarchy reporting view.

        This view contains one row per hierarchy node, including derived level
        columns and a derived leaf flag. It remains structural and generic so
        downstream consumers can choose leaf-only or all-node projections.
        """
        logger.info("Rebuilding hierarchy flat view: %s", flat_view)
        max_depth = self.target_max_depth #self._get_max_depth(paths_view)

        select_cols = [
            "p.hierarchy_id",
            "p.version_id",
            "p.account_key",
            "p.account_name",
            "p.parent_account_key",
            "p.depth",
            "p.root_account_key",
            "p.root_account_name",
            "p.path_keys",
            "p.path_names",
            "child.account_key IS NULL AS derived_is_leaf",
        ]

        for idx in range(max_depth):
            level_num = idx + 1
            key_expr = f"""
                CASE
                    WHEN p.depth > {idx} THEN get(p.path_keys, {idx})
                    ELSE element_at(p.path_keys, -1)
                END
            """.strip()
            name_expr = f"""
                CASE
                    WHEN p.depth > {idx} THEN get(p.path_names, {idx})
                    ELSE element_at(p.path_names, -1)
                END
            """.strip()

            select_cols.append(f"{key_expr} AS level{level_num}_key")
            select_cols.append(f"{name_expr} AS level{level_num}_name")
            select_cols.append(f"CAST({key_expr} AS INT) AS level{level_num}_sort")

        select_clause = ",\n            ".join(select_cols)
        sql_stmt = f"""
        CREATE OR REPLACE VIEW {flat_view} AS
        SELECT
            {select_clause}
        FROM {paths_view} p
        LEFT JOIN {node_table} child
          ON p.hierarchy_id = child.hierarchy_id
         AND p.version_id = child.version_id
         AND p.account_key = child.parent_account_key
        """
        self.spark.sql(sql_stmt)
        return flat_view

    def rebuild_leaf_dims_view(
        self,
        registry_table: str,
        version_table: str,
        flat_view: str,
        dims_view: str,
    ) -> str:
        """
        Rebuild the leaf-level reporting dimension view.

        This view is intended for fact-to-leaf mapping scenarios.
        """
        logger.info("Rebuilding hierarchy leaf dims view: %s", dims_view)
        max_depth = self.target_max_depth  #self._get_max_depth(flat_view)

        select_cols = [
            "f.hierarchy_id",
            "r.hierarchy_name",
            "r.hierarchy_description",
            "r.owner_team",
            "r.business_domain",
            "f.version_id",
            "concat(f.hierarchy_id, '||', f.version_id) AS hier_ver_key",
            "v.version_name",
            "v.version_status",
            "v.effective_start_date",
            "v.effective_end_date",
            "v.is_current",
            "f.account_key AS leaf_key",
            "f.account_name AS leaf_name",
            "concat(f.hierarchy_id, '||', f.account_key) AS hier_leaf_key",
            "concat(f.hierarchy_id, '||', f.version_id, '||', f.account_key) "
            "AS hier_ver_leaf_key",
            "f.depth",
            "f.root_account_key",
            "f.root_account_name",
            "f.path_keys", #3-21-26
            "f.path_names", #3-21-26
            "array_join(f.path_keys, '|') AS path_key_path",  #3-21-26
            "array_join(f.path_names, '|') AS path_name_path", #3-21-26
        ]

        for idx in range(max_depth):
            level_num = idx + 1
            select_cols.append(f"f.level{level_num}_key")
            select_cols.append(f"f.level{level_num}_name")
            select_cols.append(f"f.level{level_num}_sort")

        select_clause = ",\n            ".join(select_cols)
        sql_stmt = f"""
        CREATE OR REPLACE VIEW {dims_view} AS
        SELECT
            {select_clause}
        FROM {flat_view} f
        JOIN {registry_table} r
          ON f.hierarchy_id = r.hierarchy_id
        JOIN {version_table} v
          ON f.hierarchy_id = v.hierarchy_id
         AND f.version_id = v.version_id
        WHERE f.derived_is_leaf = TRUE
        """
        self.spark.sql(sql_stmt)
        return dims_view

    def rebuild_nodes_dims_view(
        self,
        registry_table: str,
        version_table: str,
        flat_view: str,
        nodes_dims_view: str,
    ) -> str:
        """
        Rebuild the all-nodes reporting dimension view.

        This view is intended for consumers that need access to the full
        hierarchy tree, including non-leaf rows, such as semantic models,
        tree navigation, UI rendering, or downstream presentation logic.
        """
        logger.info("Rebuilding hierarchy nodes dims view: %s", nodes_dims_view)
        max_depth = self.target_max_depth #self._get_max_depth(flat_view)

        select_cols = [
            "f.hierarchy_id",
            "r.hierarchy_name",
            "r.hierarchy_description",
            "r.owner_team",
            "r.business_domain",
            "f.version_id",
            "concat(f.hierarchy_id, '||', f.version_id) AS hier_ver_key",
            "v.version_name",
            "v.version_status",
            "v.effective_start_date",
            "v.effective_end_date",
            "v.is_current",
            "f.account_key AS node_key",
            "f.account_name AS node_name",
            "f.parent_account_key",
            "concat(f.hierarchy_id, '||', f.account_key) AS hier_node_key",
            "concat(f.hierarchy_id, '||', f.version_id, '||', f.account_key) "
            "AS hier_ver_node_key",
            "f.depth",
            "f.root_account_key",
            "f.root_account_name",
            "f.derived_is_leaf",
            "f.path_keys", #3-21-26
            "f.path_names", #3-21-26
            "array_join(f.path_keys, '|') AS path_key_path",  #3-21-26
            "array_join(f.path_names, '|') AS path_name_path", #3-21-26
        ]

        for idx in range(max_depth):
            level_num = idx + 1
            select_cols.append(f"f.level{level_num}_key")
            select_cols.append(f"f.level{level_num}_name")
            select_cols.append(f"f.level{level_num}_sort")

        select_clause = ",\n            ".join(select_cols)
        sql_stmt = f"""
        CREATE OR REPLACE VIEW {nodes_dims_view} AS
        SELECT
            {select_clause}
        FROM {flat_view} f
        JOIN {registry_table} r
          ON f.hierarchy_id = r.hierarchy_id
        JOIN {version_table} v
          ON f.hierarchy_id = v.hierarchy_id
         AND f.version_id = v.version_id
        """
        self.spark.sql(sql_stmt)
        return nodes_dims_view

    def rebuild_reporting_view(
        self,
        dims_view: str,
        reporting_view: str,
        published_status: str = "published",
    ) -> str:
        """
        Rebuild the final leaf-level reporting view for all published versions.
        """
        logger.info("Rebuilding hierarchy leaf reporting view: %s", reporting_view)
        max_depth = self.target_max_depth #self._get_max_depth(dims_view)

        publish_cols = [
            "hierarchy_id",
            "hierarchy_name",
            "hierarchy_description",
            "owner_team",
            "business_domain",
            "version_id",
            "hier_ver_key",
            "version_name",
            "version_status",
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "leaf_key",
            "leaf_name",
            "hier_leaf_key",
            "hier_ver_leaf_key",
            "depth",
            "root_account_key",
            "root_account_name",
            "path_keys", # 3-21-26
            "path_names", # 3-21-26
            "path_key_path", # 3-21-26
            "path_name_path", # 3-21-26
        ]

        for idx in range(max_depth):
            level_num = idx + 1
            publish_cols.extend(
                [
                    f"level{level_num}_key",
                    f"level{level_num}_name",
                    f"level{level_num}_sort",
                ]
            )

        publish_clause = ",\n            ".join(publish_cols)
        published_status_literal = self._sql_string_literal(published_status)
        sql_stmt = f"""
        CREATE OR REPLACE VIEW {reporting_view} AS
        SELECT
            {publish_clause}
        FROM {dims_view}
        WHERE version_status = {published_status_literal}
        """
        self.spark.sql(sql_stmt)
        return reporting_view

    def rebuild_nodes_reporting_view(
        self,
        nodes_dims_view: str,
        nodes_reporting_view: str,
        published_status: str = "published",
    ) -> str:
        """
        Rebuild the final all-nodes reporting view for all published versions.
        """
        logger.info(
            "Rebuilding hierarchy nodes reporting view: %s",
            nodes_reporting_view,
        )
        max_depth = self.target_max_depth #self._get_max_depth(nodes_dims_view)

        publish_cols = [
            "hierarchy_id",
            "hierarchy_name",
            "hierarchy_description",
            "owner_team",
            "business_domain",
            "version_id",
            "hier_ver_key",
            "version_name",
            "version_status",
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "node_key",
            "node_name",
            "parent_account_key",
            "hier_node_key",
            "hier_ver_node_key",
            "depth",
            "root_account_key",
            "root_account_name",
            "derived_is_leaf",
        ]

        for idx in range(max_depth):
            level_num = idx + 1
            publish_cols.extend(
                [
                    f"level{level_num}_key",
                    f"level{level_num}_name",
                    f"level{level_num}_sort",
                ]
            )

        publish_clause = ",\n            ".join(publish_cols)
        published_status_literal = self._sql_string_literal(published_status)
        sql_stmt = f"""
        CREATE OR REPLACE VIEW {nodes_reporting_view} AS
        SELECT
            {publish_clause}
        FROM {nodes_dims_view}
        WHERE version_status = {published_status_literal}
        """
        self.spark.sql(sql_stmt)
        return nodes_reporting_view

    def rebuild_all(
        self,
        registry_table: str,
        version_table: str,
        node_table: str,
        paths_view: str,
        flat_view: str,
        dims_view: str,
        reporting_view: str,
        nodes_dims_view: str,
        nodes_reporting_view: str,
    ) -> dict[str, str]:
        """
        Rebuild the full hierarchy reporting view stack.
        """
        logger.info("Rebuilding full hierarchy reporting view stack")

        self.rebuild_paths_view(
            node_table=node_table,
            paths_view=paths_view,
        )
        self.rebuild_flat_view(
            node_table=node_table,
            paths_view=paths_view,
            flat_view=flat_view,
        )
        self.rebuild_leaf_dims_view(
            registry_table=registry_table,
            version_table=version_table,
            flat_view=flat_view,
            dims_view=dims_view,
        )
        self.rebuild_reporting_view(
            dims_view=dims_view,
            reporting_view=reporting_view,
        )
        self.rebuild_nodes_dims_view(
            registry_table=registry_table,
            version_table=version_table,
            flat_view=flat_view,
            nodes_dims_view=nodes_dims_view,
        )
        self.rebuild_nodes_reporting_view(
            nodes_dims_view=nodes_dims_view,
            nodes_reporting_view=nodes_reporting_view,
        )

        return {
            "paths_view": paths_view,
            "flat_view": flat_view,
            "dims_view": dims_view,
            "reporting_view": reporting_view,
            "nodes_dims_view": nodes_dims_view,
            "nodes_reporting_view": nodes_reporting_view,
        }

    def _get_max_depth(self, relation_name: str) -> int:
        row = self.spark.sql(
            f"""
            SELECT MAX(depth) AS max_depth
            FROM {relation_name}
            """
        ).first()
        max_depth = int(row["max_depth"]) if row and row["max_depth"] is not None else 0
        if max_depth == 0:
            raise ValueError(f"No hierarchy depth found in {relation_name}")
        return max_depth

    def _sql_string_literal(self, value: str) -> str:
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
