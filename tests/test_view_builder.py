import pytest

from hierarchy_engine.errors import HierarchyValidationError
from hierarchy_engine.view_builder import HierarchyViewBuilder


class FakeRow(dict):
    def __getattr__(self, item):
        return self[item]


class FakeQueryResult:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None


class FakeSpark:
    def __init__(self, depth_by_relation=None):
        self.depth_by_relation = depth_by_relation or {}
        self.queries = []

    def sql(self, query):
        self.queries.append(query)

        normalized_query = " ".join(query.split())
        if "SELECT MAX(depth) AS max_depth FROM" in normalized_query:
            relation_name = normalized_query.split("FROM", 1)[1].strip()
            max_depth = self.depth_by_relation.get(relation_name)
            return FakeQueryResult([FakeRow(max_depth=max_depth)])

        return FakeQueryResult([])


def test_rebuild_all_creates_leaf_and_all_node_views_in_order():
    """
    What: Rebuilds every reporting view in the expected dependency order and returns their names.
    Why: View creation order matters because downstream views depend on upstream path and dimension views existing first.
    Fails when: View rebuild sequencing changes or any expected `vw_hierarchy_*` target is skipped.
    """
    spark = FakeSpark()

    result = HierarchyViewBuilder(spark).rebuild_all(
        version_table="catalog.schema.hierarchy_versions",
        node_table="catalog.schema.base_hierarchy_node",
        paths_view="catalog.schema.vw_hierarchy_paths",
        flat_view="catalog.schema.vw_hierarchy_flat_nodes",
        dims_view="catalog.schema.vw_hierarchy_leaf_dimensions",
        reporting_view="catalog.schema.vw_hierarchy_published_leaves",
        nodes_dims_view="catalog.schema.vw_hierarchy_node_dimensions",
        nodes_reporting_view="catalog.schema.vw_hierarchy_published_nodes",
    )

    assert result == {
        "paths_view": "catalog.schema.vw_hierarchy_paths",
        "flat_view": "catalog.schema.vw_hierarchy_flat_nodes",
        "dims_view": "catalog.schema.vw_hierarchy_leaf_dimensions",
        "reporting_view": "catalog.schema.vw_hierarchy_published_leaves",
        "nodes_dims_view": "catalog.schema.vw_hierarchy_node_dimensions",
        "nodes_reporting_view": "catalog.schema.vw_hierarchy_published_nodes",
    }
    assert "CREATE OR REPLACE VIEW catalog.schema.vw_hierarchy_paths AS" in spark.queries[0]
    assert "CREATE OR REPLACE VIEW catalog.schema.vw_hierarchy_flat_nodes AS" in spark.queries[1]
    assert "CREATE OR REPLACE VIEW catalog.schema.vw_hierarchy_leaf_dimensions AS" in spark.queries[2]
    assert (
        "CREATE OR REPLACE VIEW catalog.schema.vw_hierarchy_published_leaves AS"
        in spark.queries[3]
    )
    assert (
        "CREATE OR REPLACE VIEW catalog.schema.vw_hierarchy_node_dimensions AS"
        in spark.queries[4]
    )
    assert (
        "CREATE OR REPLACE VIEW catalog.schema.vw_hierarchy_published_nodes AS"
        in spark.queries[5]
    )


def test_rebuild_flat_view_generates_level_columns_from_target_max_depth():
    """
    What: Generates level-specific key, name, and sort columns up to the configured maximum depth.
    Why: Flattened reporting views expose denormalized level columns for downstream consumers that cannot traverse arrays.
    Fails when: Configured depth limits are ignored or the flat view stops deduplicating child joins for leaf detection.
    """
    spark = FakeSpark()

    HierarchyViewBuilder(spark, target_max_depth=2).rebuild_flat_view(
        node_table="catalog.schema.base_hierarchy_node",
        paths_view="catalog.schema.vw_hierarchy_paths",
        flat_view="catalog.schema.vw_hierarchy_flat_nodes",
    )

    view_sql = spark.queries[0]
    assert "level1_key" in view_sql
    assert "level1_name" in view_sql
    assert "level1_sort" in view_sql
    assert "level2_key" in view_sql
    assert "level2_name" in view_sql
    assert "level2_sort" in view_sql
    assert "level3_key" not in view_sql
    assert "SELECT DISTINCT" in view_sql
    assert "FROM catalog.schema.base_hierarchy_node" in view_sql
    assert "WHERE parent_account_key IS NOT NULL" in view_sql


def test_rebuild_leaf_reporting_view_filters_to_published_versions():
    """
    What: Builds the published leaf reporting view from leaf dimensions and filters it to published versions.
    Why: Consumer-facing leaf reporting must exclude retired versions while preserving path metadata.
    Fails when: The published leaf view drops lifecycle filtering or omits the exported path columns.
    """
    spark = FakeSpark()

    HierarchyViewBuilder(spark, target_max_depth=2).rebuild_reporting_view(
        dims_view="catalog.schema.vw_hierarchy_leaf_dimensions",
        reporting_view="catalog.schema.vw_hierarchy_published_leaves",
    )

    view_sql = spark.queries[0]
    assert "FROM catalog.schema.vw_hierarchy_leaf_dimensions" in view_sql
    assert "WHERE status = 'published'" in view_sql
    assert "leaf_key" in view_sql
    assert "hier_ver_key" in view_sql
    assert "path_key_path" in view_sql
    assert "path_name_path" in view_sql


def test_rebuild_nodes_reporting_view_filters_to_published_versions():
    """
    What: Builds the published all-node reporting view from node dimensions and filters it to published versions.
    Why: All-node reporting should expose non-leaf rows without surfacing retired hierarchy versions.
    Fails when: Lifecycle filtering, node identity, or path columns disappear from the published node view.
    """
    spark = FakeSpark()

    HierarchyViewBuilder(spark, target_max_depth=2).rebuild_nodes_reporting_view(
        nodes_dims_view="catalog.schema.vw_hierarchy_node_dimensions",
        nodes_reporting_view="catalog.schema.vw_hierarchy_published_nodes",
    )

    view_sql = spark.queries[0]
    assert "FROM catalog.schema.vw_hierarchy_node_dimensions" in view_sql
    assert "WHERE status = 'published'" in view_sql
    assert "node_key" in view_sql
    assert "derived_is_leaf" in view_sql
    assert "hier_ver_key" in view_sql
    assert "path_key_path" in view_sql
    assert "path_name_path" in view_sql


def test_rebuild_nodes_dims_view_keeps_non_leaf_rows_available():
    """
    What: Builds the node-dimensions view without filtering away non-leaf rows.
    Why: Downstream all-node reporting depends on branch nodes remaining available in the dimensional layer.
    Fails when: Non-leaf rows are filtered out or `||`-delimited path strings are no longer projected.
    """
    spark = FakeSpark()

    HierarchyViewBuilder(spark, target_max_depth=2).rebuild_nodes_dims_view(
        version_table="catalog.schema.hierarchy_versions",
        flat_view="catalog.schema.vw_hierarchy_flat_nodes",
        nodes_dims_view="catalog.schema.vw_hierarchy_node_dimensions",
    )

    view_sql = spark.queries[0]
    assert "FROM catalog.schema.vw_hierarchy_flat_nodes f" in view_sql
    assert "WHERE f.derived_is_leaf = TRUE" not in view_sql
    assert "parent_account_key" in view_sql
    assert "derived_is_leaf" in view_sql
    assert "array_join(f.path_keys, '||') AS path_key_path" in view_sql
    assert "array_join(f.path_names, '||') AS path_name_path" in view_sql


def test_get_max_depth_raises_when_no_depth_exists():
    """
    What: Raises when a relation reports no usable hierarchy depth.
    Why: Level-column generation needs an explicit failure instead of silently building a zero-depth flat view.
    Fails when: Empty or null depth results are treated as valid rebuild inputs.
    """
    spark = FakeSpark(depth_by_relation={"catalog.schema.vw_hierarchy_paths": 0})

    with pytest.raises(ValueError, match="No hierarchy depth found"):
        HierarchyViewBuilder(spark)._get_max_depth("catalog.schema.vw_hierarchy_paths")


def test_view_builder_rejects_invalid_identifier():
    """
    What: Rejects invalid view identifiers before emitting SQL.
    Why: View names are interpolated into `CREATE VIEW` statements and need the same identifier hardening as tables.
    Fails when: Unsafe view names reach the SQL generator unchecked.
    """
    spark = FakeSpark()

    with pytest.raises(HierarchyValidationError, match="Invalid view identifier"):
        HierarchyViewBuilder(spark).rebuild_reporting_view(
            dims_view="catalog.schema.vw_hierarchy_leaf_dimensions",
            reporting_view="bad view",
        )

