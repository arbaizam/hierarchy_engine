import pytest

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
    spark = FakeSpark()

    result = HierarchyViewBuilder(spark).rebuild_all(
        registry_table="catalog.schema.hierarchy_registry",
        version_table="catalog.schema.hierarchy_version",
        node_table="catalog.schema.base_hierarchy_node",
        paths_view="catalog.schema.v_hierarchy_paths",
        flat_view="catalog.schema.v_hierarchy_flat",
        dims_view="catalog.schema.v_hierarchy_dims",
        reporting_view="catalog.schema.dim_reporting_hierarchy",
        nodes_dims_view="catalog.schema.v_hierarchy_nodes_dims",
        nodes_reporting_view="catalog.schema.dim_reporting_hierarchy_nodes",
    )

    assert result == {
        "paths_view": "catalog.schema.v_hierarchy_paths",
        "flat_view": "catalog.schema.v_hierarchy_flat",
        "dims_view": "catalog.schema.v_hierarchy_dims",
        "reporting_view": "catalog.schema.dim_reporting_hierarchy",
        "nodes_dims_view": "catalog.schema.v_hierarchy_nodes_dims",
        "nodes_reporting_view": "catalog.schema.dim_reporting_hierarchy_nodes",
    }
    assert "CREATE OR REPLACE VIEW catalog.schema.v_hierarchy_paths AS" in spark.queries[0]
    assert "CREATE OR REPLACE VIEW catalog.schema.v_hierarchy_flat AS" in spark.queries[1]
    assert "CREATE OR REPLACE VIEW catalog.schema.v_hierarchy_dims AS" in spark.queries[2]
    assert (
        "CREATE OR REPLACE VIEW catalog.schema.dim_reporting_hierarchy AS"
        in spark.queries[3]
    )
    assert (
        "CREATE OR REPLACE VIEW catalog.schema.v_hierarchy_nodes_dims AS"
        in spark.queries[4]
    )
    assert (
        "CREATE OR REPLACE VIEW catalog.schema.dim_reporting_hierarchy_nodes AS"
        in spark.queries[5]
    )


def test_rebuild_flat_view_generates_level_columns_from_target_max_depth():
    spark = FakeSpark()

    HierarchyViewBuilder(spark, target_max_depth=2).rebuild_flat_view(
        node_table="catalog.schema.base_hierarchy_node",
        paths_view="catalog.schema.v_hierarchy_paths",
        flat_view="catalog.schema.v_hierarchy_flat",
    )

    view_sql = spark.queries[0]
    assert "level1_key" in view_sql
    assert "level1_name" in view_sql
    assert "level1_sort" in view_sql
    assert "level2_key" in view_sql
    assert "level2_name" in view_sql
    assert "level2_sort" in view_sql
    assert "level3_key" not in view_sql
    assert "LEFT JOIN catalog.schema.base_hierarchy_node child" in view_sql


def test_rebuild_leaf_reporting_view_filters_to_published_versions():
    spark = FakeSpark()

    HierarchyViewBuilder(spark, target_max_depth=2).rebuild_reporting_view(
        dims_view="catalog.schema.v_hierarchy_dims",
        reporting_view="catalog.schema.dim_reporting_hierarchy",
    )

    view_sql = spark.queries[0]
    assert "FROM catalog.schema.v_hierarchy_dims" in view_sql
    assert "WHERE version_status = 'published'" in view_sql
    assert "leaf_key" in view_sql
    assert "hier_ver_key" in view_sql
    assert "path_key_path" in view_sql
    assert "path_name_path" in view_sql


def test_rebuild_nodes_reporting_view_filters_to_published_versions():
    spark = FakeSpark()

    HierarchyViewBuilder(spark, target_max_depth=2).rebuild_nodes_reporting_view(
        nodes_dims_view="catalog.schema.v_hierarchy_nodes_dims",
        nodes_reporting_view="catalog.schema.dim_reporting_hierarchy_nodes",
    )

    view_sql = spark.queries[0]
    assert "FROM catalog.schema.v_hierarchy_nodes_dims" in view_sql
    assert "WHERE version_status = 'published'" in view_sql
    assert "node_key" in view_sql
    assert "derived_is_leaf" in view_sql
    assert "hier_ver_key" in view_sql


def test_rebuild_nodes_dims_view_keeps_non_leaf_rows_available():
    spark = FakeSpark()

    HierarchyViewBuilder(spark, target_max_depth=2).rebuild_nodes_dims_view(
        registry_table="catalog.schema.hierarchy_registry",
        version_table="catalog.schema.hierarchy_version",
        flat_view="catalog.schema.v_hierarchy_flat",
        nodes_dims_view="catalog.schema.v_hierarchy_nodes_dims",
    )

    view_sql = spark.queries[0]
    assert "FROM catalog.schema.v_hierarchy_flat f" in view_sql
    assert "WHERE f.derived_is_leaf = TRUE" not in view_sql
    assert "parent_account_key" in view_sql
    assert "derived_is_leaf" in view_sql
    assert "array_join(f.path_keys, '|') AS path_key_path" in view_sql
    assert "array_join(f.path_names, '|') AS path_name_path" in view_sql


def test_get_max_depth_raises_when_no_depth_exists():
    spark = FakeSpark(depth_by_relation={"catalog.schema.v_hierarchy_paths": 0})

    with pytest.raises(ValueError, match="No hierarchy depth found"):
        HierarchyViewBuilder(spark)._get_max_depth("catalog.schema.v_hierarchy_paths")
