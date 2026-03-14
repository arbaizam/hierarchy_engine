from datetime import date

from hierarchy_engine.repository import HierarchyRepository


class FakeWriter:
    def __init__(self):
        self.mode_value = None
        self.table_name = None

    def mode(self, mode):
        self.mode_value = mode
        return self

    def saveAsTable(self, table_name):
        self.table_name = table_name


class FakeDataFrame:
    def __init__(self, data):
        self.data = data
        self.write = FakeWriter()


class FakeCatalog:
    def __init__(self, existing_tables=None):
        self.existing_tables = set(existing_tables or [])

    def tableExists(self, table_name):
        return table_name in self.existing_tables


class FakeRow(dict):
    def __getattr__(self, item):
        return self[item]


class FakeQueryResult:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0]

    def collect(self):
        return self._rows


class FakeSpark:
    def __init__(self, sql_results=None, existing_tables=None):
        self.created_frames = []
        self.sql_results = sql_results or {}
        self.catalog = FakeCatalog(existing_tables)

    def createDataFrame(self, data, schema=None):
        df = FakeDataFrame(data)
        df.schema = schema
        self.created_frames.append(df)
        return df

    def sql(self, query):
        for query_fragment, rows in self.sql_results.items():
            if query_fragment in query:
                return FakeQueryResult(rows)
        raise AssertionError(f"Unexpected SQL query: {query}")


class Metadata:
    hierarchy_id = "TEST"
    hierarchy_name = "Test Hierarchy"
    hierarchy_description = "Description"
    owner_team = "Finance"
    business_domain = "ALM"
    version_id = "V1"
    version_name = "Version 1"
    version_status = "published"
    effective_start_date = date(2026, 1, 1)
    effective_end_date = None


def test_rows_to_dataframe_passes_rows_to_spark():
    spark = FakeSpark()
    rows = [{"account_key": "10000"}]

    result = HierarchyRepository(spark).rows_to_dataframe(rows)

    assert result.data == rows
    assert len(result.schema) == len(HierarchyRepository(spark).node_schema)
    assert result.schema[0].name == "hierarchy_id"


def test_create_base_tables_creates_empty_tables_from_explicit_schemas():
    spark = FakeSpark()

    HierarchyRepository(spark).create_base_tables(
        registry_table="registry_table",
        version_table="version_table",
        node_table="node_table",
        mode="overwrite",
    )

    assert len(spark.created_frames) == 3
    assert spark.created_frames[0].data == []
    assert spark.created_frames[0].write.mode_value == "overwrite"
    assert spark.created_frames[0].write.table_name == "registry_table"
    assert spark.created_frames[1].write.table_name == "version_table"
    assert spark.created_frames[2].write.table_name == "node_table"
    assert spark.created_frames[0].schema[0].name == "hierarchy_id"
    assert spark.created_frames[1].schema[1].name == "version_id"
    assert spark.created_frames[2].schema[2].name == "account_key"


def test_write_registry_creates_append_table_payload():
    spark = FakeSpark()

    HierarchyRepository(spark).write_registry(Metadata(), "registry_table")

    df = spark.created_frames[0]
    assert df.data[0]["hierarchy_id"] == "TEST"
    assert df.data[0]["hierarchy_name"] == "Test Hierarchy"
    assert df.write.mode_value == "append"
    assert df.write.table_name == "registry_table"


def test_write_version_sets_is_current_for_published_versions():
    spark = FakeSpark()

    HierarchyRepository(spark).write_version(Metadata(), "version_table")

    df = spark.created_frames[0]
    assert df.data[0]["version_id"] == "V1"
    assert df.data[0]["is_current"] is True
    assert df.write.mode_value == "append"
    assert df.write.table_name == "version_table"


def test_write_nodes_uses_requested_mode():
    spark = FakeSpark()
    rows_df = FakeDataFrame([{"account_key": "10000"}])

    HierarchyRepository(spark).write_nodes(rows_df, "node_table", mode="overwrite")

    assert rows_df.write.mode_value == "overwrite"
    assert rows_df.write.table_name == "node_table"


def test_table_exists_delegates_to_catalog():
    spark = FakeSpark(existing_tables={"registry_table"})

    repo = HierarchyRepository(spark)

    assert repo.table_exists("registry_table") is True
    assert repo.table_exists("missing_table") is False


def test_registry_entry_exists_returns_false_when_table_missing():
    spark = FakeSpark(existing_tables=set())

    assert HierarchyRepository(spark).registry_entry_exists("registry_table", "TEST") is False


def test_registry_entry_exists_queries_row_count():
    spark = FakeSpark(
        existing_tables={"registry_table"},
        sql_results={
            "FROM registry_table": [FakeRow(row_count=1)],
        },
    )

    assert HierarchyRepository(spark).registry_entry_exists("registry_table", "TEST") is True
