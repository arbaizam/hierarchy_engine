import pytest

from hierarchy_engine.errors import HierarchyValidationError
from hierarchy_engine.repository import HierarchyRepository
from tests.helpers import build_definition


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
        self.queries = []

    def createDataFrame(self, data, schema=None):
        df = FakeDataFrame(data)
        df.schema = schema
        self.created_frames.append(df)
        return df

    def sql(self, query):
        self.queries.append(query)
        for query_fragment, rows in self.sql_results.items():
            if query_fragment in query:
                return FakeQueryResult(rows)
        return FakeQueryResult([])


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
        version_table="version_table",
        node_table="node_table",
        mode="overwrite",
    )

    assert len(spark.created_frames) == 2
    assert spark.created_frames[0].data == []
    assert spark.created_frames[0].write.mode_value == "overwrite"
    assert spark.created_frames[0].write.table_name == "version_table"
    assert spark.created_frames[1].write.table_name == "node_table"
    assert spark.created_frames[0].schema[2].name == "version"
    assert spark.created_frames[1].schema[2].name == "account_key"


def test_write_version_creates_append_table_payload():
    spark = FakeSpark()

    HierarchyRepository(spark).write_version(
        build_definition(),
        "version_table",
        published_by="engineer",
        published_at="2026-04-26T12:00:00Z",
    )

    df = spark.created_frames[0]
    assert df.data[0]["hierarchy_id"] == "TEST"
    assert df.data[0]["version"] == "V1"
    assert df.data[0]["status"] == "published"
    assert df.data[0]["payload_json"]
    assert df.write.mode_value == "append"
    assert df.write.table_name == "version_table"


def test_write_nodes_uses_requested_mode():
    spark = FakeSpark()
    rows_df = FakeDataFrame([{"account_key": "10000"}])

    HierarchyRepository(spark).write_nodes(rows_df, "node_table", mode="overwrite")

    assert rows_df.write.mode_value == "overwrite"
    assert rows_df.write.table_name == "node_table"


def test_table_exists_delegates_to_catalog():
    spark = FakeSpark(existing_tables={"version_table"})

    repo = HierarchyRepository(spark)

    assert repo.table_exists("version_table") is True
    assert repo.table_exists("missing_table") is False


def test_version_exists_returns_false_when_table_missing():
    spark = FakeSpark(existing_tables=set())

    assert HierarchyRepository(spark).version_exists("version_table", "TEST", "V1") is False


def test_version_exists_queries_row_count():
    spark = FakeSpark(
        existing_tables={"version_table"},
        sql_results={
            "FROM version_table": [FakeRow(row_count=1)],
        },
    )

    assert HierarchyRepository(spark).version_exists("version_table", "TEST", "V1") is True


def test_published_version_exists_queries_row_count():
    spark = FakeSpark(
        existing_tables={"version_table"},
        sql_results={
            "FROM version_table": [FakeRow(row_count=1)],
        },
    )

    assert HierarchyRepository(spark).published_version_exists("version_table", "TEST") is True


def test_repository_rejects_invalid_table_identifier():
    spark = FakeSpark()

    with pytest.raises(HierarchyValidationError, match="Invalid table identifier"):
        HierarchyRepository(spark).version_exists("bad table", "TEST", "V1")


def test_get_version_status_returns_none_when_row_missing():
    spark = FakeSpark(existing_tables={"version_table"})

    assert HierarchyRepository(spark).get_version_status("version_table", "TEST", "V1") is None


def test_get_version_status_reads_persisted_status():
    spark = FakeSpark(
        existing_tables={"version_table"},
        sql_results={
            "SELECT status": [FakeRow(status="published")],
        },
    )

    assert HierarchyRepository(spark).get_version_status("version_table", "TEST", "V1") == "published"


def test_retire_version_emits_update_statement():
    spark = FakeSpark(
        existing_tables={"version_table"},
        sql_results={
            "SELECT status": [FakeRow(status="published")],
        },
    )

    HierarchyRepository(spark).retire_version(
        "version_table",
        "TEST",
        "V1",
        retired_by="engineer",
        retired_at="2026-04-26T12:00:00Z",
    )

    assert any("UPDATE version_table" in query for query in spark.queries)
    assert any("status = 'retired'" in query for query in spark.queries)


def test_retire_version_raises_when_version_missing():
    spark = FakeSpark(existing_tables={"version_table"})

    with pytest.raises(HierarchyValidationError, match="does not exist in persistence"):
        HierarchyRepository(spark).retire_version(
            "version_table",
            "TEST",
            "V1",
        )


def test_retire_version_raises_when_not_published():
    spark = FakeSpark(
        existing_tables={"version_table"},
        sql_results={
            "SELECT status": [FakeRow(status="retired")],
        },
    )

    with pytest.raises(HierarchyValidationError, match="is not currently published"):
        HierarchyRepository(spark).retire_version(
            "version_table",
            "TEST",
            "V1",
        )
