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
    """
    What: Converts flattened row dictionaries into a Spark DataFrame using the node table schema.
    Why: Repository writes depend on stable schema ordering and typed DataFrame creation.
    Fails when: Row payloads stop being passed through intact or the node schema columns drift unexpectedly.
    """
    spark = FakeSpark()
    rows = [{"account_key": "10000"}]

    result = HierarchyRepository(spark).rows_to_dataframe(rows)

    assert result.data == rows
    assert len(result.schema) == len(HierarchyRepository(spark).node_schema)
    assert result.schema[0].name == "hierarchy_id"


def test_create_base_tables_creates_empty_tables_from_explicit_schemas():
    """
    What: Creates empty version and node tables using explicit Delta DDL.
    Why: Bootstrapping a new environment should preserve NOT NULL table metadata instead of relying on empty DataFrame writes.
    Fails when: Base-table creation loses DDL ownership, nullability metadata, or table targets.
    """
    spark = FakeSpark()

    HierarchyRepository(spark).create_base_tables(
        version_table="version_table",
        node_table="node_table",
        mode="overwrite",
    )

    assert len(spark.created_frames) == 0
    assert any("DROP TABLE IF EXISTS version_table" in query for query in spark.queries)
    assert any("DROP TABLE IF EXISTS node_table" in query for query in spark.queries)
    assert any("CREATE TABLE version_table" in query for query in spark.queries)
    assert any("CREATE TABLE node_table" in query for query in spark.queries)
    assert any("effective_start_date STRING NOT NULL" in query for query in spark.queries)
    assert any("effective_end_date STRING NOT NULL" in query for query in spark.queries)
    assert any("published_by STRING NOT NULL" in query for query in spark.queries)
    assert any("published_at STRING NOT NULL" in query for query in spark.queries)
    assert any("account_key STRING NOT NULL" in query for query in spark.queries)
    assert any("created_at STRING NOT NULL" in query for query in spark.queries)
    assert any("updated_at STRING NOT NULL" in query for query in spark.queries)
    assert all("USING DELTA" in query for query in spark.queries if "CREATE TABLE" in query)


def test_create_base_tables_uses_if_not_exists_for_ignore_mode():
    """
    What: Uses CREATE TABLE IF NOT EXISTS for idempotent environment bootstrap.
    Why: Deployment notebooks should be safely rerunnable without dropping existing hierarchy data.
    Fails when: Ignore mode emits destructive DDL or non-idempotent CREATE TABLE statements.
    """
    spark = FakeSpark()

    HierarchyRepository(spark).create_base_tables(
        version_table="version_table",
        node_table="node_table",
        mode="ignore",
    )

    assert not any("DROP TABLE" in query for query in spark.queries)
    assert any("CREATE TABLE IF NOT EXISTS version_table" in query for query in spark.queries)
    assert any("CREATE TABLE IF NOT EXISTS node_table" in query for query in spark.queries)


def test_create_base_tables_rejects_unknown_mode():
    """
    What: Rejects unsupported table creation modes before emitting DDL.
    Why: Silent interpretation of unknown modes could accidentally create or replace deployment tables incorrectly.
    Fails when: Invalid modes are accepted or produce Spark SQL side effects.
    """
    spark = FakeSpark()

    with pytest.raises(HierarchyValidationError, match="mode must be one of"):
        HierarchyRepository(spark).create_base_tables(
            version_table="version_table",
            node_table="node_table",
            mode="append",
        )

    assert spark.queries == []


def test_write_version_creates_append_table_payload():
    """
    What: Serializes and appends a published hierarchy version row to the authoritative version table.
    Why: The authoritative persistence model depends on a complete version payload with status and canonical JSON.
    Fails when: Version rows lose lifecycle metadata, payload JSON, or append-write semantics.
    """
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
    assert df.data[0]["effective_start_date"] == "2026-04-26"
    assert df.data[0]["effective_end_date"] == "2999-12-31"
    assert df.data[0]["payload_json"]
    assert df.write.mode_value == "append"
    assert df.write.table_name == "version_table"


def test_write_nodes_uses_requested_mode():
    """
    What: Writes derived node rows using the caller-specified save mode.
    Why: Rebuild and test flows rely on being able to switch between append and overwrite semantics deliberately.
    Fails when: Node writes ignore the requested mode or target the wrong table.
    """
    spark = FakeSpark()
    rows_df = FakeDataFrame([{"account_key": "10000"}])

    HierarchyRepository(spark).write_nodes(rows_df, "node_table", mode="overwrite")

    assert rows_df.write.mode_value == "overwrite"
    assert rows_df.write.table_name == "node_table"


def test_table_exists_delegates_to_catalog():
    """
    What: Delegates table-existence checks to the Spark catalog.
    Why: Persistence validators should use the runtime catalog truth instead of maintaining their own cache.
    Fails when: Existing tables are missed or missing tables are reported as present.
    """
    spark = FakeSpark(existing_tables={"version_table"})

    repo = HierarchyRepository(spark)

    assert repo.table_exists("version_table") is True
    assert repo.table_exists("missing_table") is False


def test_version_exists_returns_false_when_table_missing():
    """
    What: Returns `False` for version existence checks when the version table is absent.
    Why: Fresh environments should not fail existence probes before base tables are created.
    Fails when: Missing version tables raise unexpectedly or report phantom version rows.
    """
    spark = FakeSpark(existing_tables=set())

    assert HierarchyRepository(spark).version_exists("version_table", "TEST", "V1") is False


def test_version_exists_queries_row_count():
    """
    What: Interprets a positive row count as an existing `(hierarchy_id, version)` record.
    Why: Publish idempotency depends on a precise existence check against the authoritative version table.
    Fails when: Persisted version counts are ignored or evaluated with the wrong truthiness.
    """
    spark = FakeSpark(
        existing_tables={"version_table"},
        sql_results={
            "FROM version_table": [FakeRow(row_count=1)],
        },
    )

    assert HierarchyRepository(spark).version_exists("version_table", "TEST", "V1") is True


def test_published_version_exists_queries_row_count():
    """
    What: Detects whether a hierarchy already has a published version row.
    Why: Some callers may need to inspect published-version presence without blocking valid additional versions.
    Fails when: Published sibling detection stops honoring persisted row counts.
    """
    spark = FakeSpark(
        existing_tables={"version_table"},
        sql_results={
            "FROM version_table": [FakeRow(row_count=1)],
        },
    )

    assert HierarchyRepository(spark).published_version_exists("version_table", "TEST") is True


def test_repository_rejects_invalid_table_identifier():
    """
    What: Rejects invalid table identifiers before issuing repository SQL.
    Why: Repository methods are the lowest common SQL boundary and need identifier hardening.
    Fails when: Unsafe table names bypass validation and reach the query builder.
    """
    spark = FakeSpark()

    with pytest.raises(HierarchyValidationError, match="Invalid table identifier"):
        HierarchyRepository(spark).version_exists("bad table", "TEST", "V1")


def test_get_version_status_returns_none_when_row_missing():
    """
    What: Returns `None` when no persisted version row exists for the requested hierarchy/version.
    Why: Retirement and validation flows need a clean missing-row signal instead of a fabricated lifecycle state.
    Fails when: Missing versions produce the wrong default status.
    """
    spark = FakeSpark(existing_tables={"version_table"})

    assert HierarchyRepository(spark).get_version_status("version_table", "TEST", "V1") is None


def test_get_version_status_reads_persisted_status():
    """
    What: Reads the persisted lifecycle status from the authoritative version table.
    Why: Retirement guards should act on the stored state, not on assumptions from the caller.
    Fails when: Status lookups stop returning the value stored in persistence.
    """
    spark = FakeSpark(
        existing_tables={"version_table"},
        sql_results={
            "SELECT status": [FakeRow(status="published")],
        },
    )

    assert HierarchyRepository(spark).get_version_status("version_table", "TEST", "V1") == "published"


def test_retire_version_emits_update_statement():
    """
    What: Updates a published version row to `retired` with actor and timestamp metadata.
    Why: Retirement is the persisted lifecycle transition that removes a version from published reporting views.
    Fails when: The repository stops issuing the expected status update, omits retirement metadata, or weakens the target WHERE clause.
    """
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
    assert any("retired_at = '2026-04-26T12:00:00Z'" in query for query in spark.queries)
    assert any("effective_end_date = '2026-04-26'" in query for query in spark.queries)
    assert any("WHERE hierarchy_id = 'TEST'" in query for query in spark.queries)
    assert any("AND version = 'V1'" in query for query in spark.queries)


def test_retire_version_allows_explicit_effective_end_date():
    """
    What: Allows retirement callers to close the effective window independently from the audit timestamp.
    Why: Some replacement workflows need end-of-business dating while preserving the exact retirement time.
    Fails when: Explicit effective end dates are ignored during retirement.
    """
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
        retired_at="2026-04-26T23:59:59Z",
        effective_end_date="2026-04-25",
    )

    assert any("effective_end_date = '2026-04-25'" in query for query in spark.queries)


def test_retire_version_raises_when_version_missing():
    """
    What: Raises when asked to retire a hierarchy version that does not exist.
    Why: Silent no-op retirements would hide operator mistakes and make lifecycle state ambiguous.
    Fails when: Missing version rows are treated as successful retirements.
    """
    spark = FakeSpark(existing_tables={"version_table"})

    with pytest.raises(HierarchyValidationError, match="does not exist in persistence"):
        HierarchyRepository(spark).retire_version(
            "version_table",
            "TEST",
            "V1",
        )


def test_retire_version_raises_when_not_published():
    """
    What: Raises when asked to retire a version whose persisted status is not `published`.
    Why: Lifecycle transitions should enforce the published-to-retired path explicitly.
    Fails when: Already retired or otherwise non-published rows can be retired again silently.
    """
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

