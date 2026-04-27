import pytest

from hierarchy_engine.errors import HierarchyValidationError
from hierarchy_engine.pre_publish_validator import PrePublishHierarchyValidator
from tests.helpers import build_definition


class FakeCatalog:
    def __init__(self, existing_tables):
        self.existing_tables = set(existing_tables)

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
    def __init__(self, sql_results, existing_tables):
        self.sql_results = sql_results
        self.catalog = FakeCatalog(existing_tables)

    def sql(self, query):
        normalized_query = " ".join(query.split())
        for query_fragment, rows in sorted(
            self.sql_results.items(),
            key=lambda item: len(item[0]),
            reverse=True,
        ):
            if " ".join(query_fragment.split()) in normalized_query:
                return FakeQueryResult(rows)
        raise AssertionError(f"Unexpected SQL query: {query}")


def test_pre_publish_validator_accepts_clean_persistence_state():
    """
    What: Accepts a persistence state with no existing version, no published conflict, and no derived node rows.
    Why: Clean first-time publishes should not be blocked by the pre-write gate.
    Fails when: Empty version/node tables still produce persistence conflicts.
    """
    metadata = build_definition().metadata
    spark = FakeSpark(
        existing_tables={"version", "nodes"},
        sql_results={
            "SELECT COUNT(*) AS row_count FROM version": [FakeRow(row_count=0)],
            "SELECT COUNT(*) AS published_count": [FakeRow(published_count=0)],
            "SELECT account_key, COUNT(*) AS row_count": [],
            "SELECT COUNT(*) AS row_count FROM nodes": [FakeRow(row_count=0)],
        },
    )

    result = PrePublishHierarchyValidator(spark).validate_publish(
        metadata=metadata,
        version_table="version",
        node_table="nodes",
    )

    assert result.passed is True


def test_pre_publish_validator_reports_existing_version_and_node_rows():
    """
    What: Reports duplicate persisted versions, existing node rows, and duplicated node keys for the target publish.
    Why: The pre-write gate must protect idempotency and detect partially corrupted persistence state before another publish.
    Fails when: Existing version or node collisions stop surfacing as explicit blocking issues.
    """
    metadata = build_definition().metadata
    spark = FakeSpark(
        existing_tables={"version", "nodes"},
        sql_results={
            "SELECT COUNT(*) AS row_count FROM version": [FakeRow(row_count=2)],
            "SELECT COUNT(*) AS published_count": [FakeRow(published_count=0)],
            "SELECT account_key, COUNT(*) AS row_count": [
                FakeRow(account_key="10000", row_count=2)
            ],
            "SELECT COUNT(*) AS row_count FROM nodes": [FakeRow(row_count=3)],
        },
    )

    result = PrePublishHierarchyValidator(spark).validate_publish(
        metadata=metadata,
        version_table="version",
        node_table="nodes",
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "duplicate_version_rows" in check_names
    assert "version_already_exists" in check_names
    assert "node_rows_already_exist" in check_names
    assert "duplicate_persisted_node_rows" in check_names


def test_pre_publish_validator_reports_published_name_conflict():
    """
    What: Reports a published-version conflict when the same hierarchy identifier already has a published row.
    Why: Only one published version per hierarchy should be visible to reporting consumers at a time.
    Fails when: Existing published siblings keyed by `hierarchy_id` no longer block a new publish.
    """
    metadata = build_definition().metadata
    spark = FakeSpark(
        existing_tables={"version", "nodes"},
        sql_results={
            "SELECT COUNT(*) AS row_count FROM version": [FakeRow(row_count=0)],
            "SELECT COUNT(*) AS published_count": [FakeRow(published_count=1)],
            "SELECT account_key, COUNT(*) AS row_count": [],
            "SELECT COUNT(*) AS row_count FROM nodes": [FakeRow(row_count=0)],
        },
    )

    result = PrePublishHierarchyValidator(spark).validate_publish(
        metadata=metadata,
        version_table="version",
        node_table="nodes",
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "published_version_conflict" in check_names


def test_pre_publish_validator_rejects_invalid_table_identifier():
    """
    What: Rejects invalid SQL identifiers before running any pre-publish queries.
    Why: Table names are interpolated into Spark SQL and need explicit validation at the boundary.
    Fails when: Unsafe table identifiers reach the query builder instead of raising a validation error.
    """
    metadata = build_definition().metadata

    with pytest.raises(HierarchyValidationError, match="Invalid table identifier"):
        PrePublishHierarchyValidator(FakeSpark({}, {"nodes"})).validate_publish(
            metadata=metadata,
            version_table="bad table",
            node_table="nodes",
        )

