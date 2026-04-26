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
    metadata = build_definition().metadata

    with pytest.raises(HierarchyValidationError, match="Invalid table identifier"):
        PrePublishHierarchyValidator(FakeSpark({}, {"nodes"})).validate_publish(
            metadata=metadata,
            version_table="bad table",
            node_table="nodes",
        )
