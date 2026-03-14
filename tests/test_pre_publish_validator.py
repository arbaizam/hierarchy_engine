from datetime import date

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
        for query_fragment, rows in sorted(
            self.sql_results.items(),
            key=lambda item: len(item[0]),
            reverse=True,
        ):
            if query_fragment in query:
                return FakeQueryResult(rows)
        raise AssertionError(f"Unexpected SQL query: {query}")


def test_pre_publish_validator_accepts_clean_persistence_state():
    metadata = build_definition().metadata
    spark = FakeSpark(
        existing_tables={"registry", "version", "nodes"},
        sql_results={
            "FROM registry": [],
            "SELECT COUNT(*) AS row_count\n            FROM version": [FakeRow(row_count=0)],
            "SELECT\n                account_key,\n                COUNT(*) AS row_count": [],
            "SELECT COUNT(*) AS row_count\n            FROM nodes": [FakeRow(row_count=0)],
            "SELECT\n                version_id,\n                effective_start_date,": [],
        },
    )

    result = PrePublishHierarchyValidator(spark).validate_publish(
        metadata=metadata,
        registry_table="registry",
        version_table="version",
        node_table="nodes",
    )

    assert result.passed is True


def test_pre_publish_validator_reports_registry_conflicts_and_duplicates():
    metadata = build_definition().metadata
    spark = FakeSpark(
        existing_tables={"registry", "version", "nodes"},
        sql_results={
            "FROM registry": [
                FakeRow(
                    hierarchy_id="TEST",
                    hierarchy_name="Wrong Name",
                    hierarchy_description="Description",
                    owner_team="Wrong Team",
                    business_domain="ALM",
                ),
                FakeRow(
                    hierarchy_id="TEST",
                    hierarchy_name="Wrong Name",
                    hierarchy_description="Description",
                    owner_team="Wrong Team",
                    business_domain="ALM",
                ),
            ],
            "SELECT COUNT(*) AS row_count\n            FROM version": [FakeRow(row_count=0)],
            "SELECT\n                account_key,\n                COUNT(*) AS row_count": [],
            "SELECT COUNT(*) AS row_count\n            FROM nodes": [FakeRow(row_count=0)],
            "SELECT\n                version_id,\n                effective_start_date,": [],
        },
    )

    result = PrePublishHierarchyValidator(spark).validate_publish(
        metadata=metadata,
        registry_table="registry",
        version_table="version",
        node_table="nodes",
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "duplicate_registry_rows" in check_names
    assert "registry_hierarchy_name_conflict" in check_names
    assert "registry_owner_team_conflict" in check_names


def test_pre_publish_validator_reports_existing_version_and_node_rows():
    metadata = build_definition().metadata
    spark = FakeSpark(
        existing_tables={"registry", "version", "nodes"},
        sql_results={
            "FROM registry": [],
            "SELECT COUNT(*) AS row_count\n            FROM version": [FakeRow(row_count=2)],
            "SELECT\n                account_key,\n                COUNT(*) AS row_count": [
                FakeRow(account_key="10000", row_count=2)
            ],
            "SELECT COUNT(*) AS row_count\n            FROM nodes": [FakeRow(row_count=3)],
            "SELECT\n                version_id,\n                effective_start_date,": [],
        },
    )

    result = PrePublishHierarchyValidator(spark).validate_publish(
        metadata=metadata,
        registry_table="registry",
        version_table="version",
        node_table="nodes",
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "duplicate_version_rows" in check_names
    assert "version_already_exists" in check_names
    assert "node_rows_already_exist" in check_names
    assert "duplicate_persisted_node_rows" in check_names


def test_pre_publish_validator_reports_published_state_conflicts():
    metadata = build_definition(
        metadata_overrides={
            "version_status": "published",
            "effective_start_date": date(2026, 1, 1),
            "effective_end_date": date(2026, 12, 31),
        }
    ).metadata
    spark = FakeSpark(
        existing_tables={"registry", "version", "nodes"},
        sql_results={
            "FROM registry": [],
            "SELECT COUNT(*) AS row_count\n            FROM version": [FakeRow(row_count=0)],
            "SELECT\n                account_key,\n                COUNT(*) AS row_count": [],
            "SELECT COUNT(*) AS row_count\n            FROM nodes": [FakeRow(row_count=0)],
            "COUNT(*) AS current_count": [FakeRow(current_count=1)],
            "SELECT\n                version_id,\n                effective_start_date,": [
                FakeRow(
                    version_id="V0",
                    effective_start_date=date(2025, 1, 1),
                    effective_end_date=None,
                )
            ],
        },
    )

    result = PrePublishHierarchyValidator(spark).validate_publish(
        metadata=metadata,
        registry_table="registry",
        version_table="version",
        node_table="nodes",
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "current_version_already_exists" in check_names
    assert "overlapping_effective_window" in check_names
