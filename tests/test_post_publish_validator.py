from datetime import date

from hierarchy_engine.post_publish_validator import PostPublishHierarchyValidator


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
    def __init__(self, sql_results):
        self.sql_results = sql_results

    def sql(self, query):
        for query_fragment, rows in sorted(
            self.sql_results.items(),
            key=lambda item: len(item[0]),
            reverse=True,
        ):
            if query_fragment in query:
                return FakeQueryResult(rows)
        raise AssertionError(f"Unexpected SQL query: {query}")


def test_post_publish_validator_accepts_clean_persisted_state():
    spark = FakeSpark(
        {
            "SELECT\n                account_key,\n                COUNT(*) AS row_count": [],
            "SELECT\n                child.account_key,": [],
            "SELECT\n                COUNT(*) AS current_count": [FakeRow(current_count=1)],
            "SELECT\n                a.version_id AS version_id_1,": [],
        }
    )

    result = PostPublishHierarchyValidator(spark).validate_version(
        hierarchy_id="TEST",
        version_id="V1",
        node_table="nodes",
        version_table="versions",
    )

    assert result.passed is True


def test_post_publish_validator_reports_all_supported_issue_types():
    spark = FakeSpark(
        {
            "SELECT\n                account_key,\n                COUNT(*) AS row_count": [
                FakeRow(account_key="10000", row_count=2)
            ],
            "SELECT\n                child.account_key,": [
                FakeRow(
                    account_key="10100",
                    account_name="Investments",
                    parent_account_key="99999",
                )
            ],
            "SELECT\n                COUNT(*) AS current_count": [FakeRow(current_count=2)],
            "SELECT\n                a.version_id AS version_id_1,": [
                FakeRow(
                    version_id_1="V1",
                    version_id_2="V2",
                    start_1=date(2026, 1, 1),
                    end_1=None,
                    start_2=date(2026, 6, 1),
                    end_2=None,
                )
            ],
        }
    )

    result = PostPublishHierarchyValidator(spark).validate_version(
        hierarchy_id="TEST",
        version_id="V1",
        node_table="nodes",
        version_table="versions",
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "duplicate_persisted_node_rows" in check_names
    assert "missing_persisted_parent" in check_names
    assert "multiple_current_versions" in check_names
    assert "overlapping_effective_windows" in check_names
