import pytest

from hierarchy_engine.errors import HierarchyValidationError
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
        normalized_query = " ".join(query.split())
        for query_fragment, rows in sorted(
            self.sql_results.items(),
            key=lambda item: len(item[0]),
            reverse=True,
        ):
            if " ".join(query_fragment.split()) in normalized_query:
                return FakeQueryResult(rows)
        raise AssertionError(f"Unexpected SQL query: {query}")


def test_post_publish_validator_accepts_clean_persisted_state():
    """
    What: Accepts a published persistence state with unique node rows, valid parents, and one published version.
    Why: Post-publish audit should confirm a healthy write without inventing drift where none exists.
    Fails when: Clean persisted versions start producing duplicate, parent, or lifecycle audit issues.
    """
    spark = FakeSpark(
        {
            "SELECT account_key, COUNT(*) AS row_count": [],
            "SELECT child.account_key,": [],
            "SELECT COUNT(*) AS current_count": [FakeRow(current_count=1)],
        }
    )

    result = PostPublishHierarchyValidator(spark).validate_version(
        hierarchy_id="TEST",
        version="V1",
        node_table="nodes",
        version_table="versions",
    )

    assert result.passed is True


def test_post_publish_validator_reports_all_supported_issue_types():
    """
    What: Reports duplicate node rows, missing parents, and multiple published versions in one audit pass.
    Why: The persisted-state validator should surface every supported structural and lifecycle defect together.
    Fails when: Any supported post-publish issue type stops being detected or is masked by earlier checks.
    """
    spark = FakeSpark(
        {
            "SELECT account_key, COUNT(*) AS row_count": [
                FakeRow(account_key="10000", row_count=2)
            ],
            "SELECT child.account_key,": [
                FakeRow(
                    account_key="10100",
                    account_name="Investments",
                    parent_account_key="99999",
                )
            ],
            "SELECT COUNT(*) AS current_count": [FakeRow(current_count=2)],
        }
    )

    result = PostPublishHierarchyValidator(spark).validate_version(
        hierarchy_id="TEST",
        version="V1",
        node_table="nodes",
        version_table="versions",
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "duplicate_persisted_node_rows" in check_names
    assert "missing_persisted_parent" in check_names
    assert "multiple_published_versions" in check_names


def test_post_publish_validator_rejects_invalid_table_identifier():
    """
    What: Rejects invalid version-table identifiers before the post-publish audit runs.
    Why: Audit queries are string-built and should fail safely at identifier validation time.
    Fails when: Unsafe version table names reach the SQL layer unchecked.
    """
    with pytest.raises(HierarchyValidationError, match="Invalid table identifier"):
        PostPublishHierarchyValidator(FakeSpark({})).validate_version(
            hierarchy_id="TEST",
            version="V1",
            node_table="nodes",
            version_table="bad table",
        )

