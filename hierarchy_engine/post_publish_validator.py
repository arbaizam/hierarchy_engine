"""
Post-publish audit validation for persisted hierarchy data.
"""
 
from __future__ import annotations
 
from pyspark.sql import SparkSession
 
from hierarchy_engine.models import ValidationResult
from hierarchy_engine.sql_identifiers import validate_sql_identifier
 
class PostPublishHierarchyValidator:
    """
    Validate persisted hierarchy artifacts in Spark tables.
    """
 
    def __init__(self, spark: SparkSession):
        """
        Create a post-publish validator for persisted hierarchy tables.
        """
        self.spark = spark

    def _sql_string_literal(self, value: str) -> str:
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
 
    def validate_version(
        self,
        hierarchy_id: str,
        version: str,
        node_table: str,
        version_table: str,
    ) -> ValidationResult:
        """
        Validate a published hierarchy version across persisted tables.

        This method is intentionally read-only. It checks persisted node rows
        and lifecycle metadata after publish or retire operations have already
        occurred.
        """
        validate_sql_identifier(node_table, kind="table")
        validate_sql_identifier(version_table, kind="table")
        result = ValidationResult()
 
        self._validate_persisted_duplicate_node_rows(
            hierarchy_id=hierarchy_id,
            version=version,
            node_table=node_table,
            result=result,
        )
        self._validate_persisted_missing_parents(
            hierarchy_id=hierarchy_id,
            version=version,
            node_table=node_table,
            result=result,
        )
        self._validate_multiple_published_versions(
            hierarchy_id=hierarchy_id,
            version_table=version_table,
            result=result,
        )
 
        return result.finalize()
 
    # ---------------------------------------------------------------------
    # Persisted duplicate row check
    # ---------------------------------------------------------------------
 
    def _validate_persisted_duplicate_node_rows(
        self,
        hierarchy_id: str,
        version: str,
        node_table: str,
        result: ValidationResult,
    ) -> None:
        """
        Detect duplicate persisted node rows.
 
        Parameters
        ----------
        hierarchy_id : str
            Hierarchy identifier.
        version : str
            Version identifier.
        node_table : str
            Fully qualified node table name.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        This check validates uniqueness of:
            (hierarchy_id, version, account_key)
 
        If duplicates exist in persistence, it usually indicates one of:
        - repeated publish operations without replacement/upsert behavior
        - accidental append behavior during development
        - malformed pipeline orchestration
        """
        duplicate_df = self.spark.sql(f"""
            SELECT
                account_key,
                COUNT(*) AS row_count
            FROM {node_table}
            WHERE hierarchy_id = {self._sql_string_literal(hierarchy_id)}
              AND version = {self._sql_string_literal(version)}
            GROUP BY account_key
            HAVING COUNT(*) > 1
        """)
 
        duplicate_rows = duplicate_df.collect()
 
        for row in duplicate_rows:
            result.add_issue(
                severity="ERROR",
                check_name="duplicate_persisted_node_rows",
                message=(
                    f"Duplicate persisted node rows found for account_key "
                    f"'{row['account_key']}'"
                ),
                details={
                    "hierarchy_id": hierarchy_id,
                    "version": version,
                    "account_key": row["account_key"],
                    "row_count": row["row_count"],
                },
            )
 
    # ---------------------------------------------------------------------
    # Missing parent check
    # ---------------------------------------------------------------------
 
    def _validate_persisted_missing_parents(
        self,
        hierarchy_id: str,
        version: str,
        node_table: str,
        result: ValidationResult,
    ) -> None:
        """
        Detect missing parent rows in persisted node data.
 
        Parameters
        ----------
        hierarchy_id : str
            Hierarchy identifier.
        version : str
            Version identifier.
        node_table : str
            Fully qualified node table name.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        In the flattened adjacency-list representation, every non-root row
        should reference an existing parent row with the same hierarchy_id
        and version.
 
        This check performs a self left join:
        - child rows reference parent_account_key
        - parent rows are matched on account_key
        - if parent is missing, the child is orphaned
        """
        missing_parent_df = self.spark.sql(f"""
            SELECT
                child.account_key,
                child.account_name,
                child.parent_account_key
            FROM {node_table} child
            LEFT JOIN {node_table} parent
              ON child.hierarchy_id = parent.hierarchy_id
             AND child.version = parent.version
             AND child.parent_account_key = parent.account_key
            WHERE child.hierarchy_id = {self._sql_string_literal(hierarchy_id)}
              AND child.version = {self._sql_string_literal(version)}
              AND child.parent_account_key IS NOT NULL
              AND parent.account_key IS NULL
        """)
 
        missing_parent_rows = missing_parent_df.collect()
 
        for row in missing_parent_rows:
            result.add_issue(
                severity="ERROR",
                check_name="missing_persisted_parent",
                message=(
                    f"Node '{row['account_key']}' references missing parent "
                    f"'{row['parent_account_key']}'"
                ),
                details={
                    "hierarchy_id": hierarchy_id,
                    "version": version,
                    "account_key": row["account_key"],
                    "account_name": row["account_name"],
                    "parent_account_key": row["parent_account_key"],
                },
            )
 
    # ---------------------------------------------------------------------
    # Multiple published versions check
    # ---------------------------------------------------------------------
 
    def _validate_multiple_published_versions(
        self,
        hierarchy_id: str,
        version_table: str,
        result: ValidationResult,
    ) -> None:
        """
        Detect multiple published versions for the same hierarchy id.

        Most environments should allow at most one `status = published`
        version for a given hierarchy_id at a time. If multiple published rows
        exist, downstream consumers may not know which version to treat as the
        active release.
        """
        current_df = self.spark.sql(f"""
            SELECT
                COUNT(*) AS current_count
            FROM {version_table}
            WHERE hierarchy_id = {self._sql_string_literal(hierarchy_id)}
              AND status = 'published'
        """)
 
        current_count = current_df.first()["current_count"]
 
        if current_count > 1:
            result.add_issue(
                severity="ERROR",
                check_name="multiple_published_versions",
                message=(
                    f"Hierarchy '{hierarchy_id}' has more than one published version"
                ),
                details={
                    "hierarchy_id": hierarchy_id,
                    "current_count": current_count,
                },
            )
 
