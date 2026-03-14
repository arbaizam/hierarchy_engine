 
"""
Post-publish Spark validation for persisted hierarchy data.
 
This module validates hierarchy artifacts *after* they have been written to
Spark / Databricks tables.
 
Why this module exists
----------------------
The in-memory validator checks the authored hierarchy definition before publish.
That catches structural issues in the YAML-derived object model.
 
The normal publish path should also perform a pre-write persistence validation
pass before any table writes occur.

This module remains useful after persistence when you need audit or diagnostic
checks such as:
 
- duplicate persisted node rows
- missing parent rows in the persisted node table
- multiple current versions in the version table
- overlapping effective date windows across versions
 
These checks are best implemented against persisted tables and DataFrames.
They are read-only and intended for already-persisted data rather than for
blocking the normal publish flow.
 
Design notes
------------
This validator returns the same `ValidationResult` model used by the
in-memory validator. That keeps the validation experience consistent across
both layers.
 
This module is intentionally read-only. It does not modify tables.
"""
 
from __future__ import annotations
 
from pyspark.sql import SparkSession
 
from hierarchy_engine.models import ValidationResult
 
class PostPublishHierarchyValidator:
    """
    Validate published hierarchy artifacts in Spark tables.
 
    Parameters
    ----------
    spark : SparkSession
        Active Spark session used to query persisted hierarchy tables.
    """
 
    def __init__(self, spark: SparkSession):
        """
        Initialize the post-publish validator.
 
        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        """
        self.spark = spark

    def _sql_string_literal(self, value: str) -> str:
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
 
    def validate_version(
        self,
        hierarchy_id: str,
        version_id: str,
        node_table: str,
        version_table: str,
    ) -> ValidationResult:
        """
        Validate a published hierarchy version across persisted tables.
 
        Parameters
        ----------
        hierarchy_id : str
            Hierarchy identifier to validate.
        version_id : str
            Hierarchy version identifier to validate.
        node_table : str
            Fully qualified Spark table containing flattened hierarchy nodes.
        version_table : str
            Fully qualified Spark table containing hierarchy version metadata.
 
        Returns
        -------
        ValidationResult
            Structured validation result.
 
        Notes
        -----
        This method runs all currently supported post-publish checks and
        accumulates issues rather than stopping at the first failure.
        """
        result = ValidationResult()
 
        self._validate_persisted_duplicate_node_rows(
            hierarchy_id=hierarchy_id,
            version_id=version_id,
            node_table=node_table,
            result=result,
        )
        self._validate_persisted_missing_parents(
            hierarchy_id=hierarchy_id,
            version_id=version_id,
            node_table=node_table,
            result=result,
        )
        self._validate_multiple_current_versions(
            hierarchy_id=hierarchy_id,
            version_table=version_table,
            result=result,
        )
        self._validate_overlapping_effective_windows(
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
        version_id: str,
        node_table: str,
        result: ValidationResult,
    ) -> None:
        """
        Detect duplicate persisted node rows.
 
        Parameters
        ----------
        hierarchy_id : str
            Hierarchy identifier.
        version_id : str
            Version identifier.
        node_table : str
            Fully qualified node table name.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        This check validates uniqueness of:
            (hierarchy_id, version_id, account_key)
 
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
              AND version_id = {self._sql_string_literal(version_id)}
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
                    "version_id": version_id,
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
        version_id: str,
        node_table: str,
        result: ValidationResult,
    ) -> None:
        """
        Detect missing parent rows in persisted node data.
 
        Parameters
        ----------
        hierarchy_id : str
            Hierarchy identifier.
        version_id : str
            Version identifier.
        node_table : str
            Fully qualified node table name.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        In the flattened adjacency-list representation, every non-root row
        should reference an existing parent row with the same hierarchy_id
        and version_id.
 
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
             AND child.version_id = parent.version_id
             AND child.parent_account_key = parent.account_key
            WHERE child.hierarchy_id = {self._sql_string_literal(hierarchy_id)}
              AND child.version_id = {self._sql_string_literal(version_id)}
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
                    "version_id": version_id,
                    "account_key": row["account_key"],
                    "account_name": row["account_name"],
                    "parent_account_key": row["parent_account_key"],
                },
            )
 
    # ---------------------------------------------------------------------
    # Multiple current versions check
    # ---------------------------------------------------------------------
 
    def _validate_multiple_current_versions(
        self,
        hierarchy_id: str,
        version_table: str,
        result: ValidationResult,
    ) -> None:
        """
        Detect multiple current versions for the same hierarchy.
 
        Parameters
        ----------
        hierarchy_id : str
            Hierarchy identifier.
        version_table : str
            Fully qualified version table name.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        Most environments should allow at most one `is_current = true`
        version for a given hierarchy_id at a time.
 
        If multiple current versions exist, downstream consumers may not know
        which version to treat as authoritative.
        """
        current_df = self.spark.sql(f"""
            SELECT
                COUNT(*) AS current_count
            FROM {version_table}
            WHERE hierarchy_id = {self._sql_string_literal(hierarchy_id)}
              AND is_current = TRUE
        """)
 
        current_count = current_df.first()["current_count"]
 
        if current_count > 1:
            result.add_issue(
                severity="ERROR",
                check_name="multiple_current_versions",
                message=(
                    f"Hierarchy '{hierarchy_id}' has more than one current version"
                ),
                details={
                    "hierarchy_id": hierarchy_id,
                    "current_count": current_count,
                },
            )
 
    # ---------------------------------------------------------------------
    # Overlapping effective date windows check
    # ---------------------------------------------------------------------
 
    def _validate_overlapping_effective_windows(
        self,
        hierarchy_id: str,
        version_table: str,
        result: ValidationResult,
    ) -> None:
        """
        Detect overlapping effective date windows for hierarchy versions.
 
        Parameters
        ----------
        hierarchy_id : str
            Hierarchy identifier.
        version_table : str
            Fully qualified version table name.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        Effective windows overlap when two versions of the same hierarchy
        are active over intersecting date ranges.
 
        This check uses a self-join pattern:
        - compare each version row to later version rows
        - treat null effective_end_date as open-ended
        - report overlaps as errors
 
        Overlapping windows can create ambiguity for date-based consumers.
        """
        overlap_df = self.spark.sql(f"""
            SELECT
                a.version_id AS version_id_1,
                b.version_id AS version_id_2,
                a.effective_start_date AS start_1,
                a.effective_end_date AS end_1,
                b.effective_start_date AS start_2,
                b.effective_end_date AS end_2
            FROM {version_table} a
            JOIN {version_table} b
             ON a.hierarchy_id = b.hierarchy_id
             AND a.version_id < b.version_id
             AND a.hierarchy_id = {self._sql_string_literal(hierarchy_id)}
             AND a.effective_start_date <= COALESCE(b.effective_end_date, DATE '9999-12-31')
             AND b.effective_start_date <= COALESCE(a.effective_end_date, DATE '9999-12-31')
        """)
 
        overlap_rows = overlap_df.collect()
 
        for row in overlap_rows:
            result.add_issue(
                severity="ERROR",
                check_name="overlapping_effective_windows",
                message=(
                    f"Hierarchy '{hierarchy_id}' has overlapping effective windows "
                    f"between versions '{row['version_id_1']}' and '{row['version_id_2']}'"
                ),
                details={
                    "hierarchy_id": hierarchy_id,
                    "version_id_1": row["version_id_1"],
                    "version_id_2": row["version_id_2"],
                    "start_1": row["start_1"].isoformat() if row["start_1"] else None,
                    "end_1": row["end_1"].isoformat() if row["end_1"] else None,
                    "start_2": row["start_2"].isoformat() if row["start_2"] else None,
                    "end_2": row["end_2"].isoformat() if row["end_2"] else None,
                },
            )
 
