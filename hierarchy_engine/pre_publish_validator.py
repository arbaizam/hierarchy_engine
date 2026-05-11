"""
Pre-write persistence validation for hierarchy publishing.

The authoritative persistence model is one immutable hierarchy-version row per
`(hierarchy_id, version)`. Flattened node rows are derived artifacts that may
be rebuilt from the authoritative payload, but normal publish flow still
validates that duplicate or conflicting derived rows do not already exist.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from hierarchy_engine.models import HierarchyMetadata, ValidationResult
from hierarchy_engine.sql_identifiers import validate_sql_identifier


class PrePublishHierarchyValidator:
    """
    Validate a candidate hierarchy publish against persisted tables.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate_publish(
        self,
        metadata: HierarchyMetadata,
        version_table: str,
        node_table: str,
    ) -> ValidationResult:
        """
        Validate a candidate publish before any writes are performed.
        """
        validate_sql_identifier(version_table, kind="table")
        validate_sql_identifier(node_table, kind="table")
        result = ValidationResult()

        self._validate_version_does_not_exist(
            metadata=metadata,
            version_table=version_table,
            result=result,
        )
        self._validate_no_existing_published_sibling(
            metadata=metadata,
            version_table=version_table,
            result=result,
        )
        self._validate_node_rows_do_not_exist(
            metadata=metadata,
            node_table=node_table,
            result=result,
        )

        return result.finalize()

    def _table_exists(self, table_name: str) -> bool:
        return bool(self.spark.catalog.tableExists(table_name))

    def _sql_string_literal(self, value: str) -> str:
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"

    def _validate_version_does_not_exist(
        self,
        metadata: HierarchyMetadata,
        version_table: str,
        result: ValidationResult,
    ) -> None:
        if not self._table_exists(version_table):
            return

        existing_count = self.spark.sql(
            f"""
            SELECT COUNT(*) AS row_count
            FROM {version_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
              AND version = {self._sql_string_literal(metadata.version)}
            """
        ).first()["row_count"]

        if existing_count > 1:
            result.add_issue(
                severity="ERROR",
                check_name="duplicate_version_rows",
                message=(
                    f"Version table contains duplicate rows for hierarchy "
                    f"'{metadata.hierarchy_id}' version '{metadata.version}'"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "version": metadata.version,
                    "row_count": existing_count,
                },
            )

        if existing_count > 0:
            result.add_issue(
                severity="ERROR",
                check_name="version_already_exists",
                message=(
                    f"Hierarchy '{metadata.hierarchy_id}' version "
                    f"'{metadata.version}' already exists in persistence"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "version": metadata.version,
                    "row_count": existing_count,
                },
            )

    def _validate_no_existing_published_sibling(
        self,
        metadata: HierarchyMetadata,
        version_table: str,
        result: ValidationResult,
    ) -> None:
        if not self._table_exists(version_table):
            return

        current_count = self.spark.sql(
            f"""
            SELECT COUNT(*) AS published_count
            FROM {version_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
              AND status = 'published'
            """
        ).first()["published_count"]

        if current_count > 0:
            result.add_issue(
                severity="ERROR",
                check_name="published_version_conflict",
                message=(
                    f"Hierarchy '{metadata.hierarchy_id}' already has a published version"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "published_count": current_count,
                },
            )

    def _validate_node_rows_do_not_exist(
        self,
        metadata: HierarchyMetadata,
        node_table: str,
        result: ValidationResult,
    ) -> None:
        if not self._table_exists(node_table):
            return

        existing_count = self.spark.sql(
            f"""
            SELECT COUNT(*) AS row_count
            FROM {node_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
              AND version = {self._sql_string_literal(metadata.version)}
            """
        ).first()["row_count"]

        duplicate_rows = self.spark.sql(
            f"""
            SELECT
                account_key,
                COUNT(*) AS row_count
            FROM {node_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
              AND version = {self._sql_string_literal(metadata.version)}
            GROUP BY account_key
            HAVING COUNT(*) > 1
            """
        ).collect()

        if existing_count > 0:
            result.add_issue(
                severity="ERROR",
                check_name="node_rows_already_exist",
                message=(
                    f"Node rows already exist for hierarchy '{metadata.hierarchy_id}' "
                    f"version '{metadata.version}'"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "version": metadata.version,
                    "row_count": existing_count,
                },
            )

        for row in duplicate_rows:
            result.add_issue(
                severity="ERROR",
                check_name="duplicate_persisted_node_rows",
                message=(
                    f"Node table already contains duplicate rows for account_key "
                    f"'{row['account_key']}' in hierarchy '{metadata.hierarchy_id}' "
                    f"version '{metadata.version}'"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "version": metadata.version,
                    "account_key": row["account_key"],
                    "row_count": row["row_count"],
                },
            )
