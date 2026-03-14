"""
Pre-write persistence validation for hierarchy publishing.

This module validates a candidate hierarchy version against existing persisted
state before any table writes are attempted.

Why this module exists
----------------------
In-memory validation and post-structural validation are necessary but not
sufficient. Some failures only become visible when the candidate publish is
compared against the existing persisted state, for example:

- the same hierarchy/version already exists in the version table
- node rows already exist for the same hierarchy/version
- a new published version would create multiple current versions
- a new version would overlap an existing effective-date window

Those checks should happen before the publish workflow writes anything. That
keeps the write path fail-fast and avoids creating duplicate or conflicting
records that would otherwise need cleanup.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession

from hierarchy_engine.models import HierarchyMetadata, ValidationResult


class PrePublishHierarchyValidator:
    """
    Validate a candidate hierarchy publish against persisted tables.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate_publish(
        self,
        metadata: HierarchyMetadata,
        registry_table: str,
        node_table: str,
        version_table: str,
    ) -> ValidationResult:
        """
        Validate a candidate publish before any writes are performed.
        """
        result = ValidationResult()

        self._validate_registry_integrity(
            metadata=metadata,
            registry_table=registry_table,
            result=result,
        )
        self._validate_version_does_not_exist(
            metadata=metadata,
            version_table=version_table,
            result=result,
        )
        self._validate_node_rows_do_not_exist(
            metadata=metadata,
            node_table=node_table,
            result=result,
        )
        self._validate_publish_state_conflicts(
            metadata=metadata,
            version_table=version_table,
            result=result,
        )

        return result.finalize()

    def _table_exists(self, table_name: str) -> bool:
        return bool(self.spark.catalog.tableExists(table_name))

    def _sql_string_literal(self, value: str) -> str:
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"

    def _validate_registry_integrity(
        self,
        metadata: HierarchyMetadata,
        registry_table: str,
        result: ValidationResult,
    ) -> None:
        if not self._table_exists(registry_table):
            return

        registry_rows = self.spark.sql(f"""
            SELECT
                hierarchy_id,
                hierarchy_name,
                hierarchy_description,
                owner_team,
                business_domain
            FROM {registry_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
        """).collect()

        if len(registry_rows) > 1:
            result.add_issue(
                severity="ERROR",
                check_name="duplicate_registry_rows",
                message=(
                    f"Registry contains multiple rows for hierarchy "
                    f"'{metadata.hierarchy_id}'"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "row_count": len(registry_rows),
                },
            )

        for row in registry_rows:
            self._validate_registry_field_match(
                metadata=metadata,
                field_name="hierarchy_name",
                persisted_value=row["hierarchy_name"],
                result=result,
            )
            self._validate_registry_field_match(
                metadata=metadata,
                field_name="hierarchy_description",
                persisted_value=row["hierarchy_description"],
                result=result,
            )
            self._validate_registry_field_match(
                metadata=metadata,
                field_name="owner_team",
                persisted_value=row["owner_team"],
                result=result,
            )
            self._validate_registry_field_match(
                metadata=metadata,
                field_name="business_domain",
                persisted_value=row["business_domain"],
                result=result,
            )

    def _validate_registry_field_match(
        self,
        metadata: HierarchyMetadata,
        field_name: str,
        persisted_value: str | None,
        result: ValidationResult,
    ) -> None:
        candidate_value = getattr(metadata, field_name)
        if persisted_value == candidate_value:
            return

        result.add_issue(
            severity="ERROR",
            check_name=f"registry_{field_name}_conflict",
            message=(
                f"Registry field '{field_name}' for hierarchy "
                f"'{metadata.hierarchy_id}' conflicts with the candidate publish"
            ),
            details={
                "hierarchy_id": metadata.hierarchy_id,
                "field_name": field_name,
                "persisted_value": persisted_value,
                "candidate_value": candidate_value,
            },
        )

    def _validate_version_does_not_exist(
        self,
        metadata: HierarchyMetadata,
        version_table: str,
        result: ValidationResult,
    ) -> None:
        if not self._table_exists(version_table):
            return

        existing_count = self.spark.sql(f"""
            SELECT COUNT(*) AS row_count
            FROM {version_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
              AND version_id = {self._sql_string_literal(metadata.version_id)}
        """).first()["row_count"]

        if existing_count > 1:
            result.add_issue(
                severity="ERROR",
                check_name="duplicate_version_rows",
                message=(
                    f"Version table contains duplicate rows for hierarchy "
                    f"'{metadata.hierarchy_id}' version '{metadata.version_id}'"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "version_id": metadata.version_id,
                    "row_count": existing_count,
                },
            )

        if existing_count > 0:
            result.add_issue(
                severity="ERROR",
                check_name="version_already_exists",
                message=(
                    f"Hierarchy '{metadata.hierarchy_id}' version "
                    f"'{metadata.version_id}' already exists in persistence"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "version_id": metadata.version_id,
                    "row_count": existing_count,
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

        existing_count = self.spark.sql(f"""
            SELECT COUNT(*) AS row_count
            FROM {node_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
              AND version_id = {self._sql_string_literal(metadata.version_id)}
        """).first()["row_count"]

        duplicate_rows = self.spark.sql(f"""
            SELECT
                account_key,
                COUNT(*) AS row_count
            FROM {node_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
              AND version_id = {self._sql_string_literal(metadata.version_id)}
            GROUP BY account_key
            HAVING COUNT(*) > 1
        """).collect()

        if existing_count > 0:
            result.add_issue(
                severity="ERROR",
                check_name="node_rows_already_exist",
                message=(
                    f"Node rows already exist for hierarchy '{metadata.hierarchy_id}' "
                    f"version '{metadata.version_id}'"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "version_id": metadata.version_id,
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
                    f"version '{metadata.version_id}'"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "version_id": metadata.version_id,
                    "account_key": row["account_key"],
                    "row_count": row["row_count"],
                },
            )

    def _validate_publish_state_conflicts(
        self,
        metadata: HierarchyMetadata,
        version_table: str,
        result: ValidationResult,
    ) -> None:
        if not self._table_exists(version_table):
            return

        if metadata.version_status == "published":
            self._validate_no_existing_current_version(
                metadata=metadata,
                version_table=version_table,
                result=result,
            )

        self._validate_no_overlapping_effective_windows(
            metadata=metadata,
            version_table=version_table,
            result=result,
        )

    def _validate_no_existing_current_version(
        self,
        metadata: HierarchyMetadata,
        version_table: str,
        result: ValidationResult,
    ) -> None:
        current_count = self.spark.sql(f"""
            SELECT COUNT(*) AS current_count
            FROM {version_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
              AND is_current = TRUE
        """).first()["current_count"]

        if current_count > 0:
            result.add_issue(
                severity="ERROR",
                check_name="current_version_already_exists",
                message=(
                    f"Hierarchy '{metadata.hierarchy_id}' already has a current "
                    "published version"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "current_count": current_count,
                },
            )

    def _validate_no_overlapping_effective_windows(
        self,
        metadata: HierarchyMetadata,
        version_table: str,
        result: ValidationResult,
    ) -> None:
        if not isinstance(metadata.effective_start_date, date):
            return

        overlap_rows = self.spark.sql(f"""
            SELECT
                version_id,
                effective_start_date,
                effective_end_date
            FROM {version_table}
            WHERE hierarchy_id = {self._sql_string_literal(metadata.hierarchy_id)}
              AND effective_start_date <= COALESCE(
                    {self._sql_date_literal(metadata.effective_end_date)},
                    DATE '9999-12-31'
                  )
              AND COALESCE(effective_end_date, DATE '9999-12-31') >=
                  {self._sql_date_literal(metadata.effective_start_date)}
        """).collect()

        for row in overlap_rows:
            result.add_issue(
                severity="ERROR",
                check_name="overlapping_effective_window",
                message=(
                    f"Hierarchy '{metadata.hierarchy_id}' version "
                    f"'{metadata.version_id}' overlaps existing version "
                    f"'{row['version_id']}'"
                ),
                details={
                    "hierarchy_id": metadata.hierarchy_id,
                    "candidate_version_id": metadata.version_id,
                    "existing_version_id": row["version_id"],
                    "candidate_effective_start_date": (
                        metadata.effective_start_date.isoformat()
                        if isinstance(metadata.effective_start_date, date)
                        else None
                    ),
                    "candidate_effective_end_date": (
                        metadata.effective_end_date.isoformat()
                        if isinstance(metadata.effective_end_date, date)
                        else None
                    ),
                    "existing_effective_start_date": (
                        row["effective_start_date"].isoformat()
                        if row["effective_start_date"] is not None
                        else None
                    ),
                    "existing_effective_end_date": (
                        row["effective_end_date"].isoformat()
                        if row["effective_end_date"] is not None
                        else None
                    ),
                },
            )

    def _sql_date_literal(self, value) -> str:
        if value is None or not isinstance(value, date):
            return "NULL"
        return f"DATE '{value.isoformat()}'"
