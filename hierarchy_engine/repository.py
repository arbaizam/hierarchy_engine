"""
Spark repository for hierarchy version metadata and derived node rows.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from hierarchy_engine.errors import HierarchyValidationError
from hierarchy_engine.models import HierarchyDefinition
from hierarchy_engine.serializer import HierarchyVersionSerializer
from hierarchy_engine.sql_identifiers import validate_sql_identifier


logger = logging.getLogger(__name__)


class HierarchyRepository:
    """
    Spark-backed repository for hierarchy metadata tables.
    """

    def __init__(
        self,
        spark: SparkSession,
        serializer: HierarchyVersionSerializer | None = None,
    ) -> None:
        """
        Create a Spark repository for hierarchy metadata tables.
        """
        self.spark = spark
        self.serializer = serializer or HierarchyVersionSerializer()

    def table_exists(self, table_name: str) -> bool:
        """
        Return whether a Spark table exists.
        """
        validate_sql_identifier(table_name, kind="table")
        return bool(self.spark.catalog.tableExists(table_name))

    def version_exists(self, table_name: str, hierarchy_id: str, version: str) -> bool:
        """
        Return whether the authoritative version table already contains the row.
        """
        validate_sql_identifier(table_name, kind="table")
        if not self.table_exists(table_name):
            return False

        row_count = self.spark.sql(
            f"""
            SELECT COUNT(*) AS row_count
            FROM {table_name}
            WHERE hierarchy_id = {self._sql_string_literal(hierarchy_id)}
              AND version = {self._sql_string_literal(version)}
            """
        ).first()["row_count"]

        return row_count > 0

    def published_version_exists(
        self,
        table_name: str,
        hierarchy_id: str,
    ) -> bool:
        """
        Return whether a published row already exists for the hierarchy id.
        """
        validate_sql_identifier(table_name, kind="table")
        if not self.table_exists(table_name):
            return False

        row_count = self.spark.sql(
            f"""
            SELECT COUNT(*) AS row_count
            FROM {table_name}
            WHERE hierarchy_id = {self._sql_string_literal(hierarchy_id)}
              AND status = 'published'
            """
        ).first()["row_count"]

        return row_count > 0

    def get_version_status(
        self,
        table_name: str,
        hierarchy_id: str,
        version: str,
    ) -> str | None:
        """
        Return the persisted status for one authoritative version row.
        """
        validate_sql_identifier(table_name, kind="table")
        if not self.table_exists(table_name):
            return None

        rows = self.spark.sql(
            f"""
            SELECT status
            FROM {table_name}
            WHERE hierarchy_id = {self._sql_string_literal(hierarchy_id)}
              AND version = {self._sql_string_literal(version)}
            """
        ).collect()

        if not rows:
            return None

        return rows[0]["status"]

    def _sql_string_literal(self, value: str) -> str:
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"

    @property
    def version_schema(self) -> StructType:
        """
        Return the explicit schema for the authoritative hierarchy version table.
        """
        return StructType(
            [
                StructField("hierarchy_id", StringType(), False),
                StructField("hierarchy_name", StringType(), False),
                StructField("version", StringType(), False),
                StructField("status", StringType(), False),
                StructField("effective_start_date", StringType(), False),
                StructField("effective_end_date", StringType(), False),
                StructField("description", StringType(), True),
                StructField("payload_json", StringType(), False),
                StructField("content_hash", StringType(), False),
                StructField("node_count", IntegerType(), False),
                StructField("leaf_count", IntegerType(), False),
                StructField("max_depth", IntegerType(), False),
                StructField("owner", StringType(), True),
                StructField("owner_department", StringType(), True),
                StructField("published_by", StringType(), True),
                StructField("published_at", StringType(), True),
                StructField("retired_by", StringType(), True),
                StructField("retired_at", StringType(), True),
            ]
        )

    @property
    def node_schema(self) -> StructType:
        """
        Return the explicit schema for the derived base hierarchy node table.
        """
        return StructType(
            [
                StructField("hierarchy_id", StringType(), False),
                StructField("version", StringType(), False),
                StructField("account_key", StringType(), False),
                StructField("account_name", StringType(), False),
                StructField("parent_account_key", StringType(), True),
                StructField("account_level", IntegerType(), False),
                StructField("node_path", StringType(), False),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
            ]
        )

    def rows_to_dataframe(self, rows: list[dict]):
        """
        Convert flattened node row dictionaries to a Spark DataFrame.
        """
        return self.spark.createDataFrame(rows, schema=self.node_schema)

    def create_base_tables(
        self,
        version_table: str,
        node_table: str,
        mode: str = "errorifexists",
    ) -> None:
        """
        Create the empty authoritative and derived tables from explicit schemas.
        """
        logger.info(
            "Creating empty base tables: version=%s node=%s mode=%s",
            version_table,
            node_table,
            mode,
        )
        validate_sql_identifier(version_table, kind="table")
        validate_sql_identifier(node_table, kind="table")
        self.spark.createDataFrame([], schema=self.version_schema).write.mode(
            mode
        ).saveAsTable(version_table)
        self.spark.createDataFrame([], schema=self.node_schema).write.mode(
            mode
        ).saveAsTable(node_table)

    def write_version(
        self,
        definition: HierarchyDefinition,
        table_name: str,
        *,
        status: str = "published",
        published_by: Optional[str] = None,
        published_at: Optional[str] = None,
        retired_by: Optional[str] = None,
        retired_at: Optional[str] = None,
        effective_start_date: Optional[str] = None,
        effective_end_date: Optional[str] = None,
    ) -> None:
        """
        Append one authoritative hierarchy version record.
        """
        logger.info(
            "Writing authoritative version row to %s for hierarchy_id=%s version=%s status=%s",
            table_name,
            definition.metadata.hierarchy_id,
            definition.metadata.version,
            status,
        )
        validate_sql_identifier(table_name, kind="table")
        row = self.serializer.serialize_version(
            definition,
            status=status,
            published_by=self._actor_or_system(published_by) if status == "published" else published_by,
            published_at=published_at or self._utc_now() if status == "published" else published_at,
            retired_by=retired_by,
            retired_at=retired_at,
            effective_start_date=effective_start_date,
            effective_end_date=effective_end_date,
        )
        df = self.spark.createDataFrame([asdict(row)], schema=self.version_schema)
        df.write.mode("append").saveAsTable(table_name)

    def retire_version(
        self,
        table_name: str,
        hierarchy_id: str,
        version: str,
        *,
        retired_by: Optional[str] = None,
        retired_at: Optional[str] = None,
        effective_end_date: Optional[str] = None,
    ) -> None:
        """
        Update one authoritative version row to retired status.
        """
        logger.info(
            "Retiring hierarchy version in %s for hierarchy_id=%s version=%s",
            table_name,
            hierarchy_id,
            version,
        )
        validate_sql_identifier(table_name, kind="table")
        current_status = self.get_version_status(
            table_name=table_name,
            hierarchy_id=hierarchy_id,
            version=version,
        )

        if current_status is None:
            raise HierarchyValidationError(
                f"Hierarchy '{hierarchy_id}' version '{version}' does not exist in persistence"
            )

        if current_status != "published":
            raise HierarchyValidationError(
                f"Hierarchy '{hierarchy_id}' version '{version}' is not currently published"
            )

        retirement_timestamp = retired_at or self._utc_now()
        retirement_effective_end_date = (
            effective_end_date or self._date_from_timestamp(retirement_timestamp)
        )

        self.spark.sql(
            f"""
            UPDATE {table_name}
            SET status = 'retired',
                retired_by = {self._sql_nullable(self._actor_or_system(retired_by))},
                retired_at = {self._sql_nullable(retirement_timestamp)},
                effective_end_date = {self._sql_string_literal(retirement_effective_end_date)}
            WHERE hierarchy_id = {self._sql_string_literal(hierarchy_id)}
              AND version = {self._sql_string_literal(version)}
            """
        )

    def write_nodes(self, rows_df, table_name: str, mode: str = "append") -> None:
        """
        Write flattened node rows to the derived base hierarchy node table.
        """
        validate_sql_identifier(table_name, kind="table")
        logger.info("Writing node rows to %s with mode=%s", table_name, mode)
        rows_df.write.mode(mode).saveAsTable(table_name)

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _actor_or_system(self, value: str | None) -> str:
        if value is None:
            return "system"
        stripped = value.strip()
        return stripped or "system"

    def _sql_nullable(self, value: str | None) -> str:
        return "NULL" if value is None else self._sql_string_literal(value)

    def _date_from_timestamp(self, value: str) -> str:
        return value[:10]
