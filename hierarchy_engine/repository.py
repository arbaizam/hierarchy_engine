"""
Persistence helpers for hierarchy publishing.
 
This module is intentionally lightweight. We can enhance it later as needs
become more clear. Or maybe the need for a richer repository abstraction
never materializes. For now it provides a clean place to put DataFrame and
table-writing logic.
 
Design notes
------------
This module uses explicit Spark schemas rather than schema inference.
That is important because inference can become unstable when:
- optional fields contain None
- date-like fields are sometimes strings and sometimes date objects
- repeated development writes create inconsistent expectations
 
By defining schemas explicitly, we ensure that persisted hierarchy artifacts
are written with stable types every time.
"""
 
from __future__ import annotations
 
from datetime import date
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
 
class HierarchyRepository:
    """
    Repository for writing hierarchy objects to Spark tables.
     Added to capture bypass of validation.
    """
 
    def __init__(self, spark: SparkSession):
        """
        Initialize the repository.
 
        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        """
        self.spark = spark

    def table_exists(self, table_name: str) -> bool:
        """
        Return whether a Spark table exists.
        """
        return bool(self.spark.catalog.tableExists(table_name))

    def registry_entry_exists(self, table_name: str, hierarchy_id: str) -> bool:
        """
        Return whether the registry already contains the hierarchy_id.
        """
        if not self.table_exists(table_name):
            return False

        row_count = self.spark.sql(f"""
            SELECT COUNT(*) AS row_count
            FROM {table_name}
            WHERE hierarchy_id = '{hierarchy_id}'
        """).first()["row_count"]

        return row_count > 0
 
    # ---------------------------------------------------------------------
    # Explicit schemas
    # ---------------------------------------------------------------------
 
    @property
    def registry_schema(self) -> StructType:
        """
        Return the explicit schema for the hierarchy registry table.
 
        Returns
        -------
        StructType
            Registry table schema.
        """
        return StructType(
            [
                StructField("hierarchy_id", StringType(), False),
                StructField("hierarchy_name", StringType(), False),
                StructField("hierarchy_description", StringType(), True),
                StructField("owner_team", StringType(), True),
                StructField("business_domain", StringType(), True),
                StructField("created_date", DateType(), True),
                StructField("updated_date", DateType(), True),
            ]
        )
 
    @property
    def version_schema(self) -> StructType:
        """
        Return the explicit schema for the hierarchy version table.
 
        Returns
        -------
        StructType
            Version table schema.
        """
        return StructType(
            [
                StructField("hierarchy_id", StringType(), False),
                StructField("version_id", StringType(), False),
                StructField("version_name", StringType(), False),
                StructField("version_status", StringType(), False),
                StructField("effective_start_date", DateType(), False),
                StructField("effective_end_date", DateType(), True),
                StructField("is_current", BooleanType(), True),
                StructField("change_description", StringType(), True),
                StructField("created_date", DateType(), True),
                StructField("created_by", StringType(), True),
                StructField("published_date", DateType(), True),
                StructField("published_by", StringType(), True),
            ]
        )
 
    @property
    def node_schema(self) -> StructType:
        """
        Return the explicit schema for the base hierarchy node table.
 
        Returns
        -------
        StructType
            Node table schema.
        """
        return StructType(
            [
                StructField("hierarchy_id", StringType(), False),
                StructField("version_id", StringType(), False),
                StructField("account_key", StringType(), False),
                StructField("account_name", StringType(), False),
                StructField("parent_account_key", StringType(), True),
                StructField("account_level", IntegerType(), False),
                StructField("node_path", StringType(), False),
                StructField("created_date", DateType(), True),
                StructField("updated_date", DateType(), True),
            ]
        )
 
    # ---------------------------------------------------------------------
    # DataFrame construction
    # ---------------------------------------------------------------------
 
    def rows_to_dataframe(self, rows: list[dict]):
        """
        Convert flattened node row dictionaries to a Spark DataFrame.
 
        Parameters
        ----------
        rows : list[dict]
            Flattened hierarchy rows.
 
        Returns
        -------
        pyspark.sql.DataFrame
            Spark DataFrame of flattened hierarchy rows.
 
        Notes
        -----
        This method uses an explicit node schema to avoid Spark inference
        problems across repeated development writes.
        """
        return self.spark.createDataFrame(rows, schema=self.node_schema)
 
    # ---------------------------------------------------------------------
    # Registry writes
    # ---------------------------------------------------------------------
 
    def write_registry(
        self,
        metadata,
        table_name: str,
        created_date: Optional[date] = None,
        updated_date: Optional[date] = None,
    ) -> None:
        """
        Append one hierarchy registry record.
 
        Parameters
        ----------
        metadata : HierarchyMetadata
            Hierarchy metadata object.
        table_name : str
            Target registry table.
        created_date : date | None, default None
            System-created date for the record.
        updated_date : date | None, default None
            System-updated date for the record.
 
        Notes
        -----
        `created_date` and `updated_date` are system metadata and should not be
        inferred from authored hierarchy business dates.
        """
        data = [
            {
                "hierarchy_id": metadata.hierarchy_id,
                "hierarchy_name": metadata.hierarchy_name,
                "hierarchy_description": metadata.hierarchy_description,
                "owner_team": metadata.owner_team,
                "business_domain": metadata.business_domain,
                "created_date": created_date,
                "updated_date": updated_date,
            }
        ]
 
        df = self.spark.createDataFrame(data, schema=self.registry_schema)
        df.write.mode("append").saveAsTable(table_name)
 
    # ---------------------------------------------------------------------
    # Version writes
    # ---------------------------------------------------------------------
 
    def write_version(
        self,
        metadata,
        table_name: str,
        created_date: Optional[date] = None,
        created_by: Optional[str] = None,
        published_date: Optional[date] = None,
        published_by: Optional[str] = None,
        change_description: Optional[str] = None,
    ) -> None:
        """
        Append one hierarchy version record.
 
        Parameters
        ----------
        metadata : HierarchyMetadata
            Hierarchy metadata object.
        table_name : str
            Target version table.
        created_date : date | None, default None
            System-created date for the version record.
        created_by : str | None, default None
            User or process that created the version row.
        published_date : date | None, default None
            Publish date if the version is published.
        published_by : str | None, default None
            User or process that published the version.
        change_description : str | None, default None
            Optional description of the version change.
 
        Notes
        -----
        Effective dates come from the authored hierarchy definition.
 
        Created/published fields are operational metadata and should be supplied
        by the publish workflow rather than inferred from business dates.
        """
        data = [
            {
                "hierarchy_id": metadata.hierarchy_id,
                "version_id": metadata.version_id,
                "version_name": metadata.version_name,
                "version_status": metadata.version_status,
                "effective_start_date": metadata.effective_start_date,
                "effective_end_date": metadata.effective_end_date,
                "is_current": metadata.version_status == "published",
                "change_description": change_description,
                "created_date": created_date,
                "created_by": created_by,
                "published_date": published_date,
                "published_by": published_by,
            }
        ]
 
        df = self.spark.createDataFrame(data, schema=self.version_schema)
        df.write.mode("append").saveAsTable(table_name)
 
    # ---------------------------------------------------------------------
    # Node writes
    # ---------------------------------------------------------------------
 
    def write_nodes(self, rows_df, table_name: str, mode: str = "append") -> None:
        """
        Write flattened node rows to the base hierarchy node table.
 
        Parameters
        ----------
        rows_df : pyspark.sql.DataFrame
            Flattened hierarchy DataFrame.
        table_name : str
            Target node table.
        mode : str, default "append"
            Spark write mode.
        """
        rows_df.write.mode(mode).saveAsTable(table_name)
