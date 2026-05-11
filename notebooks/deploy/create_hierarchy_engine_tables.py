# Databricks notebook source
"""Create the hierarchy engine schema and base tables.

This notebook is intended to run from a Databricks Asset Bundle deployment job.
It creates the target schema if needed, then creates the two base tables used by
the hierarchy engine publish flow:

- hierarchy_versions
- base_hierarchy_node
"""

# COMMAND ----------

from hierarchy_engine.service import HierarchyService
from hierarchy_engine.sql_identifiers import validate_sql_identifier

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "hierarchy_engine")
dbutils.widgets.text("version_table", "hierarchy_versions")
dbutils.widgets.text("node_table", "base_hierarchy_node")
dbutils.widgets.dropdown("mode", "ignore", ["ignore", "errorifexists", "overwrite"])

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
version_table_name = dbutils.widgets.get("version_table").strip()
node_table_name = dbutils.widgets.get("node_table").strip()
mode = dbutils.widgets.get("mode").strip()

if not schema:
    raise ValueError("Widget 'schema' is required.")

schema_identifier = f"{catalog}.{schema}" if catalog else schema
version_table = f"{schema_identifier}.{version_table_name}"
node_table = f"{schema_identifier}.{node_table_name}"

validate_sql_identifier(schema_identifier, kind="schema")
validate_sql_identifier(version_table, kind="table")
validate_sql_identifier(node_table, kind="table")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_identifier}")

HierarchyService().create_base_tables(
    spark=spark,
    version_table=version_table,
    node_table=node_table,
    mode=mode,
)

print(f"Created or verified schema: {schema_identifier}")
print(f"Created or verified table:  {version_table}")
print(f"Created or verified table:  {node_table}")
print(f"Write mode:                 {mode}")
