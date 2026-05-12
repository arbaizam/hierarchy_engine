# Databricks notebook source
"""Run hierarchy engine system tests against real Databricks Spark objects.

This notebook executes the ST-001 through ST-022 test cases from the system
test plan. It creates disposable tables and views using a configurable prefix,
records pass/fail details for every test, and writes a results table for audit
evidence.
"""

# COMMAND ----------

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import traceback
import uuid

import yaml

from hierarchy_engine.errors import HierarchyParseError, HierarchyValidationError
from hierarchy_engine.models import HierarchyDefinition, HierarchyMetadata, HierarchyNode
from hierarchy_engine.repository import HierarchyRepository
from hierarchy_engine.service import HierarchyService
from hierarchy_engine.sql_identifiers import validate_sql_identifier

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "hierarchy_engine_system_test")
dbutils.widgets.text("object_prefix", "")
dbutils.widgets.text("published_by", "system-test")
dbutils.widgets.dropdown("cleanup_test_objects", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
published_by = dbutils.widgets.get("published_by").strip() or "system-test"
cleanup_test_objects = dbutils.widgets.get("cleanup_test_objects").strip().lower() == "true"

object_prefix = dbutils.widgets.get("object_prefix").strip()
if not object_prefix:
    object_prefix = f"he_st_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

if not schema:
    raise ValueError("Widget 'schema' is required.")

schema_identifier = f"{catalog}.{schema}" if catalog else schema
validate_sql_identifier(schema_identifier, kind="schema")

def relation(name: str) -> str:
    validate_sql_identifier(name, kind="object")
    identifier = f"{schema_identifier}.{object_prefix}_{name}"
    validate_sql_identifier(identifier, kind="relation")
    return identifier

version_table = relation("hierarchy_versions")
node_table = relation("base_hierarchy_node")
paths_view = relation("vw_hierarchy_paths")
flat_view = relation("vw_hierarchy_flat_nodes")
dims_view = relation("vw_hierarchy_leaf_dimensions")
reporting_view = relation("vw_hierarchy_published_leaves")
nodes_dims_view = relation("vw_hierarchy_node_dimensions")
nodes_reporting_view = relation("vw_hierarchy_published_nodes")
results_table = relation("system_test_results")

service = HierarchyService()
repo = HierarchyRepository(spark)
run_id = str(uuid.uuid4())
work_dir = Path(f"/tmp/hierarchy_engine_system_tests/{run_id}")
work_dir.mkdir(parents=True, exist_ok=True)

test_objects = [
    nodes_reporting_view,
    nodes_dims_view,
    reporting_view,
    dims_view,
    flat_view,
    paths_view,
    node_table,
    version_table,
]

print(f"System test run_id: {run_id}")
print(f"Target schema:      {schema_identifier}")
print(f"Object prefix:      {object_prefix}")
print(f"Results table:      {results_table}")

# COMMAND ----------

results: list[dict] = []

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

def table_exists(name: str) -> bool:
    return bool(spark.catalog.tableExists(name))

def relation_columns(name: str) -> set[str]:
    return set(spark.table(name).columns)

def row_count(name: str, where_clause: str | None = None) -> int:
    where_sql = f" WHERE {where_clause}" if where_clause else ""
    return int(spark.sql(f"SELECT COUNT(*) AS row_count FROM {name}{where_sql}").first()["row_count"])

def first_row(name: str, where_clause: str):
    rows = spark.sql(f"SELECT * FROM {name} WHERE {where_clause}").collect()
    if not rows:
        raise AssertionError(f"No row found in {name} for {where_clause}")
    return rows[0]

def write_yaml_file(file_name: str, payload: dict) -> str:
    path = work_dir / file_name
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return str(path)

def valid_payload(hierarchy_id: str = "HE_TEST", version: str = "V1") -> dict:
    return {
        "hierarchy_id": hierarchy_id,
        "hierarchy_name": f"{hierarchy_id} Hierarchy",
        "version": version,
        "owner": "ALM Engineering",
        "owner_department": "ALM",
        "description": "System test hierarchy",
        "nodes": [
            {
                "account_key": "10000",
                "account_name": "Assets",
                "children": [
                    {"account_key": "10100", "account_name": "Investments"},
                    {"account_key": "10200", "account_name": "Cash"},
                ],
            },
            {"account_key": "20000", "account_name": "Liabilities"},
        ],
    }

def build_definition(
    hierarchy_id: str = "HE_TEST",
    version: str = "V1",
    *,
    nodes: list[HierarchyNode] | None = None,
    metadata_overrides: dict | None = None,
) -> HierarchyDefinition:
    metadata = {
        "hierarchy_id": hierarchy_id,
        "hierarchy_name": f"{hierarchy_id} Hierarchy",
        "version": version,
        "owner": "ALM Engineering",
        "owner_department": "ALM",
        "description": "System test hierarchy",
    }
    metadata.update(metadata_overrides or {})
    if nodes is None:
        nodes = [
            HierarchyNode(
                account_key="10000",
                account_name="Assets",
                children=[
                    HierarchyNode(account_key="10100", account_name="Investments"),
                    HierarchyNode(account_key="10200", account_name="Cash"),
                ],
            ),
            HierarchyNode(account_key="20000", account_name="Liabilities"),
        ]
    return HierarchyDefinition(metadata=HierarchyMetadata(**metadata), nodes=nodes)

def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)

def run_test(test_id: str, area: str, description: str, test_func) -> None:
    started_at = utc_now()
    try:
        details = test_func()
        status = "Pass"
        error_message = ""
    except Exception as exc:
        details = traceback.format_exc(limit=8)
        status = "Fail"
        error_message = str(exc)
    results.append(
        {
            "run_id": run_id,
            "test_id": test_id,
            "area": area,
            "description": description,
            "status": status,
            "started_at": started_at,
            "finished_at": utc_now(),
            "details": "" if details is None else str(details),
            "error_message": error_message,
        }
    )
    print(f"{test_id}: {status} - {description}")
    if status == "Fail":
        print(error_message)

# COMMAND ----------

def st_001_create_schema_and_tables():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_identifier}")
    service.create_base_tables(
        spark=spark,
        version_table=version_table,
        node_table=node_table,
        mode="ignore",
    )
    assert_true(table_exists(version_table), f"Missing table: {version_table}")
    assert_true(table_exists(node_table), f"Missing table: {node_table}")
    return f"Created or verified schema {schema_identifier}, {version_table}, and {node_table}."

def st_002_version_table_schema():
    expected = {
        "hierarchy_id",
        "hierarchy_name",
        "version",
        "status",
        "effective_start_date",
        "effective_end_date",
        "description",
        "payload_json",
        "content_hash",
        "node_count",
        "leaf_count",
        "max_depth",
        "owner",
        "owner_department",
        "published_by",
        "published_at",
        "retired_by",
        "retired_at",
    }
    actual = relation_columns(version_table)
    missing = sorted(expected - actual)
    assert_true(not missing, f"Missing version table columns: {missing}")
    engine_required = {
        field.name
        for field in repo.version_schema.fields
        if field.name in {"effective_start_date", "effective_end_date"} and not field.nullable
    }
    assert_true(
        engine_required == {"effective_start_date", "effective_end_date"},
        "Engine schema does not mark effective dates as non-null.",
    )
    return f"Version table has all expected columns. Effective date fields are non-null in the engine schema: {sorted(engine_required)}."

def st_003_valid_yaml_loads():
    path = write_yaml_file("valid_hierarchy.yaml", valid_payload())
    definition = service.load_from_yaml(path)
    assert_true(definition.metadata.hierarchy_id == "HE_TEST", "Unexpected hierarchy_id.")
    assert_true(len(definition.nodes) == 2, "Expected two root nodes.")
    assert_true(not definition.load_issues, f"Unexpected load issues: {definition.load_issues}")
    return f"Loaded {path}; roots={len(definition.nodes)}."

def st_004_legacy_wrapper_rejected():
    path = write_yaml_file("legacy_wrapper.yaml", {"hierarchy": valid_payload()})
    try:
        service.load_from_yaml(path)
    except HierarchyParseError as exc:
        assert_true("Legacy top-level 'hierarchy' wrapper" in str(exc), "Unexpected parse error.")
        return str(exc)
    raise AssertionError("Legacy wrapper loaded successfully but should have been rejected.")

def st_005_required_metadata_enforced():
    definition = build_definition(metadata_overrides={"owner": ""})
    try:
        service.validate_definition(definition)
    except HierarchyValidationError as exc:
        assert_true("missing_owner" in str(exc), "Validation failed for an unexpected reason.")
        return "Missing owner was blocked before publish."
    raise AssertionError("Definition with missing owner passed validation.")

def st_006_duplicate_account_keys_blocked():
    definition = build_definition(
        nodes=[
            HierarchyNode(account_key="10000", account_name="Assets"),
            HierarchyNode(account_key="10000", account_name="Duplicate Assets"),
        ]
    )
    try:
        service.validate_definition(definition)
    except HierarchyValidationError as exc:
        assert_true("duplicate_account_key" in str(exc), "Duplicate key was not reported.")
        return "Duplicate account key was blocked."
    raise AssertionError("Duplicate account key passed validation.")

def st_007_flattening_parent_child_rows():
    rows = service.flatten_definition(build_definition())
    by_key = {row.account_key: row for row in rows}
    assert_true(len(rows) == 4, f"Expected 4 rows, found {len(rows)}.")
    assert_true(by_key["10100"].parent_account_key == "10000", "Child parent key is incorrect.")
    assert_true(by_key["10100"].account_level == 2, "Child level is incorrect.")
    assert_true(by_key["10100"].node_path == "10000||10100", "Child path is incorrect.")
    return "Flattened rows have expected parent, level, and path values."

def st_008_first_publish_writes_rows():
    definition = build_definition("HE_TEST", "V1")
    service.publish_to_tables(
        definition=definition,
        spark=spark,
        version_table=version_table,
        node_table=node_table,
        published_by=published_by,
        published_at="2026-04-26T12:00:00Z",
    )
    version_rows = row_count(version_table, "hierarchy_id = 'HE_TEST' AND version = 'V1'")
    node_rows = row_count(node_table, "hierarchy_id = 'HE_TEST' AND version = 'V1'")
    assert_true(version_rows == 1, f"Expected 1 version row, found {version_rows}.")
    assert_true(node_rows == 4, f"Expected 4 node rows, found {node_rows}.")
    return f"Published HE_TEST V1 with {version_rows} version row and {node_rows} node rows."

def st_009_default_effective_dates():
    row = first_row(version_table, "hierarchy_id = 'HE_TEST' AND version = 'V1'")
    assert_true(row["effective_start_date"] == "2026-04-26", f"Unexpected effective_start_date: {row['effective_start_date']}")
    assert_true(row["effective_end_date"] == "2999-12-31", f"Unexpected effective_end_date: {row['effective_end_date']}")
    return f"effective_start_date={row['effective_start_date']}; effective_end_date={row['effective_end_date']}."

def st_010_explicit_effective_start_date():
    definition = build_definition("HE_TEST_EXPLICIT", "V1")
    service.publish_to_tables(
        definition=definition,
        spark=spark,
        version_table=version_table,
        node_table=node_table,
        published_by=published_by,
        published_at="2026-04-26T12:00:00Z",
        effective_start_date="2026-05-01",
    )
    row = first_row(version_table, "hierarchy_id = 'HE_TEST_EXPLICIT' AND version = 'V1'")
    assert_true(row["effective_start_date"] == "2026-05-01", "Explicit effective_start_date was not used.")
    assert_true(row["effective_end_date"] == "2999-12-31", "Default effective_end_date was not used.")
    return f"Explicit start date persisted: {row['effective_start_date']}."

def st_011_duplicate_publish_blocked():
    try:
        service.publish_to_tables(
            definition=build_definition("HE_TEST", "V1"),
            spark=spark,
            version_table=version_table,
            node_table=node_table,
            published_by=published_by,
            published_at="2026-04-26T12:05:00Z",
        )
    except HierarchyValidationError as exc:
        assert_true("version_already_exists" in str(exc), "Duplicate publish failed for an unexpected reason.")
        return "Duplicate hierarchy_id/version publish was blocked."
    raise AssertionError("Duplicate publish succeeded.")

def st_012_second_published_version_allowed():
    service.publish_to_tables(
        definition=build_definition("HE_TEST", "V2"),
        spark=spark,
        version_table=version_table,
        node_table=node_table,
        published_by=published_by,
        published_at="2026-04-27T12:00:00Z",
    )
    published_count = row_count(
        version_table,
        "hierarchy_id = 'HE_TEST' AND status = 'published'",
    )
    assert_true(
        published_count == 2,
        f"Expected two published HE_TEST versions, found {published_count}.",
    )
    return "Second published version for same hierarchy was allowed."

def st_013_retire_closes_effective_window():
    service.retire_version(
        spark=spark,
        version_table=version_table,
        hierarchy_id="HE_TEST",
        version="V1",
        retired_by=published_by,
        retired_at="2026-04-30T23:59:59Z",
    )
    row = first_row(version_table, "hierarchy_id = 'HE_TEST' AND version = 'V1'")
    assert_true(row["status"] == "retired", f"Unexpected status: {row['status']}")
    assert_true(row["effective_end_date"] == "2026-04-30", f"Unexpected effective_end_date: {row['effective_end_date']}")
    return f"Retired HE_TEST V1 with effective_end_date={row['effective_end_date']}."

def st_014_explicit_retirement_effective_end_date():
    definition = build_definition("HE_TEST_RETIRE_EXPLICIT", "V1")
    service.publish_to_tables(
        definition=definition,
        spark=spark,
        version_table=version_table,
        node_table=node_table,
        published_by=published_by,
        published_at="2026-04-26T12:00:00Z",
    )
    service.retire_version(
        spark=spark,
        version_table=version_table,
        hierarchy_id="HE_TEST_RETIRE_EXPLICIT",
        version="V1",
        retired_by=published_by,
        retired_at="2026-04-30T23:59:59Z",
        effective_end_date="2026-04-25",
    )
    row = first_row(version_table, "hierarchy_id = 'HE_TEST_RETIRE_EXPLICIT' AND version = 'V1'")
    assert_true(row["effective_end_date"] == "2026-04-25", "Explicit retirement effective_end_date was not used.")
    return f"Explicit retirement effective_end_date persisted: {row['effective_end_date']}."

def st_015_already_retired_cannot_retire_again():
    try:
        service.retire_version(
            spark=spark,
            version_table=version_table,
            hierarchy_id="HE_TEST",
            version="V1",
            retired_by=published_by,
            retired_at="2026-05-01T00:00:00Z",
        )
    except HierarchyValidationError as exc:
        assert_true("not currently published" in str(exc), "Second retirement failed for an unexpected reason.")
        return "Second retirement was blocked."
    raise AssertionError("Already retired version was retired again.")

def st_016_reporting_views_rebuild():
    service.rebuild_reporting_views(
        spark=spark,
        version_table=version_table,
        node_table=node_table,
        paths_view=paths_view,
        flat_view=flat_view,
        dims_view=dims_view,
        reporting_view=reporting_view,
        nodes_dims_view=nodes_dims_view,
        nodes_reporting_view=nodes_reporting_view,
    )
    counts = {
        "paths": row_count(paths_view),
        "flat": row_count(flat_view),
        "dims": row_count(dims_view),
        "reporting": row_count(reporting_view),
        "nodes_dims": row_count(nodes_dims_view),
        "nodes_reporting": row_count(nodes_reporting_view),
    }
    assert_true(all(value > 0 for value in counts.values()), f"Expected all rebuilt views to contain rows: {counts}")
    return f"View row counts: {counts}"

def st_017_reporting_excludes_retired_versions():
    retired_count = row_count(reporting_view, "hierarchy_id = 'HE_TEST_RETIRE_EXPLICIT' AND version = 'V1'")
    assert_true(retired_count == 0, f"Retired hierarchy appeared in published reporting view: {retired_count} rows.")
    return "Retired hierarchy is excluded from published leaf reporting view."

def st_018_reporting_exposes_effective_dates():
    leaf_cols = relation_columns(reporting_view)
    node_cols = relation_columns(nodes_reporting_view)
    for col in ["effective_start_date", "effective_end_date"]:
        assert_true(col in leaf_cols, f"{col} missing from leaf reporting view.")
        assert_true(col in node_cols, f"{col} missing from node reporting view.")
    active_count = row_count(reporting_view, "hierarchy_id = 'HE_TEST_EXPLICIT' AND effective_end_date = '2999-12-31'")
    assert_true(active_count > 0, "No active reporting rows found with open-ended effective_end_date.")
    return "Published leaf and node reporting views expose effective date columns."

def st_019_post_publish_audit_passes():
    result = service.validate_published_version(
        spark=spark,
        hierarchy_id="HE_TEST_EXPLICIT",
        version="V1",
        node_table=node_table,
        version_table=version_table,
    )
    assert_true(result.passed, result.to_text())
    return result.to_text()

def st_020_rerun_table_creation_safe():
    before = {
        "versions": row_count(version_table),
        "nodes": row_count(node_table),
    }
    service.create_base_tables(
        spark=spark,
        version_table=version_table,
        node_table=node_table,
        mode="ignore",
    )
    after = {
        "versions": row_count(version_table),
        "nodes": row_count(node_table),
    }
    assert_true(before == after, f"Row counts changed after mode=ignore rerun. before={before}, after={after}")
    return f"Counts unchanged after setup rerun: {after}"

def st_021_deployment_identity_permissions():
    spark.sql(f"SELECT COUNT(*) AS row_count FROM {version_table}").first()
    spark.sql(f"SELECT COUNT(*) AS row_count FROM {node_table}").first()
    assert_true(table_exists(version_table) and table_exists(node_table), "Deployment identity cannot see created tables.")
    return "Current execution identity can create and query schema tables."

def st_022_package_import_from_deployed_code():
    import hierarchy_engine

    instance = HierarchyService()
    assert_true(instance is not None, "HierarchyService could not be instantiated.")
    return f"Imported hierarchy_engine from {getattr(hierarchy_engine, '__file__', 'unknown location')}."

# COMMAND ----------

system_tests = [
    ("ST-001", "Deployment setup", "Create schema and hierarchy engine base tables", st_001_create_schema_and_tables),
    ("ST-002", "Deployment setup", "Confirm version table schema includes lifecycle and effective-date fields", st_002_version_table_schema),
    ("ST-003", "YAML loading", "Load valid hierarchy YAML", st_003_valid_yaml_loads),
    ("ST-004", "YAML loading", "Reject legacy YAML wrapper", st_004_legacy_wrapper_rejected),
    ("ST-005", "Pre-structural validation", "Block missing required metadata", st_005_required_metadata_enforced),
    ("ST-006", "Pre-structural validation", "Block duplicate account keys", st_006_duplicate_account_keys_blocked),
    ("ST-007", "Flattening", "Flatten parent-child hierarchy rows correctly", st_007_flattening_parent_child_rows),
    ("ST-008", "Publish workflow", "First-time publish writes node and version rows", st_008_first_publish_writes_rows),
    ("ST-009", "Publish workflow", "Default effective dates are populated on publish", st_009_default_effective_dates),
    ("ST-010", "Publish workflow", "Explicit effective start date is honored", st_010_explicit_effective_start_date),
    ("ST-011", "Publish workflow", "Duplicate publish is blocked", st_011_duplicate_publish_blocked),
    ("ST-012", "Publish workflow", "Second published version for same hierarchy is allowed", st_012_second_published_version_allowed),
    ("ST-013", "Retirement workflow", "Retirement changes status and closes effective window", st_013_retire_closes_effective_window),
    ("ST-014", "Retirement workflow", "Explicit retirement effective end date is honored", st_014_explicit_retirement_effective_end_date),
    ("ST-015", "Retirement workflow", "Already retired version cannot be retired again", st_015_already_retired_cannot_retire_again),
    ("ST-016", "Reporting views", "Reporting views rebuild successfully", st_016_reporting_views_rebuild),
    ("ST-017", "Reporting views", "Published reporting views exclude retired versions", st_017_reporting_excludes_retired_versions),
    ("ST-018", "Reporting views", "Reporting views expose effective dates", st_018_reporting_exposes_effective_dates),
    ("ST-019", "Post-publish audit", "Post-publish audit passes for clean published hierarchy", st_019_post_publish_audit_passes),
    ("ST-020", "Operational recovery", "Rerunning table creation with ignore mode is safe", st_020_rerun_table_creation_safe),
    ("ST-021", "Security and access", "Deployment identity can create and query target objects", st_021_deployment_identity_permissions),
    ("ST-022", "Package import", "Asset bundle job can import hierarchy_engine package", st_022_package_import_from_deployed_code),
]

for test_id, area, description, func in system_tests:
    run_test(test_id, area, description, func)

# COMMAND ----------

results_df = spark.createDataFrame(results)
results_df.write.mode("overwrite").saveAsTable(results_table)

display(
    results_df.select(
        "test_id",
        "area",
        "description",
        "status",
        "details",
        "error_message",
    ).orderBy("test_id")
)

failed = [result for result in results if result["status"] != "Pass"]
print(f"Results table: {results_table}")
print(f"Passed: {len(results) - len(failed)}")
print(f"Failed: {len(failed)}")

# COMMAND ----------

if cleanup_test_objects:
    for object_name in test_objects:
        object_type = "VIEW" if "vw_" in object_name else "TABLE"
        spark.sql(f"DROP {object_type} IF EXISTS {object_name}")
    print("Dropped disposable test tables and views. Results table was retained.")
else:
    print("cleanup_test_objects=false; disposable test tables and views were retained for evidence review.")

if failed:
    raise AssertionError(
        f"{len(failed)} system test(s) failed. Review {results_table} for details."
    )

print("All hierarchy engine system tests passed.")
