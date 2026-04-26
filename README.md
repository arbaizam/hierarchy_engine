# Hierarchy Engine

`hierarchy_engine` is a Databricks- and Spark-oriented library for authoring, validating, flattening, comparing, rendering, exporting, and publishing versioned business hierarchies defined in YAML.

The current implementation uses a `rules_engine`-style persistence model:

- author hierarchies as nested YAML
- validate them before any write
- persist one authoritative version row per `(hierarchy_id, version)`
- store the full canonical hierarchy payload in `payload_json`
- derive flattened node rows and reporting views from that version row

The refactor design record is in [HIERARCHY_VERSIONS_REFACTOR_SPEC.md](C:\Users\aarba\pydev\hierarchy_engine\HIERARCHY_VERSIONS_REFACTOR_SPEC.md).

## Core Principles

- YAML is the authored source artifact.
- Lifecycle is owned by persistence, not by the YAML file.
- One version row is the authoritative persisted artifact.
- Flattened node rows and reporting views are derived artifacts.
- Validation is layered and should block bad publishes before any write.
- Post-publish validation is audit logic, not the primary publish safeguard.

## High-Level Architecture

```text
YAML hierarchy file
    |
    v
HierarchyConfigLoader
    |
    v
HierarchyDefinition
    |
    +--> HierarchyValidator
    |
    +--> HierarchyFlattener
            |
            v
         FlattenedHierarchyRow[]
            |
            +--> PostStructuralHierarchyValidator
            |
            +--> PrePublishHierarchyValidator
            |
            v
         HierarchyRepository
            |
            +--> hierarchy_versions        (authoritative)
            |
            +--> base_hierarchy_node       (derived)
            |
            v
         HierarchyViewBuilder
            |
            v
         reporting views                   (derived)
```

## Canonical YAML Contract

Authored YAML is root-level. There is no top-level `hierarchy:` wrapper in the canonical format, and there is no authored lifecycle field.

```yaml
hierarchy_id: MVE_DOE
hierarchy_name: Duration of Equity
version: 2026Q1
owner: ALM Systems Engineering
owner_department: ALM
description: MVE/DOE reporting hierarchy

nodes:
  - account_key: "10000"
    account_name: "Assets"
    children:
      - account_key: "10100"
        account_name: "Investments"
```

Required metadata fields:

- `hierarchy_id`
- `hierarchy_name`
- `version`
- `owner`
- `owner_department`
- `description`
- `nodes`

Required node fields:

- `account_key`
- `account_name`

Optional node fields:

- `children`

Notes:

- The loader still tolerates a wrapped legacy `hierarchy:` payload during transition.
- YAML does not carry `draft`, `published`, or `retired`.
- Publish writes `status = 'published'`.
- Retirement is an explicit persistence operation.

## Persistence Model

### Authoritative Version Table

The authoritative table stores one row per `(hierarchy_id, version)`.

Expected columns:

- `hierarchy_id`
- `hierarchy_name`
- `version`
- `status`
- `description`
- `payload_json`
- `content_hash`
- `node_count`
- `leaf_count`
- `max_depth`
- `owner`
- `owner_department`
- `published_by`
- `published_at`
- `retired_by`
- `retired_at`

Design intent:

- `payload_json` is the canonical persisted hierarchy content.
- `content_hash` is a deterministic SHA-256 hash of canonical payload bytes.
- `node_count`, `leaf_count`, and `max_depth` are convenience summary metrics.
- `status` is currently `published` or `retired`.

### Derived Node Table

The derived node table stores flattened adjacency-list rows for recursive SQL and downstream views.

Expected grain:

- one row per `(hierarchy_id, version, account_key)`

Expected columns:

- `hierarchy_id`
- `version`
- `account_key`
- `account_name`
- `parent_account_key`
- `account_level`
- `node_path`
- `created_date`
- `updated_date`

### Reporting Views

`HierarchyViewBuilder` rebuilds a standard structural view stack from the version table and node table:

- `paths_view`
- `flat_view`
- `dims_view`
- `reporting_view`
- `nodes_dims_view`
- `nodes_reporting_view`

These are generic structural views. Project-specific semantic behavior should be layered downstream.

## Lifecycle Semantics

The engine now follows a simplified lifecycle:

- `published`: active persisted version
- `retired`: no longer active for published consumers

There is no authored `draft` state in YAML. If a hierarchy is still being edited, that state exists only in source control or the working file before publish.

Recommended operating rules:

- do not edit published rows in place
- do not reuse an existing `(hierarchy_id, version)`
- retire a published version explicitly when replacing it
- treat one published version per hierarchy as the normal operating model

## Validation Strategy

The project uses four validation layers. The first three are the normal publish gates.

### 1. Load Issues

Source: [loader.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\loader.py)

Purpose:

- collect tolerant parse problems while still constructing a definition object

Examples:

- malformed root shape
- invalid `nodes` collection type
- invalid `children` collection type
- invalid node object shape

### 2. Pre-Structural Validation

Source: [pre_structural_validator.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\pre_structural_validator.py)

Purpose:

- validate the authored nested hierarchy definition before flattening

Representative checks:

- missing `hierarchy_id`
- missing `hierarchy_name`
- missing `version`
- missing `owner`
- missing `owner_department`
- missing `description`
- missing root nodes
- duplicate `account_key`
- cycle detection
- missing node key or name
- invalid child structure

### 3. Post-Structural Validation

Source: [post_structural_validator.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\post_structural_validator.py)

Purpose:

- validate the flattened artifact before any Spark write

Representative checks:

- no flattened rows produced
- mismatched row `hierarchy_id`
- mismatched row `version`
- duplicate flattened keys
- missing parent row
- invalid `account_level`
- invalid `node_path`
- parent path mismatch

### 4. Pre-Publish Persistence Validation

Source: [pre_publish_validator.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\pre_publish_validator.py)

Purpose:

- validate a candidate publish against persisted state before write

Current checks:

- duplicate persisted version rows for `(hierarchy_id, version)`
- existing persisted version row for `(hierarchy_id, version)`
- existing published sibling for the same `hierarchy_name`
- existing node rows for `(hierarchy_id, version)`
- duplicate persisted node rows by `account_key`

### 5. Post-Publish Audit Validation

Source: [post_publish_validator.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\post_publish_validator.py)

Purpose:

- audit already-persisted data

Current checks:

- duplicate persisted node rows
- missing persisted parents
- multiple published versions for the same `hierarchy_id`

Use this for diagnostics, reconciliation, and monitoring. Do not treat it as the main publish gate.

## Main Modules

### Core Models

- [models.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\models.py): canonical dataclasses for metadata, nodes, hierarchy definitions, flattened rows, authoritative version rows, and validation results
- [errors.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\errors.py): library exception types

### Authoring and Transformation

- [loader.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\loader.py): YAML loading and tolerant parsing
- [pre_structural_validator.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\pre_structural_validator.py): nested hierarchy validation
- [flattener.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\flattener.py): nested tree to adjacency-list rows
- [post_structural_validator.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\post_structural_validator.py): flattened-row validation
- [renderer.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\renderer.py): human-readable tree rendering
- [comparer.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\comparer.py): structured version-to-version diffs
- [exporter.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\exporter.py): export canonical YAML from in-memory objects
- [serializer.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\serializer.py): serialize and deserialize authoritative version rows and compute `content_hash`

### Persistence and Views

- [repository.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\repository.py): explicit Spark schemas plus version/node persistence helpers
- [pre_publish_validator.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\pre_publish_validator.py): persisted-state validation before publish
- [post_publish_validator.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\post_publish_validator.py): persisted-state audit after publish
- [view_builder.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\view_builder.py): rebuild structural reporting views
- [service.py](C:\Users\aarba\pydev\hierarchy_engine\hierarchy_engine\service.py): main orchestration entry point for notebooks and scripts

## Standard Workflow

### Author and Review

1. Create or edit a YAML hierarchy file.
2. Load it with `HierarchyService.load_from_yaml(...)`.
3. Review any `load_issues`.
4. Run pre-structural validation.
5. Run post-structural validation.
6. Optionally render or diff the hierarchy before publish.

### Publish

1. Run pre-publish validation against the target version and node tables.
2. Publish the hierarchy.
3. Rebuild reporting views.
4. Optionally run post-publish audit validation.

### Retire

1. Call `retire_version(...)` for the target `(hierarchy_id, version)`.
2. Rebuild reporting views if downstream consumers rely on `status = 'published'` filters.

## Service Examples

### Load, Validate, and Render

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()
definition = service.load_from_yaml("hierarchy_configs/MVE_DOE.yaml")

print(definition.load_issues)
print(service.get_validation_result(definition).to_text())
print(service.get_post_structural_validation_result(definition).to_text())
print(service.render_tree(definition))
```

### Compare Two Versions

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()
old_definition = service.load_from_yaml("examples/demo_baseline_hierarchy.yaml")
new_definition = service.load_from_yaml("examples/demo_modified_hierarchy.yaml")

print(service.render_diff(old_definition, new_definition))
```

### Create Empty Base Tables

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()

service.create_base_tables(
    spark=spark,
    version_table="catalog.schema.hierarchy_versions",
    node_table="catalog.schema.base_hierarchy_node",
    mode="ignore",
)
```

### Validate a Candidate Publish

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()
definition = service.load_from_yaml("hierarchy_configs/CAP_MKTS.yaml")

result = service.get_pre_publish_validation_result(
    definition=definition,
    spark=spark,
    version_table="catalog.schema.hierarchy_versions",
    node_table="catalog.schema.base_hierarchy_node",
)

print(result.to_text())
```

### Publish to Base Tables

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()
definition = service.load_from_yaml("hierarchy_configs/CAP_MKTS.yaml")

service.publish_to_tables(
    definition=definition,
    spark=spark,
    version_table="catalog.schema.hierarchy_versions",
    node_table="catalog.schema.base_hierarchy_node",
    node_write_mode="append",
    published_by="your.user",
)
```

`publish_to_tables(...)` performs:

1. strict pre-structural validation
2. flattening with system dates
3. strict post-structural validation on those exact rows
4. strict pre-publish validation against persisted state
5. authoritative version-row write
6. derived node-row write

### Publish and Rebuild Reporting Views

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()
definition = service.load_from_yaml("hierarchy_configs/CAP_MKTS.yaml")

service.publish_and_rebuild_reporting_views(
    definition=definition,
    spark=spark,
    version_table="catalog.schema.hierarchy_versions",
    node_table="catalog.schema.base_hierarchy_node",
    paths_view="catalog.schema.v_hierarchy_paths",
    flat_view="catalog.schema.v_hierarchy_flat",
    dims_view="catalog.schema.v_hierarchy_dims",
    reporting_view="catalog.schema.dim_reporting_hierarchy",
    nodes_dims_view="catalog.schema.v_hierarchy_nodes_dims",
    nodes_reporting_view="catalog.schema.dim_reporting_hierarchy_nodes",
    node_write_mode="append",
    published_by="your.user",
)
```

### Retire a Published Version

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()

service.retire_version(
    spark=spark,
    version_table="catalog.schema.hierarchy_versions",
    hierarchy_id="MVE_DOE",
    version="2026Q1",
    retired_by="your.user",
)
```

### Run Post-Publish Audit Validation

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()

audit = service.validate_published_version(
    spark=spark,
    hierarchy_id="MVE_DOE",
    version="2026Q1",
    node_table="catalog.schema.base_hierarchy_node",
    version_table="catalog.schema.hierarchy_versions",
)

print(audit.to_text())
```

Use the strict form if the notebook or job should fail on audit findings:

```python
service.validate_published_version_strict(
    spark=spark,
    hierarchy_id="MVE_DOE",
    version="2026Q1",
    node_table="catalog.schema.base_hierarchy_node",
    version_table="catalog.schema.hierarchy_versions",
)
```

## First-Time Setup

### 1. Create or Refresh the Conda Environment

```powershell
conda env create -f environment.yml
```

If the environment already exists:

```powershell
conda env update -n GeneralEnv -f environment.yml --prune
```

### 2. Activate the Environment

```powershell
conda activate GeneralEnv
```

### 3. Verify Local Dependencies

At minimum, local development expects:

- Python
- `pyyaml`
- `pytest`
- `pytest-cov`
- `pyspark`

### 4. Run the Test Suite

```powershell
python -m pytest tests -q -p no:cacheprovider
```

### 5. Confirm Spark and Java if Running Local Spark

```powershell
python -c "import pyspark; print(pyspark.__version__)"
java -version
```

For Databricks runtime usage, publish and audit workflows should run in a notebook or job cluster with Spark available.

## Testing Strategy

Use the following split:

- Python code changes: run `pytest`
- YAML hierarchy changes: run the hierarchy validators against the changed artifact
- environment bring-up: run a small Spark-backed smoke test
- normal publish jobs: run runtime validators, not the full unit suite

Fast local suite:

```powershell
python -m pytest tests -q -p no:cacheprovider
```

Coverage run:

```powershell
python -m pytest tests --cov=hierarchy_engine --cov-report=term-missing -p no:cacheprovider
```

## Current Limitations

- Publish remains append-oriented rather than fully transactional.
- Retirement currently updates the authoritative version row only; derived node rows remain historical.
- The loader still tolerates the legacy wrapped YAML shape during transition.
- The notebook [hierarchy_engine_engineer_demo.ipynb](C:\Users\aarba\pydev\hierarchy_engine\examples\hierarchy_engine_engineer_demo.ipynb) has not yet been rewritten to match the refactored architecture.
- There is not yet a CLI entry point or API layer.

## Summary

The current `hierarchy_engine` model is:

1. author a root-level YAML hierarchy
2. validate the nested structure
3. validate the flattened structure
4. validate against persisted state
5. persist one authoritative version row with canonical payload
6. persist derived node rows
7. rebuild derived reporting views
8. optionally audit the persisted result

That separation of authored payload, authoritative persistence, and derived reporting artifacts is the main architectural idea of the project now.
