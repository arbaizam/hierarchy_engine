# Hierarchy Engine

`hierarchy_engine` is a Databricks- and Spark-oriented library for authoring, validating, flattening, comparing, rendering, exporting, and publishing versioned business hierarchies defined in YAML.

The project treats a hierarchy as a managed metadata artifact rather than a collection of ad hoc SQL inserts or spreadsheet-maintained mappings. The authoring format is a nested YAML tree because that is easier for humans to review. The persisted format is an adjacency-list table because that is easier for Spark and downstream dimensional modeling.

This README documents:

- project purpose and architecture
- the role of every Python module
- hierarchy authoring format
- the end-to-end workflow from YAML to published tables
- all validation stages and their checks
- first-time setup
- how to add a new hierarchy or a new version
- testing guidance

## Core Principles

- YAML is the source of truth for authored hierarchies.
- Validation is layered. Different failures should be caught at the earliest sensible stage.
- Publishing should be blocked before any write if a conflict or structural failure is known.
- Spark tables store a flattened representation, not the nested authoring structure.
- Post-publish validation is optional audit/diagnostic logic, not the primary publish safeguard.

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
    +--> PreStructuralValidator
    |
    +--> HierarchyFlattener
            |
            v
         FlattenedHierarchyRow[]
            |
            +--> PostStructuralValidator
            |
            +--> PrePublishValidator (against registry/version/node tables)
            |
            v
         HierarchyRepository writes to Spark tables
            |
            v
         Published Spark tables
            |
            +--> optional PostPublishValidator audit
```

## Repository Layout

```text
hierarchy_engine/
├─ hierarchy_engine/
│  ├─ __init__.py
│  ├─ comparer.py
│  ├─ errors.py
│  ├─ exporter.py
│  ├─ flattener.py
│  ├─ loader.py
│  ├─ models.py
│  ├─ post_publish_validator.py
│  ├─ post_structural_validator.py
│  ├─ pre_publish_validator.py
│  ├─ pre_structural_validator.py
│  ├─ renderer.py
│  ├─ repository.py
│  └─ service.py
├─ hierarchy_configs/
│  ├─ CAP_MKTS.yaml
│  └─ MVE_DOE.yaml
├─ tests/
├─ environment.yml
└─ README.md
```

## Python Module Guide

This section explains the responsibility of every `.py` module under [`hierarchy_engine/`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine).

### [`__init__.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/__init__.py)

Package-level description. This module establishes the package as a standalone hierarchy management library. It does not contain operational logic.

### [`models.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/models.py)

Defines the core dataclasses used across the project:

- `HierarchyMetadata`
- `HierarchyNode`
- `HierarchyDefinition`
- `FlattenedHierarchyRow`
- `ValidationIssue`
- `ValidationResult`

This module is the shared vocabulary of the project. Every other module either produces, consumes, or transforms these objects.

### [`errors.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/errors.py)

Defines custom exception types:

- `HierarchyEngineError`
- `HierarchyParseError`
- `HierarchyValidationError`
- `HierarchyPublishError`

These exceptions separate parse failures, validation failures, and publish failures from generic Python exceptions.

### [`loader.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/loader.py)

Loads YAML files into `HierarchyDefinition`.

Responsibilities:

- read YAML from disk
- validate top-level file shape
- parse tolerant metadata and node objects
- collect load issues without immediately failing for every malformed field

Important distinction:

- `loader.py` is not the main structural validator
- it performs tolerant parsing and shape checks
- formal hierarchy validation happens later

### [`pre_structural_validator.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/pre_structural_validator.py)

Validates the nested in-memory hierarchy definition before flattening.

This is the first blocking validation stage. It answers:

- is the authored hierarchy structurally valid as a tree-like object?
- are the required metadata fields present?
- do any duplicate node keys or cycles exist?

This validator works against `HierarchyDefinition` and `HierarchyNode`, not Spark tables and not flattened rows.

### [`flattener.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/flattener.py)

Recursively converts the nested hierarchy tree into adjacency-list rows.

Responsibilities:

- visit each node in the hierarchy
- derive parent relationships
- derive `account_level`
- derive `node_path`
- emit `FlattenedHierarchyRow` objects

This module is fundamental because the flattened output is the publish artifact persisted to Spark.

### [`post_structural_validator.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/post_structural_validator.py)

Validates the flattened row artifact after flattening and before any Spark write.

This is the second blocking validation stage. It answers:

- does the flattened output still represent a coherent hierarchy?
- are the parent paths, levels, and node paths internally consistent?
- are there duplicate flattened rows or missing parents?

This validator works against `FlattenedHierarchyRow[]`.

### [`pre_publish_validator.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/pre_publish_validator.py)

Validates the candidate publish against persisted Spark tables before any write occurs.

This is the third blocking validation stage. It answers:

- does the registry already contain conflicting identity metadata?
- does the version already exist?
- do node rows already exist for the same version?
- would publish create current-version conflicts?
- would publish create overlapping effective windows?

This validator compares the candidate hierarchy against existing Spark tables.

### [`post_publish_validator.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/post_publish_validator.py)

Validates already-persisted Spark data after publish.

This module is intentionally optional and read-only. It is best used for:

- audit jobs
- diagnostics
- reconciliation
- manual table-edit detection
- legacy cleanup

It is not the primary publish safeguard. The project should normally prevent bad writes before publish.

### [`repository.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/repository.py)

Contains Spark persistence helpers and explicit schemas.

Responsibilities:

- define registry, version, and node schemas
- convert row dictionaries to Spark DataFrames
- write registry rows
- write version rows
- write node rows
- provide small table existence / registry lookup helpers

This module is intentionally narrow. It is not supposed to contain workflow orchestration or business validation.

### [`service.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/service.py)

The main public orchestration layer and the primary entry point for notebooks and scripts.

Responsibilities:

- load YAML
- run validation
- flatten hierarchies
- convert to Spark DataFrames
- publish to tables
- render trees
- compare versions
- export to YAML

If you are using the library from Databricks notebooks, this is usually the class you should instantiate first.

### [`renderer.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/renderer.py)

Renders a hierarchy as a human-readable indented text tree.

Useful for:

- debugging
- reviews
- walkthroughs with business users
- pull request context

### [`comparer.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/comparer.py)

Compares two hierarchy definitions and returns a structured diff.

Current supported change types:

- added nodes
- removed nodes
- renamed nodes
- reparented nodes

This module is useful for change review between hierarchy versions.

### [`exporter.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/exporter.py)

Exports an in-memory hierarchy definition back to YAML.

Useful for:

- round-tripping
- canonical serialization
- future UI save workflows
- reconstructing authored YAML from in-memory objects

## Data Model

### Hierarchy Metadata

Each hierarchy version contains top-level metadata:

- `hierarchy_id`
- `hierarchy_name`
- `hierarchy_description`
- `owner_team`
- `business_domain`
- `version_id`
- `version_name`
- `version_status`
- `effective_start_date`
- `effective_end_date`

### Version Status Lifecycle

The current lifecycle statuses are:

- `draft`
  Meaning: authored but not published
- `published`
  Meaning: active published version
- `retired`
  Meaning: previously published version that should no longer be treated as active

`validated` is not a persisted lifecycle state. Validation is an operation, not a durable status.

### Nested Authoring Model

Hierarchies are authored as a tree:

```yaml
nodes:
  - account_key: "10000"
    account_name: "Assets"
    children:
      - account_key: "10100"
        account_name: "Investments"
        children:
          - account_key: "10110"
            account_name: "Asset Swaps"
```

### Flattened Persistence Model

The flattened node rows include:

- `hierarchy_id`
- `version_id`
- `account_key`
- `account_name`
- `parent_account_key`
- `account_level`
- `node_path`
- `created_date`
- `updated_date`

## Spark Tables

The project is designed around three core Spark tables.

### Registry Table

Logical purpose: stable hierarchy identity.

Expected grain:

- one row per `hierarchy_id`

Typical columns:

- `hierarchy_id`
- `hierarchy_name`
- `hierarchy_description`
- `owner_team`
- `business_domain`
- `created_date`
- `updated_date`

### Version Table

Logical purpose: version metadata for a hierarchy.

Expected grain:

- one row per `(hierarchy_id, version_id)`

Typical columns:

- `hierarchy_id`
- `version_id`
- `version_name`
- `version_status`
- `effective_start_date`
- `effective_end_date`
- `is_current`
- `change_description`
- `created_date`
- `created_by`
- `published_date`
- `published_by`

### Node Table

Logical purpose: flattened adjacency-list hierarchy rows.

Expected grain:

- one row per `(hierarchy_id, version_id, account_key)`

Typical columns:

- `hierarchy_id`
- `version_id`
- `account_key`
- `account_name`
- `parent_account_key`
- `account_level`
- `node_path`
- `created_date`
- `updated_date`

## Validation Strategy

The project uses four validation concepts, but only the first three are normal publish gates.

### 1. Load Issues

Source: [`loader.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/loader.py)

Purpose:

- collect tolerant parse problems while still building a definition object

Examples:

- malformed date strings
- invalid `nodes` collection type
- invalid `children` collection type
- invalid node object shape

### 2. Pre-Structural Validation

Source: [`pre_structural_validator.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/pre_structural_validator.py)

Purpose:

- validate the authored nested hierarchy definition before flattening

Checks currently include:

- missing `hierarchy_id`
- missing `hierarchy_name`
- missing `hierarchy_description`
- missing `owner_team`
- missing `business_domain`
- missing `version_id`
- missing `version_name`
- invalid `version_status`
- missing `effective_start_date`
- `effective_end_date` before `effective_start_date`
- missing root nodes
- duplicate `account_key`
- cycle detection
- missing `account_key`
- missing `account_name`
- invalid `children` collection
- invalid child node object

### 3. Post-Structural Validation

Source: [`post_structural_validator.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/post_structural_validator.py)

Purpose:

- validate the flattened publish artifact before any Spark write

Checks currently include:

- no flattened rows produced
- mismatched row `hierarchy_id`
- mismatched row `version_id`
- duplicate flattened `account_key`
- missing flattened `account_key`
- missing flattened `account_name`
- invalid `account_level`
- missing `node_path`
- missing root rows
- invalid root `account_level`
- self-parenting
- missing flattened parent row
- invalid `node_path` segments
- terminal `node_path` key mismatch
- repeated keys in `node_path`
- `account_level` / `node_path` depth mismatch
- invalid root `node_path`
- parent path mismatch
- parent level mismatch

### 4. Pre-Publish Persistence Validation

Source: [`pre_publish_validator.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/pre_publish_validator.py)

Purpose:

- validate the candidate publish against existing persisted state

Checks currently include:

- duplicate registry rows for a `hierarchy_id`
- conflicting registry `hierarchy_name`
- conflicting registry `hierarchy_description`
- conflicting registry `owner_team`
- conflicting registry `business_domain`
- duplicate version rows for `(hierarchy_id, version_id)`
- existing version row for `(hierarchy_id, version_id)`
- existing node rows for `(hierarchy_id, version_id)`
- duplicate persisted node rows by `account_key`
- existing current published version for the same hierarchy
- overlapping effective windows across versions

### 5. Post-Publish Validation

Source: [`post_publish_validator.py`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_engine/post_publish_validator.py)

Purpose:

- audit persisted Spark state after publish

Checks currently include:

- duplicate persisted node rows
- missing persisted parents
- multiple current versions
- overlapping effective windows

Recommended use:

- as a diagnostics or audit step
- not as the main gate for standard publish workflows

## Recommended Workflow

### Standard Authoring-to-Publish Workflow

1. Create or edit a YAML hierarchy file.
2. Load the hierarchy with `HierarchyService.load_from_yaml(...)`.
3. Review `definition.load_issues` if any exist.
4. Run pre-structural validation.
5. Run post-structural validation.
6. Run pre-publish persistence validation against the target tables.
7. Publish only if all blocking validation stages pass.
8. Optionally run post-publish validation as an audit.

### Operational Interpretation

- During authoring: use pre-structural and post-structural validation repeatedly.
- During publish: always run pre-publish persistence validation immediately before write.
- After publish: optionally run post-publish audit checks if you want defense-in-depth or monitoring.

## First-Time Setup

### 1. Create the Conda Environment

This repository already includes an [`environment.yml`](/c:/Users/aarba/pydev/hierarchy_engine/environment.yml).

To create or refresh the environment:

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

At minimum, the project expects:

- Python
- `pyyaml`
- `pytest`
- `pytest-cov`
- `pyspark`

### 4. Run the Test Suite

```powershell
pytest tests -q -p no:cacheprovider
```

The `-p no:cacheprovider` flag is useful in restricted environments where pytest cache directories may be problematic.

### 5. Confirm Spark Availability

For local development, verify `pyspark` imports cleanly:

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

### 6. Confirm Java Availability for Local Spark

If you want to run local Spark-backed integration tests or ad hoc local Spark sessions outside Databricks, Java must also be available.

Check it with:

```powershell
java -version
```

If `java` is not available on `PATH`, local `pyspark` session startup may hang or fail even though the Python package is installed correctly.

For Databricks runtime usage, the actual publish and audit workflows should run in a notebook or job cluster with Spark available.

## YAML Authoring Format

### Full Example

```yaml
hierarchy:
  hierarchy_id: "MVE_DOE"
  hierarchy_name: "Duration of Equity"
  hierarchy_description: "MVE/DOE reporting hierarchy."
  owner_team: "ALM Systems Engineering"
  business_domain: "ALM"
  version_id: "2026Q1"
  version_name: "2026 Q1 Initial"
  version_status: "draft"
  effective_start_date: "2026-01-01"
  effective_end_date: null

  nodes:
    - account_key: "10000"
      account_name: "Assets"
      children:
        - account_key: "10100"
          account_name: "Investments"
```

### Required Metadata Fields

- `hierarchy_id`
- `hierarchy_name`
- `hierarchy_description`
- `owner_team`
- `business_domain`
- `version_id`
- `version_name`
- `version_status`
- `effective_start_date`

### Required Node Fields

- `account_key`
- `account_name`

### Optional Node Fields

- `children`

## Procedure: Add a New Hierarchy

Use this when the hierarchy does not yet exist in the registry table.

1. Create a new YAML file in [`hierarchy_configs/`](/c:/Users/aarba/pydev/hierarchy_engine/hierarchy_configs).
2. Set a new `hierarchy_id`.
3. Fill all required metadata fields.
4. Start with `version_status: "draft"`.
5. Author the node tree.
6. Load and validate locally.
7. Review the rendered tree and flattened output.
8. Run pre-publish validation against the target tables.
9. Publish.

Recommended local review steps:

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()
definition = service.load_from_yaml("hierarchy_configs/MY_NEW_HIERARCHY.yaml")

print(definition.load_issues)
print(service.get_validation_result(definition).to_text())
print(service.render_tree(definition))
```

## Procedure: Add a New Version of an Existing Hierarchy

Use this when the hierarchy already exists in the registry but you are introducing a new version.

1. Copy the prior YAML or export a baseline definition.
2. Keep the same `hierarchy_id`.
3. Set a new `version_id`.
4. Set a new `version_name`.
5. Update dates and node structure as needed.
6. Start as `draft`.
7. Compare the old and new definitions.
8. Run the full validation chain.
9. Publish only if no persistence conflicts exist.

Comparison example:

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()
old_definition = service.load_from_yaml("hierarchy_configs/OLD.yaml")
new_definition = service.load_from_yaml("hierarchy_configs/NEW.yaml")

print(service.render_diff(old_definition, new_definition))
```

## Procedure: Publish a Hierarchy

This is the standard publish path.

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()

definition = service.load_from_yaml("hierarchy_configs/CAP_MKTS.yaml")

service.publish_to_tables(
    definition=definition,
    spark=spark,
    registry_table="catalog.schema.hierarchy_registry",
    version_table="catalog.schema.hierarchy_version",
    node_table="catalog.schema.base_hierarchy_node",
    node_write_mode="append",
    created_by="your.user",
    published_by="your.user" if definition.metadata.version_status == "published" else None,
    change_description="Initial publish of 2026Q1 hierarchy",
)
```

What `publish_to_tables(...)` does:

1. run pre-structural validation
2. run post-structural validation
3. run pre-publish persistence validation
4. flatten rows with system dates
5. write the registry row if the hierarchy does not already exist
6. write the version row
7. write the node rows

## Procedure: Run Post-Publish Audit Validation

Use this when you want to inspect persisted state after publish.

```python
result = service.validate_published_version(
    spark=spark,
    hierarchy_id="MVE_DOE",
    version_id="2026Q1",
    node_table="catalog.schema.base_hierarchy_node",
    version_table="catalog.schema.hierarchy_version",
)

print(result.to_text())
```

Use the strict form if you want the notebook/job to fail on detected audit issues:

```python
service.validate_published_version_strict(
    spark=spark,
    hierarchy_id="MVE_DOE",
    version_id="2026Q1",
    node_table="catalog.schema.base_hierarchy_node",
    version_table="catalog.schema.hierarchy_version",
)
```

## Typical Notebook Workflow

For Databricks notebooks, a practical workflow is:

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()
definition = service.load_from_yaml("/Workspace/Repos/.../hierarchy_configs/CAP_MKTS.yaml")

print("Load issues:")
for issue in definition.load_issues:
    print(issue)

print(service.get_validation_result(definition).to_text())
print(service.get_post_structural_validation_result(definition).to_text())
print(service.render_tree(definition))

rows = service.flatten_definition(definition)
display(service.to_dataframe(definition, spark))
```

Then, when ready:

```python
service.publish_to_tables(
    definition=definition,
    spark=spark,
    registry_table="catalog.schema.hierarchy_registry",
    version_table="catalog.schema.hierarchy_version",
    node_table="catalog.schema.base_hierarchy_node",
    node_write_mode="append",
)
```

## Testing

The repository includes unit tests under [`tests/`](/c:/Users/aarba/pydev/hierarchy_engine/tests).

Current suite coverage includes:

- loader behavior
- flattener behavior
- pre-structural validation
- post-structural validation
- pre-publish persistence validation
- repository behavior
- service orchestration
- comparer, exporter, and renderer utilities

Run tests with:

```powershell
pytest tests -q -p no:cacheprovider
```

For a coverage report:

```powershell
pytest tests --cov=hierarchy_engine --cov-report=term-missing -p no:cacheprovider
```

## Migration Checklist

Before manually migrating this project into a work environment, verify the following:

1. The target environment includes Python, `pyyaml`, `pytest`, `pytest-cov`, and `pyspark`.
2. The target runtime has Spark available at execution time.
3. If local non-Databricks Spark testing is expected, Java is installed and visible on `PATH`.
4. The three target Spark tables and their expected grains are understood:
   `registry` = one row per `hierarchy_id`
   `version` = one row per `(hierarchy_id, version_id)`
   `node` = one row per `(hierarchy_id, version_id, account_key)`
5. The `examples/` folder is migrated along with the library so the engineer demo notebook can be used for onboarding and smoke testing.
6. The `tests/` folder is migrated if the work environment supports running unit tests.
7. The first migrated smoke test should be:
   load a demo YAML
   run the three blocking validation layers
   publish to demo tables
   run post-publish audit validation
8. Any temporary local cache or scratch files should be excluded from migration.

## Recommended Team Conventions

- Treat YAML as the reviewed source artifact.
- Do not write directly to the Spark tables except through the service workflow.
- Use `draft` during authoring and promotion workflows.
- Use `published` only when the version is intended to be active.
- Use `retired` when deactivating a prior published version.
- Keep one file per hierarchy version in source control.
- Use comparison and rendering utilities during review.
- Use post-publish validation as an audit, not as a substitute for pre-write gating.

## Current Limitations

- Publish is still append-oriented rather than fully transactional.
- The code assumes controlled write access to the target tables.
- There is not yet a CLI entry point.
- There is not yet an API layer or UI backend.
- There is not yet a formal migration or retirement orchestration workflow beyond the version metadata conventions.

## Summary

`hierarchy_engine` is designed to make hierarchies explicit, versioned, validated, and publishable in a Spark-native way.

The intended lifecycle is:

1. author hierarchy YAML
2. load and inspect
3. validate the nested structure
4. validate the flattened structure
5. validate against existing persisted state
6. publish
7. optionally audit the persisted result

That separation of concerns is the main architectural idea of the project, and the modules are organized directly around it.
