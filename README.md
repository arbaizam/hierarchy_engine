---

# Hierarchy Engine

A standalone, YAML-driven hierarchy authoring, validation, flattening, and publishing engine.

This project is designed to manage **versioned reporting hierarchies** independently of any downstream business-rule engine. It provides a clean foundation for:

* hierarchy authoring in YAML
* structural validation
* recursive flattening to adjacency-list rows
* publishing to Databricks / Spark tables
* deriving reporting-ready hierarchy dimensions
* future UI integration

---

# Why this project exists

Traditional hierarchy maintenance often lives in:

* SQL `INSERT` statements
* spreadsheets
* one-off ETL scripts
* hard-coded reporting logic

That approach is difficult to maintain, difficult to review, and hard to scale.

This project treats hierarchies as **first-class metadata artifacts**.

Instead of authoring a hierarchy as a flat database script, users define it as a **nested YAML tree**, which is easier to:

* read
* review
* version
* validate
* publish

---

# Core idea

Author a hierarchy like this:

```yaml
hierarchy:
  hierarchy_id: "MVE_DOE"
  hierarchy_name: "Duration of Equity"
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
          children:
            - account_key: "10110"
              account_name: "Asset Swaps"
            - account_key: "10120"
              account_name: "Debentures - Fix."
```

Then the engine:

1. loads the YAML
2. validates it
3. recursively flattens it
4. publishes it into base hierarchy tables
5. supports downstream derived views for reporting

---

# High-level architecture

```text
YAML hierarchy definition
        |
        v
HierarchyService
  - load
  - validate
  - flatten
  - publish
        |
        v
base tables
  - hierarchy_registry
  - hierarchy_version
  - base_hierarchy_node
        |
        v
derived views
  - v_hierarchy_paths
  - v_hierarchy_flat
  - v_hierarchy_dims
  - dim_reporting_hierarchy
        |
        v
Power BI / downstream consumers
```

---

# Project scope

This project is intentionally focused on **hierarchy management only**.

It does **not** include:

* business-rule evaluation
* fact classification
* simulation of rule outputs
* mapping bridge generation

Those are expected to live in a separate rule-engine project and integrate later.

---

# Features

## Implemented / intended in v1

* YAML-based hierarchy authoring
* nested tree structure for readability
* dataclass-based domain model
* recursive flattening to adjacency-list rows
* structural validation
* Spark/DataFrame publishing
* compatibility with Databricks workflows
* derived hierarchy view rebuild pattern

## Planned / likely future features

* CLI entrypoint
* version diff utilities
* tree rendering utilities
* YAML export from persisted tables
* replace/upsert publish semantics
* UI integration
* API layer

---

# Repository structure

```text
hierarchy_engine/
├─ hierarchy_engine/
│  ├─ __init__.py
│  ├─ errors.py
│  ├─ models.py
│  ├─ loader.py
│  ├─ flattener.py
│  ├─ validator.py
│  ├─ service.py
│  └─ repository.py
├─ examples/
│  └─ MVE_DOE_2026Q1.yaml
├─ tests/
│  ├─ test_loader.py
│  ├─ test_flattener.py
│  ├─ test_validator.py
│  └─ test_service.py
└─ README.md
```

---

# Core concepts

# 1. Hierarchy metadata

Every hierarchy has top-level metadata such as:

* `hierarchy_id`
* `hierarchy_name`
* `version_id`
* `version_name`
* `version_status`
* `effective_start_date`
* `effective_end_date`

This metadata is stored separately from the node tree and is used to support **versioned hierarchies**.

---

# 2. Nested hierarchy tree

Hierarchies are authored as a nested tree using `children`.

Example:

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

This is the preferred authoring format because it is far easier for humans to understand than a flat adjacency list.

---

# 3. Flattened adjacency-list output

Internally, the nested YAML tree is flattened into rows like:

```text
hierarchy_id
version_id
account_key
account_name
parent_account_key
account_level
node_path
created_date
updated_date
```

This is the persisted format used by downstream SQL views.

---

# 4. Recursive flattening

The tree is flattened recursively.

For each node:

1. emit the current node row
2. set the current node as parent for all children
3. recurse into children
4. build the path as traversal continues

This approach naturally supports:

* ragged hierarchies
* arbitrary depth
* clean YAML authoring

---

# YAML schema

A hierarchy YAML file should look like this:

```yaml
hierarchy:
  hierarchy_id: "MVE_DOE"
  hierarchy_name: "Duration of Equity"
  hierarchy_description: "Example DOE reporting hierarchy"
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
          children:
            - account_key: "10110"
              account_name: "Asset Swaps"
            - account_key: "10120"
              account_name: "Debentures - Fix."
```

## Required top-level fields

* `hierarchy_id`
* `hierarchy_name`
* `version_id`
* `version_name`
* `version_status`
* `effective_start_date`
* `nodes`

## Required node fields

* `account_key`
* `account_name`

## Optional node fields in v1

* `children`

---

# Example workflow

## Load and validate a hierarchy

```python
from hierarchy_engine.service import HierarchyService

service = HierarchyService()

definition = service.load_from_yaml("examples/MVE_DOE_2026Q1.yaml")
service.validate_definition(definition)
```

## Flatten a hierarchy into rows

```python
rows = service.flatten_definition(definition)

for row in rows:
    print(row)
```

## Convert to Spark DataFrame

```python
df = service.to_dataframe(definition, spark)
display(df)
```

## Publish to tables

```python
service.publish_to_tables(
    definition=definition,
    spark=spark,
    registry_table="alme_dev_silver.hierarchy_engine.hierarchy_registry",
    version_table="alme_dev_silver.hierarchy_engine.hierarchy_version",
    node_table="alme_dev_silver.hierarchy_engine.base_hierarchy_node",
    node_write_mode="append",
)
```

---

# Public API

The primary public entry point is:

```python
HierarchyService
```

## Main methods

### `load_from_yaml(path)`

Loads a hierarchy YAML file into a `HierarchyDefinition`.

### `validate_definition(definition)`

Validates metadata, structure, duplicates, and cycles.

### `flatten_definition(definition)`

Flattens the nested hierarchy tree into adjacency-list rows.

### `flatten_to_dicts(definition)`

Returns flattened rows as dictionaries.

### `to_dataframe(definition, spark)`

Converts flattened rows to a Spark DataFrame.

### `publish_to_tables(...)`

Publishes registry/version/node rows to target Spark tables.

---

# Domain model

The engine uses a small set of dataclasses.

## `HierarchyMetadata`

Stores hierarchy and version metadata.

## `HierarchyNode`

Represents one node in the nested tree.

## `HierarchyDefinition`

Represents the full hierarchy document.

## `FlattenedHierarchyRow`

Represents one emitted adjacency-list row.

---

# Validation rules

The validator checks at least the following:

## Metadata checks

* `hierarchy_id` must not be empty
* `version_id` must not be empty
* `version_status` must be valid
* `effective_end_date` cannot be before `effective_start_date`

## Structural checks

* hierarchy must contain at least one root node
* `account_key` values must be unique
* cycles are not allowed

## Why cycle validation exists

Even though a YAML tree normally should not contain cycles, cycle detection is still valuable because:

* programmatic hierarchy generation may introduce them
* object reuse bugs can create them
* future UI integrations may introduce malformed structures

---

# Databricks integration

This engine is designed to publish into three base tables:

## `hierarchy_registry`

```text
One row per hierarchy
```

## `hierarchy_version`

```text
One row per hierarchy version
```

## `base_hierarchy_node`

```text
One row per flattened node
```

Then downstream Databricks SQL / Python notebooks can build:

* `v_hierarchy_paths`
* `v_hierarchy_flat`
* `v_hierarchy_dims`
* `dim_reporting_hierarchy`

---

# Recommended Databricks workflow

## Step 1

Publish YAML to base tables using `HierarchyService`.

## Step 2

Run the rebuild notebook to generate derived views.

## Step 3

Run validation views against the published tables.

## Step 4

Inspect `dim_reporting_hierarchy`.

## Step 5

Connect downstream reporting or rule-engine systems.

---

# Validation views recommended in Databricks

After publishing, create and check views like:

* duplicate hierarchy IDs
* duplicate versions
* multiple current versions
* overlapping version dates
* duplicate node keys
* missing parents

Expected result:

```text
all validation queries return 0 rows
```

---

# Why the project is class-based

The engine is intentionally designed around classes so it can later support:

* dependency injection
* repository abstraction
* alternate publishing backends
* API wrapping
* packaging
* UI integration

### Main classes

* `HierarchyConfigLoader`
* `HierarchyFlattener`
* `HierarchyValidator`
* `HierarchyRepository`
* `HierarchyService`

This gives a clean separation of responsibilities.

---

# Design principles

## 1. YAML is for authoring

Humans should author hierarchies in a readable nested format.

## 2. Adjacency list is for persistence

Tables should store a flattened relational representation.

## 3. Validation happens before publish

Bad hierarchies should never land in persistence.

## 4. Recursion belongs in the flattener

The service orchestrates, but tree traversal lives in the flattener.

## 5. Standalone first

This project should be useful by itself before being integrated with a rule engine.

---

# Example whiteboard explanation

```text
YAML hierarchy
    |
    v
HierarchyService
    |
    +--> Loader
    +--> Validator
    +--> Flattener
    +--> Repository
    |
    v
base tables
    |
    v
derived hierarchy views
    |
    v
reporting dimension
```

---

# Development setup

## Python dependencies

At minimum:

* `pyyaml`
* `pyspark` for Spark/DataFrame publishing workflows

Example:

```bash
pip install pyyaml pyspark
```

---

# Running tests

Example:

```bash
pytest tests/
```

Suggested test focus:

* YAML loading
* recursive flattening
* duplicate key validation
* cycle detection
* service orchestration

---

# Recommended development flow

## During early development

Use:

```python
node_write_mode="overwrite"
```

or clear target rows for the same hierarchy/version before re-publishing.

This helps avoid duplicate dev data while iterating on the YAML shape.

## In more mature workflows

Move toward:

* replace-version semantics
* upsert registry/version logic
* explicit publish workflows

---

# Future enhancements

Likely high-value next additions:

## 1. Tree renderer

Indented text output of a hierarchy for debugging and design review.

## 2. Version diff utility

Compare two hierarchy versions and show added/removed/moved nodes.

## 3. YAML export utility

Reconstruct YAML from persisted base tables.

## 4. CLI entrypoint

Example:

```bash
hierarchy-engine publish examples/MVE_DOE_2026Q1.yaml
```

## 5. API layer

Expose hierarchy load/validate/publish through a service.

## 6. UI integration

Use this engine as the backend for a visual hierarchy editor.

---

# Current limitations

This initial version is intentionally conservative.

## It does not yet include

* upsert semantics
* row-level replace logic by hierarchy/version
* advanced version diffing
* YAML schema enforcement via JSON Schema / Pydantic
* control-plane lifecycle governance
* UI endpoints

These are all reasonable next steps, but not required for the first working version.

---

# Relationship to the future rule engine

This project is intended to remain useful **even if no rule engine is attached**.

Later integration will look like this:

```text
Hierarchy Engine
    provides valid hierarchy leaves
        +
Rule Engine
    maps facts to those leaves
        =
Full classification platform
```

That separation is intentional and valuable.

---

# Quick start

## 1. Create a YAML hierarchy

Place it in `examples/` or a real hierarchy config folder.

## 2. Load it

```python
service = HierarchyService()
definition = service.load_from_yaml("examples/MVE_DOE_2026Q1.yaml")
```

## 3. Validate it

```python
service.validate_definition(definition)
```

## 4. Preview flattened rows

```python
rows = service.flatten_definition(definition)
```

## 5. Publish it to Spark tables

```python
service.publish_to_tables(...)
```

## 6. Rebuild derived hierarchy views in Databricks

---

# Summary

This project provides a clean, standalone way to manage versioned reporting hierarchies using YAML and Spark-compatible publishing workflows.

It gives you:

* human-readable hierarchy authoring
* recursive tree flattening
* structural validation
* Databricks-compatible publishing
* a strong foundation for future packaging and UI integration

---

# License / internal usage

Add your internal or project-specific license language here.

---
