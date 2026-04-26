# Hierarchy Engine Refactor Spec

## Purpose

This document defines the target-state refactor for `hierarchy_engine` so the
project aligns more closely with the current `rules_engine` workflow.

The central change is architectural:

- authored YAML becomes a root-level canonical payload
- persisted lifecycle is owned by the repository and service layer
- one authoritative version row stores the full hierarchy payload
- flattened node tables and reporting views become derived artifacts

This document is the contract for the refactor. Implementation should follow
this target state rather than preserve legacy shapes where they conflict.

## Goals

- Remove the top-level `hierarchy:` wrapper from authored config files.
- Align authored metadata names with the rules-engine style while preserving
  `hierarchy_id` and `hierarchy_name`.
- Remove authored `draft` semantics entirely.
- Persist one authoritative row per `(hierarchy_id, version)`.
- Persist the canonical hierarchy payload as `payload_json`.
- Treat flattened node tables and reporting views as rebuildable derived state.
- Simplify the current registry/version split where it no longer adds value.

## Non-Goals

- Preserve backward compatibility for old YAML indefinitely.
- Preserve the current three-table publish model as the primary architecture.
- Keep effective-date semantics.
- Keep lifecycle status in authored YAML.

## Canonical Authored YAML

The canonical authoring format is a root-level mapping:

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

### Canonical top-level fields

- `hierarchy_id`
- `hierarchy_name`
- `version`
- `owner`
- `owner_department`
- `description`
- `nodes`

### Explicitly removed from authored YAML

- top-level `hierarchy`
- `version_id`
- `version_name`
- `version_status`
- `effective_start_date`
- `effective_end_date`
- `owner_team`
- `business_domain`

## In-Memory Model

`HierarchyMetadata` should be reduced to:

- `hierarchy_id: str`
- `hierarchy_name: str`
- `version: str`
- `owner: str`
- `owner_department: str`
- `description: str = ""`

`HierarchyDefinition` remains:

- `metadata: HierarchyMetadata`
- `nodes: list[HierarchyNode]`
- `load_issues: list[ValidationIssue]`

`HierarchyNode` remains unchanged:

- `account_key`
- `account_name`
- `children`

`FlattenedHierarchyRow` should continue to represent derived adjacency-list
rows. It should keep:

- `hierarchy_id`
- `version`
- `account_key`
- `account_name`
- `parent_account_key`
- `account_level`
- `node_path`
- `created_date`
- `updated_date`

The field currently named `version_id` in flattened rows should be renamed to
`version` to match the canonical metadata model.

## Lifecycle Model

Lifecycle is not authored. Lifecycle is persisted.

Supported persisted statuses:

- `published`
- `retired`

There is no persisted `draft` state and no authored `draft` state.

### Service operations

The service layer should expose two explicit lifecycle operations:

- `publish_to_tables(...)`
- `retire_version(...)`

`publish_to_tables(...)` should:

1. load or accept a compiled hierarchy definition
2. validate the authored structure
3. normalize/serialize the canonical payload
4. validate publish-time persistence constraints
5. persist one authoritative `hierarchy_versions` row with status `published`
6. rebuild or append derived node rows for that version

`retire_version(...)` should:

1. locate one persisted version row
2. update `status` to `retired`
3. stamp `retired_by` and `retired_at`
4. make the version unavailable to published-only reporting rebuilds

## Persistence Model

## Authoritative table: `hierarchy_versions`

The authoritative persisted table should be one row per
`(hierarchy_id, version)`.

Proposed schema:

- `hierarchy_id: string not null`
- `hierarchy_name: string not null`
- `version: string not null`
- `status: string not null`
- `description: string null`
- `payload_json: string not null`
- `content_hash: string not null`
- `node_count: int not null`
- `leaf_count: int not null`
- `max_depth: int not null`
- `owner: string null`
- `owner_department: string null`
- `published_by: string null`
- `published_at: string null`
- `retired_by: string null`
- `retired_at: string null`

### Notes

- `payload_json` is the canonical persisted hierarchy content.
- `content_hash` should be deterministic from `payload_json`.
- lifecycle fields are outside the payload, following the `rules_engine`
  serializer pattern.
- `node_count`, `leaf_count`, and `max_depth` are summary metadata for
  queryability, not separate source-of-truth content.

## Derived table: `base_hierarchy_node`

This table remains useful for recursive SQL and reporting rebuilds, but it is
not the authoritative store.

Expected grain:

- one row per `(hierarchy_id, version, account_key)`

Proposed schema:

- `hierarchy_id`
- `version`
- `account_key`
- `account_name`
- `parent_account_key`
- `account_level`
- `node_path`
- `created_date`
- `updated_date`

## Removed or deprecated base table

`hierarchy_registry` should be removed from the target architecture.

Rationale:

- hierarchy identity metadata is already present on `hierarchy_versions`
- the new model has no effective-dating split that justifies a separate
  registry row
- the current registry table adds joins and validation complexity without
  preserving unique information

If a convenience object is still useful later, it should be a derived view over
`hierarchy_versions`, not a separately published source table.

## Serialization

Add a hierarchy serializer parallel to the rules engine serializer pattern.

Responsibilities:

- export a canonical payload from `HierarchyDefinition`
- remove lifecycle from the payload
- serialize to stable `payload_json`
- compute `content_hash`
- reconstruct `HierarchyDefinition` from `payload_json`

The canonical payload should match the root-level YAML structure:

```json
{
  "hierarchy_id": "MVE_DOE",
  "hierarchy_name": "Duration of Equity",
  "version": "2026Q1",
  "owner": "ALM Systems Engineering",
  "owner_department": "ALM",
  "description": "MVE/DOE reporting hierarchy",
  "nodes": [...]
}
```

## Validation Changes

## Loader validation

The loader should:

- parse a root-level mapping
- no longer require a `hierarchy` wrapper
- tolerate missing required fields by recording load issues
- parse `nodes` exactly as today

Optional migration behavior:

- temporarily accept wrapped payloads under `hierarchy` for a short transition
  period
- export only the new canonical root-level shape

If temporary compatibility is added, it should be clearly marked transitional.

## Pre-structural validation

Required metadata checks should become:

- missing `hierarchy_id`
- missing `hierarchy_name`
- missing `version`
- missing `owner`
- missing `owner_department`
- missing `description`

Removed checks:

- `version_name`
- `version_status`
- `effective_start_date`
- `effective_end_date`
- `owner_team`
- `business_domain`

Node validation logic can remain largely unchanged.

## Pre-publish persistence validation

This validator should move from registry/version/node consistency checks to the
rules-engine-style authoritative-row model.

Required checks:

- the same `(hierarchy_id, version)` must not already exist
- no other version of the same `hierarchy_name` may currently be `published`
- derived node rows for the same `(hierarchy_id, version)` must not already
  exist unless an explicit rebuild mode is introduced

Removed checks:

- duplicate registry rows
- registry field conflicts
- overlapping effective windows
- multiple current-version semantics driven by `is_current`

Possible new checks:

- payload hash conflict for an existing version
- mismatch between `hierarchy_id` and `hierarchy_name` history if governance
  rules require a stable name

## View Rebuild Strategy

Reporting views should be rebuilt from:

- `base_hierarchy_node`
- `hierarchy_versions`

Published reporting views should filter on:

- `status = 'published'`

All joins to `hierarchy_registry` should be removed.

Version joins should use:

- `hierarchy_id`
- `version`

Published reporting outputs should expose:

- `hierarchy_id`
- `hierarchy_name`
- `version`
- `description`
- `owner`
- `owner_department`

along with existing structural node/path/level fields.

## Service API Target

The service layer should evolve toward:

- `load_from_yaml(path)`
- `get_validation_result(definition)`
- `flatten_definition(definition)`
- `to_dataframe(definition, spark)`
- `create_base_tables(spark, version_table, node_table, ...)`
- `publish_to_tables(definition, spark, version_table, node_table, ...)`
- `retire_version(spark, hierarchy_id, version, retired_by, ...)`
- `rebuild_reporting_views(spark, version_table, node_table, ...)`
- `validate_published_version(...)`

Parameters and method names should be updated to replace `version_id` with
`version` throughout the public surface.

## Migration Strategy

## YAML migration

Existing hierarchy YAML files should be rewritten from:

```yaml
hierarchy:
  hierarchy_id: ...
  hierarchy_name: ...
  hierarchy_description: ...
  owner_team: ...
  business_domain: ...
  version_id: ...
  version_name: ...
  version_status: draft
  effective_start_date: ...
  effective_end_date: ...
  nodes: ...
```

to:

```yaml
hierarchy_id: ...
hierarchy_name: ...
version: ...
owner: ...
owner_department: ...
description: ...
nodes: ...
```

Mapping:

- `hierarchy_description -> description`
- `owner_team -> owner`
- `business_domain -> owner_department`
- `version_id -> version`

Dropped:

- `version_name`
- `version_status`
- `effective_start_date`
- `effective_end_date`

## Table migration

Preferred migration sequence:

1. create new `hierarchy_versions` table
2. backfill authoritative version rows from existing YAML artifacts or existing
   persistence if reliable source payloads exist
3. rebuild `base_hierarchy_node` from authoritative payload rows
4. point reporting-view rebuilds at `hierarchy_versions` plus
   `base_hierarchy_node`
5. deprecate `hierarchy_registry`

If reliable historic source YAML is not available, payload backfill from the
existing node table will be lossy unless a reverse-tree reconstruction process
is implemented and validated.

## Test Plan

The refactor should update or add tests for:

- loader parsing of root-level payloads
- exporter/serializer round trip
- validation with the new metadata contract
- repository schema and published-row writes
- repository retire behavior
- publish-time uniqueness and published-sibling checks
- derived node rebuilds from authoritative payload rows
- reporting view joins against `hierarchy_versions`

Fixtures should be rewritten to the new canonical YAML shape.

## Suggested Implementation Order

1. update models to the new metadata vocabulary
2. update loader and exporter to the root-level canonical payload
3. add serializer for authoritative version rows
4. redesign repository around `hierarchy_versions`
5. update validators for the new contract
6. update service publish/retire orchestration
7. update derived node rebuild and view builder logic
8. migrate fixtures and tests
9. update README and examples

## Decisions Locked By This Spec

- canonical YAML has no `hierarchy:` wrapper
- canonical YAML has no authored lifecycle field
- lifecycle statuses are `published` and `retired`
- `effective_*` metadata is removed
- `hierarchy_versions` is the authoritative persisted table
- `payload_json` is the canonical persisted hierarchy content
- `base_hierarchy_node` is derived
- `hierarchy_registry` is removed from the target architecture
