"""
Validation for flattened hierarchy rows before persistence.

This validator runs after the nested hierarchy has been flattened but before
any DataFrame creation or table writes occur.

Why this module exists
----------------------
Some structural problems are easiest to reason about in the flattened
adjacency-list representation rather than in the nested in-memory object model.

Examples:

- duplicate flattened rows for the same account key
- missing parent rows
- invalid root/non-root conventions
- broken or inconsistent node_path values
- level/path mismatches
- self-parenting rows

These are still pre-write validation failures. They should be detected before
the candidate hierarchy is written anywhere.
"""

from __future__ import annotations

from hierarchy_engine.models import (
    FlattenedHierarchyRow,
    HierarchyMetadata,
    ValidationResult,
)


class PostStructuralHierarchyValidator:
    """
    Validate flattened hierarchy rows before persistence.
    """

    def validate_rows(
        self,
        metadata: HierarchyMetadata,
        rows: list[FlattenedHierarchyRow],
    ) -> ValidationResult:
        """
        Validate the flattened publish artifact.
        """
        result = ValidationResult()

        self._validate_rows_present(result=result, rows=rows)
        if not rows:
            return result.finalize()

        row_by_key = {row.account_key: row for row in rows if row.account_key}

        self._validate_row_identity(metadata=metadata, rows=rows, result=result)
        self._validate_duplicate_account_keys(rows=rows, result=result)
        self._validate_required_row_content(rows=rows, result=result)
        self._validate_root_conventions(rows=rows, result=result)
        self._validate_parent_relationships(rows=rows, row_by_key=row_by_key, result=result)
        self._validate_path_and_level_consistency(rows=rows, row_by_key=row_by_key, result=result)

        return result.finalize()

    def _validate_rows_present(
        self,
        rows: list[FlattenedHierarchyRow],
        result: ValidationResult,
    ) -> None:
        if not rows:
            result.add_issue(
                severity="ERROR",
                check_name="missing_flattened_rows",
                message="Flattened hierarchy must contain at least one row",
            )

    def _validate_row_identity(
        self,
        metadata: HierarchyMetadata,
        rows: list[FlattenedHierarchyRow],
        result: ValidationResult,
    ) -> None:
        for row in rows:
            if row.hierarchy_id != metadata.hierarchy_id:
                result.add_issue(
                    severity="ERROR",
                    check_name="mismatched_row_hierarchy_id",
                    message=(
                        f"Row '{row.account_key}' has hierarchy_id '{row.hierarchy_id}' "
                        f"but expected '{metadata.hierarchy_id}'"
                    ),
                    details={
                        "account_key": row.account_key,
                        "row_hierarchy_id": row.hierarchy_id,
                        "expected_hierarchy_id": metadata.hierarchy_id,
                    },
                )

            if row.version_id != metadata.version_id:
                result.add_issue(
                    severity="ERROR",
                    check_name="mismatched_row_version_id",
                    message=(
                        f"Row '{row.account_key}' has version_id '{row.version_id}' "
                        f"but expected '{metadata.version_id}'"
                    ),
                    details={
                        "account_key": row.account_key,
                        "row_version_id": row.version_id,
                        "expected_version_id": metadata.version_id,
                    },
                )

    def _validate_duplicate_account_keys(
        self,
        rows: list[FlattenedHierarchyRow],
        result: ValidationResult,
    ) -> None:
        counts: dict[str, int] = {}
        for row in rows:
            counts[row.account_key] = counts.get(row.account_key, 0) + 1

        for account_key, row_count in counts.items():
            if row_count > 1:
                result.add_issue(
                    severity="ERROR",
                    check_name="duplicate_flattened_account_key",
                    message=(
                        f"Flattened rows contain duplicate account_key '{account_key}'"
                    ),
                    details={
                        "account_key": account_key,
                        "row_count": row_count,
                    },
                )

    def _validate_required_row_content(
        self,
        rows: list[FlattenedHierarchyRow],
        result: ValidationResult,
    ) -> None:
        for row in rows:
            if not row.account_key:
                result.add_issue(
                    severity="ERROR",
                    check_name="missing_flattened_account_key",
                    message="A flattened row is missing account_key",
                )

            if not row.account_name:
                result.add_issue(
                    severity="ERROR",
                    check_name="missing_flattened_account_name",
                    message=f"Flattened row '{row.account_key}' is missing account_name",
                    details={"account_key": row.account_key},
                )

            if row.account_level is None or row.account_level < 1:
                result.add_issue(
                    severity="ERROR",
                    check_name="invalid_flattened_account_level",
                    message=(
                        f"Flattened row '{row.account_key}' has invalid account_level "
                        f"'{row.account_level}'"
                    ),
                    details={
                        "account_key": row.account_key,
                        "account_level": row.account_level,
                    },
                )

            if not row.node_path:
                result.add_issue(
                    severity="ERROR",
                    check_name="missing_flattened_node_path",
                    message=f"Flattened row '{row.account_key}' is missing node_path",
                    details={"account_key": row.account_key},
                )

    def _validate_root_conventions(
        self,
        rows: list[FlattenedHierarchyRow],
        result: ValidationResult,
    ) -> None:
        root_rows = [row for row in rows if row.parent_account_key is None]

        if not root_rows:
            result.add_issue(
                severity="ERROR",
                check_name="missing_flattened_root_rows",
                message="Flattened hierarchy must contain at least one root row",
            )

        for row in root_rows:
            if row.account_level != 1:
                result.add_issue(
                    severity="ERROR",
                    check_name="invalid_root_account_level",
                    message=(
                        f"Root row '{row.account_key}' must have account_level 1"
                    ),
                    details={
                        "account_key": row.account_key,
                        "account_level": row.account_level,
                    },
                )

    def _validate_parent_relationships(
        self,
        rows: list[FlattenedHierarchyRow],
        row_by_key: dict[str, FlattenedHierarchyRow],
        result: ValidationResult,
    ) -> None:
        for row in rows:
            if row.parent_account_key is None:
                continue

            if row.parent_account_key == row.account_key:
                result.add_issue(
                    severity="ERROR",
                    check_name="self_parent_row",
                    message=(
                        f"Flattened row '{row.account_key}' cannot reference itself as parent"
                    ),
                    details={"account_key": row.account_key},
                )
                continue

            parent_row = row_by_key.get(row.parent_account_key)
            if parent_row is None:
                result.add_issue(
                    severity="ERROR",
                    check_name="missing_flattened_parent",
                    message=(
                        f"Flattened row '{row.account_key}' references missing parent "
                        f"'{row.parent_account_key}'"
                    ),
                    details={
                        "account_key": row.account_key,
                        "parent_account_key": row.parent_account_key,
                    },
                )

    def _validate_path_and_level_consistency(
        self,
        rows: list[FlattenedHierarchyRow],
        row_by_key: dict[str, FlattenedHierarchyRow],
        result: ValidationResult,
    ) -> None:
        for row in rows:
            if not row.node_path:
                continue

            path_keys = row.node_path.split("||")

            if any(not path_key for path_key in path_keys):
                result.add_issue(
                    severity="ERROR",
                    check_name="invalid_node_path_segment",
                    message=(
                        f"Flattened row '{row.account_key}' contains an empty node_path segment"
                    ),
                    details={
                        "account_key": row.account_key,
                        "node_path": row.node_path,
                    },
                )
                continue

            if path_keys[-1] != row.account_key:
                result.add_issue(
                    severity="ERROR",
                    check_name="node_path_terminal_key_mismatch",
                    message=(
                        f"Flattened row '{row.account_key}' has node_path that does not end "
                        "with its own account_key"
                    ),
                    details={
                        "account_key": row.account_key,
                        "node_path": row.node_path,
                    },
                )

            if len(path_keys) != len(set(path_keys)):
                result.add_issue(
                    severity="ERROR",
                    check_name="repeated_key_in_node_path",
                    message=(
                        f"Flattened row '{row.account_key}' contains repeated keys in node_path"
                    ),
                    details={
                        "account_key": row.account_key,
                        "node_path": row.node_path,
                    },
                )

            if row.account_level is not None and len(path_keys) != row.account_level:
                result.add_issue(
                    severity="ERROR",
                    check_name="account_level_path_mismatch",
                    message=(
                        f"Flattened row '{row.account_key}' has account_level "
                        f"'{row.account_level}' but node_path depth '{len(path_keys)}'"
                    ),
                    details={
                        "account_key": row.account_key,
                        "account_level": row.account_level,
                        "node_path": row.node_path,
                        "path_depth": len(path_keys),
                    },
                )

            if row.parent_account_key is None:
                if len(path_keys) != 1:
                    result.add_issue(
                        severity="ERROR",
                        check_name="invalid_root_node_path",
                        message=(
                            f"Root row '{row.account_key}' must have a single-key node_path"
                        ),
                        details={
                            "account_key": row.account_key,
                            "node_path": row.node_path,
                        },
                    )
                continue

            parent_row = row_by_key.get(row.parent_account_key)
            if parent_row is None or not parent_row.node_path:
                continue

            expected_parent_path = path_keys[:-1]
            parent_path_keys = parent_row.node_path.split("||")

            if expected_parent_path != parent_path_keys:
                result.add_issue(
                    severity="ERROR",
                    check_name="parent_path_mismatch",
                    message=(
                        f"Flattened row '{row.account_key}' has node_path inconsistent with "
                        f"parent '{row.parent_account_key}'"
                    ),
                    details={
                        "account_key": row.account_key,
                        "parent_account_key": row.parent_account_key,
                        "node_path": row.node_path,
                        "parent_node_path": parent_row.node_path,
                    },
                )

            if (
                row.account_level is not None
                and parent_row.account_level is not None
                and row.account_level != parent_row.account_level + 1
            ):
                result.add_issue(
                    severity="ERROR",
                    check_name="parent_level_mismatch",
                    message=(
                        f"Flattened row '{row.account_key}' account_level is inconsistent "
                        f"with parent '{row.parent_account_key}'"
                    ),
                    details={
                        "account_key": row.account_key,
                        "account_level": row.account_level,
                        "parent_account_key": row.parent_account_key,
                        "parent_account_level": parent_row.account_level,
                    },
                )
