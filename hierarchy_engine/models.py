
"""
Domain models for the hierarchy engine.
 
These dataclasses define the canonical in-memory representation of a hierarchy
and its validation artifacts.
 
Design notes
------------
The YAML authoring format is nested and tree-shaped because that is easiest
for users to read and maintain.
 
The database representation is adjacency-list shaped because that is easiest
to store, validate, and derive downstream views from.
 
This module defines:
- nested tree objects for authoring/loading
- flattened row objects for persistence
- validation result objects for structured error reporting
"""
 
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class HierarchyMetadata:
    """
    Top-level hierarchy metadata.
 
    Parameters
    ----------
    hierarchy_id : str
        Stable identifier for the hierarchy.
    hierarchy_name : str
        Human-readable hierarchy name.
    version : str
        Unique identifier for the hierarchy version.
    owner : str
        Team or actor responsible for maintaining the hierarchy.
    owner_department : str
        Organization or department responsible for the hierarchy.
    description : str
        Optional description of the hierarchy.
    """
 
    hierarchy_id: str
    hierarchy_name: str
    version: str = ""
    owner: str = ""
    owner_department: str = ""
    description: str = ""


@dataclass
class HierarchyNode:
    """
    One node in the nested hierarchy tree.
 
    Parameters
    ----------
    account_key : str
        Unique key identifying the node.
    account_name : str
        Human-readable node name.
    children : list[HierarchyNode]
        Child nodes directly beneath this node.
 
    Notes
    -----
    This nested representation is the preferred authoring and in-memory format.
    It preserves the natural tree structure of a hierarchy.
    """
 
    account_key: str
    account_name: str
    children: list["HierarchyNode"] = field(default_factory=list)


@dataclass
class HierarchyDefinition:
    """
    Full hierarchy definition loaded from YAML.
 
    Parameters
    ----------
    metadata : HierarchyMetadata
        Hierarchy and version metadata.
    nodes : list[HierarchyNode]
        Root-level nodes of the hierarchy tree.
    """
 
    metadata: HierarchyMetadata
    nodes: list[HierarchyNode] = field(default_factory=list)
    load_issues: list["ValidationIssue"] = field(default_factory=list)


@dataclass
class FlattenedHierarchyRow:
    """
    Flattened adjacency-list row for persistence.
 
    Parameters
    ----------
    hierarchy_id : str
        Hierarchy identifier.
    version : str
        Hierarchy version identifier.
    account_key : str
        Node key.
    account_name : str
        Node name.
    parent_account_key : str | None
        Parent node key. Null for root nodes.
    account_level : int
        Depth of the node in the hierarchy, starting at 1 for roots.
    node_path : str
        Delimited path of keys from root to the node.
    created_date : date
        Creation date string used for loading.
    updated_date : date
        Update date string used for loading.
    """
 
    hierarchy_id: str
    version: str
    account_key: str
    account_name: str
    parent_account_key: str | None
    account_level: int
    node_path: str
    created_date: date
    updated_date: date


@dataclass(frozen=True)
class HierarchyVersionRow:
    """Authoritative persisted hierarchy version row."""

    hierarchy_id: str
    hierarchy_name: str
    version: str
    status: str
    description: str | None
    payload_json: str
    content_hash: str
    node_count: int
    leaf_count: int
    max_depth: int
    owner: str | None
    owner_department: str | None
    published_by: str | None
    published_at: str | None
    retired_by: str | None
    retired_at: str | None


# ---------------------------------------------------------------------------
# Validation result models
# ---------------------------------------------------------------------------


@dataclass
class ValidationIssue:
    """
    One validation issue produced by the hierarchy validator.
 
    Parameters
    ----------
    severity : str
        Severity classification, typically 'ERROR' or 'WARNING'.
    check_name : str
        Stable identifier for the validation check that produced the issue.
    message : str
        Human-readable description of the issue.
    details : dict[str, Any] | None
        Optional structured details, such as the offending account_key or
        effective date values.
    """
 
    severity: str
    check_name: str
    message: str
    details: dict[str, Any] | None = None


@dataclass
class ValidationResult:
    """
    Structured validation result for a hierarchy validation run.
 
    Notes
    -----
    This object is intentionally UI/API friendly. It lets callers:
    - inspect validation output programmatically
    - print readable summaries
    - persist results later if needed
    - decide whether to fail fast or continue with warnings
    """
 
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """
        Return whether the validation result currently contains no errors.
        """
        return not self.has_errors()
 
    def add_issue(
        self,
        severity: str,
        check_name: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        Add a validation issue to the result.
 
        Parameters
        ----------
        severity : str
            Severity level, typically 'ERROR' or 'WARNING'.
        check_name : str
            Stable identifier for the validation check.
        message : str
            Human-readable issue description.
        details : dict[str, Any] | None, default None
            Optional structured context.
        """
        self.issues.append(
            ValidationIssue(
                severity=severity,
                check_name=check_name,
                message=message,
                details=details,
            )
        )
 
    def has_errors(self) -> bool:
        """
        Return whether the validation result contains any errors.
 
        Returns
        -------
        bool
            True when at least one issue has severity 'ERROR'.
        """
        return any(issue.severity.upper() == "ERROR" for issue in self.issues)
 
    def has_warnings(self) -> bool:
        """
        Return whether the validation result contains any warnings.
 
        Returns
        -------
        bool
            True when at least one issue has severity 'WARNING'.
        """
        return any(issue.severity.upper() == "WARNING" for issue in self.issues)
 
    def finalize(self) -> "ValidationResult":
        """
        Finalize overall pass/fail status based on recorded issues.
 
        Returns
        -------
        ValidationResult
            The same validation result instance with `passed` updated.
        """
        return self
 
    def to_text(self) -> str:
        """
        Render the validation result as readable multi-line text.
 
        Returns
        -------
        str
            Human-readable summary of the validation result.
        """
        if not self.issues:
            return "Validation passed with no issues."
 
        lines = [f"Validation passed: {self.passed}"]
        for issue in self.issues:
            details_text = f" | details={issue.details}" if issue.details else ""
            lines.append(
                f"[{issue.severity}] {issue.check_name}: {issue.message}{details_text}"
            )
        return "\n".join(lines)
