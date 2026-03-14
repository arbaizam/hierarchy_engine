"""
YAML loader for hierarchy definitions.
 
This module is intentionally limited in responsibility to:
- reading YAML
- validating the top-level shape
- converting dictionaries into dataclasses
 
It does not perform structural hierarchy validation beyond basic shape checks.
That responsibility is designated to the validator.
"""
 
from __future__ import annotations
 
from datetime import date
from pathlib import Path
from typing import Any
 
import yaml
 
from hierarchy_engine.errors import HierarchyParseError
from hierarchy_engine.models import (
    HierarchyDefinition,
    HierarchyMetadata,
    HierarchyNode,
    ValidationIssue,
)
 
class HierarchyConfigLoader:
    """
    Load hierarchy definitions from YAML files.
    """
 
    def load_from_yaml(self, path: str | Path) -> HierarchyDefinition:
        """
        Load a hierarchy definition from a YAML file.
 
        Parameters
        ----------
        path : str | Path
            Path to the YAML file.
 
        Returns
        -------
        HierarchyDefinition
            Parsed hierarchy definition.
 
        Raises
        ------
        HierarchyParseError
            Raised when the file is missing, invalid YAML, or malformed.
        """
        file_path = Path(path)
 
        if not file_path.exists():
            raise HierarchyParseError(f"Hierarchy YAML file not found: {file_path}")
 
        try:
            with file_path.open("r", encoding="utf-8") as handle:
                raw = yaml.safe_load(handle)
        except yaml.YAMLError as exc:
            raise HierarchyParseError(f"Failed to parse YAML: {exc}") from exc
 
        if not isinstance(raw, dict):
            raise HierarchyParseError("Root YAML object must be a dictionary")
 
        if "hierarchy" not in raw:
            raise HierarchyParseError("YAML must contain a top-level 'hierarchy' section")
 
        return self._parse_hierarchy(raw["hierarchy"])
 
    def _add_issue(
        self,
        issues: list[ValidationIssue],
        check_name: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Record a non-fatal load issue for later validation reporting."""
        issues.append(
            ValidationIssue(
                severity="ERROR",
                check_name=check_name,
                message=message,
                details=details,
            )
        )
 
    def _string_or_empty(self, value: Any) -> str:
        """Return a normalized string field value for tolerant loading."""
        if value is None:
            return ""
        return str(value)
 
    def _parse_date(self, value: Any, field_name: str):
        """
        Parse an ISO date field from the YAML payload.
 
        Parameters
        ----------
        value : Any
            Raw field value.
        field_name : str
            Field name used in parse errors.
 
        Returns
        -------
        date | None
            Parsed date, or None for null values.
        """
        if value is None:
            return None
 
        if isinstance(value, date):
            return value
 
        if not isinstance(value, str):
            self._add_issue(
                issues,
                f"invalid_{field_name}_type",
                f"Field '{field_name}' must be an ISO date string or null",
                details={"field_name": field_name, "value_type": type(value).__name__},
            )
            return None
 
        try:
            return date.fromisoformat(value)
        except ValueError:
            self._add_issue(
                issues,
                f"invalid_{field_name}_format",
                f"Field '{field_name}' must be a valid ISO date: {value}",
                details={"field_name": field_name, "value": value},
            )
            return value
 
    def _parse_hierarchy(self, raw: dict[str, Any]) -> HierarchyDefinition:
        """
        Parse the top-level hierarchy object.
 
        Parameters
        ----------
        raw : dict[str, Any]
            Raw hierarchy dictionary.
 
        Returns
        -------
        HierarchyDefinition
            Parsed hierarchy definition.
        """
        if not isinstance(raw, dict):
            raise HierarchyParseError("Top-level 'hierarchy' section must be a dictionary")
 
        issues: list[ValidationIssue] = []
        raw_nodes = raw.get("nodes", [])
        if raw_nodes is None:
            raw_nodes = []
        if not isinstance(raw_nodes, list):
            self._add_issue(
                issues,
                "invalid_nodes_collection",
                "Field 'nodes' must be a list",
            )
            raw_nodes = []
 
        metadata = HierarchyMetadata(
            hierarchy_id=self._string_or_empty(raw.get("hierarchy_id")),
            hierarchy_name=self._string_or_empty(raw.get("hierarchy_name")),
            hierarchy_description=self._string_or_empty(raw.get("hierarchy_description")),
            owner_team=self._string_or_empty(raw.get("owner_team")),
            business_domain=self._string_or_empty(raw.get("business_domain")),
            version_id=self._string_or_empty(raw.get("version_id")),
            version_name=self._string_or_empty(raw.get("version_name")),
            version_status=self._string_or_empty(raw.get("version_status")),
            effective_start_date=self._parse_date(
                raw.get("effective_start_date"),
                "effective_start_date",
                issues,
            ),
            effective_end_date=self._parse_date(
                raw.get("effective_end_date"),
                "effective_end_date",
                issues,
            ),
        )
 
        nodes = [self._parse_node(node, issues) for node in raw_nodes]
 
        return HierarchyDefinition(metadata=metadata, nodes=nodes, load_issues=issues)
 
    def _parse_node(
        self,
        raw: dict[str, Any],
        issues: list[ValidationIssue],
    ) -> HierarchyNode:
        """
        Parse a single hierarchy node recursively.
 
        Parameters
        ----------
        raw : dict[str, Any]
            Raw node dictionary.
 
        Returns
        -------
        HierarchyNode
            Parsed hierarchy node.
 
        Raises
        ------
        HierarchyParseError
            Raised when required node fields are missing.
 
        Notes
        -----
        This recursion mirrors the tree structure of the YAML file.
        Each call parses one node, then recursively parses all of its children.
        """
        if not isinstance(raw, dict):
            self._add_issue(
                issues,
                "invalid_node_object",
                "Each node must be a dictionary",
                details={"node_type": type(raw).__name__},
            )
            return HierarchyNode(account_key="", account_name="", children=[])
 
        raw_children = raw.get("children", [])
        if raw_children is None:
            raw_children = []
        if not isinstance(raw_children, list):
            self._add_issue(
                issues,
                "invalid_children_collection",
                "Field 'children' must be a list when present",
                details={"account_key": self._string_or_empty(raw.get("account_key"))},
            )
            raw_children = []
 
        children = [self._parse_node(child, issues) for child in raw_children]
 
        return HierarchyNode(
            account_key=self._string_or_empty(raw.get("account_key")),
            account_name=self._string_or_empty(raw.get("account_name")),
            children=children,
        )
 
 
Flatner.py
"""
Recursive hierarchy flattener.
 
This module converts a nested hierarchy tree into adjacency-list rows better
suited for storage in relational tables.
 
The YAML hierarchy is naturally tree-shaped. Recursion is the cleanest way to:
- visit the current node
- emit its contents
- descend into children
- propagate parent context and path information to descendants
 
Please ensure changes to this module remain clearly and robustly commented.
The recursive traversal within this module is fundamental to the project.
"""
 
from __future__ import annotations
 
from dataclasses import asdict
from datetime import date
from typing import Optional
 
from hierarchy_engine.models import (
    FlattenedHierarchyRow,
    HierarchyDefinition,
    HierarchyNode,
)
 
class HierarchyFlattener:
    """
    Flatten a nested hierarchy into adjacency-list rows.
    """
 
    def flatten(
        self,
        definition: HierarchyDefinition,
        created_date: Optional[date] = None,
        updated_date: Optional[date] = None,
    ) -> list[FlattenedHierarchyRow]:
        """
        Flatten a full hierarchy definition into row objects.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to flatten.
        created_date : date | None, default None
            System-created date to apply to emitted rows.
        updated_date : date | None, default None
            System-updated date to apply to emitted rows.
 
        Returns
        -------
        list[FlattenedHierarchyRow]
            Flattened adjacency-list rows.
 
        Notes
        -----
        These date fields are operational metadata. If not provided, this method
        defaults them to today's date. In more controlled publish workflows,
        the service layer should pass them explicitly.
        """
        rows: list[FlattenedHierarchyRow] = []
 
        row_created_date = created_date or date.today()
        row_updated_date = updated_date or date.today()
 
        for root_node in definition.nodes:
            self._flatten_node(
                node=root_node,
                metadata=definition.metadata,
                parent_account_key=None,
                account_level=1,
                path_keys=[],
                rows=rows,
                created_date=row_created_date,
                updated_date=row_updated_date,
            )
 
        return rows
 
    def _flatten_node(
        self,
        node: HierarchyNode,
        metadata,
        parent_account_key: Optional[str],
        account_level: int,
        path_keys: list[str],
        rows: list[FlattenedHierarchyRow],
        created_date: date,
        updated_date: date,
    ) -> None:
        """
        Recursively flatten one node and all descendants.
 
        Parameters
        ----------
        node : HierarchyNode
            Current node being visited.
        metadata : HierarchyMetadata
            Top-level hierarchy metadata.
        parent_account_key : str | None
            Parent key for the current node. Null for root nodes.
        account_level : int
            Current depth in the hierarchy tree.
        path_keys : list[str]
            Path of ancestor keys from the root down to the parent.
        rows : list[FlattenedHierarchyRow]
            Mutable output accumulator.
        created_date : date
            System-created date used for emitted rows.
        updated_date : date
            System-updated date used for emitted rows.
 
        Notes
        -----
        Recursive traversal logic:
 
        Base action
        -----------
        Emit one row for the current node.
 
        Recursive step
        --------------
        For each child:
        - current node's account_key becomes the child's parent_account_key
        - depth increases by 1
        - path extends with the current node's account_key
 
        Termination
        -----------
        Recursion stops naturally when a node has no children.
 
        Why this works
        --------------
        A hierarchy is a tree. Each invocation of this method handles exactly one
        node and delegates its descendants to recursive child calls. That keeps
        parent-child context explicit and makes path construction straightforward.
        """
        current_path = path_keys + [node.account_key]
 
        rows.append(
            FlattenedHierarchyRow(
                hierarchy_id=metadata.hierarchy_id,
                version_id=metadata.version_id,
                account_key=node.account_key,
                account_name=node.account_name,
                parent_account_key=parent_account_key,
                account_level=account_level,
                node_path="||".join(current_path),
                created_date=created_date,
                updated_date=updated_date,
            )
        )
 
        # Recurse into children, passing the current node as the parent context.
        # This is the key recursive step that transforms the nested tree into
        # flat adjacency-list rows while preserving lineage.
        for child in node.children:
            self._flatten_node(
                node=child,
                metadata=metadata,
                parent_account_key=node.account_key,
                account_level=account_level + 1,
                path_keys=current_path,
                rows=rows,
                created_date=created_date,
                updated_date=updated_date,
            )
 
    def to_dicts(self, rows: list[FlattenedHierarchyRow]) -> list[dict]:
        """
        Convert flattened rows to dictionaries.
 
        Parameters
        ----------
        rows : list[FlattenedHierarchyRow]
            Flattened row objects.
 
        Returns
        -------
        list[dict]
            Row dictionaries suitable for DataFrame creation.
        """
        return [asdict(row) for row in rows]
 
 
errors.py
"""
Custom exceptions for the hierarchy engine.
 
"""
 
class HierarchyEngineError(Exception):
    """Base exception for hierarchy engine failures."""
 
class HierarchyParseError(HierarchyEngineError):
    """Raised when a hierarchy YAML file cannot be parsed or is malformed."""
 
class HierarchyValidationError(HierarchyEngineError):
    """Raised when a hierarchy definition fails structural validation."""
 
class HierarchyPublishError(HierarchyEngineError):
    """Raised when a hierarchy cannot be published to a target repository."""
 
 
Comparer.py
"""
Hierarchy version comparison utilities.
 
This module compares two hierarchy definitions and identifies structural changes.
 
Current comparison scope
------------------------
The comparer currently detects:
 
- added nodes
- removed nodes
- renamed nodes
- reparented nodes
 
This is enough for a very useful v1 diff and can later be extended to support:
- moved subtree summaries
- level changes
- path changes
- ordering changes
"""
 
from __future__ import annotations
 
from dataclasses import dataclass, field
from typing import Dict, Optional
 
from hierarchy_engine.flattener import HierarchyFlattener
from hierarchy_engine.models import HierarchyDefinition
 
@dataclass
class HierarchyDiffItem:
    """
    One hierarchy difference item.
 
    Parameters
    ----------
    change_type : str
        Type of change, such as 'added', 'removed', 'renamed', or 'reparented'.
    account_key : str
        Node key affected by the change.
    old_value : str | None
        Previous value relevant to the change.
    new_value : str | None
        New value relevant to the change.
    """
 
    change_type: str
    account_key: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None
 
@dataclass
class HierarchyDiffResult:
    """
    Collection of hierarchy diff items.
    """
 
    items: list[HierarchyDiffItem] = field(default_factory=list)
 
    def add(
        self,
        change_type: str,
        account_key: str,
        old_value: Optional[str] = None,
        new_value: Optional[str] = None,
    ) -> None:
        """
        Add a diff item.
 
        Parameters
        ----------
        change_type : str
            Type of change.
        account_key : str
            Node key affected.
        old_value : str | None, default None
            Previous value.
        new_value : str | None, default None
            New value.
        """
        self.items.append(
            HierarchyDiffItem(
                change_type=change_type,
                account_key=account_key,
                old_value=old_value,
                new_value=new_value,
            )
        )
 
class HierarchyComparer:
    """
    Compare two hierarchy definitions.
    """
 
    def __init__(self) -> None:
        """Initialize the comparer."""
        self.flattener = HierarchyFlattener()
 
    def compare(
        self,
        old_definition: HierarchyDefinition,
        new_definition: HierarchyDefinition,
    ) -> HierarchyDiffResult:
        """
        Compare two hierarchy definitions.
 
        Parameters
        ----------
        old_definition : HierarchyDefinition
            Baseline hierarchy definition.
        new_definition : HierarchyDefinition
            Proposed or changed hierarchy definition.
 
        Returns
        -------
        HierarchyDiffResult
            Structured diff result.
        """
        old_rows = self.flattener.flatten(old_definition)
        new_rows = self.flattener.flatten(new_definition)
 
        old_map = {row.account_key: row for row in old_rows}
        new_map = {row.account_key: row for row in new_rows}
 
        result = HierarchyDiffResult()
 
        old_keys = set(old_map)
        new_keys = set(new_map)
 
        for added_key in sorted(new_keys - old_keys):
            result.add(
                change_type="added",
                account_key=added_key,
                new_value=new_map[added_key].account_name,
            )
 
        for removed_key in sorted(old_keys - new_keys):
            result.add(
                change_type="removed",
                account_key=removed_key,
                old_value=old_map[removed_key].account_name,
            )
 
        for shared_key in sorted(old_keys & new_keys):
            old_row = old_map[shared_key]
            new_row = new_map[shared_key]
 
            if old_row.account_name != new_row.account_name:
                result.add(
                    change_type="renamed",
                    account_key=shared_key,
                    old_value=old_row.account_name,
                    new_value=new_row.account_name,
                )
 
            if old_row.parent_account_key != new_row.parent_account_key:
                result.add(
                    change_type="reparented",
                    account_key=shared_key,
                    old_value=old_row.parent_account_key,
                    new_value=new_row.parent_account_key,
                )
 
        return result
 
    def render_diff(self, diff: HierarchyDiffResult) -> str:
        """
        Render a diff result as readable text.
 
        Parameters
        ----------
        diff : HierarchyDiffResult
            Diff result to render.
 
        Returns
        -------
        str
            Human-readable diff output.
        """
        if not diff.items:
            return "No differences found."
 
        lines: list[str] = []
        for item in diff.items:
            if item.change_type == "added":
                lines.append(f"ADDED      | {item.account_key} | {item.new_value}")
            elif item.change_type == "removed":
                lines.append(f"REMOVED    | {item.account_key} | {item.old_value}")
            elif item.change_type == "renamed":
                lines.append(
                    f"RENAMED    | {item.account_key} | {item.old_value} -> {item.new_value}"
                )
            elif item.change_type == "reparented":
                lines.append(
                    f"REPARENTED | {item.account_key} | {item.old_value} -> {item.new_value}"
                )
            else:
                lines.append(
                    f"{item.change_type.upper():<10} | {item.account_key} | "
                    f"{item.old_value} -> {item.new_value}"
                )
 
        return "\n".join(lines)
