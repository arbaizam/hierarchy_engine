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
from pathlib import Path
from typing import Any
 
import yaml
 
from hierarchy_engine.errors import HierarchyParseError
from hierarchy_engine.models import (
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
        file_path = Path(path)
 
        if not file_path.exists():
            raise HierarchyParseError(f"Hierarchy YAML file not found: {file_path}")
 
        try:
            with file_path.open("r", encoding="utf-8") as handle:
                raw = yaml.safe_load(handle)
        except yaml.YAMLError as exc:
            raise HierarchyParseError(f"Failed to parse YAML: {exc}") from exc
 
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
 
    def _parse_date(
        self,
        value: Any,
        field_name: str,
        issues: list[ValidationIssue],
    ):
        """
        Parse an ISO date field from the YAML payload.
 
        Parameters
        ----------
        value : Any
            Raw field value.
        field_name : str
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
                f"Field '{field_name}' must be an ISO date string or null",
                details={"field_name": field_name, "value_type": type(value).__name__},
            )
            return None
 
        try:
            return date.fromisoformat(value)
        except ValueError:
            self._add_issue(
                issues,
                f"invalid_{field_name}_format",
                f"Field '{field_name}' must be a valid ISO date: {value}",
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
        raw_nodes = raw.get("nodes", [])
        if raw_nodes is None:
            raw_nodes = []
        if not isinstance(raw_nodes, list):
            self._add_issue(
                issues,
                "invalid_nodes_collection",
                "Field 'nodes' must be a list",
            )
            raw_nodes = []
 
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
 
        raw_children = raw.get("children", [])
        if raw_children is None:
            raw_children = []
        if not isinstance(raw_children, list):
            self._add_issue(
                issues,
                "invalid_children_collection",
                "Field 'children' must be a list when present",
                details={"account_key": self._string_or_empty(raw.get("account_key"))},
            )
            raw_children = []
 
        children = [self._parse_node(child, issues) for child in raw_children]
 
        return HierarchyNode(
            account_key=self._string_or_empty(raw.get("account_key")),
            account_name=self._string_or_empty(raw.get("account_name")),
            children=children,
        )
 
 
