"""
Hierarchy YAML export utilities.

This module exports an in-memory hierarchy definition back to YAML.

Why this matters
----------------
Export is useful for:

- round-trip validation
- regenerating authored files from in-memory objects
- future "edit in UI, save to YAML" workflows
- comparing canonical serialized versions in source control
"""

from __future__ import annotations

from typing import Any, Dict

import yaml

from hierarchy_engine.models import HierarchyDefinition, HierarchyNode


class HierarchyYamlExporter:
    """
    Export hierarchy definitions to YAML.
    """

    def _serialize_date(self, value: Any) -> str | None:
        """Serialize date-like values without requiring pre-validation."""
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def to_dict(self, definition: HierarchyDefinition) -> dict[str, Any]:
        """
        Convert a hierarchy definition to a YAML-friendly dictionary.

        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to serialize.

        Returns
        -------
        dict[str, Any]
            YAML-friendly dictionary representation.
        """
        meta = definition.metadata

        return {
            "hierarchy": {
                "hierarchy_id": meta.hierarchy_id,
                "hierarchy_name": meta.hierarchy_name,
                "hierarchy_description": meta.hierarchy_description,
                "owner_team": meta.owner_team,
                "business_domain": meta.business_domain,
                "version_id": meta.version_id,
                "version_name": meta.version_name,
                "version_status": meta.version_status,
                "effective_start_date": self._serialize_date(
                    meta.effective_start_date
                ),
                "effective_end_date": self._serialize_date(meta.effective_end_date),
                "nodes": [self._node_to_dict(node) for node in definition.nodes],
            }
        }

    def _node_to_dict(self, node: HierarchyNode) -> Dict[str, Any]:
        """
        Recursively convert a hierarchy node to a dictionary.

        Parameters
        ----------
        node : HierarchyNode
            Node to convert.

        Returns
        -------
        dict[str, Any]
            Dictionary representation of the node.

        Notes
        -----
        Recursion mirrors the tree structure:
        each node is converted, then its children are converted in turn.
        """
        node_dict: Dict[str, Any] = {
            "account_key": node.account_key,
            "account_name": node.account_name,
        }

        if node.children:
            node_dict["children"] = [
                self._node_to_dict(child) for child in node.children
            ]

        return node_dict

    def to_yaml(self, definition: HierarchyDefinition) -> str:
        """
        Serialize a hierarchy definition to YAML text.

        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to serialize.

        Returns
        -------
        str
            YAML string.
        """
        payload = self.to_dict(definition)
        return yaml.safe_dump(
            payload,
            sort_keys=False,
            allow_unicode=True,
        )

    def write_yaml(self, definition: HierarchyDefinition, path: str) -> None:
        """
        Write a hierarchy definition to a YAML file.

        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to serialize.
        path : str
            Target output file path.
        """
        yaml_text = self.to_yaml(definition)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(yaml_text)
