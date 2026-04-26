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

import yaml

from hierarchy_engine.models import HierarchyDefinition, HierarchyNode


class HierarchyYamlExporter:
    """
    Export hierarchy definitions to YAML.
    """

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
            "hierarchy_id": meta.hierarchy_id,
            "hierarchy_name": meta.hierarchy_name,
            "version": meta.version,
            "owner": meta.owner,
            "owner_department": meta.owner_department,
            "description": meta.description,
            "nodes": [self._node_to_dict(node) for node in definition.nodes],
        }

    def _node_to_dict(self, node: HierarchyNode) -> dict[str, Any]:
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
        node_dict: dict[str, Any] = {
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
