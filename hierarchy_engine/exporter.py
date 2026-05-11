"""
YAML exporter for canonical hierarchy metadata.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from hierarchy_engine.models import HierarchyDefinition, HierarchyNode


class HierarchyYamlExporter:
    """
    Export hierarchy dataclasses into canonical YAML authoring payloads.
    """

    def export_payload(self, definition: HierarchyDefinition) -> dict[str, Any]:
        """
        Convert a hierarchy model into a YAML-safe dictionary.

        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy metadata to export.

        Returns
        -------
        dict[str, Any]
            Canonical authoring payload suitable for ``yaml.safe_dump``.
        """
        meta = definition.metadata
        return {
            "hierarchy_id": meta.hierarchy_id,
            "hierarchy_name": meta.hierarchy_name,
            "version": meta.version,
            "owner": meta.owner,
            "owner_department": meta.owner_department,
            "description": meta.description,
            "nodes": [self._export_node(node) for node in definition.nodes],
        }

    def to_dict(self, definition: HierarchyDefinition) -> dict[str, Any]:
        """
        Return the canonical authoring payload.
        """
        return self.export_payload(definition)

    def export_text(self, definition: HierarchyDefinition) -> str:
        """
        Render a hierarchy model as YAML text.
        """
        return yaml.safe_dump(
            self.export_payload(definition),
            sort_keys=False,
            allow_unicode=True,
        )

    def to_yaml(self, definition: HierarchyDefinition) -> str:
        """
        Render a hierarchy model as YAML text.
        """
        return self.export_text(definition)

    def export_path(self, definition: HierarchyDefinition, path: str | Path) -> None:
        """
        Write a hierarchy model to a YAML file.
        """
        Path(path).write_text(self.export_text(definition), encoding="utf-8")

    def write_yaml(self, definition: HierarchyDefinition, path: str) -> None:
        """
        Write a hierarchy model to a YAML file.
        """
        self.export_path(definition, path)

    def _export_node(self, node: HierarchyNode) -> dict[str, Any]:
        """
        Export one hierarchy node into canonical YAML node syntax.
        """
        payload: dict[str, Any] = {
            "account_key": node.account_key,
            "account_name": node.account_name,
        }
        if node.children:
            payload["children"] = [self._export_node(child) for child in node.children]
        return payload
