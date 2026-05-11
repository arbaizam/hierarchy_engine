"""
Serialization between canonical hierarchy models and persisted version rows.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date

from hierarchy_engine.exporter import HierarchyYamlExporter
from hierarchy_engine.models import (
    HierarchyDefinition,
    HierarchyMetadata,
    HierarchyNode,
    HierarchyVersionRow,
)


class HierarchyVersionSerializer:
    """
    Convert canonical hierarchy models to and from persisted version rows.
    """

    DEFAULT_EFFECTIVE_END_DATE = "2999-12-31"

    def serialize_version(
        self,
        definition: HierarchyDefinition,
        *,
        status: str,
        published_by: str | None = None,
        published_at: str | None = None,
        retired_by: str | None = None,
        retired_at: str | None = None,
        effective_start_date: str | None = None,
        effective_end_date: str | None = None,
    ) -> HierarchyVersionRow:
        """
        Serialize one hierarchy version to the authoritative row shape.

        The payload intentionally excludes lifecycle status. The table row
        status is authoritative, which lets publish and retire update lifecycle
        metadata without rewriting hierarchy content.
        """
        payload_json = self._payload_json(definition)
        return HierarchyVersionRow(
            hierarchy_id=definition.metadata.hierarchy_id,
            hierarchy_name=definition.metadata.hierarchy_name,
            version=definition.metadata.version,
            status=status,
            effective_start_date=effective_start_date
            or self._date_from_timestamp(published_at),
            effective_end_date=effective_end_date or self.DEFAULT_EFFECTIVE_END_DATE,
            description=definition.metadata.description,
            payload_json=payload_json,
            content_hash=self.content_hash_from_payload_json(payload_json),
            node_count=self._count_nodes(definition.nodes),
            leaf_count=self._count_leaves(definition.nodes),
            max_depth=self._max_depth(definition.nodes),
            owner=definition.metadata.owner,
            owner_department=definition.metadata.owner_department,
            published_by=published_by,
            published_at=published_at,
            retired_by=retired_by,
            retired_at=retired_at,
        )

    def deserialize_version(self, row: HierarchyVersionRow) -> HierarchyDefinition:
        """
        Reconstruct a canonical hierarchy from one authoritative version row.
        """
        payload = json.loads(row.payload_json)
        metadata = HierarchyMetadata(
            hierarchy_id=str(payload.get("hierarchy_id", "")),
            hierarchy_name=str(payload.get("hierarchy_name", "")),
            version=str(payload.get("version", "")),
            owner=str(payload.get("owner", "")),
            owner_department=str(payload.get("owner_department", "")),
            description=str(payload.get("description", "")),
        )
        raw_nodes = payload.get("nodes", [])
        return HierarchyDefinition(
            metadata=metadata,
            nodes=[self._node_from_dict(node) for node in raw_nodes],
        )

    def content_hash(self, definition: HierarchyDefinition) -> str:
        """
        Return a deterministic SHA-256 hash of canonical hierarchy content.

        Lifecycle provenance and status are intentionally excluded. The hash
        changes when hierarchy semantics or authoring metadata change, not when
        a hierarchy is published by a different operator.
        """
        return self.content_hash_from_payload_json(self._payload_json(definition))

    def content_hash_from_payload_json(self, payload_json: str) -> str:
        """
        Return the SHA-256 hash of the persisted payload JSON bytes.
        """
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    def _payload_json(self, definition: HierarchyDefinition) -> str:
        """
        Serialize the canonical authoring payload used for persistence.
        """
        payload = HierarchyYamlExporter().export_payload(definition)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def _date_from_timestamp(self, value: str | None) -> str:
        """
        Return a non-null effective date from a publish timestamp.
        """
        if value:
            return value[:10]
        return date.today().isoformat()

    def _count_nodes(self, nodes: list[HierarchyNode]) -> int:
        """
        Count all hierarchy nodes in the tree.
        """
        return sum(1 + self._count_nodes(node.children) for node in nodes)

    def _count_leaves(self, nodes: list[HierarchyNode]) -> int:
        """
        Count all leaf nodes in the tree.
        """
        leaf_count = 0
        for node in nodes:
            if not node.children:
                leaf_count += 1
            else:
                leaf_count += self._count_leaves(node.children)
        return leaf_count

    def _max_depth(self, nodes: list[HierarchyNode]) -> int:
        """
        Return the maximum hierarchy depth across root nodes.
        """
        if not nodes:
            return 0
        return max(self._node_depth(node) for node in nodes)

    def _node_depth(self, node: HierarchyNode) -> int:
        """
        Return the depth of one node subtree.
        """
        if not node.children:
            return 1
        return 1 + max(self._node_depth(child) for child in node.children)

    def _node_from_dict(self, payload: dict) -> HierarchyNode:
        """
        Reconstruct one hierarchy node from payload data.
        """
        children_payload = payload.get("children", [])
        return HierarchyNode(
            account_key=str(payload.get("account_key", "")),
            account_name=str(payload.get("account_name", "")),
            children=[self._node_from_dict(child) for child in children_payload],
        )
