"""
Serialization between canonical hierarchy models and authoritative version rows.
"""

from __future__ import annotations

import hashlib
import json

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

    def serialize_version(
        self,
        definition: HierarchyDefinition,
        *,
        status: str,
        published_by: str | None = None,
        published_at: str | None = None,
        retired_by: str | None = None,
        retired_at: str | None = None,
    ) -> HierarchyVersionRow:
        """
        Serialize one hierarchy definition to the authoritative version row shape.
        """
        payload_json = self._payload_json(definition)
        return HierarchyVersionRow(
            hierarchy_id=definition.metadata.hierarchy_id,
            hierarchy_name=definition.metadata.hierarchy_name,
            version=definition.metadata.version,
            status=status,
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
        Reconstruct a canonical hierarchy definition from one version row.
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
        """
        return self.content_hash_from_payload_json(self._payload_json(definition))

    def content_hash_from_payload_json(self, payload_json: str) -> str:
        """
        Return the SHA-256 hash of the persisted payload JSON bytes.
        """
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    def _payload_json(self, definition: HierarchyDefinition) -> str:
        payload = self._canonical_payload_dict(definition)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def _canonical_payload_dict(
        self,
        definition: HierarchyDefinition,
    ) -> dict[str, object]:
        metadata = definition.metadata
        return {
            "hierarchy_id": metadata.hierarchy_id,
            "hierarchy_name": metadata.hierarchy_name,
            "version": metadata.version,
            "owner": metadata.owner,
            "owner_department": metadata.owner_department,
            "description": metadata.description,
            "nodes": [self._node_to_payload_dict(node) for node in definition.nodes],
        }

    def _count_nodes(self, nodes: list[HierarchyNode]) -> int:
        return sum(1 + self._count_nodes(node.children) for node in nodes)

    def _count_leaves(self, nodes: list[HierarchyNode]) -> int:
        leaf_count = 0
        for node in nodes:
            if not node.children:
                leaf_count += 1
            else:
                leaf_count += self._count_leaves(node.children)
        return leaf_count

    def _max_depth(self, nodes: list[HierarchyNode]) -> int:
        if not nodes:
            return 0
        return max(self._node_depth(node) for node in nodes)

    def _node_depth(self, node: HierarchyNode) -> int:
        if not node.children:
            return 1
        return 1 + max(self._node_depth(child) for child in node.children)

    def _node_from_dict(self, payload: dict) -> HierarchyNode:
        children_payload = payload.get("children", [])
        return HierarchyNode(
            account_key=str(payload.get("account_key", "")),
            account_name=str(payload.get("account_name", "")),
            children=[self._node_from_dict(child) for child in children_payload],
        )

    def _node_to_payload_dict(self, node: HierarchyNode) -> dict[str, object]:
        payload = {
            "account_key": node.account_key,
            "account_name": node.account_name,
        }
        if node.children:
            payload["children"] = [
                self._node_to_payload_dict(child) for child in node.children
            ]
        return payload
