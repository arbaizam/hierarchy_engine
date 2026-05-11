"""
Recursive hierarchy flattener.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from hierarchy_engine.models import (
    FlattenedHierarchyRow,
    HierarchyDefinition,
    HierarchyMetadata,
    HierarchyNode,
)


class HierarchyFlattener:
    """
    Flatten a nested hierarchy into adjacency-list rows.
    """

    def flatten(
        self,
        definition: HierarchyDefinition,
        created_at: str | None = None,
        updated_at: str | None = None,
    ) -> list[FlattenedHierarchyRow]:
        """
        Flatten a full hierarchy definition into row objects.

        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to flatten.
        created_at : str | None, default None
            ISO-8601 timestamp applied to emitted row creation metadata.
        updated_at : str | None, default None
            ISO-8601 timestamp applied to emitted row update metadata.

        Returns
        -------
        list[FlattenedHierarchyRow]
            Flattened adjacency-list rows.
        """
        rows: list[FlattenedHierarchyRow] = []
        row_created_at = created_at or self._utc_now()
        row_updated_at = updated_at or row_created_at
        visited_nodes: set[int] = set()

        for root_node in definition.nodes:
            self._flatten_node(
                node=root_node,
                metadata=definition.metadata,
                parent_account_key=None,
                account_level=1,
                path_keys=[],
                rows=rows,
                created_at=row_created_at,
                updated_at=row_updated_at,
                visited_nodes=visited_nodes,
            )

        return rows

    def _flatten_node(
        self,
        node: HierarchyNode,
        metadata: HierarchyMetadata,
        parent_account_key: str | None,
        account_level: int,
        path_keys: list[str],
        rows: list[FlattenedHierarchyRow],
        created_at: str,
        updated_at: str,
        visited_nodes: set[int],
    ) -> None:
        """
        Recursively flatten one node and all descendants.

        Parameters
        ----------
        node : HierarchyNode
            Current node being visited.
        metadata : HierarchyMetadata
            Top-level hierarchy metadata.
        parent_account_key : str | None
            Parent key for the current node. Null for root nodes.
        account_level : int
            Current depth in the hierarchy tree.
        path_keys : list[str]
            Path of ancestor keys from the root down to the parent.
        rows : list[FlattenedHierarchyRow]
            Mutable output accumulator.
        created_at : str
            ISO-8601 timestamp used for emitted rows.
        updated_at : str
            ISO-8601 timestamp used for emitted rows.
        visited_nodes : set[int]
            Object-identity guard to prevent infinite recursion on cycles.
        """
        node_id = id(node)
        if node_id in visited_nodes:
            return
        visited_nodes.add(node_id)

        current_path = path_keys + [node.account_key]

        rows.append(
            FlattenedHierarchyRow(
                hierarchy_id=metadata.hierarchy_id,
                version=metadata.version,
                account_key=node.account_key,
                account_name=node.account_name,
                parent_account_key=parent_account_key,
                account_level=account_level,
                node_path="||".join(current_path),
                created_at=created_at,
                updated_at=updated_at,
            )
        )

        for child in node.children or []:
            self._flatten_node(
                node=child,
                metadata=metadata,
                parent_account_key=node.account_key,
                account_level=account_level + 1,
                path_keys=current_path,
                rows=rows,
                created_at=created_at,
                updated_at=updated_at,
                visited_nodes=visited_nodes,
            )

    def to_dicts(self, rows: list[FlattenedHierarchyRow]) -> list[dict]:
        """
        Convert flattened rows to dictionaries.
        """
        return [asdict(row) for row in rows]

    def _utc_now(self) -> str:
        """
        Return the current UTC timestamp in ISO-8601 string form.
        """
        return datetime.now(timezone.utc).isoformat()
