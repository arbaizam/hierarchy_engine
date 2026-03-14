"""
Recursive hierarchy flattener.

This module converts a nested hierarchy tree into adjacency-list rows better
for storage in relational tables.

The YAML hierarchy is naturally tree-shaped. Recursion is the cleanest way to
visit the current node, emit its contents, descend into children, and broadcast
parent context and path information to children.  

Please ensure changes to this module are clearly and robust commented.  The 
recursive traversal within this module is fundamental to the project.  
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Optional

from hierarchy_engine.models import (
    FlattenedHierarchyRow,
    HierarchyDefinition,
    HierarchyNode,
)


class HierarchyFlattener:
    """
    Flatten a nested hierarchy into adjacency-list rows.
    """

    def flatten(self, definition: HierarchyDefinition) -> list[FlattenedHierarchyRow]:
        """
        Flatten a full hierarchy definition into row objects.

        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to flatten.

        Returns
        -------
        list[FlattenedHierarchyRow]
            Flattened adjacency-list rows.
        """
        rows: list[FlattenedHierarchyRow] = []
        today = date.today().isoformat()

        for root_node in definition.nodes:
            self._flatten_node(
                node=root_node,
                metadata=definition.metadata,
                parent_account_key=None,
                account_level=1,
                path_keys=[],
                rows=rows,
                created_date=today,
                updated_date=today,
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
        created_date: str,
        updated_date: str,
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
        created_date : str
            Creation date used for emitted rows.
        updated_date : str
            Update date used for emitted rows.

        Notes
        -----
        Recursive traversal logic:

        Base action:
            Emit one row for the current node.

        Recursive step:
            For each child:
            - current node's account_key becomes child's parent_account_key
            - depth increases by 1
            - path extends with current node's account_key

        Termination:
            Recursion stops naturally when a node has no children.
        """
        current_path = path_keys + [node.account_key]

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
        for child in node.children:
            self._flatten_node(
                node=child,
                metadata=metadata,
                parent_account_key=node.account_key,
                account_level=account_level + 1,
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
            Row dictionaries suitable for DataFrame creation.
        """
        return [asdict(row) for row in rows]