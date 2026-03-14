"""
Hierarchy tree rendering utilities.

This module provides human-readable renderers for hierarchy definitions.
It is especially useful for:

- quick debugging
- architect / whiteboard sessions
- validation walkthroughs
- change reviews in pull requests

The renderer works directly from the nested hierarchy definition so that
the output preserves the authored tree structure.
"""

from __future__ import annotations

from hierarchy_engine.models import HierarchyDefinition, HierarchyNode


class HierarchyTreeRenderer:
    """
    Render a hierarchy definition as an indented text tree.
    """

    def render(self, definition: HierarchyDefinition, show_keys: bool = True) -> str:
        """
        Render the full hierarchy as a multi-line text tree.

        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to render.
        show_keys : bool, default True
            Whether to include account keys in the rendered output.

        Returns
        -------
        str
            Indented tree representation of the hierarchy.
        """
        lines: list[str] = []

        header = (
            f"Hierarchy: {definition.metadata.hierarchy_id} | "
            f"Version: {definition.metadata.version_id} | "
            f"Name: {definition.metadata.hierarchy_name}"
        )
        lines.append(header)

        for root in definition.nodes:
            self._render_node(
                node=root,
                lines=lines,
                level=0,
                show_keys=show_keys,
            )

        return "\n".join(lines)

    def _render_node(
        self,
        node: HierarchyNode,
        lines: list[str],
        level: int,
        show_keys: bool,
    ) -> None:
        """
        Recursively render a node and its descendants.

        Parameters
        ----------
        node : HierarchyNode
            Current node being rendered.
        lines : list[str]
            Mutable accumulator of output lines.
        level : int
            Current depth level in the tree.
        show_keys : bool
            Whether to include account keys.

        Notes
        -----
        The recursion is straightforward:
        1. render the current node
        2. recurse into each child at one deeper indentation level
        """
        indent = "  " * level
        label = (
            f"{node.account_key} {node.account_name}"
            if show_keys
            else node.account_name
        )
        lines.append(f"{indent}- {label}")

        for child in node.children:
            self._render_node(
                node=child,
                lines=lines,
                level=level + 1,
                show_keys=show_keys,
            )
