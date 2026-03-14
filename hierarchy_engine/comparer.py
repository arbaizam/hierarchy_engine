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

from dataclasses import dataclass, field
from typing import Dict, Optional

from hierarchy_engine.flattener import HierarchyFlattener
from hierarchy_engine.models import HierarchyDefinition


@dataclass
class HierarchyDiffItem:
    """
    One hierarchy difference item.

    Parameters
    ----------
    change_type : str
        Type of change, such as 'added', 'removed', 'renamed', or 'reparented'.
    account_key : str
        Node key affected by the change.
    old_value : str | None
        Previous value relevant to the change.
    new_value : str | None
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
        change_type : str
            Type of change.
        account_key : str
            Node key affected.
        old_value : str | None, default None
            Previous value.
        new_value : str | None, default None
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
        self.flattener = HierarchyFlattener()

    def compare(
        self,
        old_definition: HierarchyDefinition,
        new_definition: HierarchyDefinition,
    ) -> HierarchyDiffResult:
        """
        Compare two hierarchy definitions.

        Parameters
        ----------
        old_definition : HierarchyDefinition
            Baseline hierarchy definition.
        new_definition : HierarchyDefinition
            Proposed or changed hierarchy definition.

        Returns
        -------
        HierarchyDiffResult
            Structured diff result.
        """
        old_rows = self.flattener.flatten(old_definition)
        new_rows = self.flattener.flatten(new_definition)

        old_map = {row.account_key: row for row in old_rows}
        new_map = {row.account_key: row for row in new_rows}

        result = HierarchyDiffResult()

        old_keys = set(old_map)
        new_keys = set(new_map)

        for added_key in sorted(new_keys - old_keys):
            result.add(
                change_type="added",
                account_key=added_key,
                new_value=new_map[added_key].account_name,
            )

        for removed_key in sorted(old_keys - new_keys):
            result.add(
                change_type="removed",
                account_key=removed_key,
                old_value=old_map[removed_key].account_name,
            )

        for shared_key in sorted(old_keys & new_keys):
            old_row = old_map[shared_key]
            new_row = new_map[shared_key]

            if old_row.account_name != new_row.account_name:
                result.add(
                    change_type="renamed",
                    account_key=shared_key,
                    old_value=old_row.account_name,
                    new_value=new_row.account_name,
                )

            if old_row.parent_account_key != new_row.parent_account_key:
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
            if item.change_type == "added":
                lines.append(f"ADDED      | {item.account_key} | {item.new_value}")
            elif item.change_type == "removed":
                lines.append(f"REMOVED    | {item.account_key} | {item.old_value}")
            elif item.change_type == "renamed":
                lines.append(
                    f"RENAMED    | {item.account_key} | {item.old_value} -> {item.new_value}"
                )
            elif item.change_type == "reparented":
                lines.append(
                    f"REPARENTED | {item.account_key} | {item.old_value} -> {item.new_value}"
                )
            else:
                lines.append(
                    f"{item.change_type.upper():<10} | {item.account_key} | "
                    f"{item.old_value} -> {item.new_value}"
                )

        return "\n".join(lines)