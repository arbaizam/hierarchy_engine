from hierarchy_engine.comparer import HierarchyComparer
from hierarchy_engine.comparer import HierarchyDiffItem, HierarchyDiffResult
from hierarchy_engine.models import HierarchyNode
from tests.helpers import build_definition


def test_compare_definitions_detects_rename():
    """
    What: Marks a node as renamed when its key stays constant but its display name changes across versions.
    Why: Diff output should distinguish label changes from structural adds or removals.
    Fails when: Renames disappear from the diff or are misclassified as another change type.
    """
    old_def = build_definition(nodes=[HierarchyNode(account_key="10000", account_name="Assets")])
    new_def = build_definition(
        metadata_overrides={"version": "V2"},
        nodes=[HierarchyNode(account_key="10000", account_name="Assets Renamed")],
    )

    diff = HierarchyComparer().compare(old_def, new_def)
    assert any(item.change_type == "renamed" for item in diff.items)


def test_compare_definitions_detects_added_removed_and_reparented():
    """
    What: Detects added descendants and reparented nodes across hierarchy versions and renders them in the diff text.
    Why: Operators need change reports that separate structural movement from simple value edits.
    Fails when: Reparented nodes are missed, additions disappear, or rendered diff labels stop matching the change types.
    """
    old_def = build_definition(
        nodes=[
            HierarchyNode(
                account_key="10000",
                account_name="Assets",
                children=[HierarchyNode(account_key="10100", account_name="Investments")],
            ),
            HierarchyNode(account_key="20000", account_name="Liabilities"),
        ]
    )
    new_def = build_definition(
        metadata_overrides={"version": "V2"},
        nodes=[
            HierarchyNode(
                account_key="10000",
                account_name="Assets",
                children=[HierarchyNode(account_key="30000", account_name="Cash")],
            ),
            HierarchyNode(
                account_key="20000",
                account_name="Liabilities",
                children=[HierarchyNode(account_key="10100", account_name="Investments")],
            ),
        ],
    )

    diff = HierarchyComparer().compare(old_def, new_def)
    rendered = HierarchyComparer().render_diff(diff)

    change_types = {item.change_type for item in diff.items}
    assert "added" in change_types
    assert "removed" not in change_types
    assert "reparented" in change_types
    assert "ADDED" in rendered
    assert "REPARENTED" in rendered


def test_render_diff_handles_no_changes():
    """
    What: Renders a stable no-op message when two definitions are identical.
    Why: Empty diffs should communicate clearly instead of producing blank or misleading output.
    Fails when: Identical hierarchies emit change text or stop returning the canonical no-differences message.
    """
    definition = build_definition()

    diff = HierarchyComparer().compare(definition, definition)

    assert HierarchyComparer().render_diff(diff) == "No differences found."


def test_render_diff_handles_unknown_change_type():
    """
    What: Preserves unknown change types in uppercase when rendering a diff result.
    Why: The renderer should degrade gracefully if new diff types are introduced before custom formatting is added.
    Fails when: Unrecognized change types are dropped or rendered ambiguously.
    """
    diff = HierarchyDiffResult(
        items=[
            HierarchyDiffItem(
                change_type="custom",
                account_key="10000",
                old_value="A",
                new_value="B",
            )
        ]
    )

    rendered = HierarchyComparer().render_diff(diff)

    assert "CUSTOM" in rendered

