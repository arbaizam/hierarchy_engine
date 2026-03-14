from hierarchy_engine.comparer import HierarchyComparer
from hierarchy_engine.comparer import HierarchyDiffItem, HierarchyDiffResult
from hierarchy_engine.models import HierarchyNode
from tests.helpers import build_definition


def test_compare_definitions_detects_rename():
    old_def = build_definition(nodes=[HierarchyNode(account_key="10000", account_name="Assets")])
    new_def = build_definition(
        metadata_overrides={"version_id": "V2", "version_name": "Version 2"},
        nodes=[HierarchyNode(account_key="10000", account_name="Assets Renamed")],
    )

    diff = HierarchyComparer().compare(old_def, new_def)
    assert any(item.change_type == "renamed" for item in diff.items)


def test_compare_definitions_detects_added_removed_and_reparented():
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
        metadata_overrides={"version_id": "V2", "version_name": "Version 2"},
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
    definition = build_definition()

    diff = HierarchyComparer().compare(definition, definition)

    assert HierarchyComparer().render_diff(diff) == "No differences found."


def test_render_diff_handles_unknown_change_type():
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
