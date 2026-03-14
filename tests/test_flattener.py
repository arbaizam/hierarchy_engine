from datetime import date

from hierarchy_engine.flattener import HierarchyFlattener
from hierarchy_engine.models import HierarchyNode
from tests.helpers import build_definition


def test_flattener_emits_parent_child_rows():
    definition = build_definition()

    rows = HierarchyFlattener().flatten(definition)

    assert len(rows) == 2
    assert rows[0].parent_account_key is None
    assert rows[1].parent_account_key == "10000"
    assert rows[1].account_level == 2
    assert rows[1].node_path == "10000||10100"


def test_flattener_handles_multiple_roots_and_to_dicts():
    definition = build_definition(
        nodes=[
            HierarchyNode(account_key="10000", account_name="Assets"),
            HierarchyNode(account_key="20000", account_name="Liabilities"),
        ]
    )

    flattener = HierarchyFlattener()
    rows = flattener.flatten(definition)
    row_dicts = flattener.to_dicts(rows)

    assert [row.account_key for row in rows] == ["10000", "20000"]
    assert row_dicts[0]["hierarchy_id"] == "TEST"
    assert row_dicts[1]["parent_account_key"] is None


def test_flattener_uses_explicit_created_and_updated_dates():
    definition = build_definition()

    rows = HierarchyFlattener().flatten(
        definition,
        created_date=date(2026, 2, 1),
        updated_date=date(2026, 2, 2),
    )

    assert rows[0].created_date == date(2026, 2, 1)
    assert rows[0].updated_date == date(2026, 2, 2)
