from hierarchy_engine.flattener import HierarchyFlattener
from hierarchy_engine.models import HierarchyNode
from tests.helpers import build_definition


def test_flattener_emits_parent_child_rows():
    """
    What: Flattens a two-level hierarchy into parent and child rows with lineage metadata.
    Why: The derived node table and reporting views depend on correct parent keys, levels, and path strings.
    Fails when: Child rows lose their parent, account level, or `||`-delimited node path during flattening.
    """
    definition = build_definition()

    rows = HierarchyFlattener().flatten(definition)

    assert len(rows) == 2
    assert rows[0].parent_account_key is None
    assert rows[1].parent_account_key == "10000"
    assert rows[1].account_level == 2
    assert rows[1].node_path == "10000||10100"


def test_flattener_handles_multiple_roots_and_to_dicts():
    """
    What: Flattens multiple root nodes and converts the result into repository-ready dictionaries.
    Why: Independent root branches must remain distinct while still producing a writeable row payload.
    Fails when: Root ordering changes unexpectedly or `to_dicts` drops hierarchy metadata from flattened rows.
    """
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


def test_flattener_uses_explicit_created_and_updated_timestamps():
    """
    What: Applies explicit persisted timestamps to every flattened row.
    Why: Derived node rows should share the same ISO timestamp contract as the authoritative version row.
    Fails when: Caller-supplied `created_at` or `updated_at` values are ignored or reformatted.
    """
    definition = build_definition()

    rows = HierarchyFlattener().flatten(
        definition,
        created_at="2026-02-01T00:00:00+00:00",
        updated_at="2026-02-02T00:00:00+00:00",
    )

    assert rows[0].created_at == "2026-02-01T00:00:00+00:00"
    assert rows[0].updated_at == "2026-02-02T00:00:00+00:00"


def test_flattener_defends_against_cycles_when_called_directly():
    """
    What: Stops recursive traversal when a cyclic node reference is encountered.
    Why: Direct flattener callers should not blow up or loop forever on malformed in-memory structures.
    Fails when: A cycle re-enters already visited nodes and produces duplicate rows or unbounded recursion.
    """
    root = HierarchyNode(account_key="10000", account_name="Assets")
    child = HierarchyNode(account_key="10100", account_name="Investments")
    root.children = [child]
    child.children = [root]

    rows = HierarchyFlattener().flatten(build_definition(nodes=[root]))

    assert len(rows) == 2
    assert [row.account_key for row in rows] == ["10000", "10100"]

