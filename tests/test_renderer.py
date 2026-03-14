from hierarchy_engine.renderer import HierarchyTreeRenderer
from tests.helpers import build_definition


def test_render_tree():
    definition = build_definition()

    output = HierarchyTreeRenderer().render(definition)
    assert "10000 Assets" in output
    assert "10100 Investments" in output
    assert "Hierarchy: TEST | Version: V1 | Name: Test Hierarchy" in output


def test_render_tree_hides_keys():
    output = HierarchyTreeRenderer().render(build_definition(), show_keys=False)

    assert "- Assets" in output
    assert "- Investments" in output
    assert "10000 Assets" not in output
