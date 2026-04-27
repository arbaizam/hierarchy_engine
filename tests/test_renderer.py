from hierarchy_engine.renderer import HierarchyTreeRenderer
from tests.helpers import build_definition


def test_render_tree():
    """
    What: Renders hierarchy metadata and node lines into a readable tree representation.
    Why: Engineers rely on text rendering to inspect authored structures quickly in notebooks and tests.
    Fails when: Header metadata, account keys, or nested child lines disappear from the rendered output.
    """
    definition = build_definition()

    output = HierarchyTreeRenderer().render(definition)
    assert "10000 Assets" in output
    assert "10100 Investments" in output
    assert "Hierarchy: TEST | Version: V1 | Name: Test Hierarchy" in output


def test_render_tree_hides_keys():
    """
    What: Suppresses account keys in the rendered tree when `show_keys` is disabled.
    Why: The renderer should support a label-only view for readability when business keys are not needed.
    Fails when: Hidden-key rendering still leaks keys or stops showing node names.
    """
    output = HierarchyTreeRenderer().render(build_definition(), show_keys=False)

    assert "- Assets" in output
    assert "- Investments" in output
    assert "10000 Assets" not in output

