from pathlib import Path

import pytest

from hierarchy_engine.errors import HierarchyParseError
from hierarchy_engine.loader import HierarchyConfigLoader
from hierarchy_engine.pre_structural_validator import HierarchyValidator


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_loader_parses_hierarchy_yaml():
    """
    What: Loads a valid canonical hierarchy YAML file into metadata and node models.
    Why: Authoring files are the system entry point and must round-trip into the in-memory contract cleanly.
    Fails when: Root-level metadata fields, node parsing, or load issue suppression regress for valid input.
    """
    definition = HierarchyConfigLoader().load_from_yaml(
        FIXTURES_DIR / "valid_hierarchy.yaml"
    )

    assert definition.metadata.hierarchy_id == "TEST"
    assert definition.metadata.version == "V1"
    assert len(definition.nodes) == 1
    assert definition.nodes[0].account_key == "10000"
    assert definition.load_issues == []


@pytest.mark.parametrize(
    ("fixture_name", "message"),
    [
        ("malformed_root.yaml", "Root YAML object must be a dictionary"),
        (
            "malformed_hierarchy.yaml",
            "Top-level 'hierarchy' section must be a dictionary",
        ),
    ],
)
def test_loader_raises_parse_error_for_malformed_yaml(fixture_name, message):
    """
    What: Rejects malformed root payloads and malformed compatibility wrapper payloads.
    Why: Structural corruption at the YAML object boundary should fail fast before tolerant field parsing begins.
    Fails when: Non-dictionary root objects or invalid top-level wrapper objects slip through without a parse error.
    """
    with pytest.raises(HierarchyParseError, match=message):
        HierarchyConfigLoader().load_from_yaml(FIXTURES_DIR / fixture_name)


def test_loader_raises_parse_error_for_missing_file():
    """
    What: Raises a parse error when the requested hierarchy file does not exist.
    Why: Notebook and job callers need a loader failure that points at the missing source artifact immediately.
    Fails when: Missing files are swallowed, misclassified, or produce an unhelpful exception type.
    """
    with pytest.raises(HierarchyParseError, match="not found"):
        HierarchyConfigLoader().load_from_yaml(FIXTURES_DIR / "missing.yaml")


def test_loader_raises_parse_error_for_invalid_yaml_syntax():
    """
    What: Raises a parse error when the YAML text itself is syntactically invalid.
    Why: Syntax failures should stop authoring workflows before partial metadata defaults are introduced.
    Fails when: YAML parser failures leak through as raw exceptions or are treated as tolerant field issues.
    """
    with pytest.raises(HierarchyParseError, match="Failed to parse YAML"):
        HierarchyConfigLoader().load_from_yaml(FIXTURES_DIR / "invalid_yaml_syntax.yaml")


def test_loader_collects_field_level_issues_without_raising():
    """
    What: Collects field-level load issues while still returning a partial definition object.
    Why: The validator layer needs access to degraded payloads so it can report precise authoring defects.
    Fails when: Invalid scalar fields, missing owners, or malformed node collections abort loading too early.
    """
    definition = HierarchyConfigLoader().load_from_yaml(
        FIXTURES_DIR / "tolerant_invalid_fields.yaml"
    )

    load_issue_names = {issue.check_name for issue in definition.load_issues}
    validation_result = HierarchyValidator().validate(definition)

    assert definition.metadata.hierarchy_id == ""
    assert definition.metadata.owner == ""
    assert definition.nodes == []
    assert "invalid_nodes_collection" in load_issue_names
    assert any(
        issue.check_name == "missing_hierarchy_id"
        for issue in validation_result.issues
    )
    assert any(
        issue.check_name == "missing_owner"
        for issue in validation_result.issues
    )


def test_loader_collects_invalid_children_issue_without_raising():
    """
    What: Preserves a parent node while recording a load issue for malformed child collections.
    Why: Nested authoring defects should be surfaced without losing the surrounding valid node context.
    Fails when: Invalid child collections crash loading or drop the otherwise valid parent node.
    """
    definition = HierarchyConfigLoader().load_from_yaml(
        FIXTURES_DIR / "invalid_children.yaml"
    )

    assert len(definition.nodes) == 1
    assert any(
        issue.check_name == "invalid_children_collection"
        for issue in definition.load_issues
    )

