from pathlib import Path

import pytest

from hierarchy_engine.errors import HierarchyParseError
from hierarchy_engine.loader import HierarchyConfigLoader
from hierarchy_engine.pre_structural_validator import HierarchyValidator


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_loader_parses_hierarchy_yaml():
    definition = HierarchyConfigLoader().load_from_yaml(
        FIXTURES_DIR / "valid_hierarchy.yaml"
    )

    assert definition.metadata.hierarchy_id == "TEST"
    assert definition.metadata.effective_start_date.isoformat() == "2026-01-01"
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
    with pytest.raises(HierarchyParseError, match=message):
        HierarchyConfigLoader().load_from_yaml(FIXTURES_DIR / fixture_name)


def test_loader_raises_parse_error_for_missing_file():
    with pytest.raises(HierarchyParseError, match="not found"):
        HierarchyConfigLoader().load_from_yaml(FIXTURES_DIR / "missing.yaml")


def test_loader_raises_parse_error_for_invalid_yaml_syntax():
    with pytest.raises(HierarchyParseError, match="Failed to parse YAML"):
        HierarchyConfigLoader().load_from_yaml(FIXTURES_DIR / "invalid_yaml_syntax.yaml")


def test_loader_raises_parse_error_when_hierarchy_section_is_missing():
    with pytest.raises(HierarchyParseError, match="top-level 'hierarchy' section"):
        HierarchyConfigLoader().load_from_yaml(FIXTURES_DIR / "missing_hierarchy_section.yaml")


def test_loader_collects_field_level_issues_without_raising():
    definition = HierarchyConfigLoader().load_from_yaml(
        FIXTURES_DIR / "tolerant_invalid_fields.yaml"
    )

    load_issue_names = {issue.check_name for issue in definition.load_issues}
    validation_result = HierarchyValidator().validate(definition)

    assert definition.metadata.hierarchy_id == ""
    assert definition.metadata.effective_start_date is None
    assert definition.nodes == []
    assert "invalid_effective_start_date_format" in load_issue_names
    assert "invalid_nodes_collection" in load_issue_names
    assert any(
        issue.check_name == "missing_hierarchy_id"
        for issue in validation_result.issues
    )
    assert any(
        issue.check_name == "missing_effective_start_date"
        for issue in validation_result.issues
    )


def test_loader_collects_invalid_children_issue_without_raising():
    definition = HierarchyConfigLoader().load_from_yaml(
        FIXTURES_DIR / "invalid_children.yaml"
    )

    assert len(definition.nodes) == 1
    assert any(
        issue.check_name == "invalid_children_collection"
        for issue in definition.load_issues
    )
