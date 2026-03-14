from datetime import date

from hierarchy_engine.models import HierarchyNode
from hierarchy_engine.pre_structural_validator import HierarchyValidator
from tests.helpers import build_definition


def test_validator_returns_structured_result_for_duplicate_keys():
    definition = build_definition(
        nodes=[
            HierarchyNode(
                account_key="10000",
                account_name="Assets",
                children=[
                    HierarchyNode(account_key="10100", account_name="Investments"),
                    HierarchyNode(account_key="10100", account_name="Duplicate"),
                ],
            )
        ],
    )

    result = HierarchyValidator().validate(definition)

    assert result.passed is False
    assert result.has_errors() is True
    assert any(issue.check_name == "duplicate_account_key" for issue in result.issues)


def test_validator_accepts_supported_version_statuses():
    validator = HierarchyValidator()

    for status in ("draft", "published", "retired"):
        result = validator.validate(
            build_definition(metadata_overrides={"version_status": status})
        )
        assert not any(
            issue.check_name == "invalid_version_status" for issue in result.issues
        )


def test_validator_rejects_removed_validated_status():
    result = HierarchyValidator().validate(
        build_definition(metadata_overrides={"version_status": "validated"})
    )

    assert result.has_errors() is True
    assert any(issue.check_name == "invalid_version_status" for issue in result.issues)


def test_validator_reports_metadata_errors():
    result = HierarchyValidator().validate(
        build_definition(
            metadata_overrides={
                "hierarchy_id": "",
                "hierarchy_name": "",
                "hierarchy_description": "",
                "owner_team": "",
                "business_domain": "",
                "version_id": "",
                "version_name": "",
                "version_status": "bad",
                "effective_start_date": None,
            }
        )
    )

    check_names = {issue.check_name for issue in result.issues}

    assert {
        "missing_hierarchy_id",
        "missing_hierarchy_name",
        "missing_hierarchy_description",
        "missing_owner_team",
        "missing_business_domain",
        "missing_version_id",
        "missing_version_name",
        "invalid_version_status",
        "missing_effective_start_date",
    }.issubset(check_names)


def test_validator_reports_invalid_effective_date_range():
    result = HierarchyValidator().validate(
        build_definition(
            metadata_overrides={
                "effective_start_date": date(2026, 1, 2),
                "effective_end_date": date(2026, 1, 1),
            }
        )
    )

    assert any(
        issue.check_name == "invalid_effective_date_range" for issue in result.issues
    )


def test_validator_reports_missing_root_nodes():
    result = HierarchyValidator().validate(build_definition(nodes=[]))

    assert result.passed is False
    assert any(issue.check_name == "missing_root_nodes" for issue in result.issues)


def test_validator_reports_cycle():
    root = HierarchyNode(account_key="10000", account_name="Assets")
    child = HierarchyNode(account_key="10100", account_name="Investments")
    root.children = [child]
    child.children = [root]

    result = HierarchyValidator().validate(build_definition(nodes=[root]))

    assert any(issue.check_name == "cycle_detected" for issue in result.issues)


def test_validator_reports_missing_node_content():
    definition = build_definition(
        nodes=[HierarchyNode(account_key="", account_name="", children=[])]
    )

    result = HierarchyValidator().validate(definition)

    check_names = {issue.check_name for issue in result.issues}
    assert "missing_account_key" in check_names
    assert "missing_account_name" in check_names


def test_validator_reports_invalid_children_collection_without_crashing():
    definition = build_definition(
        nodes=[HierarchyNode(account_key="10000", account_name="Assets", children=None)]
    )

    result = HierarchyValidator().validate(definition)

    assert result.passed is False
    assert any(
        issue.check_name == "invalid_children_collection" for issue in result.issues
    )


def test_validator_reports_non_node_children_without_crashing():
    definition = build_definition(
        nodes=[HierarchyNode(account_key="10000", account_name="Assets", children=["bad"])]
    )

    result = HierarchyValidator().validate(definition)

    assert result.passed is False
    assert any(issue.check_name == "invalid_child_node" for issue in result.issues)
