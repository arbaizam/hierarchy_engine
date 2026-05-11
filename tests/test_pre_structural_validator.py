from hierarchy_engine.models import HierarchyNode
from hierarchy_engine.pre_structural_validator import HierarchyValidator
from tests.helpers import build_definition


def test_validator_returns_structured_result_for_duplicate_keys():
    """
    What: Reports duplicate account keys as a structured validation failure.
    Why: Duplicate business keys break flattening, persistence, and downstream reporting joins.
    Fails when: Duplicate descendants stop producing `duplicate_account_key` errors or the result shape becomes inconsistent.
    """
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


def test_validator_reports_metadata_errors():
    """
    What: Flags every required hierarchy metadata field when it is blank.
    Why: Publish-time identity and ownership semantics depend on a complete canonical metadata block.
    Fails when: Required fields such as owner, version, or hierarchy identifiers stop being enforced.
    """
    result = HierarchyValidator().validate(
        build_definition(
            metadata_overrides={
                "hierarchy_id": "",
                "hierarchy_name": "",
                "description": "",
                "owner": "",
                "owner_department": "",
                "version": "",
            }
        )
    )

    check_names = {issue.check_name for issue in result.issues}

    assert {
        "missing_hierarchy_id",
        "missing_hierarchy_name",
        "missing_description",
        "missing_owner",
        "missing_owner_department",
        "missing_version",
    }.issubset(check_names)


def test_validator_reports_missing_root_nodes():
    """
    What: Rejects a hierarchy definition that has no root nodes.
    Why: An empty hierarchy is not publishable and should be blocked before any flattening or persistence work begins.
    Fails when: Empty node collections are treated as valid authored hierarchies.
    """
    result = HierarchyValidator().validate(build_definition(nodes=[]))

    assert result.passed is False
    assert any(issue.check_name == "missing_root_nodes" for issue in result.issues)


def test_validator_reports_cycle():
    """
    What: Detects a cycle in the authored node graph during pre-structural validation.
    Why: Cycles are a structural authoring error and should be caught before flattening has to defend against them.
    Fails when: Recursive parent-child loops no longer emit `cycle_detected`.
    """
    root = HierarchyNode(account_key="10000", account_name="Assets")
    child = HierarchyNode(account_key="10100", account_name="Investments")
    root.children = [child]
    child.children = [root]

    result = HierarchyValidator().validate(build_definition(nodes=[root]))

    assert any(issue.check_name == "cycle_detected" for issue in result.issues)


def test_validator_reports_missing_node_content():
    """
    What: Reports blank account keys and account names on authored nodes.
    Why: Every flattened row and reporting dimension depends on stable node identity and human-readable labels.
    Fails when: Empty node identity fields pass validation or produce the wrong issue names.
    """
    definition = build_definition(
        nodes=[HierarchyNode(account_key="", account_name="", children=[])]
    )

    result = HierarchyValidator().validate(definition)

    check_names = {issue.check_name for issue in result.issues}
    assert "missing_account_key" in check_names
    assert "missing_account_name" in check_names


def test_validator_reports_invalid_children_collection_without_crashing():
    """
    What: Treats a non-list `children` payload as a validation issue instead of crashing traversal.
    Why: The validator should tolerate malformed authoring structures well enough to report them cleanly.
    Fails when: `children=None` causes an exception or no `invalid_children_collection` issue is emitted.
    """
    definition = build_definition(
        nodes=[HierarchyNode(account_key="10000", account_name="Assets", children=None)]
    )

    result = HierarchyValidator().validate(definition)

    assert result.passed is False
    assert any(
        issue.check_name == "invalid_children_collection" for issue in result.issues
    )


def test_validator_reports_non_node_children_without_crashing():
    """
    What: Reports child entries that are not hierarchy node objects.
    Why: Mixed-type child collections should fail validation explicitly rather than corrupt recursive traversal.
    Fails when: Invalid child payloads are ignored or cause the validator to crash.
    """
    definition = build_definition(
        nodes=[HierarchyNode(account_key="10000", account_name="Assets", children=["bad"])]
    )

    result = HierarchyValidator().validate(definition)

    assert result.passed is False
    assert any(issue.check_name == "invalid_child_node" for issue in result.issues)

