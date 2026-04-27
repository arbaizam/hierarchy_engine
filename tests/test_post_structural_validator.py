from hierarchy_engine.models import FlattenedHierarchyRow
from hierarchy_engine.post_structural_validator import PostStructuralHierarchyValidator
from tests.helpers import build_definition


def build_row(
    *,
    hierarchy_id="TEST",
    version="V1",
    account_key="10000",
    account_name="Assets",
    parent_account_key=None,
    account_level=1,
    node_path="10000",
):
    return FlattenedHierarchyRow(
        hierarchy_id=hierarchy_id,
        version=version,
        account_key=account_key,
        account_name=account_name,
        parent_account_key=parent_account_key,
        account_level=account_level,
        node_path=node_path,
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
    )


def test_post_structural_validator_accepts_valid_rows():
    """
    What: Accepts a coherent flattened row set that matches hierarchy metadata and lineage expectations.
    Why: The post-structural gate should stay quiet on valid derived rows before persistence begins.
    Fails when: Correct parent links, paths, or metadata identity checks produce false positives.
    """
    rows = [
        build_row(),
        build_row(
            account_key="10100",
            account_name="Investments",
            parent_account_key="10000",
            account_level=2,
            node_path="10000||10100",
        ),
    ]

    result = PostStructuralHierarchyValidator().validate_rows(
        metadata=build_definition().metadata,
        rows=rows,
    )

    assert result.passed is True
    assert result.issues == []


def test_post_structural_validator_reports_duplicate_account_keys():
    """
    What: Reports duplicate account keys in the flattened row set.
    Why: Persisted node rows must maintain unique business keys within a hierarchy version.
    Fails when: Duplicate flattened keys stop producing `duplicate_flattened_account_key`.
    """
    rows = [
        build_row(account_key="10000", node_path="10000"),
        build_row(account_key="10000", node_path="10000"),
    ]

    result = PostStructuralHierarchyValidator().validate_rows(
        metadata=build_definition().metadata,
        rows=rows,
    )

    assert any(
        issue.check_name == "duplicate_flattened_account_key"
        for issue in result.issues
    )


def test_post_structural_validator_reports_missing_parent_and_bad_path():
    """
    What: Flags rows whose parent key is missing and whose path does not align with that parent lineage.
    Why: Derived paths and parent pointers must agree before the node table can support reporting views safely.
    Fails when: Broken parent references or mismatched path prefixes pass the post-structural audit.
    """
    rows = [
        build_row(),
        build_row(
            account_key="10100",
            account_name="Investments",
            parent_account_key="99999",
            account_level=2,
            node_path="10000||10100",
        ),
        build_row(
            account_key="10200",
            account_name="Trading",
            parent_account_key="10000",
            account_level=2,
            node_path="99999||10200",
        ),
    ]

    result = PostStructuralHierarchyValidator().validate_rows(
        metadata=build_definition().metadata,
        rows=rows,
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "missing_flattened_parent" in check_names
    assert "parent_path_mismatch" in check_names


def test_post_structural_validator_reports_identity_and_root_errors():
    """
    What: Reports rows whose hierarchy identity, version, or root semantics disagree with the definition metadata.
    Why: Derived rows should never drift away from the authoritative hierarchy/version identity they were built from.
    Fails when: Non-root rows masquerade as roots or mismatched hierarchy/version values escape validation.
    """
    rows = [
        build_row(hierarchy_id="OTHER", version="V2", account_level=2, node_path="10000||10000"),
    ]

    result = PostStructuralHierarchyValidator().validate_rows(
        metadata=build_definition().metadata,
        rows=rows,
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "mismatched_row_hierarchy_id" in check_names
    assert "mismatched_row_version" in check_names
    assert "invalid_root_account_level" in check_names
    assert "invalid_root_node_path" in check_names


def test_post_structural_validator_reports_level_and_self_parent_errors():
    """
    What: Reports rows that point to themselves as parent or whose level disagrees with path depth.
    Why: Self-parenting and level/path mismatches indicate broken flattening logic or corrupted row payloads.
    Fails when: Invalid parent identity or depth calculations no longer trigger the expected issues.
    """
    rows = [
        build_row(),
        build_row(
            account_key="10100",
            account_name="Investments",
            parent_account_key="10100",
            account_level=3,
            node_path="10000||10100",
        ),
    ]

    result = PostStructuralHierarchyValidator().validate_rows(
        metadata=build_definition().metadata,
        rows=rows,
    )

    check_names = {issue.check_name for issue in result.issues}
    assert "self_parent_row" in check_names
    assert "account_level_path_mismatch" in check_names

