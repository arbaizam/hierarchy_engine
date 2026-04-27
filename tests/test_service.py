from pathlib import Path
from unittest.mock import Mock

import pytest

from hierarchy_engine.comparer import HierarchyDiffResult
from hierarchy_engine.errors import HierarchyValidationError
from hierarchy_engine.models import ValidationResult
from hierarchy_engine.service import HierarchyService
from tests.helpers import build_definition


def test_service_load_validate_flatten():
    """
    What: Runs the basic load, validate, and flatten workflow through the service facade.
    Why: The service is the notebook-facing orchestration layer and needs a working happy path end to end.
    Fails when: Valid YAML stops validating cleanly or flattening no longer produces the expected rows.
    """
    fixture_path = Path(__file__).parent / "fixtures" / "valid_hierarchy.yaml"

    svc = HierarchyService()
    definition = svc.load_from_yaml(fixture_path)
    result = svc.validate_definition(definition)
    rows = svc.flatten_definition(definition)

    assert result.passed is True
    assert len(rows) == 2


def test_service_get_validation_result_returns_validator_output():
    """
    What: Returns the validator result object produced by the injected pre-structural validator.
    Why: Service helpers should be thin orchestration wrappers, not alternate validation implementations.
    Fails when: The service stops delegating to the configured validator or mutates its result.
    """
    validator = Mock()
    validator.validate.return_value = ValidationResult()
    service = HierarchyService(validator=validator)
    definition = build_definition()

    result = service.get_validation_result(definition)

    assert result.passed is True
    validator.validate.assert_called_once_with(definition)


def test_service_helper_methods_delegate():
    """
    What: Delegates rendering, diffing, and YAML export helper methods to the injected collaborators.
    Why: The service facade should preserve collaborator behavior instead of duplicating presentation logic.
    Fails when: Tree rendering, diff rendering, or export no longer route through the configured helpers.
    """
    renderer = Mock()
    comparer = Mock()
    exporter = Mock()
    comparer.compare.return_value = HierarchyDiffResult()
    comparer.render_diff.return_value = "diff"
    renderer.render.return_value = "tree"
    exporter.to_yaml.return_value = "yaml"
    service = HierarchyService(
        renderer=renderer,
        comparer=comparer,
        exporter=exporter,
    )
    definition = build_definition()

    assert service.render_tree(definition) == "tree"
    assert service.compare_definitions(definition, definition) == comparer.compare.return_value
    assert service.render_diff(definition, definition) == "diff"
    assert service.export_to_yaml(definition) == "yaml"


def test_service_to_dataframe_uses_repository(monkeypatch):
    """
    What: Uses the repository to convert flattened rows into a Spark DataFrame.
    Why: DataFrame schema ownership lives in the repository and should not be reimplemented in the service.
    Fails when: The service bypasses repository schema logic during DataFrame conversion.
    """
    repo_instance = Mock()
    repo_instance.rows_to_dataframe.return_value = "df"
    repo_class = Mock(return_value=repo_instance)
    monkeypatch.setattr("hierarchy_engine.service.HierarchyRepository", repo_class)
    service = HierarchyService()
    definition = build_definition()
    spark = object()

    result = service.to_dataframe(definition, spark)

    assert result == "df"
    repo_class.assert_called_once_with(spark)
    repo_instance.rows_to_dataframe.assert_called_once()


def test_service_create_base_tables_delegates_to_repository(monkeypatch):
    """
    What: Delegates base-table creation to the repository with the caller's table names and mode.
    Why: Environment bootstrap should stay centralized in the persistence layer.
    Fails when: Base-table creation stops passing through the requested targets or save mode.
    """
    repo_instance = Mock()
    repo_class = Mock(return_value=repo_instance)
    monkeypatch.setattr("hierarchy_engine.service.HierarchyRepository", repo_class)

    HierarchyService().create_base_tables(
        spark="spark",
        version_table="versions",
        node_table="nodes",
        mode="overwrite",
    )

    repo_class.assert_called_once_with("spark")
    repo_instance.create_base_tables.assert_called_once_with(
        version_table="versions",
        node_table="nodes",
        mode="overwrite",
    )


def test_service_publish_to_tables_runs_all_validation_layers_before_writing(monkeypatch):
    """
    What: Runs pre-structural, post-structural, and pre-publish validation before writing version and node tables.
    Why: Publish orchestration should only persist data after every in-memory and persistence gate has passed.
    Fails when: Validation order changes, flattened rows are not audited, or non-transactional publish writes the authoritative row before derived nodes.
    """
    repo_instance = Mock()
    repo_instance.rows_to_dataframe.return_value = "rows_df"
    repo_class = Mock(return_value=repo_instance)
    monkeypatch.setattr("hierarchy_engine.service.HierarchyRepository", repo_class)

    service = HierarchyService()
    service.validate_definition = Mock()
    service.validate_post_structural = Mock()
    service.validate_pre_publish = Mock()
    definition = build_definition()
    spark = object()

    service.publish_to_tables(
        definition=definition,
        spark=spark,
        version_table="version",
        node_table="nodes",
        node_write_mode="overwrite",
        published_by="engineer",
        published_at="2026-04-26T12:00:00Z",
    )

    service.validate_definition.assert_called_once_with(definition)
    service.validate_post_structural.assert_called_once()
    post_structural_call = service.validate_post_structural.call_args
    assert post_structural_call.args[0] == definition
    assert "rows" in post_structural_call.kwargs
    assert len(post_structural_call.kwargs["rows"]) == 2
    service.validate_pre_publish.assert_called_once_with(
        definition=definition,
        spark=spark,
        version_table="version",
        node_table="nodes",
    )
    repo_class.assert_called_once_with(spark)
    repo_instance.rows_to_dataframe.assert_called_once()
    assert repo_instance.method_calls[1][0] == "write_nodes"
    assert repo_instance.method_calls[2][0] == "write_version"
    repo_instance.write_nodes.assert_called_once_with(
        rows_df="rows_df",
        table_name="nodes",
        mode="overwrite",
    )
    repo_instance.write_version.assert_called_once_with(
        definition=definition,
        table_name="version",
        status="published",
        published_by="engineer",
        published_at="2026-04-26T12:00:00Z",
    )


def test_service_publish_to_tables_blocks_invalid_definitions_before_writing(monkeypatch):
    """
    What: Stops the publish workflow before repository creation when authored metadata is invalid.
    Why: Bad definitions should fail fast without touching persistence infrastructure at all.
    Fails when: Invalid authored hierarchies still instantiate repository writes.
    """
    repo_class = Mock()
    monkeypatch.setattr("hierarchy_engine.service.HierarchyRepository", repo_class)

    service = HierarchyService()
    definition = build_definition(metadata_overrides={"owner": ""})

    with pytest.raises(HierarchyValidationError, match="missing_owner"):
        service.publish_to_tables(
            definition=definition,
            spark=object(),
            version_table="version",
            node_table="nodes",
        )

    repo_class.assert_not_called()


def test_service_publish_to_tables_blocks_pre_publish_failures_before_repository_writes(monkeypatch):
    """
    What: Stops the publish workflow when pre-publish persistence validation reports a conflict.
    Why: Existing version or node collisions should block writes before any repository side effect occurs.
    Fails when: Pre-publish failures still instantiate the repository or attempt version/node writes.
    """
    repo_instance = Mock()
    repo_class = Mock(return_value=repo_instance)
    monkeypatch.setattr("hierarchy_engine.service.HierarchyRepository", repo_class)

    service = HierarchyService()
    service.validate_definition = Mock()
    service.validate_post_structural = Mock()
    service.validate_pre_publish = Mock(
        side_effect=HierarchyValidationError("Pre-write hierarchy validation failed.")
    )

    with pytest.raises(HierarchyValidationError, match="Pre-write"):
        service.publish_to_tables(
            definition=build_definition(),
            spark=object(),
            version_table="version",
            node_table="nodes",
        )

    repo_class.assert_not_called()


def test_service_validate_post_structural_raises_when_flattened_rows_are_invalid(monkeypatch):
    """
    What: Raises a service-level validation error when post-structural row validation fails.
    Why: Broken flattened rows should halt publish orchestration with a clear phase-specific failure.
    Fails when: Post-structural errors are returned silently instead of blocking the caller.
    """
    failed_result = ValidationResult()
    failed_result.add_issue("ERROR", "bad_rows", "bad rows")
    failed_result.finalize()
    validator = Mock()
    validator.validate_rows.return_value = failed_result
    monkeypatch.setattr(
        "hierarchy_engine.service.PostStructuralHierarchyValidator",
        Mock(return_value=validator),
    )

    with pytest.raises(HierarchyValidationError, match="Post-structural"):
        HierarchyService().validate_post_structural(build_definition())


def test_service_validate_pre_publish_raises_when_persistence_conflicts_exist(monkeypatch):
    """
    What: Raises a service-level validation error when persistence state blocks a publish.
    Why: Notebook callers should get a blocking exception instead of having to inspect raw validator results manually.
    Fails when: Pre-publish conflicts stop surfacing as `HierarchyValidationError`.
    """
    failed_result = ValidationResult()
    failed_result.add_issue("ERROR", "conflict", "conflict")
    failed_result.finalize()
    validator = Mock()
    validator.validate_publish.return_value = failed_result
    monkeypatch.setattr(
        "hierarchy_engine.service.PrePublishHierarchyValidator",
        Mock(return_value=validator),
    )

    with pytest.raises(HierarchyValidationError, match="Pre-write"):
        HierarchyService().validate_pre_publish(
            definition=build_definition(),
            spark=object(),
            version_table="version",
            node_table="nodes",
        )


def test_service_retire_version_delegates_to_repository(monkeypatch):
    """
    What: Delegates lifecycle retirement to the repository with explicit actor and timestamp metadata.
    Why: The service should stay thin while still exposing the full retirement contract to callers.
    Fails when: Retirement arguments stop flowing through to the persistence layer intact.
    """
    repo_instance = Mock()
    repo_class = Mock(return_value=repo_instance)
    monkeypatch.setattr("hierarchy_engine.service.HierarchyRepository", repo_class)

    HierarchyService().retire_version(
        spark="spark",
        version_table="versions",
        hierarchy_id="TEST",
        version="V1",
        retired_by="engineer",
        retired_at="2026-04-26T12:00:00Z",
    )

    repo_class.assert_called_once_with("spark")
    repo_instance.retire_version.assert_called_once_with(
        table_name="versions",
        hierarchy_id="TEST",
        version="V1",
        retired_by="engineer",
        retired_at="2026-04-26T12:00:00Z",
    )


def test_service_validate_published_version_delegates_to_post_publish_validator(monkeypatch):
    """
    What: Delegates published-version audits to the post-publish validator and returns its result.
    Why: Operational audit behavior should stay encapsulated in the validator layer, not recreated in the service.
    Fails when: Published-version audits stop using the injected validator or stop returning its output.
    """
    validator = Mock()
    validator.validate_version.return_value = ValidationResult()
    validator_class = Mock(return_value=validator)
    monkeypatch.setattr(
        "hierarchy_engine.service.PostPublishHierarchyValidator",
        validator_class,
    )

    result = HierarchyService().validate_published_version(
        spark="spark",
        hierarchy_id="TEST",
        version="V1",
        node_table="nodes",
        version_table="versions",
    )

    assert result.passed is True
    validator_class.assert_called_once_with("spark")
    validator.validate_version.assert_called_once_with(
        hierarchy_id="TEST",
        version="V1",
        node_table="nodes",
        version_table="versions",
    )


def test_service_rebuild_reporting_views_delegates_to_view_builder(monkeypatch):
    """
    What: Delegates reporting-view rebuilds to the view builder with the expected view names.
    Why: Published reporting surfaces should be rebuilt through one consistent builder interface.
    Fails when: The service stops forwarding the configured view names or returning the builder result.
    """
    builder = Mock()
    builder.rebuild_all.return_value = {"reporting_view": "vw_hierarchy_published_leaves"}
    builder_class = Mock(return_value=builder)
    monkeypatch.setattr("hierarchy_engine.service.HierarchyViewBuilder", builder_class)

    result = HierarchyService().rebuild_reporting_views(
        spark="spark",
        version_table="versions",
        node_table="nodes",
        paths_view="vw_hierarchy_paths",
        flat_view="vw_hierarchy_flat_nodes",
        dims_view="vw_hierarchy_leaf_dimensions",
        reporting_view="vw_hierarchy_published_leaves",
        nodes_dims_view="vw_hierarchy_node_dimensions",
        nodes_reporting_view="vw_hierarchy_published_nodes",
    )

    assert result == {"reporting_view": "vw_hierarchy_published_leaves"}
    builder_class.assert_called_once_with("spark")
    builder.rebuild_all.assert_called_once_with(
        version_table="versions",
        node_table="nodes",
        paths_view="vw_hierarchy_paths",
        flat_view="vw_hierarchy_flat_nodes",
        dims_view="vw_hierarchy_leaf_dimensions",
        reporting_view="vw_hierarchy_published_leaves",
        nodes_dims_view="vw_hierarchy_node_dimensions",
        nodes_reporting_view="vw_hierarchy_published_nodes",
    )


def test_service_retire_and_rebuild_reporting_views_runs_retire_then_rebuild():
    """
    What: Runs retirement before rebuilding reporting views in the composite retirement workflow.
    Why: Published views should only rebuild after the underlying lifecycle state has been updated.
    Fails when: The service changes the retire-then-rebuild order or drops either step from the composite flow.
    """
    service = HierarchyService()
    service.retire_version = Mock()
    service.rebuild_reporting_views = Mock(return_value={"reporting_view": "dim"})

    result = service.retire_and_rebuild_reporting_views(
        spark="spark",
        version_table="versions",
        node_table="nodes",
        hierarchy_id="TEST",
        version="V1",
        paths_view="vw_hierarchy_paths",
        flat_view="vw_hierarchy_flat_nodes",
        dims_view="vw_hierarchy_leaf_dimensions",
        reporting_view="vw_hierarchy_published_leaves",
        nodes_dims_view="vw_hierarchy_node_dimensions",
        nodes_reporting_view="vw_hierarchy_published_nodes",
        retired_by="engineer",
    )

    assert result == {"reporting_view": "dim"}
    service.retire_version.assert_called_once_with(
        spark="spark",
        version_table="versions",
        hierarchy_id="TEST",
        version="V1",
        retired_by="engineer",
        retired_at=None,
    )
    service.rebuild_reporting_views.assert_called_once_with(
        spark="spark",
        version_table="versions",
        node_table="nodes",
        paths_view="vw_hierarchy_paths",
        flat_view="vw_hierarchy_flat_nodes",
        dims_view="vw_hierarchy_leaf_dimensions",
        reporting_view="vw_hierarchy_published_leaves",
        nodes_dims_view="vw_hierarchy_node_dimensions",
        nodes_reporting_view="vw_hierarchy_published_nodes",
    )


def test_service_publish_and_rebuild_reporting_views_runs_publish_then_rebuild():
    """
    What: Runs publish before rebuilding reporting views in the composite publish workflow.
    Why: Published reporting surfaces should reflect the newly persisted version and node rows immediately after publish.
    Fails when: The composite publish flow changes ordering or stops forwarding default append semantics.
    """
    service = HierarchyService()
    service.publish_to_tables = Mock()
    service.rebuild_reporting_views = Mock(return_value={"reporting_view": "dim"})
    definition = build_definition()

    result = service.publish_and_rebuild_reporting_views(
        definition=definition,
        spark="spark",
        version_table="versions",
        node_table="nodes",
        paths_view="vw_hierarchy_paths",
        flat_view="vw_hierarchy_flat_nodes",
        dims_view="vw_hierarchy_leaf_dimensions",
        reporting_view="vw_hierarchy_published_leaves",
        nodes_dims_view="vw_hierarchy_node_dimensions",
        nodes_reporting_view="vw_hierarchy_published_nodes",
        published_by="engineer",
    )

    assert result == {"reporting_view": "dim"}
    service.publish_to_tables.assert_called_once_with(
        definition=definition,
        spark="spark",
        version_table="versions",
        node_table="nodes",
        node_write_mode="append",
        published_by="engineer",
        published_at=None,
    )
    service.rebuild_reporting_views.assert_called_once_with(
        spark="spark",
        version_table="versions",
        node_table="nodes",
        paths_view="vw_hierarchy_paths",
        flat_view="vw_hierarchy_flat_nodes",
        dims_view="vw_hierarchy_leaf_dimensions",
        reporting_view="vw_hierarchy_published_leaves",
        nodes_dims_view="vw_hierarchy_node_dimensions",
        nodes_reporting_view="vw_hierarchy_published_nodes",
    )


def test_service_validate_published_version_strict_raises_on_audit_failures(monkeypatch):
    """
    What: Raises when a post-publish audit result contains blocking issues.
    Why: Strict validation helpers are meant for release gates and should fail closed on persisted-state defects.
    Fails when: Audit failures stop escalating to `HierarchyValidationError`.
    """
    failed_result = ValidationResult()
    failed_result.add_issue("ERROR", "audit", "failed")
    failed_result.finalize()

    service = HierarchyService()
    service.validate_published_version = Mock(return_value=failed_result)

    with pytest.raises(HierarchyValidationError, match="Post-publish"):
        service.validate_published_version_strict(
            spark="spark",
            hierarchy_id="TEST",
            version="V1",
            node_table="nodes",
            version_table="versions",
        )

