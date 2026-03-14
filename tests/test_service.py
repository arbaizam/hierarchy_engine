from pathlib import Path
from unittest.mock import Mock

import pytest

from hierarchy_engine.comparer import HierarchyDiffResult
from hierarchy_engine.errors import HierarchyValidationError
from hierarchy_engine.models import ValidationResult
from hierarchy_engine.service import HierarchyService
from tests.helpers import build_definition


def test_service_load_validate_flatten():
    fixture_path = Path(__file__).parent / "fixtures" / "valid_hierarchy.yaml"

    svc = HierarchyService()
    definition = svc.load_from_yaml(fixture_path)
    result = svc.validate_definition(definition)
    rows = svc.flatten_definition(definition)

    assert result.passed is True
    assert len(rows) == 2


def test_service_get_validation_result_returns_validator_output():
    validator = Mock()
    validator.validate.return_value = ValidationResult(passed=True)
    service = HierarchyService(validator=validator)
    definition = build_definition()

    result = service.get_validation_result(definition)

    assert result.passed is True
    validator.validate.assert_called_once_with(definition)


def test_service_helper_methods_delegate():
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
    repo_instance = Mock()
    repo_class = Mock(return_value=repo_instance)
    monkeypatch.setattr("hierarchy_engine.service.HierarchyRepository", repo_class)

    HierarchyService().create_base_tables(
        spark="spark",
        registry_table="registry",
        version_table="versions",
        node_table="nodes",
        mode="overwrite",
    )

    repo_class.assert_called_once_with("spark")
    repo_instance.create_base_tables.assert_called_once_with(
        registry_table="registry",
        version_table="versions",
        node_table="nodes",
        mode="overwrite",
    )


def test_service_publish_to_tables_runs_all_validation_layers_before_writing(monkeypatch):
    repo_instance = Mock()
    repo_instance.rows_to_dataframe.return_value = "rows_df"
    repo_instance.registry_entry_exists.return_value = False
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
        registry_table="registry",
        version_table="version",
        node_table="nodes",
        node_write_mode="overwrite",
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
        registry_table="registry",
        version_table="version",
        node_table="nodes",
    )
    repo_class.assert_called_once_with(spark)
    repo_instance.rows_to_dataframe.assert_called_once()
    repo_instance.write_registry.assert_called_once()
    repo_instance.write_version.assert_called_once()
    repo_instance.write_nodes.assert_called_once_with(
        rows_df="rows_df",
        table_name="nodes",
        mode="overwrite",
    )


def test_service_publish_to_tables_skips_registry_write_when_entry_exists(monkeypatch):
    repo_instance = Mock()
    repo_instance.rows_to_dataframe.return_value = "rows_df"
    repo_instance.registry_entry_exists.return_value = True
    repo_class = Mock(return_value=repo_instance)
    monkeypatch.setattr("hierarchy_engine.service.HierarchyRepository", repo_class)

    service = HierarchyService()
    service.validate_definition = Mock()
    service.validate_post_structural = Mock()
    service.validate_pre_publish = Mock()

    service.publish_to_tables(
        definition=build_definition(),
        spark=object(),
        registry_table="registry",
        version_table="version",
        node_table="nodes",
    )

    repo_instance.write_registry.assert_not_called()
    repo_instance.write_version.assert_called_once()
    repo_instance.write_nodes.assert_called_once()


def test_service_publish_to_tables_blocks_invalid_definitions_before_writing(monkeypatch):
    repo_class = Mock()
    monkeypatch.setattr("hierarchy_engine.service.HierarchyRepository", repo_class)

    service = HierarchyService()
    definition = build_definition(metadata_overrides={"effective_start_date": None})

    with pytest.raises(HierarchyValidationError, match="missing_effective_start_date"):
        service.publish_to_tables(
            definition=definition,
            spark=object(),
            registry_table="registry",
            version_table="version",
            node_table="nodes",
        )

    repo_class.assert_not_called()


def test_service_validate_post_structural_raises_when_flattened_rows_are_invalid(monkeypatch):
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
            registry_table="registry",
            version_table="version",
            node_table="nodes",
        )


def test_service_validate_published_version_delegates_to_post_publish_validator(monkeypatch):
    validator = Mock()
    validator.validate_version.return_value = ValidationResult(passed=True)
    validator_class = Mock(return_value=validator)
    monkeypatch.setattr(
        "hierarchy_engine.service.PostPublishHierarchyValidator",
        validator_class,
    )

    result = HierarchyService().validate_published_version(
        spark="spark",
        hierarchy_id="TEST",
        version_id="V1",
        node_table="nodes",
        version_table="versions",
    )

    assert result.passed is True
    validator_class.assert_called_once_with("spark")
    validator.validate_version.assert_called_once_with(
        hierarchy_id="TEST",
        version_id="V1",
        node_table="nodes",
        version_table="versions",
    )


def test_service_rebuild_reporting_views_delegates_to_view_builder(monkeypatch):
    builder = Mock()
    builder.rebuild_all.return_value = {"reporting_view": "dim_reporting_hierarchy"}
    builder_class = Mock(return_value=builder)
    monkeypatch.setattr("hierarchy_engine.service.HierarchyViewBuilder", builder_class)

    result = HierarchyService().rebuild_reporting_views(
        spark="spark",
        registry_table="registry",
        version_table="versions",
        node_table="nodes",
        paths_view="v_paths",
        flat_view="v_flat",
        dims_view="v_dims",
        reporting_view="dim_reporting_hierarchy",
    )

    assert result == {"reporting_view": "dim_reporting_hierarchy"}
    builder_class.assert_called_once_with("spark")
    builder.rebuild_all.assert_called_once_with(
        registry_table="registry",
        version_table="versions",
        node_table="nodes",
        paths_view="v_paths",
        flat_view="v_flat",
        dims_view="v_dims",
        reporting_view="dim_reporting_hierarchy",
    )


def test_service_publish_and_rebuild_reporting_views_runs_publish_then_rebuild():
    service = HierarchyService()
    service.publish_to_tables = Mock()
    service.rebuild_reporting_views = Mock(return_value={"reporting_view": "dim"})
    definition = build_definition()

    result = service.publish_and_rebuild_reporting_views(
        definition=definition,
        spark="spark",
        registry_table="registry",
        version_table="versions",
        node_table="nodes",
        paths_view="v_paths",
        flat_view="v_flat",
        dims_view="v_dims",
        reporting_view="dim",
        created_by="engineer",
    )

    assert result == {"reporting_view": "dim"}
    service.publish_to_tables.assert_called_once_with(
        definition=definition,
        spark="spark",
        registry_table="registry",
        version_table="versions",
        node_table="nodes",
        node_write_mode="append",
        publish_date=None,
        created_by="engineer",
        published_by=None,
        change_description=None,
    )
    service.rebuild_reporting_views.assert_called_once_with(
        spark="spark",
        registry_table="registry",
        version_table="versions",
        node_table="nodes",
        paths_view="v_paths",
        flat_view="v_flat",
        dims_view="v_dims",
        reporting_view="dim",
    )


def test_service_validate_published_version_strict_raises_on_audit_failures(monkeypatch):
    failed_result = ValidationResult()
    failed_result.add_issue("ERROR", "audit", "failed")
    failed_result.finalize()

    service = HierarchyService()
    service.validate_published_version = Mock(return_value=failed_result)

    with pytest.raises(HierarchyValidationError, match="Post-publish"):
        service.validate_published_version_strict(
            spark="spark",
            hierarchy_id="TEST",
            version_id="V1",
            node_table="nodes",
            version_table="versions",
        )
