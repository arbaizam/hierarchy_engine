"""
High-level orchestration service for the hierarchy engine.
 
This is the main public entry point for notebooks, scripts, and future APIs.
It coordinates:
 
- loading
- validation
- flattening
- DataFrame conversion
- publishing
- rendering
- comparison
- YAML export
 
"""
 
from __future__ import annotations
 
from datetime import datetime, timezone
import logging
from pathlib import Path

from hierarchy_engine.comparer import HierarchyComparer, HierarchyDiffResult
from hierarchy_engine.errors import HierarchyValidationError
from hierarchy_engine.exporter import HierarchyYamlExporter
from hierarchy_engine.flattener import HierarchyFlattener
from hierarchy_engine.loader import HierarchyConfigLoader
from hierarchy_engine.models import ValidationResult
from hierarchy_engine.pre_publish_validator import PrePublishHierarchyValidator
from hierarchy_engine.post_publish_validator import PostPublishHierarchyValidator
from hierarchy_engine.post_structural_validator import PostStructuralHierarchyValidator
from hierarchy_engine.pre_structural_validator import HierarchyValidator
from hierarchy_engine.renderer import HierarchyTreeRenderer
from hierarchy_engine.repository import HierarchyRepository
from hierarchy_engine.view_builder import HierarchyViewBuilder


logger = logging.getLogger(__name__)


class HierarchyService:
    """
    Main service class for hierarchy workflows.
    """
 
    def __init__(
        self,
        loader: HierarchyConfigLoader | None = None,
        validator: HierarchyValidator | None = None,
        flattener: HierarchyFlattener | None = None,
        renderer: HierarchyTreeRenderer | None = None,
        comparer: HierarchyComparer | None = None,
        exporter: HierarchyYamlExporter | None = None,
    ):
        """
        Initialize the service.
 
        Parameters
        ----------
        loader : HierarchyConfigLoader | None
            Optional custom loader instance.
        validator : HierarchyValidator | None
            Optional custom validator instance.
        flattener : HierarchyFlattener | None
            Optional custom flattener instance.
        renderer : HierarchyTreeRenderer | None
            Optional custom renderer instance.
        comparer : HierarchyComparer | None
            Optional custom comparer instance.
        exporter : HierarchyYamlExporter | None
            Optional custom exporter instance.
        """
        self.loader = loader or HierarchyConfigLoader()
        self.validator = validator or HierarchyValidator()
        self.flattener = flattener or HierarchyFlattener()
        self.renderer = renderer or HierarchyTreeRenderer()
        self.comparer = comparer or HierarchyComparer()
        self.exporter = exporter or HierarchyYamlExporter()
 
    # -----------------------------------------------------------------------
    # Load
    # -----------------------------------------------------------------------
 
    def load_from_yaml(self, path: str | Path):
        """
        Load a hierarchy definition from YAML.
 
        Parameters
        ----------
        path : str | Path
            Path to the hierarchy YAML file.
 
        Returns
        -------
        HierarchyDefinition
            Parsed hierarchy definition.
        """
        logger.info("Loading hierarchy definition from YAML: %s", path)
        return self.loader.load_from_yaml(path)
 
    # -----------------------------------------------------------------------
    # In-memory validation
    # -----------------------------------------------------------------------
 
    def get_validation_result(self, definition) -> ValidationResult:
        """
        Validate a hierarchy definition and return a structured result.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to validate.
 
        Returns
        -------
        ValidationResult
            Structured validation result.
        """
        logger.info(
            "Running pre-structural validation for hierarchy_id=%s version=%s",
            definition.metadata.hierarchy_id,
            definition.metadata.version,
        )
        return self.validator.validate(definition)
 
    def validate_definition(self, definition) -> ValidationResult:
        """
        Validate a hierarchy definition in strict mode.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to validate.
 
        Returns
        -------
        ValidationResult
            Validation result when validation passes.
 
        Raises
        ------
        HierarchyValidationError
            Raised when validation fails.
        """
        result = self.get_validation_result(definition)
 
        if not result.passed:
            raise HierarchyValidationError(
                "Hierarchy validation failed.\n"
                + result.to_text()
            )
 
        return result
 
    # -----------------------------------------------------------------------
    # Flattening
    # -----------------------------------------------------------------------
 
    def flatten_definition(self, definition):
        """
        Flatten a hierarchy definition into adjacency-list rows.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition.
 
        Returns
        -------
        list[FlattenedHierarchyRow]
            Flattened rows.
        """
        logger.info(
            "Flattening hierarchy definition for hierarchy_id=%s version=%s",
            definition.metadata.hierarchy_id,
            definition.metadata.version,
        )
        return self.flattener.flatten(definition)
 
    def flatten_to_dicts(self, definition) -> list[dict]:
        """
        Flatten a hierarchy definition and convert rows to dictionaries.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition.
 
        Returns
        -------
        list[dict]
            Flattened row dictionaries.
        """
        rows = self.flatten_definition(definition)
        return self.flattener.to_dicts(rows)
 
    # -----------------------------------------------------------------------
    # Spark / DataFrame helpers
    # -----------------------------------------------------------------------

    def create_base_tables(
        self,
        spark,
        version_table: str,
        node_table: str,
        mode: str = "errorifexists",
    ) -> None:
        """
        Create the empty base Spark tables required by the publish workflow.
        """
        logger.info(
            "Creating base hierarchy tables: version=%s node=%s mode=%s",
            version_table,
            node_table,
            mode,
        )
        repo = HierarchyRepository(spark)
        repo.create_base_tables(
            version_table=version_table,
            node_table=node_table,
            mode=mode,
        )

    def to_dataframe(self, definition, spark):
        """
        Convert a hierarchy definition directly to a Spark DataFrame.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition.
        spark : SparkSession
            Active Spark session.
 
        Returns
        -------
        pyspark.sql.DataFrame
            Flattened hierarchy DataFrame.
        """
        logger.info(
            "Converting flattened hierarchy to DataFrame for hierarchy_id=%s version=%s",
            definition.metadata.hierarchy_id,
            definition.metadata.version,
        )
        rows = self.flatten_to_dicts(definition)
        repo = HierarchyRepository(spark)
        return repo.rows_to_dataframe(rows)
 
    # -----------------------------------------------------------------------
    # Post-structural validation
    # -----------------------------------------------------------------------

    def get_post_structural_validation_result(
        self,
        definition,
        rows=None,
    ) -> ValidationResult:
        """
        Validate the flattened hierarchy artifact before persistence.
        """
        logger.info(
            "Running post-structural validation for hierarchy_id=%s version=%s",
            definition.metadata.hierarchy_id,
            definition.metadata.version,
        )
        rows = rows if rows is not None else self.flatten_definition(definition)
        validator = PostStructuralHierarchyValidator()
        return validator.validate_rows(
            metadata=definition.metadata,
            rows=rows,
        )

    def validate_post_structural(self, definition, rows=None) -> ValidationResult:
        """
        Run strict flattened-row validation before persistence.
        """
        result = self.get_post_structural_validation_result(definition, rows=rows)

        if not result.passed:
            raise HierarchyValidationError(
                "Post-structural hierarchy validation failed.\n"
                + result.to_text()
            )

        return result

    # -----------------------------------------------------------------------
    # Pre-write persistence validation
    # -----------------------------------------------------------------------

    def get_pre_publish_validation_result(
        self,
        definition,
        spark,
        version_table: str,
        node_table: str,
    ) -> ValidationResult:
        """
        Validate a candidate publish against persisted tables before writing.
        """
        logger.info(
            "Running pre-publish validation for hierarchy_id=%s version=%s against version=%s node=%s",
            definition.metadata.hierarchy_id,
            definition.metadata.version,
            version_table,
            node_table,
        )
        validator = PrePublishHierarchyValidator(spark)
        return validator.validate_publish(
            metadata=definition.metadata,
            version_table=version_table,
            node_table=node_table,
        )

    def validate_pre_publish(
        self,
        definition,
        spark,
        version_table: str,
        node_table: str,
    ) -> ValidationResult:
        """
        Run strict pre-write validation against persisted tables.
        """
        result = self.get_pre_publish_validation_result(
            definition=definition,
            spark=spark,
            version_table=version_table,
            node_table=node_table,
        )

        if not result.passed:
            raise HierarchyValidationError(
                "Pre-write hierarchy validation failed.\n"
                + result.to_text()
            )

        return result

    # -----------------------------------------------------------------------
    # Publish
    # -----------------------------------------------------------------------
 
    def publish_to_tables(
        self,
        definition,
        spark,
        version_table: str,
        node_table: str,
        node_write_mode: str = "append",
        published_by: str | None = None,
        published_at: str | None = None,
    ) -> None:
        """
        Publish a hierarchy definition to target Spark tables.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to publish.
        spark : SparkSession
            Active Spark session.
        version_table : str
            Target authoritative hierarchy version table.
        node_table : str
            Target base hierarchy node table.
        node_write_mode : str, default "append"
            Write mode for node rows.
        published_by : str, optional
            User or process that published the hierarchy.
        published_at : str, optional
            Explicit publish timestamp. Defaults to repository current UTC time.
 
        Raises
        ------
        HierarchyValidationError
            Raised when in-memory validation fails.
 
        Notes
        -----
        This method performs three blocking validation passes before publishing:
        1. strict in-memory structural validation
        2. strict post-structural validation of flattened rows
        3. strict pre-write persistence validation

        Optional post-publish validation remains available separately through
        `validate_published_version(...)` for audit or diagnostics use cases.

        This workspace does not support atomic multi-table transactions. The
        service therefore writes derived node rows first and the authoritative
        version row second so a failure does not leave a published version row
        with no node rows behind it.
        """
        logger.info(
            "Publishing hierarchy_id=%s version=%s to version=%s node=%s",
            definition.metadata.hierarchy_id,
            definition.metadata.version,
            version_table,
            node_table,
        )
        self.validate_definition(definition)
        # Keep authoritative and derived row timestamps aligned to one persisted
        # publish instant so audit and comparison logic can reason about them
        # consistently.
        persisted_at = published_at or self._utc_now()
        rows = self.flattener.flatten(
            definition=definition,
            created_at=persisted_at,
            updated_at=persisted_at,
        )
        self.validate_post_structural(definition, rows=rows)
        self.validate_pre_publish(
            definition=definition,
            spark=spark,
            version_table=version_table,
            node_table=node_table,
        )
        row_dicts = self.flattener.to_dicts(rows)
 
        repo = HierarchyRepository(spark)
        rows_df = repo.rows_to_dataframe(row_dicts)

        logger.info(
            "Writing node rows for hierarchy_id=%s version=%s to %s with mode=%s",
            definition.metadata.hierarchy_id,
            definition.metadata.version,
            node_table,
            node_write_mode,
        )
        repo.write_nodes(
            rows_df=rows_df,
            table_name=node_table,
            mode=node_write_mode,
        )

        logger.info(
            "Writing authoritative version row for hierarchy_id=%s version=%s to %s",
            definition.metadata.hierarchy_id,
            definition.metadata.version,
            version_table,
        )
        repo.write_version(
            definition=definition,
            table_name=version_table,
            status="published",
            published_by=published_by,
            published_at=persisted_at,
        )
 
    def retire_version(
        self,
        spark,
        version_table: str,
        hierarchy_id: str,
        version: str,
        retired_by: str | None = None,
        retired_at: str | None = None,
    ) -> None:
        """
        Retire a persisted hierarchy version.
        """
        logger.info(
            "Retiring hierarchy_id=%s version=%s in %s",
            hierarchy_id,
            version,
            version_table,
        )
        repo = HierarchyRepository(spark)
        repo.retire_version(
            table_name=version_table,
            hierarchy_id=hierarchy_id,
            version=version,
            retired_by=retired_by,
            retired_at=retired_at,
        )

    def retire_and_rebuild_reporting_views(
        self,
        spark,
        version_table: str,
        node_table: str,
        hierarchy_id: str,
        version: str,
        paths_view: str,
        flat_view: str,
        dims_view: str,
        reporting_view: str,
        nodes_dims_view: str,
        nodes_reporting_view: str,
        retired_by: str | None = None,
        retired_at: str | None = None,
    ) -> dict[str, str]:
        """
        Retire a persisted hierarchy version, then rebuild reporting views.
        """
        logger.info(
            "Retiring hierarchy_id=%s version=%s and rebuilding reporting views",
            hierarchy_id,
            version,
        )
        self.retire_version(
            spark=spark,
            version_table=version_table,
            hierarchy_id=hierarchy_id,
            version=version,
            retired_by=retired_by,
            retired_at=retired_at,
        )
        return self.rebuild_reporting_views(
            spark=spark,
            version_table=version_table,
            node_table=node_table,
            paths_view=paths_view,
            flat_view=flat_view,
            dims_view=dims_view,
            reporting_view=reporting_view,
            nodes_dims_view=nodes_dims_view,
            nodes_reporting_view=nodes_reporting_view,
        )

    # -----------------------------------------------------------------------
    # Reporting view rebuilds
    # -----------------------------------------------------------------------

    def rebuild_reporting_views(
        self,
        spark,
        version_table: str,
        node_table: str,
        paths_view: str,
        flat_view: str,
        dims_view: str,
        reporting_view: str,
        nodes_dims_view: str,
        nodes_reporting_view: str,
    ) -> dict[str, str]:
        """
        Rebuild all derived reporting views from the published base tables.

        Notes
        -----
        This method is intended to run after base-table publishing. It rebuilds
        reporting views for all published versions, not only the current one.
        """
        logger.info(
            "Rebuilding reporting views from version=%s node=%s into paths=%s flat=%s dims=%s reporting=%s nodes_dims=%s nodes_reporting=%s",
            version_table,
            node_table,
            paths_view,
            flat_view,
            dims_view,
            reporting_view,
            nodes_dims_view,
            nodes_reporting_view,
        )
        builder = HierarchyViewBuilder(spark)
        return builder.rebuild_all(
            version_table=version_table,
            node_table=node_table,
            paths_view=paths_view,
            flat_view=flat_view,
            dims_view=dims_view,
            reporting_view=reporting_view,
            nodes_dims_view=nodes_dims_view,
            nodes_reporting_view=nodes_reporting_view,
        )

    def publish_and_rebuild_reporting_views(
        self,
        definition,
        spark,
        version_table: str,
        node_table: str,
        paths_view: str,
        flat_view: str,
        dims_view: str,
        reporting_view: str,
        nodes_dims_view: str,
        nodes_reporting_view: str,
        node_write_mode: str = "append",
        published_by: str | None = None,
        published_at: str | None = None,
    ) -> dict[str, str]:
        """
        Publish base tables, then rebuild all derived reporting views.
        """
        logger.info(
            "Publishing and rebuilding reporting views for hierarchy_id=%s version=%s",
            definition.metadata.hierarchy_id,
            definition.metadata.version,
        )
        self.publish_to_tables(
            definition=definition,
            spark=spark,
            version_table=version_table,
            node_table=node_table,
            node_write_mode=node_write_mode,
            published_by=published_by,
            published_at=published_at,
        )
        return self.rebuild_reporting_views(
            spark=spark,
            version_table=version_table,
            node_table=node_table,
            paths_view=paths_view,
            flat_view=flat_view,
            dims_view=dims_view,
            reporting_view=reporting_view,
            nodes_dims_view=nodes_dims_view,
            nodes_reporting_view=nodes_reporting_view,
        )

    # -----------------------------------------------------------------------
    # Post-publish Spark validation
    # -----------------------------------------------------------------------
 
    def validate_published_version(
        self,
        spark,
        hierarchy_id: str,
        version: str,
        node_table: str,
        version_table: str,
    ) -> ValidationResult:
        """
        Validate a published hierarchy version against persisted Spark tables.
 
        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        hierarchy_id : str
            Hierarchy identifier to validate.
        version : str
            Version identifier to validate.
        node_table : str
            Fully qualified flattened node table name.
        version_table : str
            Fully qualified hierarchy version table name.
 
        Returns
        -------
        ValidationResult
            Structured validation result.
 
        Notes
        -----
        This validation is read-only and intended for audit/diagnostics.

        Normal publish protection should come from:
        1. `validate_definition(...)`
        2. `validate_pre_publish(...)`

        Use this method when you need to inspect already-persisted data for
        drift, manual edits, partial writes, or legacy cleanup.
        """
        logger.info(
            "Running post-publish audit validation for hierarchy_id=%s version=%s against node=%s version_table=%s",
            hierarchy_id,
            version,
            node_table,
            version_table,
        )
        validator = PostPublishHierarchyValidator(spark)
        return validator.validate_version(
            hierarchy_id=hierarchy_id,
            version=version,
            node_table=node_table,
            version_table=version_table,
        )
 
    def validate_published_version_strict(
        self,
        spark,
        hierarchy_id: str,
        version: str,
        node_table: str,
        version_table: str,
    ) -> ValidationResult:
        """
        Validate a published hierarchy version in strict mode.
 
        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        hierarchy_id : str
            Hierarchy identifier to validate.
        version : str
            Version identifier to validate.
        node_table : str
            Fully qualified flattened node table name.
        version_table : str
            Fully qualified hierarchy version table name.
 
        Returns
        -------
        ValidationResult
            Validation result when validation passes.
 
        Raises
        ------
        HierarchyValidationError
            Raised when post-publish validation fails.
        """
        result = self.validate_published_version(
            spark=spark,
            hierarchy_id=hierarchy_id,
            version=version,
            node_table=node_table,
            version_table=version_table,
        )
 
        if not result.passed:
            raise HierarchyValidationError(
                "Post-publish hierarchy validation failed.\n"
                + result.to_text()
            )
 
        return result
 
    # -----------------------------------------------------------------------
    # Rendering
    # -----------------------------------------------------------------------
 
    def render_tree(self, definition, show_keys: bool = True) -> str:
        """
        Render a hierarchy definition as an indented tree.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to render.
        show_keys : bool, default True
            Whether to include account keys.
 
        Returns
        -------
        str
            Indented tree representation.
        """
        return self.renderer.render(definition, show_keys=show_keys)
 
    # -----------------------------------------------------------------------
    # Comparison
    # -----------------------------------------------------------------------
 
    def compare_definitions(self, old_definition, new_definition) -> HierarchyDiffResult:
        """
        Compare two hierarchy definitions.
 
        Parameters
        ----------
        old_definition : HierarchyDefinition
            Baseline hierarchy definition.
        new_definition : HierarchyDefinition
            Proposed hierarchy definition.
 
        Returns
        -------
        HierarchyDiffResult
            Structured diff result.
        """
        return self.comparer.compare(old_definition, new_definition)
 
    def render_diff(self, old_definition, new_definition) -> str:
        """
        Compare two hierarchy definitions and render the diff as text.
 
        Parameters
        ----------
        old_definition : HierarchyDefinition
            Baseline hierarchy definition.
        new_definition : HierarchyDefinition
            Proposed hierarchy definition.
 
        Returns
        -------
        str
            Human-readable diff output.
        """
        diff = self.compare_definitions(old_definition, new_definition)
        return self.comparer.render_diff(diff)
 
    # -----------------------------------------------------------------------
    # YAML export
    # -----------------------------------------------------------------------
 
    def export_to_yaml(self, definition) -> str:
        """
        Export a hierarchy definition to YAML text.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to export.
 
        Returns
        -------
        str
            YAML string.
        """
        return self.exporter.to_yaml(definition)
 
    def write_yaml(self, definition, path: str) -> None:
        """
        Write a hierarchy definition to a YAML file.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to export.
        path : str
            Output file path.
        """
        self.exporter.write_yaml(definition, path)

    def _utc_now(self) -> str:
        """
        Return the current UTC timestamp in ISO-8601 string form.
        """
        return datetime.now(timezone.utc).isoformat()
