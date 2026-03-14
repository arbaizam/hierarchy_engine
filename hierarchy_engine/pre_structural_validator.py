"""
Hierarchy validation logic.
 
This module validates hierarchy definitions before they are flattened or published.
 
Validation philosophy
---------------------
Validation is split into three blocking layers:
 
1. In-memory structural validation
   Performed directly on the loaded HierarchyDefinition object before anything
   is written to persistence.
 
2. Post-structural validation
   Performed after flattening and before persistence. This layer validates the
   adjacency-list publish artifact itself.

3. Pre-write persistence validation
   Performed by the service layer against persisted tables before any write is
   attempted. That layer catches publish-time conflicts that are not visible in
   the in-memory or flattened object models.

Optional audit validation can still be performed later against persisted Spark
tables after publish, but that is not this validator's responsibility.
 
This validator focuses on the first layer only.
 
Validation goals
----------------
We want to catch issues as early as possible, especially:
 
- invalid or incomplete metadata
- empty hierarchy definitions
- duplicate account keys
- cycles in the nested tree
- malformed node content
 
Why return ValidationResult instead of only raising exceptions?
---------------------------------------------------------------
Returning a structured ValidationResult gives the project much more flexibility:
 
- notebooks can print issues without crashing immediately
- APIs can return clean payloads
- a future UI can render friendly validation messages
- tests can assert on exact validation outcomes
- publish workflows can decide whether warnings are acceptable
 
The service layer can still choose to raise an exception when strict behavior
is desired.
"""
 
from __future__ import annotations
 
from datetime import date

from hierarchy_engine.models import (
    HierarchyDefinition,
    HierarchyNode,
    ValidationResult,
)
 
class HierarchyValidator:
    """
    Validate hierarchy definitions prior to flattening and publishing.
 
    Notes
    -----
    This validator is intentionally responsible only for object-level validation.
    It does not depend on Spark, SQL views, flattened rows, or persisted tables.
    """
 
    VALID_VERSION_STATUS = {"draft", "published", "retired"}
 
    def validate(self, definition: HierarchyDefinition) -> ValidationResult:
        """
        Validate a full hierarchy definition.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition to validate.
 
        Returns
        -------
        ValidationResult
            Structured validation result containing all discovered issues.
 
        Notes
        -----
        This method runs all validation checks and accumulates issues instead
        of failing on the first problem. That behavior is generally friendlier
        for developers and future UI users because it surfaces all detectable
        issues in a single pass.
        """
        result = ValidationResult()
 
        self._validate_metadata(definition, result)
        self._validate_nodes_present(definition, result)
        self._validate_node_content(definition, result)
        self._validate_duplicate_keys(definition, result)
        self._validate_cycles(definition, result)
 
        return result.finalize()
 
    def _iter_child_nodes(
        self,
        node: HierarchyNode,
    ) -> list[HierarchyNode]:
        """
        Return only valid child nodes for recursive traversals.
        """
        if not isinstance(node.children, list):
            return []
 
        return [child for child in node.children if isinstance(child, HierarchyNode)]
 
    # -----------------------------------------------------------------------
    # Metadata validation
    # -----------------------------------------------------------------------
 
    def _validate_metadata(
        self,
        definition: HierarchyDefinition,
        result: ValidationResult,
    ) -> None:
        """
        Validate top-level hierarchy metadata.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition being validated.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        This method records issues rather than raising directly so that
        validation can continue and report multiple problems in one run.
        """
        meta = definition.metadata
 
        if not meta.hierarchy_id:
            result.add_issue(
                severity="ERROR",
                check_name="missing_hierarchy_id",
                message="hierarchy_id must not be empty",
            )
 
        if not meta.hierarchy_name:
            result.add_issue(
                severity="ERROR",
                check_name="missing_hierarchy_name",
                message="hierarchy_name must not be empty",
            )
 
        if not meta.version_id:
            result.add_issue(
                severity="ERROR",
                check_name="missing_version_id",
                message="version_id must not be empty",
            )
 
        if not meta.version_name:
            result.add_issue(
                severity="ERROR",
                check_name="missing_version_name",
                message="version_name must not be empty",
            )
 
        if not meta.hierarchy_description:
            result.add_issue(
                severity="ERROR",
                check_name="missing_hierarchy_description",
                message="hierarchy_description is empty",
            )
 
        if not meta.owner_team:
            result.add_issue(
                severity="ERROR",
                check_name="missing_owner_team",
                message="owner_team is empty",
            )
 
        if not meta.business_domain:
            result.add_issue(
                severity="ERROR",
                check_name="missing_business_domain",
                message="business_domain is empty",
            )
 
        if meta.version_status not in self.VALID_VERSION_STATUS:
            result.add_issue(
                severity="ERROR",
                check_name="invalid_version_status",
                message=f"version_status '{meta.version_status}' is invalid",
                details={"allowed_values": sorted(self.VALID_VERSION_STATUS)},
            )
 
        if meta.effective_start_date is None:
            result.add_issue(
                severity="ERROR",
                check_name="missing_effective_start_date",
                message="effective_start_date must not be null",
            )
 
        if (
            isinstance(meta.effective_start_date, date)
            and isinstance(meta.effective_end_date, date)
            and meta.effective_end_date < meta.effective_start_date
        ):
            result.add_issue(
                severity="ERROR",
                check_name="invalid_effective_date_range",
                message="effective_end_date cannot be before effective_start_date",
                details={
                    "effective_start_date": meta.effective_start_date.isoformat(),
                    "effective_end_date": meta.effective_end_date.isoformat(),
                },
            )
 
    # -----------------------------------------------------------------------
    # Root / presence validation
    # -----------------------------------------------------------------------
 
    def _validate_nodes_present(
        self,
        definition: HierarchyDefinition,
        result: ValidationResult,
    ) -> None:
        """
        Ensure the hierarchy contains at least one root node.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition being validated.
        result : ValidationResult
            Mutable validation result accumulator.
        """
        if not definition.nodes:
            result.add_issue(
                severity="ERROR",
                check_name="missing_root_nodes",
                message="Hierarchy must contain at least one root node",
            )
 
    # -----------------------------------------------------------------------
    # Duplicate key validation
    # -----------------------------------------------------------------------
 
    def _validate_duplicate_keys(
        self,
        definition: HierarchyDefinition,
        result: ValidationResult,
    ) -> None:
        """
        Detect duplicate account keys in the nested hierarchy tree.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition being validated.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        This is a recursive tree walk.
 
        Logic:
        1. visit the current node
        2. check whether its key has been seen before
        3. recurse into all children
 
        Because account_key must be unique within a hierarchy version, any
        repeated key is recorded as an error.
        """
        seen: set[str] = set()
 
        visited_nodes: set[int] = set()
 
        def walk(node: HierarchyNode) -> None:
            node_id = id(node)
            if node_id in visited_nodes:
                return
            visited_nodes.add(node_id)
 
            if node.account_key in seen:
                result.add_issue(
                    severity="ERROR",
                    check_name="duplicate_account_key",
                    message=f"Duplicate account_key detected: {node.account_key}",
                    details={"account_key": node.account_key},
                )
            else:
                seen.add(node.account_key)
 
            for child in self._iter_child_nodes(node):
                walk(child)
 
        for root in definition.nodes:
            walk(root)
 
    # -----------------------------------------------------------------------
    # Cycle validation
    # -----------------------------------------------------------------------
 
    def _validate_cycles(
        self,
        definition: HierarchyDefinition,
        result: ValidationResult,
    ) -> None:
        """
        Detect cycles in the nested hierarchy tree.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition being validated.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        In a normal YAML tree, cycles are uncommon, but they are still worth
        checking because they can arise from:
 
        - programmatically created hierarchy objects
        - accidental object reuse
        - future UI bugs
        - malformed in-memory structures
 
        Recursive cycle detection approach:
        -----------------------------------
        We track the active recursion path, not just a global set.
 
        Why?
        A global set would incorrectly flag legitimate repeated visits in some
        graph structures. For tree validation, what we really care about is:
        "Did this node appear again in the current active path of descent?"
 
        So:
        - when entering a node, add its key to the active path
        - recurse into children using a copied/extended path
        - if a child key already exists in the active path, a cycle exists
        """
        def walk(node: HierarchyNode, active_path: set[str]) -> None:
            if node.account_key in active_path:
                result.add_issue(
                    severity="ERROR",
                    check_name="cycle_detected",
                    message=f"Cycle detected at account_key: {node.account_key}",
                    details={"account_key": node.account_key},
                )
                # Once a cycle is found at this node, stop descending this branch.
                return
 
            next_path = set(active_path)
            next_path.add(node.account_key)
 
            for child in self._iter_child_nodes(node):
                walk(child, next_path)
 
        for root in definition.nodes:
            walk(root, set())
 
    # -----------------------------------------------------------------------
    # Node content validation
    # -----------------------------------------------------------------------
 
    def _validate_node_content(
        self,
        definition: HierarchyDefinition,
        result: ValidationResult,
    ) -> None:
        """
        Validate required content on each node.
 
        Parameters
        ----------
        definition : HierarchyDefinition
            Hierarchy definition being validated.
        result : ValidationResult
            Mutable validation result accumulator.
 
        Notes
        -----
        This check ensures that every node has the required minimal content
        needed for downstream flattening and publishing.
        """
        def walk(node: HierarchyNode, active_nodes: set[int]) -> None:
            if not isinstance(node, HierarchyNode):
                result.add_issue(
                    severity="ERROR",
                    check_name="invalid_node_object",
                    message="A node is not a HierarchyNode instance",
                    details={"node_type": type(node).__name__},
                )
                return
 
            node_id = id(node)
            if node_id in active_nodes:
                return
 
            next_active_nodes = set(active_nodes)
            next_active_nodes.add(node_id)
 
            if not node.account_key:
                result.add_issue(
                    severity="ERROR",
                    check_name="missing_account_key",
                    message="A node is missing account_key",
                    details={"account_name": node.account_name},
                )
 
            if not node.account_name:
                result.add_issue(
                    severity="ERROR",
                    check_name="missing_account_name",
                    message=f"Node '{node.account_key}' is missing account_name",
                    details={"account_key": node.account_key},
                )
 
            if node.children is None:
                result.add_issue(
                    severity="ERROR",
                    check_name="invalid_children_collection",
                    message=(
                        f"Node '{node.account_key}' has children=None; "
                        "children must be a list"
                    ),
                    details={"account_key": node.account_key},
                )
                return
 
            if not isinstance(node.children, list):
                result.add_issue(
                    severity="ERROR",
                    check_name="invalid_children_collection",
                    message=(
                        f"Node '{node.account_key}' has non-list children; "
                        "children must be a list"
                    ),
                    details={
                        "account_key": node.account_key,
                        "children_type": type(node.children).__name__,
                    },
                )
                return

            children = node.children or []
            for child in children:
                if not isinstance(child, HierarchyNode):
                    result.add_issue(
                        severity="ERROR",
                        check_name="invalid_child_node",
                        message=(
                            f"Node '{node.account_key}' contains a non-HierarchyNode child"
                        ),
                        details={
                            "account_key": node.account_key,
                            "child_type": type(child).__name__,
                        },
                    )
                    continue
                walk(child, next_active_nodes)
 
        for root in definition.nodes:
            walk(root, set())
