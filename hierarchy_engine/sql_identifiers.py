"""
Validation helpers for trusted Spark SQL object identifiers.
"""

from __future__ import annotations

import re

from hierarchy_engine.errors import HierarchyValidationError


_IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$"
)


def validate_sql_identifier(identifier: str, kind: str = "object") -> str:
    """
    Validate a trusted Spark SQL identifier such as a table or view name.
    """
    if not _IDENTIFIER_PATTERN.fullmatch(identifier):
        raise HierarchyValidationError(
            f"Invalid {kind} identifier '{identifier}'. Expected object, schema.object, or catalog.schema.object."
        )
    return identifier
