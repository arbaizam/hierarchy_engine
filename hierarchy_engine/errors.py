"""
Custom exceptions for the hierarchy engine.

"""


class HierarchyEngineError(Exception):
    """Base exception for hierarchy engine failures."""


class HierarchyParseError(HierarchyEngineError):
    """Raised when a hierarchy YAML file cannot be parsed or is malformed."""


class HierarchyValidationError(HierarchyEngineError):
    """Raised when a hierarchy definition fails structural validation."""


class HierarchyPublishError(HierarchyEngineError):
    """Raised when a hierarchy cannot be published to a target repository."""