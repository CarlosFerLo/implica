"""
Implica - Type theoretical graph modeling library

This module provides tools for working with type theory and graph models.
"""

from .implica import (
    # Type system
    Variable,
    Application,
    # Terms
    Term,
    # Graph components
    Node,
    Edge,
    Graph,
    # Query system
    TypeSchema,
    NodePattern,
    EdgePattern,
    PathPattern,
    Query,
)

__all__ = [
    "Variable",
    "Application",
    "Term",
    "Node",
    "Edge",
    "Graph",
    "TypeSchema",
    "NodePattern",
    "EdgePattern",
    "PathPattern",
    "Query",
]
