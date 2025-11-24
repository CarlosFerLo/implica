"""
Implica - Type theoretical graph modeling library

This module provides tools for working with type theory and graph models.
"""

from .implica import (
    # Type system
    Variable,
    Arrow,
    # Terms
    Term,
    # Graph components
    Node,
    Edge,
    Graph,
    # Graph configuration
    IndexConfig,
    # Query system
    TypeSchema,
    NodePattern,
    EdgePattern,
    PathPattern,
    Query,
)

__all__ = [
    "Variable",
    "Arrow",
    "Term",
    "Node",
    "Edge",
    "Graph",
    "IndexConfig",
    "TypeSchema",
    "NodePattern",
    "EdgePattern",
    "PathPattern",
    "Query",
]
