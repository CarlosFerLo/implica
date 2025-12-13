from .implica import (
    Variable,
    Arrow,
    BasicTerm,
    Application,
    Constant,
    TypeSchema,
    TermSchema,
    NodePattern,
    EdgePattern,
    PathPattern,
    Node,
    Edge,
    Graph,
    Query,
)

from typing import Union

Type = Union[Variable, Arrow]
Term = Union[BasicTerm, Application]

__all__ = [
    "Variable",
    "Arrow",
    "Type",
    "Term",
    "Constant",
    "BasicTerm",
    "Application",
    "TypeSchema",
    "TermSchema",
    "NodePattern",
    "EdgePattern",
    "PathPattern",
    "Node",
    "Edge",
    "Graph",
    "Query",
]
