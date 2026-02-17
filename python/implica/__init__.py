from typing import Union

from .implica import Graph, Query, Edge, Node, Term, Type, Constant

Element = Union[Edge, Node, Term, Type]

__all__ = ["Graph", "Query", "Edge", "Node", "Term", "Type", "Element", "Constant"]
