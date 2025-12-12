from typing import Dict, Optional, Any, List, Callable

# --- TYPING ---

## -- Type -----
class BaseType:
    def uid(self) -> str: ...
    def get_type_vars(self) -> List[Variable]: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __eq__(self, value: Type) -> bool: ...

class Variable(BaseType):
    name: str

    def __init__(self, name: str) -> None: ...

class Arrow(BaseType):
    left: Type
    right: Type

    def __init__(self, left: Type, right: Type) -> None: ...

Type = Variable | Arrow

## -- Term -----
class BaseTerm:
    def uid(self) -> str: ...
    def type(self) -> Type: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __eq__(self, value: Term) -> bool: ...
    def __call__(self, other: Term) -> Term: ...

class BasicTerm(BaseTerm):

    name: str
    type: Type

class Application(BaseTerm):
    function: Term
    argument: Term

Term = BasicTerm | Application

## -- Constant -----
class Constant:
    name: str

    def __init__(self, name: str, func: Callable) -> None: ...

# --- Graph -----

## -- Node ------

class Node:

    type: Type
    term: Optional[Term]
    properties: Dict[str, Any]

    def __init__(
        self, type: Type, term: Optional[Term] = None, properties: Dict[str, Any] = {}
    ) -> None: ...
    def uid(self) -> str: ...
    def __eq__(self, value: Node) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

## -- Edge ------
class Edge:

    term: Term
    start: Node
    end: Node

    def __init__(self, term: Term, start: Node, end: Node) -> None: ...
    def uid(self) -> str: ...
    def __eq__(self, value: Edge) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

## -- Graph ------
class Graph:
    def __init__(self) -> None: ...
    def query(self) -> Query: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def _get_all_nodes(self) -> List[Node]: ...  # discouraged for public use
    def _get_all_edges(self) -> List[Edge]: ...  # discouraged for public use

# --- Patterns -----

Context = Dict[str, Type | Term]

## -- TypeSchema ---
class TypeSchema:

    pattern: str

    def __init__(self, pattern: str) -> None: ...
    def matches(
        self, type: Type, context: Context = {}, constants: List[Constant] = []
    ) -> bool: ...
    def get_type_vars(self, context: Context = {}) -> List[Variable]: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

## -- TermSchema ---
class TermSchema:

    pattern: str

    def __init__(self, pattern: str) -> None: ...
    def matches(
        self, term: Term, context: Context = {}, constants: List[Constant] = []
    ) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

## -- NodePattern ---
class NodePattern:

    variable: Optional[str]

    type: Optional[TypeSchema]
    type_schema: Optional[TypeSchema]

    term: Optional[Term]
    term_schema: Optional[TermSchema]

    properties: Dict[str, Any]

    def __init__(
        self,
        variable: Optional[str] = None,
        type: Optional[TypeSchema] = None,
        type_schema: Optional[TypeSchema] = None,
        term: Optional[Term] = None,
        term_schema: Optional[TermSchema] = None,
        properties: Dict[str, Any] = {},
    ) -> None: ...
    def matches(self, node, context: Context = {}, constants: List[Constant] = []) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class EdgePattern:

    variable: Optional[str]

    type: Optional[Type]
    type_schema: Optional[TypeSchema]

    term: Optional[Term]
    term_schema: Optional[TermSchema]

    properties: Dict[str, Any]

    direction: str

    def __init__(
        self,
        variable: Optional[str] = None,
        type: Optional[Type] = None,
        type_schema: Optional[TypeSchema] = None,
        term: Optional[Term] = None,
        term_schema: Optional[TermSchema] = None,
        properties: Dict[str, Any] = {},
        direction: str = "forward",
    ) -> None: ...
    def matches(self, edge, context: Context = {}, constants: List[Constant] = []) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

## -- PathPattern ---
class PathPattern:

    nodes: list[NodePattern]
    edges: list[EdgePattern]

    def __init__(self, pattern: Optional[str] = None) -> None: ...
    def add_node(self, node_pattern: NodePattern) -> None: ...
    def add_edge(self, edge_pattern: EdgePattern) -> None: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

# --- Query -----

QueryResult = Node | Edge

class Query:
    def __init__(self, graph: Graph) -> None: ...
    def match(
        self,
        pattern: Optional[str] = None,
        node: Optional[str] = None,
        edge: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        type: Optional[Type] = None,
        type_schema: Optional[TypeSchema] = None,
        term: Optional[Term] = None,
        term_schema: Optional[TermSchema] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> "Query": ...
    def where(self, condition: str) -> "Query": ...
    def create(
        self,
        pattern: Optional[str] = None,
        node: Optional[str] = None,
        edge: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        type: Optional[Type] = None,
        type_schema: Optional[TypeSchema] = None,
        term: Optional[Term] = None,
        term_schema: Optional[TermSchema] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> "Query": ...
    def merge(
        self,
        pattern: Optional[str] = None,
        node: Optional[str] = None,
        edge: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        type: Optional[Type] = None,
        type_schema: Optional[TypeSchema] = None,
        term: Optional[Term] = None,
        term_schema: Optional[TermSchema] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> "Query": ...
    def add(
        self, variable: str, type: Optional[Type] = None, term: Optional[Term] = None
    ) -> "Query": ...
    def set(self, variable: str, properties: Dict[str, Any]) -> "Query": ...
    def delete(self, *variables: str) -> "Query": ...
    def with_(self, *variables: str) -> "Query": ...
    def order_by(self, *variables: str, ascending: bool = True) -> "Query": ...
    def limit(self, count: int) -> "Query": ...
    def skip(self, count: int) -> "Query": ...
    def execute(self) -> "Query": ...
    def return_(self, *variables: str) -> List[Dict[str, QueryResult]]: ...
    def return_count(self) -> int: ...
