from typing import Dict, Optional, Any

# --- TYPING ---

## -- Type -----
class BaseType:
    def uid(self) -> str: ...
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

# --- Patterns -----

## -- TypeSchema ---
class TypeSchema:

    pattern: str

    def __init__(self, pattern: str) -> None: ...
    def matches(self, type: Type, context: Dict[str, Type | Term] = {}) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

## -- TermSchema ---
class TermSchema:

    pattern: str

    def __init__(self, pattern: str) -> None: ...
    def matches(self, term: Term, context: Dict[str, Type | Term] = {}) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

## TODO: Add NodePattern, EdgePattern, PathPattern classes here

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
