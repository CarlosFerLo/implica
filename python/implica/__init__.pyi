from typing import Dict

# --- TYPING ---

## -- Type -----
class BaseType :
    
    def uid(self) -> str : ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __eq__(self, value: object) -> bool: ...
    
class Variable(BaseType) :    
    name: str
    
    def __init__(self, name: str) -> None: ...
    
class Arrow(BaseType) :
    left: Type
    right: Type
    
    def __init__(self, left: Type, right: Type) -> None: ...
    
Type = Variable | Arrow

## -- Term -----
class BaseTerm :
    
    def uid(self) -> str : ...
    def type(self) -> Type: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __eq__(self, value: object) -> bool: ...
    def __call__(self, other: Term) -> Term: ...
    
class BasicTerm (BaseTerm) :
    
    name: str
    type: Type
    
class Application (BaseTerm) :
    function: Term
    argument: Term
    
Term = BasicTerm | Application

# --- Patterns -----

## -- TypeSchema ---
class TypeSchema :
    
    pattern: str
    
    def __init__(self, pattern: str) -> None: ...
    def matches(self, type: Type, context: Dict[str, Type | Term] = {}) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

## -- TermSchema ---
class TermSchema :
    
    pattern: str
    
    def __init__(self, pattern: str) -> None: ...
    def matches(self, term: Term, context: Dict[str, Type | Term] = {}) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    
