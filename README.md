# Implica

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![Rust](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org/)

**Implica** is a high-performance, typed graph database library implemented in Rust with Python bindings. It features a unique type system inspired by simply typed lambda calculus, enabling powerful pattern matching and type-safe graph operations.

## Features

- 🚀 **High Performance**: Written in Rust with parallel processing via Rayon
- 🔒 **Type-Safe**: Strong typing system based on lambda calculus principles
- 🐍 **Python Bindings**: Native Python interface via PyO3
- 🔍 **Powerful Pattern Matching**: Cypher-inspired query syntax with type schemas
- 📊 **Properties Support**: Attach arbitrary JSON-like properties to nodes and edges
- 🔄 **Concurrent Access**: Thread-safe operations using DashMap

## Installation

### From PyPI (Coming Soon)

```bash
pip install implica
```

### From Source

#### Prerequisites

- Rust toolchain (1.70+)
- Python 3.8+
- [Maturin](https://github.com/PyO3/maturin)

#### Build Steps

```bash
# Clone the repository
git clone https://github.com/CarlosFerLo/implica.git
cd implica

# Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install development dependencies
pip install maturin

# Build and install
maturin develop --release
```

## Quick Start

```python
import implica

# Create a new graph
graph = implica.Graph()

# Create nodes with types
graph.query().create("(:A)").create("(:B)").execute()

# Create nodes with properties
graph.query().create("(:C { foo: 'var', number: 1 })").execute()

# Query nodes
result = graph.query().match("(N:C)").return_("N")
for row in result:
    print(row["N"])  # Node(C: {foo: "var", number: 1})
```

## Core Concepts

### Type System

Implica uses a type system based on **simply typed lambda calculus**. Types can be:

- **Variable Types**: Simple type identifiers like `A`, `B`, `Person`, `Company`
- **Arrow Types**: Function types like `A -> B`, representing a mapping from type `A` to type `B`

```python
# Simple types
graph.query().create("(:A)").execute()
graph.query().create("(:B)").execute()

# Arrow (function) types
graph.query().create("(:A -> B)").execute()  # Type representing "Person works at Company"

# Nested arrow types
graph.query().create("(:(A -> B) -> A)").execute()
```

### Terms and Constants

**Terms** are inhabitants of types. You can define **constants** that act as base terms with specific type signatures.

```python
# Define constants with their type signatures
constants = [
    implica.Constant("alice", "Person"),
    implica.Constant("worksAt", "Person -> Company"),
    implica.Constant("google", "Company"),
]

graph = implica.Graph(constants=constants)

# Create nodes with terms (using @ prefix for constant invocation)
graph.query().create("(:Person:@alice())").execute()
graph.query().create("(:Company:@google())").execute()
```

### Parametric Constants

Constants can have **type parameters**, enabling polymorphic definitions:

```python
# Parametric constant: works for any source and target types
constants = [
    implica.Constant("edge", "(A:*) -> (B:*)"),  # A and B are type parameters
]

graph = implica.Graph(constants=constants)

# Instantiate with specific types
graph.query().create("(:Person)").create("(:Company)").execute()
graph.query().create("()-[::@edge(Person, Company)]->()").execute()
```

## Pattern Syntax

Implica uses a pattern syntax inspired by Cypher (Neo4j's query language) with extensions for type schemas.

### Node Patterns

```
(variable:TypeSchema:TermSchema { properties })
```

| Component | Description | Example |
|-----------|-------------|---------|
| `variable` | Optional capture variable | `N`, `person`, `_` |
| `TypeSchema` | Type pattern to match | `Person`, `A -> B`, `*` |
| `TermSchema` | Term pattern to match | `alice`, `@f()`, `*` |
| `properties` | Property constraints | `{ name: 'Alice' }` |

#### Node Pattern Examples

```python
# Match all nodes
graph.query().match("()").return_()

# Match and capture all nodes
graph.query().match("(n:)").return_("n")

# Match nodes of specific type
graph.query().match("(p:Person)").return_("p")

# Match arrow types with wildcard
graph.query().match("(f:Person -> *)").return_("f")

# Match with type capture
graph.query().match("(n:(X:*) -> (Y:*))").return_("n", "X", "Y")

# Match with term constraint
graph.query().match("(n::alice)").return_("n")

# Match with properties
graph.query().match("(p:Person { age: 30 })").return_("p")

# Combined matching
graph.query().match("(p:Person:alice { active: true })").return_("p")
```

### Edge Patterns

```
(start)-[variable:TypeSchema:TermSchema { properties }]->(end)
```

#### Edge Pattern Examples

```python
# Define edge constant
constants = [implica.Constant("worksAt", "Person -> Company")]
graph = implica.Graph(constants=constants)

# Create nodes and edges
graph.query().create("(:Person)").create("(:Company)").execute()
graph.query().create("()-[::@worksAt()]->()").execute()

# Match all edges (forward direction)
graph.query().match("()-[]->()").return_()

# Match and capture edge
graph.query().match("()-[e]->()").return_("e")

# Match with endpoints
graph.query().match("(p:Person)-[e]->(c:Company)").return_("p", "e", "c")

# Match backward direction
graph.query().match("(c:Company)<-[e]-(p:Person)").return_("c", "e", "p")

# Match by edge type
graph.query().match("()-[e:Person -> Company]->()").return_("e")
```

### Path Patterns

Chain multiple nodes and edges:

```python
# Path with multiple edges
graph.query().match("(a)-[]->(b)-[]->(c)").return_("a", "b", "c")

# Mixed directions
graph.query().match("(a)-[]->(b)<-[]-(c)").return_("a", "b", "c")
```

## Query Operations

### CREATE

Create new nodes and edges in the graph:

```python
# Create a single node
graph.query().create("(:Person)").execute()

# Create with properties
graph.query().create("(:Person { name: 'Bob', age: 25 })").execute()

# Create with term
graph.query().create("(:Person:@bob())").execute()

# Create node and capture it
result = graph.query().create("(p:Person)").return_("p")

# Chain multiple creates
graph.query().create("(:Person)").create("(:Company)").execute()

# Create edge (nodes must exist or be created in same query)
graph.query().create("(:Person)-[::@worksAt()]->(:Company)").execute()
```

### MATCH

Query existing nodes and edges:

```python
# Basic matching
result = graph.query().match("(n:Person)").return_("n")

# Multiple match clauses (intersection semantics)
result = graph.query().match("(n:Person)").match("(n { age: 30 })").return_("n")

# Match and capture multiple elements
result = graph.query().match("(p)-[e]->(c)").return_("p", "e", "c")
```

### SET

Update properties on nodes and edges:

```python
# Overwrite all properties (default)
graph.query().match("(n:Person)").set("n", {"name": "Alice", "age": 31}).execute()

# Merge properties (preserve existing)
graph.query().match("(n:Person)").set("n", {"email": "alice@example.com"}, False).execute()

# Set edge properties
graph.query().match("()-[e]->()").set("e", {"since": 2020}).execute()
```

### REMOVE

Delete nodes and edges from the graph:

```python
# Remove matched nodes
graph.query().match("(n:Person { name: 'Bob' })").remove("n").execute()

# Remove edges
graph.query().match("()-[e]->()").remove("e").execute()
```

### RETURN

Retrieve results from the query:

```python
# Return matched elements
result = graph.query().match("(n)").return_("n")

# Return multiple variables
result = graph.query().match("(a)-[e]->(b)").return_("a", "e", "b")

# Return without variables (just execute matching)
result = graph.query().match("()").return_()

# Access results
for row in result:
    node = row["n"]
    print(node.uid())        # Unique identifier
    print(node.type())       # Type object
    print(node.term())       # Term object (or None)
    print(node.properties()) # Dict of properties
```

## API Reference

### Graph

```python
class Graph:
    def __init__(self, constants: List[Constant] = []) -> None:
        """Create a new graph with optional constants."""
        
    def query(self) -> Query:
        """Create a new query builder for this graph."""
        
    def nodes(self) -> List[Node]:
        """Get all nodes in the graph."""
        
    def edges(self) -> List[Edge]:
        """Get all edges in the graph."""
        
    def set_node_properties(self, map: Dict[str, Dict[str, Any]], overwrite: bool = True):
        """Bulk set properties on nodes by UID."""
        
    def set_edge_properties(self, map: Dict[Tuple[str, str], Dict[str, Any]], overwrite: bool = True):
        """Bulk set properties on edges by UID pair."""
```

### Query

```python
class Query:
    def match(self, pattern: str) -> Query:
        """Add a MATCH clause to the query."""
        
    def create(self, pattern: str) -> Query:
        """Add a CREATE clause to the query."""
        
    def remove(self, *variables: str) -> Query:
        """Remove the specified variables from the graph."""
        
    def set(self, variable: str, properties: Dict[str, Any], overwrite: bool = True) -> Query:
        """Set properties on a matched variable."""
        
    def execute(self) -> None:
        """Execute the query without returning results."""
        
    def return_(self, *variables: str) -> List[Dict[str, Element]]:
        """Execute the query and return specified variables."""
```

### Constant

```python
class Constant:
    name: str  # The constant's identifier
    
    def __init__(self, name: str, type_schema: str) -> None:
        """Create a constant with a name and type schema."""
```

### Node

```python
class Node:
    def uid(self) -> str:
        """Get the unique identifier (hex string)."""
        
    def type(self) -> Type:
        """Get the node's type."""
        
    def term(self) -> Optional[Term]:
        """Get the node's term (if any)."""
        
    def properties(self) -> Dict[str, Any]:
        """Get the node's properties."""
```

### Edge

```python
class Edge:
    def uid(self) -> Tuple[str, str]:
        """Get the edge's UID as (start_uid, end_uid)."""
        
    def type(self) -> Type:
        """Get the edge's type."""
        
    def term(self) -> Term:
        """Get the edge's term."""
        
    def properties(self) -> Dict[str, Any]:
        """Get the edge's properties."""
```

### Type & Term

```python
class Type:
    def uid(self) -> str:
        """Get the type's unique identifier."""

class Term:
    def uid(self) -> str:
        """Get the term's unique identifier."""
```

## Type Schemas

Type schemas define patterns for matching types:

| Pattern | Description | Example Match |
|---------|-------------|---------------|
| `A` | Exact type match | `A` |
| `*` | Wildcard (any type) | `A`, `B`, `X -> Y` |
| `A -> B` | Exact arrow type | `A -> B` |
| `A -> *` | Arrow with any target | `A -> B`, `A -> C` |
| `* -> B` | Arrow with any source | `A -> B`, `X -> B` |
| `* -> *` | Any arrow type | Any arrow type |
| `(A -> B) -> C` | Nested arrow | `(A -> B) -> C` |
| `(X:*)` | Capture any type as X | `A` (captures X=A) |
| `(X:*) -> (Y:*)` | Capture both sides | `A -> B` (captures X=A, Y=B) |
| `(X:A) -> *` | Capture specific type | `A -> B` (captures X=A) |

## Term Schemas

Term schemas define patterns for matching terms:

| Pattern | Description | Example |
|---------|-------------|---------|
| `f` | Match terms derived from constant `f` | Matches `f`, `(f a)`, etc. |
| `@f()` | Exact constant term | Only matches `f` |
| `@f() @a()` | Application pattern | Matches `(f a)` |
| `*` | Any term | Any term |

## Properties

Properties are JSON-like values attached to nodes and edges:

```python
# Supported property types
properties = {
    "string": "hello",
    "integer": 42,
    "float": 3.14,
    "boolean": True,
    "list": [1, 2, 3],
    "dict": {"nested": "value"},
}

# Create with properties
graph.query().create("(:Person { name: 'Alice', tags: ['developer', 'python'] })").execute()

# Match by properties
graph.query().match("(p:Person { name: 'Alice' })").return_("p")

# Update properties
graph.query().match("(p:Person)").set("p", {"age": 31}, False).execute()
```

## Development

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=implica
```

### Building Documentation

```bash
# Build Rust documentation
cargo doc --open
```

### Code Formatting

```bash
# Format Python code
black python/ tests/

# Format Rust code
cargo fmt
```

## Architecture

```
implica/
├── src/                    # Rust source code
│   ├── lib.rs             # PyO3 module definition
│   ├── graph/             # Graph data structure
│   │   ├── base.rs        # Core graph implementation
│   │   ├── create.rs      # CREATE operation
│   │   └── matches/       # Pattern matching logic
│   ├── patterns/          # Pattern parsing and compilation
│   ├── query/             # Query builder and execution
│   ├── typing/            # Type system implementation
│   └── utils/             # Utilities and helpers
├── python/                # Python package
│   └── implica/
│       ├── __init__.py    # Python exports
│       └── __init__.pyi   # Type stubs
└── tests/                 # Python test suite
```

## Performance

Implica leverages Rust's performance and safety guarantees:

- **Parallel matching**: Pattern matching uses Rayon for parallel iteration
- **Lock-free data structures**: DashMap provides concurrent access without global locks
- **Zero-copy where possible**: Efficient memory management with Arc references
- **Content-addressed storage**: Nodes identified by SHA-256 hashes of their types

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [PyO3](https://github.com/PyO3/pyo3) - Rust bindings for Python
- [Maturin](https://github.com/PyO3/maturin) - Build and publish Rust Python extensions
- [DashMap](https://github.com/xacrimon/dashmap) - Concurrent HashMap
- [Rayon](https://github.com/rayon-rs/rayon) - Data parallelism library

---

**Implica** - *Type-safe graph databases made easy*
