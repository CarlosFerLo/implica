# Implica

A Rust-powered Python library for type theoretical graph modeling with Cypher-like querying capabilities.

## Features

- **Type Theory**: Full support for type theoretical constructs (Variables, Applications, Terms)
- **Graph Modeling**: Represent type theoretical models as graphs with Nodes and Edges
- **Cypher-like Queries**: Intuitive query language inspired by Neo4j's Cypher
- **Type Schema Patterns**: Powerful pattern matching for types
- **High Performance**: Implemented in Rust with PyO3 bindings
- **Optimized Search**: SHA256-based UIDs and indexed lookups for O(1) performance
- **Scalable**: Efficient handling of large graphs with smart indexing

## Installation

```bash
# Development installation
maturin develop

# Build wheel
maturin build --release
```

## Quick Start

### Basic Types

```python
import implica

# Create type variables
var_a = implica.Variable("A")
var_b = implica.Variable("B")

# Create function types (applications)
func_type = implica.Application(var_a, var_b)  # A -> B
print(func_type)  # (A -> B)
```

### Terms

```python
# Create terms with types
f = implica.Term("f", implica.Application(var_a, var_b))
x = implica.Term("x", var_a)

# Apply terms
result = f(x)  # If f : A -> B and x : A, then (f x) : B
print(result.name)  # (f x)
```

### Graphs

```python
# Create a graph
graph = implica.Graph()

# Create nodes (types in the model)
node_a = implica.Node(var_a, properties={"label": "Type A"})
node_b = implica.Node(var_b, properties={"label": "Type B"})

# Create edges (terms in the model)
term = implica.Term("f", func_type)
edge = implica.Edge(term, node_a, node_b, properties={"weight": 1.0})
```

### Type Schemas

Type schemas provide pattern matching for types:

```python
# Wildcard - matches any type
schema = implica.TypeSchema("$*$")
schema.matches(var_a)  # True
schema.matches(func_type)  # True

# Specific variable
schema = implica.TypeSchema("$A$")
schema.matches(var_a)  # True
schema.matches(var_b)  # False

# Application patterns
schema = implica.TypeSchema("$A -> B$")
schema.matches(func_type)  # True

schema = implica.TypeSchema("$A -> *$")  # A -> anything
schema.matches(func_type)  # True

schema = implica.TypeSchema("$* -> *$")  # any -> any
schema.matches(func_type)  # True

# Capture patterns
schema = implica.TypeSchema("$(x:*) -> $(y:*)$")
captures = schema.capture(func_type)  # {"x": A, "y": B}
```

### Cypher-like Queries

#### Creating Nodes

```python
graph = implica.Graph()

# Programmatic creation
var_a = implica.Variable("A")
graph.query().create(node="n", type=var_a, properties={"name": "test"}).execute()

# Pattern syntax
graph.query().create("(n:A {name: 'test'})").execute()
```

#### Matching Nodes

```python
# Find all nodes of type A
results = graph.query().match("(n:A)").return_("n")

# With type schemas
results = graph.query().match("(n:$A -> B$)").return_("n")

# Programmatic matching
results = graph.query().match(node="n", type=var_a).return_("n")

# With properties filter
results = (graph.query()
    .match(node="n", type=var_a)
    .where("n.properties['value'] > 10")
    .return_("n"))
```

#### Path Queries

```python
# Simple path
results = graph.query().match("(n:A)-[e]->(m:B)").return_("n", "e", "m")

# With type schemas for edges
results = graph.query().match("(n:A)-[f:$A -> B$]->(m:B)").return_("n", "f", "m")

# Complex paths
results = graph.query().match("(a)-[e1]->(b)<-[e2]-(c)").return_("a", "b", "c")
```

#### Advanced Queries

```python
# Count
count = graph.query().match("(n:$*$ -> $*$)").return_count()

# Distinct results
results = graph.query().match("(n)").return_distinct("n")

# Ordering and pagination
results = (graph.query()
    .match("(n:A)")
    .order_by("n", "properties['name']")
    .skip(10)
    .limit(10)
    .return_("n"))

# Update properties
graph.query().match("(n:A)").set("n", {"status": "updated"}).execute()

# Delete
graph.query().match("(n:A)").where("n.properties['temp']").delete("n").execute()

# Delete with edges
graph.query().match("(n:A)").delete("n", detach=True).execute()

# Merge (create if not exists)
graph.query().merge("(n:A {id: 1})").execute()
```

## API Structure

### Types

- `Variable(name: str)` - Type variable
- `Application(left: Type, right: Type)` - Function type

### Terms

- `Term(name: str, type: Type)` - Type theoretical term
- `Term.__call__(other: Term) -> Term` - Term application

### Graph

- `Graph()` - Graph container
- `Node(type: Type, properties: dict)` - Node in graph
- `Edge(term: Term, start: Node, end: Node, properties: dict)` - Edge in graph

### Patterns

- `TypeSchema(pattern: str)` - Type pattern matcher
- `NodePattern(...)` - Node pattern for queries
- `EdgePattern(...)` - Edge pattern for queries
- `PathPattern(pattern: str)` - Path pattern parser

### Query

- `Graph.query() -> Query` - Create query builder
- Query methods: `match()`, `where()`, `create()`, `set()`, `delete()`, `merge()`, `return_()`, etc.

## Architecture

The library is implemented in Rust for performance and uses PyO3 for Python bindings. The architecture consists of:

- **types.rs** - Core type system (Variable, Application) with SHA256 UIDs
- **term.rs** - Term implementation and application with SHA256 UIDs
- **graph.rs** - Graph, Node, and Edge structures with optimized search methods
- **type_schema.rs** - Pattern matching for types
- **patterns.rs** - Query pattern structures
- **query.rs** - Cypher-like query builder
- **lib.rs** - Python module definition

### Performance Optimizations

- **SHA256 UIDs**: All elements (Variable, Application, Term, Node, Edge) use SHA256 hashes for unique identification
- **O(1) Lookups**: Node and edge lookups by UID use Python dictionaries for constant-time access
- **Type Indexing**: `build_type_index()` creates dynamic indices for fast type-based queries
- **Cached UIDs**: UIDs are computed once and cached to avoid redundant hash calculations

Example of optimized search:

```python
# Direct UID lookup - O(1)
node = graph.get_node_by_uid(uid)

# Type-based search - O(k) where k = nodes of that type
type_uid = implica.Variable("Person").uid()
person_nodes = graph.get_nodes_by_type(type_uid)
```

## Development

### Setup

1. Clone the repository
2. Install development dependencies:

```bash
# Install Python dependencies
pip install -e ".[dev]"

# Set up Git hooks (recommended)
./setup-hooks.sh
```

The Git hooks will automatically:

- Format Rust code with `cargo fmt`
- Check Rust code with `cargo clippy`
- Format Python code with `black`
- Prevent commits if linting issues can't be auto-fixed

This ensures all code follows the project's style guidelines before committing.

### Build

```bash
# Development build
maturin develop

# Release build
maturin build --release
```

### Test

```bash
# Rust tests
cargo test

# Python tests
pytest tests/

# Run all tests
python test_api.py
```

### Linting

```bash
# Format Rust code
cargo fmt --all

# Check Rust code
cargo clippy --all-targets --all-features -- -D warnings

# Format Python code
black .

# Check Python formatting
black --check .
```

## License

This project is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
