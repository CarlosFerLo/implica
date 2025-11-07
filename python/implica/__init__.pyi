from typing import Dict, Any

## Types
class BaseType:
    """
    Represents a type theoretical type.
    """

    # Methods to be implemented in subclasses
    def uid(self) -> str: ...  # cached_
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class Variable(BaseType):
    name: str

    def __init__(self, name: str) -> None: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...

class Application(BaseType):
    left: "Type"
    right: "Type"

    def __init__(self, left: "Type", right: "Type") -> None: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...

Type = Variable | Application

## Terms_

class Term:
    """
    Represents a type theoretical term.
    """

    name: str
    type: Type

    def __init__(self, name: str, type: Type) -> None: ...
    def uid(self) -> str: ...  # Only depends on name + type + cached_
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __call__(self, other: "Term") -> "Term":
        """
        If the self has an application type and other has the corresponding self.type.left type,
        then return a term of type self.type.right and with name (self.name other.name).
        """
        ...

## Graph_

### Node_

class Node:
    """
    Represents a type in the model of the corresponding theory represented as a graph.
    """

    type: Type  # Immutable_
    properties: Dict[str, Any]  # Mutable_

    def __init__(self, type: Type, properties: Dict[str, Any] = {}) -> None: ...
    def uid(self) -> str: ...  # Only depends on type + cached_
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

### Edge_

class Edge:
    """
    Represents a term in the model of the corresponding theory represented as a graph.
    """

    term: Term  # Immutable_
    start: Node  # Immutable_
    end: Node  # Immutable_

    properties: Dict[str, Any]  # Mutable_

    def __init__(
        self,
        term: Term,
        start: Node,
        end: Node,
        properties: Dict[str, Any] = {},
    ) -> None: ...
    def uid(self) -> str: ...  # Only depends on term + cached_
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

### Graph_

class Graph:
    """
    Represents the model of a type theoretical theory as a graph.
    """

    nodes: dict[str, Node]  # uid -> Node_
    edges: dict[str, Edge]  # uid -> Edge_

    def __init__(self) -> None: ...
    def query(self) -> "Query": ...

## Cypher Like querying_

### Type Schemas_

class TypeSchema:
    """
    Represents a regex-like pattern for matching types.
    Can capture type variables and match complex type structures.

    Syntax:
    - Variable matching: $name$ matches any Variable with that name
    - Wildcard: $*$ matches any type
    - Capture: $(name:pattern)$ captures matched type as variable
    - Application: $(a -> b)$ matches Application types

    Examples:
    - "$A$" matches Variable("A")
    - "$*$" matches any type
    - "$(x:*)$ -> $(y:*)$" matches any Application
    - "$A$ -> $*$" matches Application with left Variable("A")
    """

    pattern: str

    def __init__(self, pattern: str) -> None: ...
    def matches(self, type: Type) -> bool: ...
    def capture(self, type: Type) -> Dict[str, Type]: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

### Query Components_

class NodePattern:
    """
    Represents a node pattern in a Cypher query.

    Examples:
    - (n) - matches any node, binds to variable ’n’
    - (n:Type) - matches nodes with specific type
    - (n:$schema$) - matches nodes with type matching schema
    - (n {prop: value}) - matches nodes with specific properties
    """

    variable: str | None  # Variable name to bind the node_
    type: Type | None  # Exact type to match_
    type_schema: TypeSchema | None  # Type schema pattern to match_
    properties: Dict[str, Any]  # Properties to match_

    def __init__(
        self,
        variable: str | None = None,
        type: Type | None = None,
        type_schema: str | TypeSchema | None = None,
        properties: Dict[str, Any] = {},
    ) -> None: ...

class EdgePattern:
    """
    Represents an edge/relationship pattern in a Cypher query.

    Examples:
    - -[r]-> matches any edge, binds to variable ‘r’
    - -[r:term]-> matches edges with specific term
    - -[r:$schema$]-> matches edges with term type matching schema
    - -[r {prop: value}]-> matches edges with specific properties
    - -[r]- matches edge in any direction
    - <-[r]- matches edge in reverse direction
    """

    variable: str | None  # Variable name to bind the edge_
    term: Term | None  # Exact term to match_
    term_type_schema: TypeSchema | None  # Term type schema pattern to match_
    properties: Dict[str, Any]  # Properties to match_
    direction: str  # "forward" (->), "backward" (<-), or "any" (-)_

    def __init__(
        self,
        variable: str | None = None,
        term: Term | None = None,
        term_type_schema: str | TypeSchema | None = None,
        properties: Dict[str, Any] = {},
        direction: str = "forward",
    ) -> None: ...

class PathPattern:
    """
    Represents a path pattern: (node)-[edge]->(node)-[edge]->...

    Can be created from a Cypher-like string pattern or built programmatically.

    String pattern syntax:
    - Nodes: (var:type) or (var:$schema$) or (var) or (:type)
    - Edges: -[var:term]-> or -[var:$schema$]-> or -[var]-> or -[]->
    - Reverse: <-[var]-
    - Bidirectional: -[var]-

    Examples:
    - "(n:A)" - node n of type A
    - "(n:$A -> B$)" - node n matching type schema
    - "(n)-[e]->(m)" - any edge from n to m
    - "(n:A)-[f:$A -> B$]->(m:B)" - specific typed path
    - "(start)-[e1]->(middle)<-[e2]-(end)" - path with reverse edge
    """

    nodes: list[NodePattern]
    edges: list[EdgePattern]

    def __init__(self, pattern: str | None = None) -> None: ...
    def add_node(self, pattern: NodePattern) -> "PathPattern": ...
    def add_edge(self, pattern: EdgePattern) -> "PathPattern": ...
    @staticmethod
    def parse(pattern: str) -> "PathPattern":
        """
        Parse a Cypher-like path pattern string.

        Examples:
        - "(n:A)-[e]->(m:B)"
        - "(n:$*$ -> $*$)-[f:$A -> B$]->(m)"
        - "(a)-[e1]->(b)<-[e2]-(c)"
        """
        ...

### Query Builder_

class Query:
    """
    Cypher-like query builder for the graph.

    IDIOMATIC USAGE (Recommended - Cypher-like strings):

    # Find all nodes of a specific type
    results = graph.query().match("(n:A)").return_("n")

    # Find paths with type schemas
    results = graph.query().match("(n:A)-[f:$A -> B$]->(m:B)").return_("n", "f", "m")

    # Complex path patterns
    results = (graph.query()
        .match("(start:A)-[e1]->(middle)<-[e2]-(end:B)")
        .where("middle.properties[‘value’] > 10")
        .return_("start", "middle", "end"))

    # Match with wildcards
    results = graph.query().match("(n:$*$ -> $*$)").return_("n")

    # Create nodes and edges
    graph.query().create("(n:A {name: ‘test’})").execute()
    graph.query().match("(n:A)").match("(m:B)").create("(n)-[e:f]->(m)").execute()

    PROGRAMMATIC USAGE (Alternative):

    # Find nodes using keyword arguments
    results = graph.query().match(node="n", type=my_type).return_("n")

    # Create edges programmatically
    graph.query().create(
        edge="e",
        term=my_term,
        start=start_node,
        end=end_node
    )
    """

    graph: Graph

    def __init__(self, graph: Graph) -> None: ...

    # MATCH clause - for reading patterns_
    def match(
        self,
        pattern: str | PathPattern | None = None,
        *,
        node: str | None = None,
        edge: str | None = None,
        start: str | Node | None = None,
        end: str | Node | None = None,
        type: Type | None = None,
        type_schema: str | TypeSchema | None = None,
        term: Term | None = None,
        term_type_schema: str | TypeSchema | None = None,
        properties: Dict[str, Any] = {},
    ) -> "Query":
        """
        Match nodes, edges, or paths in the graph.

        IDIOMATIC USAGE (Cypher-like string patterns):
        - .match("(n:A)")  # Match node n of type A
        - .match("(n:A)-[e]->(m:B)")  # Match path from A to B
        - .match("(n:$A -> B$)")  # Match node with type schema
        - .match("(n:A)-[f:$A -> B$]->(m:B)")  # Match typed path
        - .match("(n)-[e1]->(m)<-[e2]-(p)")  # Complex path with reverse edge
        - .match("(:A)-[]->(m)")  # Anonymous node, any edge to m

        PROGRAMMATIC USAGE (keyword arguments):

        For nodes:
        - node: variable name to bind
        - type: exact type to match
        - type_schema: type schema pattern (e.g., "$A$ -> $*$")
        - properties: properties to match

        For edges:
        - edge: variable name to bind
        - start: start node (variable name or Node)
        - end: end node (variable name or Node)
        - term: exact term to match
        - term_type_schema: term type schema pattern
        - properties: properties to match

        Examples:
        - .match(node="n", type=Variable("A"))
        - .match(node="n", type_schema="$*$ -> $*$")
        - .match(edge="e", start="n1", end="n2")
        - .match(edge="e", start=node_obj, end="n2", term_type_schema="$A$ -> $B$")
        """
        ...
    # WHERE clause - for filtering_
    def where(self, condition: str) -> "Query":
        """
        Add filtering conditions.

        Condition syntax supports Python expressions with variables bound in MATCH.

        Examples:
        - .where("n.properties[‘name’] == ‘test’")
        - .where("n.type == Variable(‘A’)")
        - .where("len(e.term.name) > 5")
        """
        ...
    # RETURN clause - for projecting results_
    def return_(self, *variables: str) -> list[Dict[str, Node | Edge]]:
        """
        Return matched variables.

        Returns a list of dictionaries mapping variable names to matched nodes/edges.

        Examples:
        - .return_("n") -> [{"n": node1}, {"n": node2}, ...]
        - .return_("n", "e") -> [{"n": node1, "e": edge1}, ...]
        """
        ...

    def return_count(self) -> int:
        """
        Return count of matches.
        """
        ...

    def return_distinct(self, *variables: str) -> list[Dict[str, Node | Edge]]:
        """
        Return distinct matched variables.
        """
        ...
    # CREATE clause - for creating nodes/edges_
    def create(
        self,
        pattern: str | PathPattern | None = None,
        *,
        node: str | None = None,
        edge: str | None = None,
        type: Type | None = None,
        term: Term | None = None,
        start: str | Node | None = None,
        end: str | Node | None = None,
        properties: Dict[str, Any] = {},
    ) -> "Query":
        """
        Create nodes or edges.

        IDIOMATIC USAGE (Cypher-like string patterns):
        - .create("(n:A)")  # Create node n of type A
        - .create("(n:A {name: ‘test’, value: 42})")  # With properties
        - .create("(n:A)-[e:term]->(m:B)")  # Create path
        - .match("(n:A)").match("(m:B)").create("(n)-[e:f]->(m)")  # Connect existing nodes

        PROGRAMMATIC USAGE:

        For nodes:
        - node: variable name to bind created node
        - type: type of the node (required)
        - properties: initial properties

        For edges:
        - edge: variable name to bind created edge
        - term: term of the edge (required)
        - start: start node (variable name or Node, required)
        - end: end node (variable name or Node, required)
        - properties: initial properties

        Examples:
        - .create(node="n", type=Variable("A"), properties={"name": "test"})
        - .create(edge="e", term=my_term, start=node1, end=node2)
        - .match(node="n1").match(node="n2").create(edge="e", term=term, start="n1", end="n2")
        """
        ...
    # SET clause - for updating properties_
    def set(self, variable: str, properties: Dict[str, Any]) -> "Query":
        """
        Set properties on matched nodes or edges.

        Examples:
        - .match(node="n").set("n", {"name": "new_name", "value": 42})
        - .match(edge="e").set("e", {"weight": 1.5})
        """
        ...
    # DELETE clause - for removing nodes/edges_
    def delete(self, *variables: str, detach: bool = False) -> "Query":
        """
        Delete matched nodes or edges.

        If detach=True, also deletes all connected edges when deleting nodes.

        Examples:
        - .match(node="n").where("n.properties['temp'] == True").delete("n")
        - .match(edge="e").delete("e")
        - .match(node="n").delete("n", detach=True)  # Delete node and its edges
        """
        ...
    # MERGE clause - create if not exists_
    def merge(
        self,
        pattern: str | PathPattern | None = None,
        *,
        node: str | None = None,
        edge: str | None = None,
        type: Type | None = None,
        type_schema: str | TypeSchema | None = None,
        term: Term | None = None,
        term_type_schema: str | TypeSchema | None = None,
        start: str | Node | None = None,
        end: str | Node | None = None,
        properties: Dict[str, Any] = {},
    ) -> "Query":
        """
        Match or create node/edge if it doesn't exist.

        Similar to MATCH, but creates the node/edge if no match is found.

        IDIOMATIC USAGE:
        - .merge("(n:A)")
        - .merge("(n:A)-[e:term]->(m:B)")

        PROGRAMMATIC USAGE:
        - .merge(node="n", type=Variable("A"))
        - .merge(edge="e", term=my_term, start=node1, end=node2)
        """
        ...
    # WITH clause - for passing results to next part of query_
    def with_(self, *variables: str) -> "Query":
        """
        Pass specific variables to the next part of the query.

        Examples:
        - .match(node="n").with_("n").match(edge="e", start="n")
        """
        ...
    # ORDER BY clause_
    def order_by(self, variable: str, key: str, ascending: bool = True) -> "Query":
        """
        Order results by a property or attribute.

        Examples:
        - .match(node="n").order_by("n", "properties['name']")
        - .match(node="n").order_by("n", "type.uid()", ascending=False)
        """
        ...
    # LIMIT clause_
    def limit(self, count: int) -> "Query":
        """
        Limit the number of results.

        Examples:
        - .match(node="n").limit(10)
        """
        ...
    # SKIP clause_
    def skip(self, count: int) -> "Query":
        """
        Skip the first n results.

        Examples:
        - .match(node="n").skip(10).limit(10)  # Pagination
        """
        ...
    # Execute mutations_
    def execute(self) -> "Query":
        """
        Execute the query and return self for chaining.
        Mainly used for mutations (CREATE, SET, DELETE).

        Examples:
        - graph.query().create(node="n", type=my_type).execute()
        """
        ...

## Usage Examples_

"""
COMPLETE QUERY EXAMPLES:

# 1. Basic node matching with type schemas
results = graph.query().match("(n:$A -> B$)").return_("n")

# 2. Path matching - your example
results = graph.query().match("(n:A)-[f:$A -> B$]->(m:B)").return_("n", "f", "m")
# This finds: node n of type A, edge f with type matching schema A -> B, node m of type B

# 3. Complex paths with multiple edges
results = graph.query().match("(a:A)-[e1]->(b:B)-[e2]->(c:C)").return_("a", "b", "c")

# 4. Bidirectional and reverse edges
results = graph.query().match("(n:A)<-[e]-(m:B)").return_("n", "m")  # Reverse
results = graph.query().match("(n:A)-[e]-(m:B)").return_("n", "m")  # Any direction

# 5. Anonymous nodes and edges
results = graph.query().match("(:A)-[]->(m)").return_("m")  # Match m without naming start

# 6. Filtering with WHERE
results = (graph.query()
    .match("(n:A)-[e]->(m:B)")
    .where("n.properties['value'] > 10 and m.properties['name'] == 'test'")
    .return_("n", "e", "m"))

# 7. Creating nodes and relationships
graph.query().create("(n:A {name: 'node1', value: 42})").execute()

# 8. Creating edges between matched nodes
graph.query().match("(n:A)").match("(m:B)").create("(n)-[e:connect]->(m)").execute()

# 9. Complex creation pattern
graph.query().create("(a:A)-[e1:f]->(b:B)-[e2:g]->(c:C)").execute()

# 10. MERGE - create if not exists
graph.query().merge("(n:A {name: 'unique'})").execute()

# 11. Update properties
graph.query().match("(n:A)").where("n.properties['id'] == 1").set("n", {"status": "updated"}).execute()

# 12. Delete with detach
graph.query().match("(n:A)").where("n.properties[‘temp’] == True").delete("n", detach=True).execute()

# 13. Pagination
results = graph.query().match("(n:A)").order_by("n", "properties[‘name’]").skip(10).limit(10).return_("n")

# 14. Count matches
count = graph.query().match("(n:$*$ -> $*$)").return_count()

# 15. Chaining with WITH
results = (graph.query()
    .match("(n:A)")
    .where("n.properties[‘value’] > 5")
    .with_("n")
    .match("(n)-[e]->(m)")
    .return_("n", "e", "m"))

# 16. Type schema wildcards and captures
results = graph.query().match("(n:$*$)").return_("n")  # All nodes
results = graph.query().match("(n:$(x:*) -> $(y:*)$)").return_("n")  # All application types

# 17. Combining programmatic and string patterns
results = (graph.query()
    .match("(n:A)")
    .match(edge="e", start="n", term=specific_term)
    .return_("n", "e"))
"""
