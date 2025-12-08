import pytest
import implica


class TestPathPatternInit:
    """Test basic PathPattern initialization"""

    def test_path_pattern_empty(self):
        """Test creating an empty PathPattern"""
        path_pattern = implica.PathPattern()
        assert path_pattern.nodes == []
        assert path_pattern.edges == []

    def test_path_pattern_from_string_simple(self):
        """Test creating a simple path from string with TypeSchemas"""
        pattern_str = "(A)-[e: A -> B]->(B)"
        path_pattern = implica.PathPattern(pattern=pattern_str)

        assert len(path_pattern.nodes) == 2
        assert len(path_pattern.edges) == 1

        node_a = path_pattern.nodes[0]
        edge_e = path_pattern.edges[0]
        node_b = path_pattern.nodes[1]

        assert node_a.type_schema is None
        assert edge_e.type_schema is not None
        assert node_b.type_schema is None


class TestPathPatternNodeOnly:
    """Test PathPattern with nodes only (no edges)"""

    def test_single_node_no_variable(self):
        """Test path with single node without variable"""
        pattern = implica.PathPattern(pattern="(A)")

        assert len(pattern.nodes) == 1
        assert len(pattern.edges) == 0
        assert pattern.nodes[0].variable == "A"
        assert pattern.nodes[0].type_schema is None

    def test_single_node_with_variable(self):
        """Test path with single node with variable"""
        pattern = implica.PathPattern(pattern="(n:A)")

        assert len(pattern.nodes) == 1
        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None

    def test_single_node_with_type_and_term_schema(self):
        """Test path with single node with TypeSchema and TermSchema"""
        pattern = implica.PathPattern(pattern="(n:A:x)")

        assert len(pattern.nodes) == 1
        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].term_schema is not None

    def test_single_node_no_variable_with_term_schema(self):
        """Test path with single node without variable but with schemas"""
        pattern = implica.PathPattern(pattern="(:A:x)")

        assert len(pattern.nodes) == 1
        assert pattern.nodes[0].variable is None
        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].term_schema is not None

    def test_single_node_with_properties(self):
        """Test path with single node with properties"""
        pattern = implica.PathPattern(pattern="(n:A {label: 'Node A'})")

        assert len(pattern.nodes) == 1
        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].properties["label"] == "Node A"

    def test_single_node_empty_parens(self):
        """Test path with empty node pattern (matches any node)"""
        pattern = implica.PathPattern(pattern="()")

        assert len(pattern.nodes) == 1
        assert pattern.nodes[0].variable is None
        assert pattern.nodes[0].type_schema is None
        assert pattern.nodes[0].term_schema is None


class TestPathPatternSimplePaths:
    """Test PathPattern with simple paths (two nodes, one edge)"""

    def test_simple_forward_arrow(self):
        """Test simple forward arrow pattern"""
        pattern = implica.PathPattern(pattern="(A)-[]->(B)")

        assert len(pattern.nodes) == 2
        assert len(pattern.edges) == 1
        assert pattern.edges[0].direction == "forward"

    def test_simple_forward_arrow_with_variables(self):
        """Test forward arrow with all variables"""
        pattern = implica.PathPattern(pattern="(n:A)-[e]->(m:B)")

        assert pattern.nodes[0].variable == "n"
        assert pattern.edges[0].variable == "e"
        assert pattern.nodes[1].variable == "m"

    def test_simple_forward_arrow_with_type_schema(self):
        """Test forward arrow with TypeSchema on edge"""
        pattern = implica.PathPattern(pattern="(A)-[e:A -> B]->(B)")

        assert pattern.edges[0].type_schema is not None
        assert pattern.edges[0].variable == "e"

    def test_simple_forward_arrow_with_all_schemas(self):
        """Test forward arrow with all TypeSchemas and TermSchemas"""
        pattern = implica.PathPattern(pattern="(n:A:x)-[e:A -> B:f]->(m:B:y)")

        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].term_schema is not None

        assert pattern.edges[0].variable == "e"
        assert pattern.edges[0].type_schema is not None
        assert pattern.edges[0].term_schema is not None

        assert pattern.nodes[1].variable == "m"
        assert pattern.nodes[1].type_schema is not None
        assert pattern.nodes[1].term_schema is not None

    def test_simple_backward_arrow(self):
        """Test simple backward arrow pattern"""
        pattern = implica.PathPattern(pattern="(A)<-[]-(B)")

        assert len(pattern.nodes) == 2
        assert len(pattern.edges) == 1
        assert pattern.edges[0].direction == "backward"

    def test_simple_backward_arrow_with_schemas(self):
        """Test backward arrow with schemas"""
        pattern = implica.PathPattern(pattern="(n:A)<-[e:B -> A]-(m:B)")

        assert pattern.nodes[0].variable == "n"
        assert pattern.edges[0].variable == "e"
        assert pattern.edges[0].direction == "backward"
        assert pattern.edges[0].type_schema is not None
        assert pattern.nodes[1].variable == "m"

    def test_simple_undirected_edge(self):
        """Test undirected edge pattern"""
        pattern = implica.PathPattern(pattern="(A)-[]-(B)")

        assert len(pattern.nodes) == 2
        assert len(pattern.edges) == 1
        assert pattern.edges[0].direction == "any"

    def test_edge_with_properties(self):
        """Test edge with properties"""
        pattern = implica.PathPattern(pattern="(A)-[e:A -> B {weight: 1.0}]->(B)")

        assert pattern.edges[0].properties["weight"] == 1.0


class TestPathPatternComplexPaths:
    """Test PathPattern with complex paths (multiple nodes and edges)"""

    def test_three_node_chain(self):
        """Test path with three nodes in a chain"""
        pattern = implica.PathPattern(pattern="(A)-[]->(B)-[]->(C)")

        assert len(pattern.nodes) == 3
        assert len(pattern.edges) == 2

    def test_three_node_chain_with_variables(self):
        """Test three-node chain with all variables"""
        pattern = implica.PathPattern(pattern="(n:A)-[e1]->(m:B)-[e2]->(p:C)")

        assert pattern.nodes[0].variable == "n"
        assert pattern.edges[0].variable == "e1"
        assert pattern.nodes[1].variable == "m"
        assert pattern.edges[1].variable == "e2"
        assert pattern.nodes[2].variable == "p"

    def test_long_chain_with_schemas(self):
        """Test longer chain with TypeSchemas and TermSchemas"""
        pattern = implica.PathPattern(
            pattern="(n:A:x)-[e1:A -> B:f]->(m:B:y)-[e2:B -> C:g]->(p:C:z)"
        )

        assert len(pattern.nodes) == 3
        assert len(pattern.edges) == 2

        # Check all have schemas
        for node in pattern.nodes:
            assert node.type_schema is not None
            assert node.term_schema is not None

        for edge in pattern.edges:
            assert edge.type_schema is not None
            assert edge.term_schema is not None

    def test_mixed_directions(self):
        """Test path with mixed edge directions"""
        pattern = implica.PathPattern(pattern="(A)-[]->(B)<-[]-(C)")

        assert len(pattern.nodes) == 3
        assert len(pattern.edges) == 2
        assert pattern.edges[0].direction == "forward"
        assert pattern.edges[1].direction == "backward"

    def test_four_node_chain(self):
        """Test path with four nodes"""
        pattern = implica.PathPattern(pattern="(A)-[]->(B)-[]->(C)-[]->(D)")

        assert len(pattern.nodes) == 4
        assert len(pattern.edges) == 3


class TestPathPatternTypeSchemas:
    """Test PathPattern with various TypeSchema patterns"""

    def test_wildcard_type_schema(self):
        """Test path with wildcard TypeSchema"""
        pattern = implica.PathPattern(pattern="(n:*)")

        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].variable == "n"

    def test_arrow_type_schema(self):
        """Test path with arrow TypeSchema"""
        pattern = implica.PathPattern(pattern="(n:A -> B)")

        assert pattern.nodes[0].type_schema is not None

    def test_wildcard_arrow_type_schema(self):
        """Test path with wildcard arrow TypeSchema"""
        pattern = implica.PathPattern(pattern="(n:* -> *)")

        assert pattern.nodes[0].type_schema is not None

    def test_partial_wildcard_arrow(self):
        """Test path with partial wildcard in arrow"""
        pattern = implica.PathPattern(pattern="(n:A -> *)")

        assert pattern.nodes[0].type_schema is not None

    def test_capture_type_schema(self):
        """Test path with capture TypeSchema"""
        pattern = implica.PathPattern(pattern="(n:(X:*))")

        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].variable == "n"

    def test_capture_arrow_type_schema(self):
        """Test path with capture in arrow TypeSchema"""
        pattern = implica.PathPattern(pattern="(n:(X:*) -> (Y:*))")

        assert pattern.nodes[0].type_schema is not None

    def test_edge_with_arrow_type_schema(self):
        """Test edge with arrow TypeSchema"""
        pattern = implica.PathPattern(pattern="(A)-[e:A -> B]->(B)")

        assert pattern.edges[0].type_schema is not None

    def test_edge_with_wildcard_arrow(self):
        """Test edge with wildcard arrow TypeSchema"""
        pattern = implica.PathPattern(pattern="(A)-[e:* -> *]->(B)")

        assert pattern.edges[0].type_schema is not None


class TestPathPatternTermSchemas:
    """Test PathPattern with various TermSchema patterns"""

    def test_simple_term_schema(self):
        """Test path with simple TermSchema"""
        pattern = implica.PathPattern(pattern="(n:A:x)")

        assert pattern.nodes[0].term_schema is not None
        assert pattern.nodes[0].variable == "n"

    def test_term_schema_with_spaces(self):
        """Test path with TermSchema containing spaces (application)"""
        pattern = implica.PathPattern(pattern="(n:A:f x)")

        assert pattern.nodes[0].term_schema is not None

    def test_wildcard_term_schema(self):
        """Test path with wildcard TermSchema"""
        pattern = implica.PathPattern(pattern="(n:A:*)")

        assert pattern.nodes[0].term_schema is not None

    def test_edge_term_schema(self):
        """Test edge with TermSchema"""
        pattern = implica.PathPattern(pattern="(A)-[e:A -> B:f]->(B)")

        assert pattern.edges[0].term_schema is not None
        assert pattern.edges[0].variable == "e"

    def test_multiple_term_schemas(self):
        """Test path with multiple TermSchemas"""
        pattern = implica.PathPattern(pattern="(n:A:x)-[e:A -> B:f]->(m:B:y)")

        assert pattern.nodes[0].term_schema is not None
        assert pattern.edges[0].term_schema is not None
        assert pattern.nodes[1].term_schema is not None


class TestPathPatternProperties:
    """Test PathPattern with properties"""

    def test_node_with_string_property(self):
        """Test node with string property"""
        pattern = implica.PathPattern(pattern="(n:A {name: 'Alice'})")

        assert pattern.nodes[0].properties["name"] == "Alice"

    def test_node_with_number_property(self):
        """Test node with numeric property"""
        pattern = implica.PathPattern(pattern="(n:A {age: 30})")

        assert pattern.nodes[0].properties["age"] == 30

    def test_node_with_float_property(self):
        """Test node with float property"""
        pattern = implica.PathPattern(pattern="(n:A {score: 3.14})")

        assert pattern.nodes[0].properties["score"] == 3.14

    def test_node_with_boolean_property(self):
        """Test node with boolean property"""
        pattern = implica.PathPattern(pattern="(n:A {active: true})")

        assert pattern.nodes[0].properties["active"] is True

    def test_node_with_multiple_properties(self):
        """Test node with multiple properties"""
        pattern = implica.PathPattern(pattern="(n:A {name: 'Alice', age: 30, active: true})")

        assert pattern.nodes[0].properties["name"] == "Alice"
        assert pattern.nodes[0].properties["age"] == 30
        assert pattern.nodes[0].properties["active"] is True

    def test_edge_with_properties(self):
        """Test edge with properties"""
        pattern = implica.PathPattern(pattern="(A)-[e:A -> B {weight: 1.5, label: 'edge1'}]->(B)")

        assert pattern.edges[0].properties["weight"] == 1.5
        assert pattern.edges[0].properties["label"] == "edge1"

    def test_path_with_all_properties(self):
        """Test path where all nodes and edges have properties"""
        pattern = implica.PathPattern(pattern="(n:A {x: 1})-[e:A -> B {w: 2.0}]->(m:B {y: 3})")

        assert pattern.nodes[0].properties["x"] == 1
        assert pattern.edges[0].properties["w"] == 2.0
        assert pattern.nodes[1].properties["y"] == 3


class TestPathPatternWhitespace:
    """Test PathPattern with various whitespace patterns"""

    def test_extra_whitespace_in_pattern(self):
        """Test pattern with extra whitespace"""
        pattern = implica.PathPattern(pattern="  ( A )  -  [ e ]  ->  ( B )  ")

        assert len(pattern.nodes) == 2
        assert len(pattern.edges) == 1

    def test_no_whitespace(self):
        """Test pattern without whitespace"""
        pattern = implica.PathPattern(pattern="(A)-[e]->(B)")

        assert len(pattern.nodes) == 2
        assert len(pattern.edges) == 1

    def test_whitespace_in_schemas(self):
        """Test pattern with whitespace in TypeSchemas"""
        pattern = implica.PathPattern(pattern="(n: A -> B )")

        assert pattern.nodes[0].type_schema is not None


class TestPathPatternErrors:
    """Test PathPattern error conditions"""

    def test_empty_pattern_string(self):
        """Test that empty pattern string raises error"""
        with pytest.raises(Exception):
            implica.PathPattern(pattern="")

    def test_only_whitespace(self):
        """Test that whitespace-only pattern raises error"""
        with pytest.raises(Exception):
            implica.PathPattern(pattern="   ")

    def test_unmatched_parentheses(self):
        """Test that unmatched parentheses raise error"""
        with pytest.raises(Exception):
            implica.PathPattern(pattern="(A")

    def test_unmatched_brackets(self):
        """Test that unmatched brackets raise error"""
        with pytest.raises(Exception):
            implica.PathPattern(pattern="(A)-[e->(B)")

    def test_edge_without_nodes(self):
        """Test that edge without nodes raises error"""
        with pytest.raises(Exception):
            implica.PathPattern(pattern="-[]->")

    def test_pattern_ending_with_edge(self):
        """Test that pattern ending with edge raises error"""
        with pytest.raises(Exception):
            implica.PathPattern(pattern="(A)-[]->")

    def test_too_many_colons_in_node(self):
        """Test that too many colons in node pattern raise error"""
        with pytest.raises(Exception):
            implica.PathPattern(pattern="(n:A:x:extra)")

    def test_mixed_arrow_directions_in_edge(self):
        """Test that mixed arrow directions raise error"""
        with pytest.raises(Exception):
            implica.PathPattern(pattern="(A)<-[e]->(B)")


class TestPathPatternProgrammaticConstruction:
    """Test building PathPattern programmatically"""

    def test_add_node(self):
        """Test adding nodes programmatically"""
        path = implica.PathPattern()
        node_pattern = implica.NodePattern(variable="n", type_schema=implica.TypeSchema("A"))
        path.add_node(node_pattern)

        assert len(path.nodes) == 1

    def test_add_edge(self):
        """Test adding edges programmatically"""
        path = implica.PathPattern()
        node1 = implica.NodePattern(type_schema=implica.TypeSchema("A"))
        node2 = implica.NodePattern(type_schema=implica.TypeSchema("B"))
        edge = implica.EdgePattern(type_schema=implica.TypeSchema("A -> B"))

        path.add_node(node1)
        path.add_edge(edge)
        path.add_node(node2)

        assert len(path.nodes) == 2
        assert len(path.edges) == 1

    def test_add_multiple_nodes_and_edges(self):
        """Test building complex path programmatically"""
        path = implica.PathPattern()

        # Build: (n:A) -> (m:B) -> (p:C)
        node1 = implica.NodePattern(variable="n", type_schema=implica.TypeSchema("A"))
        edge1 = implica.EdgePattern(variable="e1", type_schema=implica.TypeSchema("A -> B"))
        node2 = implica.NodePattern(variable="m", type_schema=implica.TypeSchema("B"))
        edge2 = implica.EdgePattern(variable="e2", type_schema=implica.TypeSchema("B -> C"))
        node3 = implica.NodePattern(variable="p", type_schema=implica.TypeSchema("C"))

        path.add_node(node1)
        path.add_edge(edge1)
        path.add_node(node2)
        path.add_edge(edge2)
        path.add_node(node3)

        assert len(path.nodes) == 3
        assert len(path.edges) == 2


class TestPathPatternRepresentation:
    """Test PathPattern string representation"""

    def test_repr_empty(self):
        """Test __repr__ of empty PathPattern"""
        path = implica.PathPattern()
        repr_str = repr(path)

        assert "0 nodes" in repr_str
        assert "0 edges" in repr_str

    def test_repr_simple_path(self):
        """Test __repr__ of simple path"""
        path = implica.PathPattern(pattern="(A)-[]->(B)")
        repr_str = repr(path)

        assert "2 nodes" in repr_str
        assert "1 edges" in repr_str

    def test_repr_complex_path(self):
        """Test __repr__ of complex path"""
        path = implica.PathPattern(pattern="(A)-[]->(B)-[]->(C)-[]->(D)")
        repr_str = repr(path)

        assert "4 nodes" in repr_str
        assert "3 edges" in repr_str


class TestPathPatternEdgeCases:
    """Test edge cases and special scenarios"""

    def test_only_type_schema_no_variable(self):
        """Test nodes with only TypeSchema, no variable"""
        pattern = implica.PathPattern(pattern="(:A)-[:A -> B]->(:B)")

        assert pattern.nodes[0].variable is None
        assert pattern.nodes[0].type_schema is not None
        assert pattern.edges[0].variable is None
        assert pattern.edges[0].type_schema is not None
        assert pattern.nodes[1].variable is None
        assert pattern.nodes[1].type_schema is not None

    def test_only_variables_no_schemas(self):
        """Test pattern with only variables, no schemas"""
        pattern = implica.PathPattern(pattern="(n)-[e]->(m)")

        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is None
        assert pattern.edges[0].variable == "e"
        assert pattern.nodes[1].variable == "m"

    def test_mixed_with_and_without_variables(self):
        """Test path with mix of nodes with and without variables"""
        pattern = implica.PathPattern(pattern="(n:A)-[:A -> B]->(B)")

        assert pattern.nodes[0].variable == "n"
        assert pattern.edges[0].variable is None
        assert pattern.nodes[1].variable is not None

    def test_complex_arrow_in_node(self):
        """Test node with complex arrow TypeSchema"""
        pattern = implica.PathPattern(pattern="(n:(A -> B) -> C)")

        assert pattern.nodes[0].type_schema is not None

    def test_nested_arrows(self):
        """Test deeply nested arrow TypeSchemas"""
        pattern = implica.PathPattern(pattern="(n:A -> (B -> C))")

        assert pattern.nodes[0].type_schema is not None

    def test_single_character_variables(self):
        """Test single character variable names"""
        pattern = implica.PathPattern(pattern="(n:A)-[e]->(m:B)")

        assert pattern.nodes[0].variable == "n"
        assert pattern.edges[0].variable == "e"
        assert pattern.nodes[1].variable == "m"

    def test_long_variable_names(self):
        """Test long variable names"""
        pattern = implica.PathPattern(pattern="(node_source:A)-[edge_relation]->(node_target:B)")

        assert pattern.nodes[0].variable == "node_source"
        assert pattern.edges[0].variable == "edge_relation"
        assert pattern.nodes[1].variable == "node_target"


class TestPathPatternParsingRobustness:
    """Test PathPattern parsing robustness to prevent regression of parsing bugs"""

    def test_arrow_in_node_type_schema_not_confused_with_edge(self):
        """Test that -> inside node TypeSchema is not confused with edge direction"""
        pattern = implica.PathPattern(pattern="(n:A -> B)")

        assert len(pattern.nodes) == 1
        assert len(pattern.edges) == 0
        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].variable == "n"

    def test_arrow_in_edge_type_schema_with_backward_direction(self):
        """Test that -> inside edge TypeSchema works with backward edge direction"""
        pattern = implica.PathPattern(pattern="(A)<-[e:A -> B]-(B)")

        assert len(pattern.nodes) == 2
        assert len(pattern.edges) == 1
        assert pattern.edges[0].direction == "backward"
        assert pattern.edges[0].type_schema is not None
        assert pattern.edges[0].variable == "e"

    def test_arrow_in_edge_type_schema_with_forward_direction(self):
        """Test that -> inside edge TypeSchema works with forward edge direction"""
        pattern = implica.PathPattern(pattern="(A)-[e:A -> B]->(B)")

        assert len(pattern.nodes) == 2
        assert len(pattern.edges) == 1
        assert pattern.edges[0].direction == "forward"
        assert pattern.edges[0].type_schema is not None

    def test_colon_in_type_schema_capture(self):
        """Test that colons inside TypeSchema captures don't break parsing"""
        pattern = implica.PathPattern(pattern="(n:(X:*))")

        assert len(pattern.nodes) == 1
        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None

    def test_colon_in_complex_type_schema_capture(self):
        """Test colons in complex TypeSchema with multiple captures"""
        pattern = implica.PathPattern(pattern="(n:(X:*) -> (Y:*))")

        assert len(pattern.nodes) == 1
        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None

    def test_nested_parentheses_in_type_schema(self):
        """Test deeply nested parentheses in TypeSchema"""
        pattern = implica.PathPattern(pattern="(n:((A -> B) -> C))")

        assert len(pattern.nodes) == 1
        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None

    def test_arrow_and_colon_in_edge_with_term_schema(self):
        """Test edge with both arrow in TypeSchema and TermSchema"""
        pattern = implica.PathPattern(pattern="(A)-[e:A -> B:f]->(B)")

        assert pattern.edges[0].variable == "e"
        assert pattern.edges[0].type_schema is not None
        assert pattern.edges[0].term_schema is not None

    def test_multiple_colons_in_complex_schemas(self):
        """Test path with multiple colons in various schemas"""
        pattern = implica.PathPattern(
            pattern="(n:(X:*):term1)-[e:(A:*) -> (B:*):term2]->(m:(Y:*):term3)"
        )

        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].term_schema is not None

        assert pattern.edges[0].variable == "e"
        assert pattern.edges[0].type_schema is not None
        assert pattern.edges[0].term_schema is not None

        assert pattern.nodes[1].variable == "m"
        assert pattern.nodes[1].type_schema is not None
        assert pattern.nodes[1].term_schema is not None

    def test_backward_edge_with_complex_type_schema(self):
        """Test backward edge with complex nested TypeSchema"""
        pattern = implica.PathPattern(pattern="(n:A)<-[e:(A -> B) -> C]-(m:C)")

        assert pattern.edges[0].direction == "backward"
        assert pattern.edges[0].type_schema is not None

    def test_undirected_edge_with_arrow_in_type_schema(self):
        """Test undirected edge with arrow in TypeSchema"""
        pattern = implica.PathPattern(pattern="(A)-[e:A -> B]-(B)")

        assert pattern.edges[0].direction == "any"
        assert pattern.edges[0].type_schema is not None

    def test_chain_with_mixed_schemas_and_arrows(self):
        """Test long chain with mixed schemas containing arrows and colons"""
        pattern = implica.PathPattern(
            pattern="(n:A -> B)-[e1:(A -> B) -> C]->(m:C)-[e2:C -> (D -> E)]->(p:D -> E)"
        )

        assert len(pattern.nodes) == 3
        assert len(pattern.edges) == 2

        # All nodes have TypeSchemas
        for node in pattern.nodes:
            assert node.type_schema is not None

        # All edges have TypeSchemas
        for edge in pattern.edges:
            assert edge.type_schema is not None

    def test_properties_with_special_characters_and_complex_schemas(self):
        """Test properties combined with complex TypeSchemas"""
        pattern = implica.PathPattern(
            pattern="(n:A -> B {label: 'node'})-[e:(A -> B) -> C {weight: 1.5}]->(m:(X:*) {id: 42})"
        )

        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].properties["label"] == "node"

        assert pattern.edges[0].type_schema is not None
        assert pattern.edges[0].properties["weight"] == 1.5

        assert pattern.nodes[1].type_schema is not None
        assert pattern.nodes[1].properties["id"] == 42

    def test_edge_direction_detection_with_spaces(self):
        """Test that edge direction is detected correctly even with extra spaces"""
        patterns = [
            ("(A) <- [e] - (B)", "backward"),
            ("(A) - [e] -> (B)", "forward"),
            ("(A) - [e] - (B)", "any"),
        ]

        for pattern_str, expected_direction in patterns:
            pattern = implica.PathPattern(pattern=pattern_str)
            assert pattern.edges[0].direction == expected_direction

    def test_arrow_inside_brackets_not_affecting_direction(self):
        """Test that arrows inside brackets don't affect edge direction"""
        # Forward edge with -> in TypeSchema
        pattern1 = implica.PathPattern(pattern="(A)-[e:X -> Y]->(B)")
        assert pattern1.edges[0].direction == "forward"

        # Backward edge with -> in TypeSchema
        pattern2 = implica.PathPattern(pattern="(A)<-[e:X -> Y]-(B)")
        assert pattern2.edges[0].direction == "backward"

        # Undirected edge with -> in TypeSchema
        pattern3 = implica.PathPattern(pattern="(A)-[e:X -> Y]-(B)")
        assert pattern3.edges[0].direction == "any"

    def test_multiple_arrows_in_nested_type_schema(self):
        """Test TypeSchema with multiple nested arrows"""
        pattern = implica.PathPattern(pattern="(n:A -> (B -> (C -> D)))")

        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].variable == "n"

    def test_complex_captures_with_arrows(self):
        """Test complex capture patterns with arrows"""
        pattern = implica.PathPattern(pattern="(n:(F:(X:*) -> (Y:*)))")

        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None

    def test_whitespace_handling_in_complex_patterns(self):
        """Test that whitespace is handled correctly in complex patterns"""
        pattern = implica.PathPattern(
            pattern="( n : A -> B ) - [ e : ( A -> B ) -> C ] -> ( m : C )"
        )

        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None
        assert pattern.edges[0].variable == "e"
        assert pattern.edges[0].type_schema is not None
        assert pattern.nodes[1].variable == "m"
        assert pattern.nodes[1].type_schema is not None

    def test_term_schema_with_spaces_not_breaking_parsing(self):
        """Test that TermSchemas with spaces don't break parsing"""
        pattern = implica.PathPattern(pattern="(n:A:f x y)")

        assert pattern.nodes[0].variable == "n"
        assert pattern.nodes[0].type_schema is not None
        assert pattern.nodes[0].term_schema is not None

    def test_edge_with_term_schema_containing_spaces(self):
        """Test edge with TermSchema containing spaces"""
        pattern = implica.PathPattern(pattern="(A)-[e:A -> B:f x y]->(B)")

        assert pattern.edges[0].variable == "e"
        assert pattern.edges[0].type_schema is not None
        assert pattern.edges[0].term_schema is not None

    def test_all_node_schema_variations_in_one_path(self):
        """Test path with all different node schema variations"""
        pattern = implica.PathPattern(
            pattern="(var)-[]->(n:Type)-[]->(m:Type:term)-[]->(:Type)-[]->(:Type:term)-[]->()"
        )

        assert len(pattern.nodes) == 6
        assert len(pattern.edges) == 5

        # First node: only variable
        assert pattern.nodes[0].variable == "var"
        assert pattern.nodes[0].type_schema is None

        # Second node: variable + type
        assert pattern.nodes[1].variable == "n"
        assert pattern.nodes[1].type_schema is not None

        # Third node: variable + type + term
        assert pattern.nodes[2].variable == "m"
        assert pattern.nodes[2].type_schema is not None
        assert pattern.nodes[2].term_schema is not None

        # Fourth node: only type
        assert pattern.nodes[3].variable is None
        assert pattern.nodes[3].type_schema is not None

        # Fifth node: type + term
        assert pattern.nodes[4].variable is None
        assert pattern.nodes[4].type_schema is not None
        assert pattern.nodes[4].term_schema is not None

        # Sixth node: empty (matches any)
        assert pattern.nodes[5].variable is None
        assert pattern.nodes[5].type_schema is None
        assert pattern.nodes[5].term_schema is None
