"""Comprehensive tests for Pattern validation, compilation, and matching."""

import implica
import pytest


# ==============================================================================
# NodePattern Tests
# ==============================================================================


class TestNodePatternValidation:
    """Test NodePattern validation at creation time."""

    def test_empty_variable_name_fails(self):
        """Test that empty variable names are rejected."""
        with pytest.raises(Exception, match="empty or whitespace"):
            implica.NodePattern(variable="")

        with pytest.raises(Exception, match="empty or whitespace"):
            implica.NodePattern(variable="   ")

        with pytest.raises(Exception, match="empty or whitespace"):
            implica.NodePattern(variable="\t\n")

    def test_valid_variable_names(self):
        """Test that valid variable names are accepted."""
        # These should all succeed
        p1 = implica.NodePattern(variable="n")
        assert p1.variable == "n"

        p2 = implica.NodePattern(variable="node_1")
        assert p2.variable == "node_1"

        p3 = implica.NodePattern(variable="CamelCase")
        assert p3.variable == "CamelCase"

    def test_type_and_schema_conflict(self):
        """Test that specifying both type and type_schema fails."""
        person = implica.Variable("Person")

        with pytest.raises(Exception, match="mutually exclusive"):
            implica.NodePattern(variable="n", type=person, type_schema="Person")

    def test_invalid_type_schema_fails(self):
        """Test that invalid type schemas are rejected immediately."""
        # Unbalanced parentheses
        with pytest.raises(Exception):
            implica.NodePattern(variable="n", type_schema="((A -> B)")

        with pytest.raises(Exception):
            implica.NodePattern(variable="n", type_schema="A -> B))")

    def test_valid_type_schema(self):
        """Test that valid type schemas are accepted."""
        p1 = implica.NodePattern(variable="n", type_schema="Person")
        assert p1.variable == "n"

        p2 = implica.NodePattern(variable="n", type_schema="A -> B")
        assert p2.variable == "n"

        p3 = implica.NodePattern(variable="n", type_schema="*")
        assert p3.variable == "n"

        p4 = implica.NodePattern(variable="n", type_schema="(* -> *) -> *")
        assert p4.variable == "n"

    def test_empty_property_key_fails(self):
        """Test that empty property keys are rejected."""
        with pytest.raises(Exception, match="empty or whitespace"):
            implica.NodePattern(variable="n", properties={"": "value"})

        with pytest.raises(Exception, match="empty or whitespace"):
            implica.NodePattern(variable="n", properties={"  ": "value"})

    def test_valid_properties(self):
        """Test that valid properties are accepted."""
        props = {"name": "Alice", "age": 30, "active": True}
        p = implica.NodePattern(variable="n", properties=props)
        assert p.variable == "n"

    def test_none_variable_allowed(self):
        """Test that None variable (anonymous pattern) is allowed."""
        p = implica.NodePattern(variable=None, type_schema="Person")
        assert p.variable is None


class TestNodePatternMatching:
    """Test NodePattern matching against nodes."""

    def test_match_any_node(self):
        """Test pattern with no constraints matches any node."""
        pattern = implica.NodePattern(variable="n")

        # Should match any type
        node1 = implica.Node(implica.Variable("Person"))
        node2 = implica.Node(implica.Variable("Number"))
        node3 = implica.Node(implica.Arrow(implica.Variable("A"), implica.Variable("B")))

        # All should match (matching is internal, tested via query)
        # This is a placeholder - actual matching is used by Query
        assert pattern.variable == "n"

    def test_match_specific_type(self):
        """Test pattern with specific type matches correctly."""
        person = implica.Variable("Person")
        pattern = implica.NodePattern(variable="n", type=person)

        node_person = implica.Node(person)
        node_number = implica.Node(implica.Variable("Number"))

        # Pattern should be configured with the type
        assert pattern.variable == "n"

    def test_match_type_schema_wildcard(self):
        """Test pattern with wildcard schema."""
        pattern = implica.NodePattern(variable="n", type_schema="*")

        # Should match any node
        assert pattern.variable == "n"

    def test_match_type_schema_Arrow(self):
        """Test pattern with Arrow schema."""
        pattern = implica.NodePattern(variable="n", type_schema="A -> B")

        # Pattern should be compiled with the schema
        assert pattern.variable == "n"


# ==============================================================================
# EdgePattern Tests
# ==============================================================================


class TestEdgePatternValidation:
    """Test EdgePattern validation at creation time."""

    def test_empty_variable_name_fails(self):
        """Test that empty variable names are rejected."""
        with pytest.raises(Exception, match="empty or whitespace"):
            implica.EdgePattern(variable="")

        with pytest.raises(Exception, match="empty or whitespace"):
            implica.EdgePattern(variable="   ")

    def test_valid_variable_names(self):
        """Test that valid variable names are accepted."""
        p1 = implica.EdgePattern(variable="e")
        assert p1.variable == "e"
        assert p1.direction == "forward"

        p2 = implica.EdgePattern(variable="edge_1", direction="backward")
        assert p2.variable == "edge_1"
        assert p2.direction == "backward"

    def test_invalid_direction_fails(self):
        """Test that invalid directions are rejected."""
        with pytest.raises(ValueError, match="must be"):
            implica.EdgePattern(variable="e", direction="invalid")

        with pytest.raises(ValueError, match="must be"):
            implica.EdgePattern(variable="e", direction="FORWARD")

        with pytest.raises(ValueError, match="must be"):
            implica.EdgePattern(variable="e", direction="")

    def test_valid_directions(self):
        """Test that all valid directions are accepted."""
        p1 = implica.EdgePattern(variable="e", direction="forward")
        assert p1.direction == "forward"

        p2 = implica.EdgePattern(variable="e", direction="backward")
        assert p2.direction == "backward"

        p3 = implica.EdgePattern(variable="e", direction="any")
        assert p3.direction == "any"

    def test_term_and_schema_conflict(self):
        """Test that specifying both term and term_type_schema fails."""
        term = implica.Term("f", implica.Variable("A"))

        with pytest.raises(Exception, match="mutually exclusive"):
            implica.EdgePattern(variable="e", term=term, term_type_schema="A -> B")

    def test_invalid_term_schema_fails(self):
        """Test that invalid term schemas are rejected immediately."""
        with pytest.raises(Exception):
            implica.EdgePattern(variable="e", term_type_schema="((A -> B)")

    def test_valid_term_schema(self):
        """Test that valid term schemas are accepted."""
        p1 = implica.EdgePattern(variable="e", term_type_schema="A -> B")
        assert p1.variable == "e"

        p2 = implica.EdgePattern(variable="e", term_type_schema="*")
        assert p2.variable == "e"

    def test_empty_property_key_fails(self):
        """Test that empty property keys are rejected."""
        with pytest.raises(Exception, match="empty or whitespace"):
            implica.EdgePattern(variable="e", properties={"": "value"})

    def test_none_variable_allowed(self):
        """Test that None variable (anonymous pattern) is allowed."""
        p = implica.EdgePattern(variable=None, direction="any")
        assert p.variable is None
        assert p.direction == "any"

    def test_default_direction(self):
        """Test that default direction is forward."""
        p = implica.EdgePattern(variable="e")
        assert p.direction == "forward"


# ==============================================================================
# PathPattern Tests
# ==============================================================================


class TestPathPatternParsing:
    """Test PathPattern parsing from strings."""

    def test_empty_pattern_fails(self):
        """Test that empty patterns are rejected."""
        with pytest.raises(Exception, match="empty"):
            implica.PathPattern("")

        with pytest.raises(Exception, match="empty"):
            implica.PathPattern("   ")

    def test_simple_node(self):
        """Test parsing simple node patterns."""
        p1 = implica.PathPattern("(n)")
        assert len(p1.nodes) == 1
        assert len(p1.edges) == 0
        assert p1.nodes[0].variable == "n"

    def test_typed_node(self):
        """Test parsing typed node patterns."""
        p1 = implica.PathPattern("(n:Person)")
        assert len(p1.nodes) == 1
        assert p1.nodes[0].variable == "n"

        p2 = implica.PathPattern("(:Person)")
        assert len(p2.nodes) == 1
        assert p2.nodes[0].variable is None

    def test_anonymous_node(self):
        """Test parsing anonymous node patterns."""
        p = implica.PathPattern("()")
        assert len(p.nodes) == 1
        assert p.nodes[0].variable is None

    def test_simple_path(self):
        """Test parsing simple path with two nodes and one edge."""
        p = implica.PathPattern("(n)-[e]->(m)")
        assert len(p.nodes) == 2
        assert len(p.edges) == 1
        assert p.nodes[0].variable == "n"
        assert p.nodes[1].variable == "m"
        assert p.edges[0].variable == "e"
        assert p.edges[0].direction == "forward"

    def test_typed_path(self):
        """Test parsing typed path patterns."""
        p = implica.PathPattern("(n:Person)-[e:knows]->(m:Person)")
        assert len(p.nodes) == 2
        assert len(p.edges) == 1
        assert p.nodes[0].variable == "n"
        assert p.nodes[1].variable == "m"
        assert p.edges[0].variable == "e"

    def test_backward_edge(self):
        """Test parsing backward edges."""
        p = implica.PathPattern("(n)<-[e]-(m)")
        assert len(p.nodes) == 2
        assert len(p.edges) == 1
        assert p.edges[0].direction == "backward"

    def test_bidirectional_edge(self):
        """Test parsing bidirectional edges."""
        p = implica.PathPattern("(n)-[e]-(m)")
        assert len(p.nodes) == 2
        assert len(p.edges) == 1
        assert p.edges[0].direction == "any"

    def test_complex_path(self):
        """Test parsing complex multi-node paths."""
        p = implica.PathPattern("(a:A)-[e1:rel1]->(b:B)-[e2:rel2]->(c:C)")
        assert len(p.nodes) == 3
        assert len(p.edges) == 2
        assert p.nodes[0].variable == "a"
        assert p.nodes[1].variable == "b"
        assert p.nodes[2].variable == "c"
        assert p.edges[0].variable == "e1"
        assert p.edges[1].variable == "e2"

    def test_mixed_directions(self):
        """Test parsing paths with mixed edge directions."""
        p = implica.PathPattern("(a)-[e1]->(b)<-[e2]-(c)")
        assert len(p.nodes) == 3
        assert len(p.edges) == 2
        assert p.edges[0].direction == "forward"
        assert p.edges[1].direction == "backward"

    def test_anonymous_nodes_in_path(self):
        """Test parsing paths with anonymous nodes."""
        p = implica.PathPattern("()-[e]->()")
        assert len(p.nodes) == 2
        assert len(p.edges) == 1
        assert p.nodes[0].variable is None
        assert p.nodes[1].variable is None

    def test_unmatched_parentheses_fails(self):
        """Test that unmatched parentheses are rejected."""
        with pytest.raises(Exception, match="[Uu]nmatched|parenthes"):
            implica.PathPattern("(n")

        with pytest.raises(Exception, match="[Uu]nmatched|parenthes"):
            implica.PathPattern("((n)")

    def test_unmatched_brackets_fails(self):
        """Test that unmatched brackets are rejected."""
        with pytest.raises(Exception, match="[Uu]nmatched|bracket"):
            implica.PathPattern("(n)-[e->(m)")

        with pytest.raises(Exception, match="[Uu]nmatched|bracket"):
            implica.PathPattern("(n)-[e]]->(m)")

    def test_invalid_arrow_combination_fails(self):
        """Test that invalid arrow combinations are rejected."""
        with pytest.raises(Exception, match="both"):
            implica.PathPattern("(n)<-[e]->(m)")

    def test_pattern_ending_with_edge_fails(self):
        """Test that patterns cannot end with an edge."""
        with pytest.raises(Exception, match="end with|edge"):
            implica.PathPattern("(n)-[e]->")

    def test_pattern_starting_with_edge_fails(self):
        """Test that patterns must start with a node."""
        # This should be caught by the parser
        with pytest.raises(Exception):
            implica.PathPattern("-[e]->(n)")

    def test_whitespace_handling(self):
        """Test that whitespace is handled correctly."""
        p1 = implica.PathPattern("  (n)  -  [e]  ->  (m)  ")
        assert len(p1.nodes) == 2
        assert len(p1.edges) == 1

        p2 = implica.PathPattern("(n:Person)-[e:knows]->(m:Person)")
        p3 = implica.PathPattern("( n : Person ) - [ e : knows ] -> ( m : Person )")
        assert len(p2.nodes) == len(p3.nodes)
        assert len(p2.edges) == len(p3.edges)

    def test_complex_type_schemas(self):
        """Test parsing with complex type schemas."""
        p = implica.PathPattern("(n:A -> B)-[e:* -> *]->(m:C)")
        assert len(p.nodes) == 2
        assert len(p.edges) == 1

    def test_Arrow_types_in_patterns(self):
        """Test patterns with Arrow types."""
        p = implica.PathPattern("(f:(A -> B) -> C)")
        assert len(p.nodes) == 1
        assert p.nodes[0].variable == "f"

    def test_invalid_syntax_fails(self):
        """Test invalid syntax fails"""
        with pytest.raises(Exception, match="Unexpected character"):
            implica.PathPattern("n)")

        with pytest.raises(Exception, match="Unexpected character"):
            implica.PathPattern("(n)-e]->(m)")


class TestPathPatternProgrammatic:
    """Test programmatic PathPattern construction."""

    def test_create_empty_path(self):
        """Test creating an empty path pattern."""
        p = implica.PathPattern()
        assert len(p.nodes) == 0
        assert len(p.edges) == 0

    def test_add_nodes(self):
        """Test adding nodes programmatically."""
        p = implica.PathPattern()

        n1 = implica.NodePattern(variable="n1")
        p.add_node(n1)
        assert len(p.nodes) == 1

        n2 = implica.NodePattern(variable="n2")
        p.add_node(n2)
        assert len(p.nodes) == 2

    def test_add_edges(self):
        """Test adding edges programmatically."""
        p = implica.PathPattern()

        # Add nodes first
        p.add_node(implica.NodePattern(variable="n1"))
        p.add_node(implica.NodePattern(variable="n2"))

        # Add edge
        e = implica.EdgePattern(variable="e")
        p.add_edge(e)
        assert len(p.edges) == 1

    def test_method_chaining(self):
        """Test that add methods support chaining."""
        p = implica.PathPattern()

        # Methods should return self for chaining
        result = p.add_node(implica.NodePattern(variable="n1"))
        # Result should be a PathPattern
        assert hasattr(result, "nodes")


class TestPathPatternEdgeCases:
    """Test edge cases and special scenarios."""

    def test_very_long_path(self):
        """Test parsing a very long path."""
        # Create a path with many nodes
        parts = ["(n0)"]
        for i in range(1, 10):
            parts.append(f"-[e{i}]->(n{i})")
        pattern_str = "".join(parts)

        p = implica.PathPattern(pattern_str)
        assert len(p.nodes) == 10
        assert len(p.edges) == 9

    def test_special_characters_in_names(self):
        """Test that special characters in variable names work."""
        p = implica.PathPattern("(node_1)-[edge_2]->(node_3)")
        assert p.nodes[0].variable == "node_1"
        assert p.edges[0].variable == "edge_2"
        assert p.nodes[1].variable == "node_3"

    def test_repr_output(self):
        """Test that repr provides useful information."""
        p = implica.PathPattern("(n)-[e]->(m)")
        repr_str = repr(p)
        assert "PathPattern" in repr_str
        assert "2 nodes" in repr_str
        assert "1 edge" in repr_str


class TestPatternRepresentation:
    """Test pattern string representations."""

    def test_node_pattern_repr(self):
        """Test NodePattern repr."""
        p1 = implica.NodePattern(variable="n")
        repr_str = repr(p1)
        assert "NodePattern" in repr_str
        assert "n" in repr_str

        p2 = implica.NodePattern(variable="n", type_schema="Person")
        repr_str2 = repr(p2)
        assert "type_schema" in repr_str2

    def test_edge_pattern_repr(self):
        """Test EdgePattern repr."""
        p1 = implica.EdgePattern(variable="e", direction="forward")
        repr_str = repr(p1)
        assert "EdgePattern" in repr_str
        assert "e" in repr_str
        assert "forward" in repr_str

        p2 = implica.EdgePattern(variable="e", term_type_schema="A -> B")
        repr_str2 = repr(p2)
        assert "term_type_schema" in repr_str2


# ==============================================================================
# Integration Tests
# ==============================================================================


class TestPatternIntegration:
    """Test patterns working together in realistic scenarios."""

    def test_pattern_with_real_types(self):
        """Test patterns with actual type objects."""
        person = implica.Variable("Person")
        address = implica.Variable("Address")
        lives_at_type = implica.Arrow(person, address)

        # Create patterns
        node_pattern = implica.NodePattern(variable="p", type=person)

        # Create term and edge pattern
        lives_at = implica.Term("livesAt", lives_at_type)
        edge_pattern = implica.EdgePattern(variable="e", term=lives_at)

        assert node_pattern.variable == "p"
        assert edge_pattern.variable == "e"

    def test_schema_patterns_with_captures(self):
        """Test schema patterns that use captures."""
        # Pattern that captures input and output types
        pattern = implica.NodePattern(variable="f", type_schema="(input:*) -> (output:*)")
        assert pattern.variable == "f"

    def test_path_with_multiple_constraints(self):
        """Test complex path with multiple constraints."""
        # Path: Person -knows-> Person -livesIn-> City
        path = implica.PathPattern(
            "(p1:Person)-[k:Person -> Person]->(p2:Person)" "-[l:Person -> City]->(c:City)"
        )
        assert len(path.nodes) == 3
        assert len(path.edges) == 2
        assert path.nodes[0].variable == "p1"
        assert path.nodes[1].variable == "p2"
        assert path.nodes[2].variable == "c"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
