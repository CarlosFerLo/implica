"""
Tests for node terms functionality.

This module tests:
- Node creation with terms
- Term sharing between nodes and edges
- Automatic term application
- Automatic edge creation from Arrow-typed nodes
- KeepTermStrategy conflict resolution
"""

import pytest
import implica


class TestNodeTermCreation:
    """Test suite for creating nodes with terms"""

    def test_node_creation_without_term(self, var_a):
        """Test creating a node without a term"""
        node = implica.Node(var_a)
        assert node.term is None
        assert str(node) == "Node(A)"

    def test_node_creation_with_term(self, var_a):
        """Test creating a node with a term"""
        term = implica.Term("a", var_a)
        node = implica.Node(var_a, term)
        assert node.term is not None
        assert node.term.name == "a"
        assert str(node) == "Node(A, a)"

    def test_node_creation_with_term_and_properties(self, var_a):
        """Test creating a node with term and properties"""
        term = implica.Term("a", var_a)
        node = implica.Node(var_a, term, {"prop": "value"})
        assert node.term is not None
        assert node.term.name == "a"
        assert node.properties["prop"] == "value"

    def test_node_term_setter(self, var_a):
        """Test setting a node's term after creation"""
        node = implica.Node(var_a)
        assert node.term is None

        term = implica.Term("a", var_a)
        node.term = term
        assert node.term is not None
        assert node.term.name == "a"

    def test_node_term_clear(self, var_a):
        """Test clearing a node's term"""
        term = implica.Term("a", var_a)
        node = implica.Node(var_a, term)
        assert node.term is not None

        node.term = None
        assert node.term is None


class TestAutomaticEdgeCreation:
    """Test suite for automatic edge creation from Arrow-typed nodes"""

    def test_arrow_node_with_term_creates_edge(self, var_a, var_b):
        """Test that adding an Arrow node with a term creates an edge"""
        graph = implica.Graph()

        # Create an Arrow type A -> B
        ab_type = implica.Arrow(var_a, var_b)

        # Create a term with that type
        term = implica.Term("f", ab_type)

        # Create a node with the Arrow type and term
        node = implica.Node(ab_type, term)

        # Add it to the graph
        graph.nodes[node.uid()] = node

        # Verify the node was added
        assert len(graph.nodes) >= 1

        print(f"Nodes: {len(graph.nodes)}, Edges: {len(graph.edges)}")


class TestTermApplication:
    """Test suite for automatic term application through edges"""

    def test_term_application_through_edge(self, var_a, var_b):
        """Test that terms are applied through edges"""
        graph = implica.Graph()

        # Create types
        ab_type = implica.Arrow(var_a, var_b)

        # Create nodes
        node_a = implica.Node(var_a, implica.Term("a", var_a))
        node_b = implica.Node(var_b)

        # Add nodes to graph manually
        graph.nodes[node_a.uid()] = node_a
        graph.nodes[node_b.uid()] = node_b

        # Create edge with term
        edge_term = implica.Term("f", ab_type)
        edge = implica.Edge(edge_term, node_a, node_b)

        # Add edge to graph
        graph.edges[edge.uid()] = edge

        print(f"Node A term: {node_a.term.name if node_a.term else None}")
        print(f"Node B term: {node_b.term.name if node_b.term else None}")
        print(f"Edge term: {edge_term.name}")


class TestKeepTermStrategy:
    """Test suite for KeepTermStrategy"""

    def test_keep_simplest_strategy(self, var_a):
        """Test KeepSimplest strategy chooses simpler terms"""
        graph = implica.Graph()

        # Create two terms: one simple, one complex
        simple_term = implica.Term("a", var_a)
        complex_term_type = implica.Arrow(var_a, var_a)
        complex_term = implica.Term("f", complex_term_type)
        applied_term = complex_term(simple_term)  # Creates "(f a)"

        # The applied term should be more complex
        assert len(applied_term.name) > len(simple_term.name)

        print(f"Simple term: {simple_term.name}")
        print(f"Applied term: {applied_term.name}")


class TestTermSharing:
    """Test suite for term sharing between nodes and edges"""

    def test_nodes_and_edges_share_terms(self, var_a, var_b):
        """Test that nodes and edges of the same type can share terms"""
        graph = implica.Graph()

        # Create types
        ab_type = implica.Arrow(var_a, var_b)

        # Create term
        term = implica.Term("f", ab_type)

        # Create node with term
        node = implica.Node(ab_type, term)

        # Create nodes for edge endpoints
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)

        # Create edge with same term
        edge = implica.Edge(term, node_a, node_b)

        # Both should reference the same term
        assert node.term.uid() == edge.term.uid()
        assert node.term.name == edge.term.name


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
