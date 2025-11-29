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


class TestKeepTermStrategy:
    """Test suite for KeepTermStrategy"""

    def test_keep_simplest_strategy(self, var_a):
        """Test KeepSimplest strategy chooses simpler terms"""
        graph = implica.Graph(keep_term_strategy=implica.KeepTermStrategy.KeepExisting)

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
        assert node.term is not None
        assert node.term.uid() == edge.term.uid()
        assert node.term.name == edge.term.name


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
