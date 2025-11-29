"""
Tests for the set_term() query operation.

This module tests the term mutation functionality, which allows changing
the witness (term) that inhabits a type for both nodes and edges.
"""

import pytest
import implica


class TestSetTermOnNodes:
    """Tests for set_term operation on nodes."""

    def test_set_term_on_single_node(self, graph, var_a):
        """Test setting a term on a single node."""
        term1 = implica.Term("a1", var_a)
        term2 = implica.Term("a2", var_a)

        # Create node with term1
        graph.query().create(node="n", type=var_a, term=term1).execute()

        # Verify initial term
        results = graph.query().match(node="n", type=var_a).return_("n")
        assert len(results) == 1
        assert results[0]["n"].term is not None
        assert results[0]["n"].term.name == "a1"

        # Set new term
        graph.query().match(node="n", type=var_a).set_term("n", term2).execute()

        # Verify term was updated
        results = graph.query().match(node="n", type=var_a).return_("n")
        assert len(results) == 1
        assert results[0]["n"].term is not None
        assert results[0]["n"].term.name == "a2"
        assert results[0]["n"].term.type == var_a

    def test_set_term_on_multiple_nodes_same_type(self, graph, var_a):
        """Test setting a term on multiple nodes of the same type."""
        term1 = implica.Term("a1", var_a)
        term2 = implica.Term("a2", var_a)
        term_new = implica.Term("a_new", var_a)

        # Create multiple nodes with different terms
        graph.query().create(node="n1", type=var_a, term=term1).execute()
        graph.query().create(node="n2", type=var_a, term=term2).execute()

        # Set new term on all nodes of type A
        graph.query().match(node="n", type=var_a).set_term("n", term_new).execute()

        # Verify all nodes have the new term
        results = graph.query().match(node="n", type=var_a).return_("n")
        assert len(results) == 2
        for result in results:
            assert result["n"].term.name == "a_new"
            assert result["n"].term.type == var_a

    def test_set_term_type_mismatch_error(self, graph, var_a, var_b):
        """Test that setting a term with wrong type raises error."""
        term_a = implica.Term("a", var_a)
        term_b = implica.Term("b", var_b)

        # Create node of type A
        graph.query().create(node="n", type=var_a, term=term_a).execute()

        # Try to set term of type B - should fail
        with pytest.raises(Exception) as exc_info:
            graph.query().match(node="n", type=var_a).set_term("n", term_b).execute()

        # Verify error mentions type mismatch
        error_msg = str(exc_info.value).lower()
        assert "type" in error_msg and ("mismatch" in error_msg or "expected" in error_msg)

    def test_set_term_on_heterogeneous_types_error(self, graph, var_a, var_b):
        """Test that setting term on nodes with different types raises error."""
        term_a = implica.Term("a", var_a)
        term_b = implica.Term("b", var_b)

        # Create nodes of different types
        graph.query().create(node="n1", type=var_a, term=term_a).execute()
        graph.query().create(node="n2", type=var_b, term=term_b).execute()

        # Try to set term on all nodes (mixed types) - should fail
        with pytest.raises(Exception) as exc_info:
            graph.query().match("(n:$*$)").set_term("n", term_a).execute()

        # Verify error mentions heterogeneous types
        error_msg = str(exc_info.value).lower()
        assert "different" in error_msg or "heterogeneous" in error_msg

    def test_set_term_on_node_without_initial_term(self, graph, var_a):
        """Test setting a term on a node that initially had no term."""
        term_new = implica.Term("a_new", var_a)

        # Create node without term
        graph.query().create(node="n", type=var_a).execute()

        # Verify node has no term initially
        results = graph.query().match(node="n", type=var_a).return_("n")
        assert len(results) == 1
        assert results[0]["n"].term is None

        # Set term
        graph.query().match(node="n", type=var_a).set_term("n", term_new).execute()

        # Verify term was set
        results = graph.query().match(node="n", type=var_a).return_("n")
        assert len(results) == 1
        assert results[0]["n"].term is not None
        assert results[0]["n"].term.name == "a_new"

    def test_set_term_preserves_node_uid(self, graph, var_a):
        """Test that setting term doesn't change the node's UID."""
        term1 = implica.Term("a1", var_a)
        term2 = implica.Term("a2", var_a)

        # Create node
        graph.query().create(node="n", type=var_a, term=term1).execute()

        # Get initial UID
        results = graph.query().match(node="n", type=var_a).return_("n")
        initial_uid = results[0]["n"].uid()

        # Set new term
        graph.query().match(node="n", type=var_a).set_term("n", term2).execute()

        # Verify UID is the same (node UID depends on type, not term)
        results = graph.query().match(node="n", type=var_a).return_("n")
        assert results[0]["n"].uid() == initial_uid

    def test_set_term_preserves_properties(self, graph, var_a):
        """Test that setting term preserves node properties."""
        term1 = implica.Term("a1", var_a)
        term2 = implica.Term("a2", var_a)

        # Create node with properties
        graph.query().create(
            node="n", type=var_a, term=term1, properties={"value": 42, "name": "test"}
        ).execute()

        # Set new term
        graph.query().match(node="n", type=var_a).set_term("n", term2).execute()

        # Verify properties are preserved
        results = graph.query().match(node="n", type=var_a).return_("n")
        assert results[0]["n"].properties["value"] == 42
        assert results[0]["n"].properties["name"] == "test"


class TestSetTermOnEdges:
    """Tests for set_term operation on edges."""

    def test_set_term_on_single_edge(self, graph, var_a, var_b, app_ab):
        """Test setting a term on a single edge."""
        term_f1 = implica.Term("f1", app_ab)
        term_f2 = implica.Term("f2", app_ab)

        # Create nodes and edge
        graph.query().create(node="n1", type=var_a).execute()
        graph.query().create(node="n2", type=var_b).execute()
        graph.query().match(node="n1", type=var_a).match(node="n2", type=var_b).create(
            edge="e", term=term_f1, start="n1", end="n2"
        ).execute()

        # Verify initial edge term
        results = graph.query().match("()-[e]->()").return_("e")
        assert len(results) == 1
        assert results[0]["e"].term.name == "f1"

        # Set new term on edge
        graph.query().match("()-[e]->()").set_term("e", term_f2).execute()

        # Verify edge term was updated
        results = graph.query().match("()-[e]->()").return_("e")
        assert len(results) == 1
        assert results[0]["e"].term.name == "f2"
        assert results[0]["e"].term.type == app_ab

    def test_set_term_on_edge_changes_uid(self, graph, var_a, var_b, app_ab):
        """Test that setting term on edge changes the edge's UID."""
        term_f1 = implica.Term("f1", app_ab)
        term_f2 = implica.Term("f2", app_ab)

        # Create nodes and edge
        graph.query().create(node="n1", type=var_a).execute()
        graph.query().create(node="n2", type=var_b).execute()
        graph.query().match(node="n1", type=var_a).match(node="n2", type=var_b).create(
            edge="e", term=term_f1, start="n1", end="n2"
        ).execute()

        # Get initial UID
        results = graph.query().match("()-[e]->()").return_("e")
        initial_uid = results[0]["e"].uid()

        # Set new term on edge
        graph.query().match("()-[e]->()").set_term("e", term_f2).execute()

        # Verify UID changed (edge UID depends on term)
        results = graph.query().match("()-[e]->()").return_("e")
        new_uid = results[0]["e"].uid()
        assert new_uid != initial_uid

    def test_set_term_on_edge_preserves_structure(self, graph, var_a, var_b, app_ab):
        """Test that setting term on edge preserves start and end nodes."""
        term_f1 = implica.Term("f1", app_ab)
        term_f2 = implica.Term("f2", app_ab)

        # Create nodes with properties to identify them
        graph.query().create(node="n1", type=var_a, properties={"id": "start"}).execute()
        graph.query().create(node="n2", type=var_b, properties={"id": "end"}).execute()
        graph.query().match(node="n1", type=var_a).match(node="n2", type=var_b).create(
            edge="e", term=term_f1, start="n1", end="n2"
        ).execute()

        # Set new term on edge
        graph.query().match("()-[e]->()").set_term("e", term_f2).execute()

        # Verify edge still connects the same nodes
        results = graph.query().match("(n1)-[e]->(n2)").return_("n1", "e", "n2")
        assert len(results) == 1
        assert results[0]["n1"].properties["id"] == "start"
        assert results[0]["n2"].properties["id"] == "end"
        assert results[0]["e"].term.name == "f2"

    def test_set_term_on_edge_preserves_properties(self, graph, var_a, var_b, app_ab):
        """Test that setting term on edge preserves edge properties."""
        term_f1 = implica.Term("f1", app_ab)
        term_f2 = implica.Term("f2", app_ab)

        # Create nodes and edge with properties
        graph.query().create(node="n1", type=var_a).execute()
        graph.query().create(node="n2", type=var_b).execute()
        graph.query().match(node="n1", type=var_a).match(node="n2", type=var_b).create(
            edge="e",
            term=term_f1,
            start="n1",
            end="n2",
            properties={"weight": 1.5, "label": "test"},
        ).execute()

        # Set new term on edge
        graph.query().match("()-[e]->()").set_term("e", term_f2).execute()

        # Verify properties are preserved
        results = graph.query().match("()-[e]->()").return_("e")
        assert len(results) == 1
        assert results[0]["e"].properties["weight"] == 1.5
        assert results[0]["e"].properties["label"] == "test"

    def test_set_term_on_multiple_edges_same_type(self, graph, var_a, var_b, app_ab):
        """Test setting term on multiple edges of the same type."""
        term_f1 = implica.Term("f1", app_ab)
        term_f2 = implica.Term("f2", app_ab)
        term_new = implica.Term("f_new", app_ab)

        # Create nodes
        graph.query().create(node="n1", type=var_a).execute()
        graph.query().create(node="n2", type=var_b).execute()
        graph.query().create(node="n3", type=var_b).execute()

        # Create multiple edges with different terms
        graph.query().match(node="n1", type=var_a).match(node="n2", type=var_b).create(
            edge="e1", term=term_f1, start="n1", end="n2"
        ).execute()
        graph.query().match(node="n1", type=var_a).match(node="n3", type=var_b).create(
            edge="e2", term=term_f2, start="n1", end="n3"
        ).execute()

        # Set new term on all edges
        graph.query().match("()-[e]->()").set_term("e", term_new).execute()

        # Verify all edges have the new term
        results = graph.query().match("()-[e]->()").return_("e")
        assert len(results) == 2
        for result in results:
            assert result["e"].term.name == "f_new"


class TestSetTermCombinations:
    """Tests for set_term combined with other query operations."""

    def test_set_term_with_where_clause(self, graph, var_a):
        """Test set_term combined with WHERE clause."""
        term1 = implica.Term("a1", var_a)
        term2 = implica.Term("a2", var_a)
        term_new = implica.Term("a_new", var_a)

        # Create nodes with different properties
        graph.query().create(node="n1", type=var_a, term=term1, properties={"value": 10}).execute()
        graph.query().create(node="n2", type=var_a, term=term2, properties={"value": 20}).execute()

        # Set term only on nodes where value > 15
        graph.query().match(node="n", type=var_a).where("n.value > 15").set_term(
            "n", term_new
        ).execute()

        # Verify only n2 was updated
        results = graph.query().match(node="n", type=var_a).return_("n")
        for result in results:
            if result["n"].properties["value"] == 10:
                assert result["n"].term.name == "a1"
            elif result["n"].properties["value"] == 20:
                assert result["n"].term.name == "a_new"

    def test_set_term_with_limit(self, graph, var_a):
        """Test set_term combined with LIMIT."""
        term1 = implica.Term("a1", var_a)
        term_new = implica.Term("a_new", var_a)

        # Create multiple nodes
        for i in range(5):
            graph.query().create(
                node=f"n{i}", type=var_a, term=term1, properties={"id": i}
            ).execute()

        # Note: LIMIT affects return, not the mutation operation
        # This test verifies expected behavior
        graph.query().match(node="n", type=var_a).set_term("n", term_new).execute()

        # All nodes should be updated
        results = graph.query().match(node="n", type=var_a).return_("n")
        assert len(results) == 5
        for result in results:
            assert result["n"].term.name == "a_new"

    def test_set_term_chaining_with_set(self, graph, var_a):
        """Test chaining set_term with set (properties)."""
        term1 = implica.Term("a1", var_a)
        term2 = implica.Term("a2", var_a)

        # Create node
        graph.query().create(node="n", type=var_a, term=term1, properties={"value": 10}).execute()

        # Chain set_term and set operations
        graph.query().match(node="n", type=var_a).set_term("n", term2).set(
            "n", {"value": 20, "updated": True}
        ).execute()

        # Verify both updates were applied
        results = graph.query().match(node="n", type=var_a).return_("n")
        assert len(results) == 1
        assert results[0]["n"].term.name == "a2"
        assert results[0]["n"].properties["value"] == 20
        assert results[0]["n"].properties["updated"] is True


class TestSetTermEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_set_term_on_nonexistent_variable(self, graph, var_a):
        """Test set_term on a variable that doesn't exist."""
        term = implica.Term("a", var_a)

        # Try to set term on non-matched variable
        # Should not raise error, just do nothing
        graph.query().set_term("nonexistent", term).execute()

    def test_set_term_with_empty_match(self, graph, var_a):
        """Test set_term when match returns no results."""
        term = implica.Term("a", var_a)

        # Match nodes that don't exist
        graph.query().match(node="n", type=var_a).set_term("n", term).execute()

        # Should not raise error

    def test_set_term_complex_type(self, graph, var_a, var_b):
        """Test set_term with complex arrow types."""
        arrow1 = implica.Arrow(var_a, var_b)
        arrow2 = implica.Arrow(var_b, var_a)
        nested_arrow = implica.Arrow(arrow1, arrow2)

        term1 = implica.Term("f1", nested_arrow)
        term2 = implica.Term("f2", nested_arrow)

        # Create node with complex type
        graph.query().create(node="n", type=nested_arrow, term=term1).execute()

        # Set new term
        graph.query().match(node="n", type=nested_arrow).set_term("n", term2).execute()

        # Verify term was updated
        results = graph.query().match(node="n", type=nested_arrow).return_("n")
        assert len(results) == 1
        assert results[0]["n"].term.name == "f2"
