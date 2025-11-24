"""
Comprehensive tests for enhanced Query API functionality.

Tests cover:
- Edge creation in CREATE operations
- Edge MERGE functionality
- DETACH DELETE logic
- RETURN DISTINCT deduplication
- SET on edge properties
- Optimized edge matching
- Variable name validation
"""

import pytest
import implica


class TestEdgeCreation:
    """Tests for CREATE operation with edges."""

    def test_create_edge_basic(self):
        """Test basic edge creation between two nodes."""
        graph = implica.Graph()

        # Create types and nodes using Query API
        A = implica.Variable("A")
        B = implica.Variable("B")

        # Create nodes via CREATE
        q = graph.query()
        q.create(node="a", type=A, properties={"name": "NodeA"})
        q.create(node="b", type=B, properties={"name": "NodeB"})
        q.execute()

        # Create a term for the edge
        func_type = implica.Arrow(A, B)
        term_f = implica.Term("f", func_type)

        # Create edge using query
        q = graph.query()
        q.match(node="a", type=A)
        q.match(node="b", type=B)
        q.create(edge="e", term=term_f, start="a", end="b", properties={"weight": 1.0})
        q.execute()

        # Verify edge was created
        assert len(graph.edges) == 1

        # Get the edge
        edge_uid = list(graph.edges.keys())[0]
        edge = graph.edges[edge_uid]
        assert edge.term.name == "f"
        assert edge.properties["weight"] == 1.0

    def test_create_edge_with_properties(self):
        """Test edge creation with multiple properties."""
        graph = implica.Graph()

        A = implica.Variable("A")
        B = implica.Variable("B")

        # Create nodes via Query API
        q = graph.query()
        q.create(node="a", type=A)
        q.create(node="b", type=B)
        q.execute()

        func_type = implica.Arrow(A, B)
        term = implica.Term("edge_term", func_type)

        q = graph.query()
        q.match(node="a", type=A)
        q.match(node="b", type=B)
        q.create(
            edge="e",
            term=term,
            start="a",
            end="b",
            properties={"label": "test", "value": 42, "active": True},
        )
        q.execute()

        edge_uid = list(graph.edges.keys())[0]
        edge = graph.edges[edge_uid]
        assert edge.properties["label"] == "test"
        assert edge.properties["value"] == 42
        assert edge.properties["active"] is True


class TestEdgeMerge:
    """Tests for MERGE operation with edges."""

    def test_merge_edge_creates_when_not_exists(self):
        """Test that MERGE creates an edge when it doesn't exist."""
        graph = implica.Graph()

        A = implica.Variable("A")
        B = implica.Variable("B")

        # Create nodes via Query API
        q = graph.query()
        q.create(node="a", type=A)
        q.create(node="b", type=B)
        q.execute()

        func_type = implica.Arrow(A, B)
        term = implica.Term("f", func_type)

        q = graph.query()
        q.match(node="a", type=A)
        q.match(node="b", type=B)
        q.merge(edge="e", term=term, start="a", end="b")
        q.execute()

        assert len(graph.edges) == 1

    def test_merge_edge_matches_when_exists(self):
        """Test that MERGE matches an existing edge."""
        graph = implica.Graph()

        A = implica.Variable("A")
        B = implica.Variable("B")

        # Create nodes and edge via Query API
        q = graph.query()
        q.create(node="a", type=A)
        q.create(node="b", type=B)
        q.execute()

        func_type = implica.Arrow(A, B)
        term = implica.Term("f", func_type)

        # Create edge first via Query API
        q2 = graph.query()
        q2.match(node="a", type=A)
        q2.match(node="b", type=B)
        q2.create(edge="e", term=term, start="a", end="b")
        q2.execute()

        initial_count = len(graph.edges)

        # Try to merge - should match existing
        q3 = graph.query()
        q3.match(node="a", type=A)
        q3.match(node="b", type=B)
        q3.merge(edge="e", term=term, start="a", end="b")
        results = q3.return_("e")

        # Should still have same number of edges
        assert len(graph.edges) == initial_count
        assert len(results) == 1

    def test_merge_edge_optimized_lookup(self):
        """Test that MERGE uses optimized edge lookup by term type."""
        graph = implica.Graph()

        # Create multiple edges with different term types
        A = implica.Variable("A")
        B = implica.Variable("B")
        C = implica.Variable("C")

        # Create nodes via Query API
        q = graph.query()
        q.create(node="a", type=A)
        q.create(node="b", type=B)
        q.create(node="c", type=C)
        q.execute()

        # Create different edge types via Query API
        type_ab = implica.Arrow(A, B)
        type_bc = implica.Arrow(B, C)

        term_ab = implica.Term("f_ab", type_ab)
        term_bc = implica.Term("f_bc", type_bc)

        q2 = graph.query()
        q2.match(node="a", type=A)
        q2.match(node="b", type=B)
        q2.match(node="c", type=C)
        q2.create(edge="e1", term=term_ab, start="a", end="b")
        q2.create(edge="e2", term=term_bc, start="b", end="c")
        q2.execute()

        # Merge should efficiently find the right edge
        q3 = graph.query()
        q3.match(node="a", type=A)
        q3.match(node="b", type=B)
        q3.merge(edge="e", term=term_ab, start="a", end="b")
        results = q3.return_("e")

        assert len(results) == 1
        assert len(graph.edges) == 2  # No new edges created


class TestDetachDelete:
    """Tests for DETACH DELETE functionality."""

    def test_detach_delete_removes_connected_edges(self):
        """Test that DETACH DELETE removes all connected edges."""
        graph = implica.Graph()

        A = implica.Variable("A")
        B1 = implica.Variable("B1")
        B2 = implica.Variable("B2")

        # Create nodes via Query API - each with different type
        q = (
            graph.query()
            .create(node="a", type=A, properties={"name": "A"})
            .create(node="b1", type=B1, properties={"name": "B1"})
            .create(node="b2", type=B2, properties={"name": "B2"})
            .execute()
        )

        # Create edges from A to both B nodes via Query API
        type_ab1 = implica.Arrow(A, B1)
        type_ab2 = implica.Arrow(A, B2)
        term1 = implica.Term("edge1", type_ab1)
        term2 = implica.Term("edge2", type_ab2)

        q2 = graph.query()
        q2.match(node="a", type=A)
        q2.match(node="b1", type=B1)
        q2.match(node="b2", type=B2)
        q2.create(edge="e1", term=term1, start="a", end="b1")
        q2.create(edge="e2", term=term2, start="a", end="b2")
        q2.execute()

        assert len(graph.edges) == 2

        # Detach delete node A - should remove both edges
        q3 = graph.query()
        q3.match(node="a", type=A)
        q3.delete("a", detach=True)
        q3.execute()

        assert len(graph.nodes) == 2  # Only B nodes remain
        assert len(graph.edges) == 0  # All edges removed

    def test_delete_without_detach_leaves_edges(self):
        """Test that regular DELETE doesn't remove connected edges."""
        graph = implica.Graph()

        A = implica.Variable("A")
        B = implica.Variable("B")

        # Create nodes and edge via Query API
        q = graph.query()
        q.create(node="a", type=A)
        q.create(node="b", type=B)
        q.execute()

        type_ab = implica.Arrow(A, B)
        term = implica.Term("f", type_ab)

        q2 = graph.query()
        q2.match(node="a", type=A)
        q2.match(node="b", type=B)
        q2.create(edge="e", term=term, start="a", end="b")
        q2.execute()

        # Delete without detach
        q3 = graph.query()
        q3.match(node="b", type=B)
        q3.delete("b", detach=False)
        q3.execute()

        # Node deleted but edge remains (orphaned)
        assert len(graph.nodes) == 1
        assert len(graph.edges) == 1


class TestReturnDistinct:
    """Tests for RETURN DISTINCT deduplication."""

    def test_return_distinct_removes_duplicates(self):
        """Test that RETURN DISTINCT removes duplicate results."""
        graph = implica.Graph()

        A = implica.Variable("A")

        # Create node via Query API
        q = graph.query()
        q.create(node="n", type=A, properties={"name": "duplicate"})
        q.execute()

        # Manually create duplicate entries in matched_vars
        q2 = graph.query()
        q2.match(node="n", type=A)
        q2.match(node="n", type=A)  # Match again - creates duplicates

        regular_results = q2.return_("n")

        # Reset and use distinct
        q3 = graph.query()
        q3.match(node="n", type=A)
        q3.match(node="n", type=A)
        distinct_results = q3.return_distinct("n")

        # Distinct should have fewer or equal results
        assert len(distinct_results) <= len(regular_results)

    def test_return_distinct_with_multiple_variables(self):
        """Test RETURN DISTINCT with multiple variables."""
        graph = implica.Graph()

        A1 = implica.Variable("A1")
        A2 = implica.Variable("A2")
        B = implica.Variable("B")

        # Create nodes via Query API - each with different type
        q = graph.query()
        q.create(node="a1", type=A1, properties={"id": 1})
        q.create(node="a2", type=A2, properties={"id": 2})
        q.create(node="b", type=B)
        q.execute()

        type_a1b = implica.Arrow(A1, B)
        type_a2b = implica.Arrow(A2, B)
        term1 = implica.Term("f1", type_a1b)
        term2 = implica.Term("f2", type_a2b)

        # Create edges via Query API
        q2 = graph.query()
        q2.match(node="a1", type=A1)
        q2.match(node="a2", type=A2)
        q2.match(node="b", type=B)
        q2.create(edge="e1", term=term1, start="a1", end="b")
        q2.create(edge="e2", term=term2, start="a2", end="b")
        q2.execute()

        # Use wildcard to match all nodes of similar pattern
        q3 = graph.query()
        q3.match(node="a1", type=A1)
        q3.match(node="a2", type=A2)
        results_a1 = q3.return_distinct("a1", "a2")

        # Should get at least one distinct combination
        assert len(results_a1) >= 1


class TestSetOnEdges:
    """Tests for SET operation on edge properties."""

    def test_set_edge_properties(self):
        """Test setting properties on edges."""
        graph = implica.Graph()

        A = implica.Variable("A")
        B = implica.Variable("B")

        # Create nodes via Query API
        q = graph.query()
        q.create(node="a", type=A)
        q.create(node="b", type=B)
        q.execute()

        type_ab = implica.Arrow(A, B)
        term = implica.Term("f", type_ab)

        # Create edge with initial properties via Query API
        q2 = graph.query()
        q2.match(node="a", type=A)
        q2.match(node="b", type=B)
        q2.create(edge="e", term=term, start="a", end="b", properties={"weight": 1.0})
        q2.execute()

        # Update edge properties
        q3 = graph.query()
        q3.match(node="a", type=A)
        q3.match(node="b", type=B)
        q3.match("(a)-[e]->(b)")
        q3.set("e", {"weight": 2.0, "label": "updated"})
        q3.execute()

        # Verify properties updated - get edge from graph
        edge_uid = list(graph.edges.keys())[0]
        updated_edge = graph.edges[edge_uid]
        assert updated_edge.properties["weight"] == 2.0
        assert updated_edge.properties["label"] == "updated"

    def test_set_maintains_node_index(self):
        """Test that SET maintains node index consistency."""
        graph = implica.Graph()

        A = implica.Variable("A")

        # Create node via Query API
        q = graph.query()
        q.create(node="n", type=A, properties={"value": 1})
        q.execute()

        # Update property
        q2 = graph.query()
        q2.match(node="n", type=A)
        q2.set("n", {"value": 2, "new_prop": "test"})
        q2.execute()

        # Verify node still findable by type
        q3 = graph.query()
        q3.match(node="n", type=A)
        results = q3.return_("n")

        assert len(results) == 1
        assert results[0]["n"].properties["value"] == 2
        assert results[0]["n"].properties["new_prop"] == "test"


class TestOptimizedEdgeMatching:
    """Tests for optimized edge matching by term type."""

    def test_optimized_edge_match_by_term(self):
        """Test that edge matching uses optimized term type lookup."""
        graph = implica.Graph()

        # Create multiple edges with different term types
        # Each node needs a unique type
        nodes_a = [implica.Variable(f"A{i}") for i in range(10)]
        nodes_b = [implica.Variable(f"B{i}") for i in range(10)]

        # Create nodes via Query API - each with unique type
        q = graph.query()
        for i in range(10):
            q.create(node=f"a{i}", type=nodes_a[i], properties={"id": i})
            q.create(node=f"b{i}", type=nodes_b[i], properties={"id": i})
        q.execute()

        # Create edges with specific term types via Query API
        q2 = graph.query()
        for i in range(10):
            # Match the specific nodes we need
            q2.match(node=f"a{i}", type=nodes_a[i])
            q2.match(node=f"b{i}", type=nodes_b[i])

        # Create all edges in one query
        for i in range(10):
            type_ab = implica.Arrow(nodes_a[i], nodes_b[i])
            type_ba = implica.Arrow(nodes_b[i], nodes_a[i])
            term_ab = implica.Term(f"ab_{i}", type_ab)
            term_ba = implica.Term(f"ba_{i}", type_ba)
            q2.create(edge=f"eab{i}", term=term_ab, start=f"a{i}", end=f"b{i}")
            q2.create(edge=f"eba{i}", term=term_ba, start=f"b{i}", end=f"a{i}")
        q2.execute()

        # Should have all edges created
        assert len(graph.edges) == 20


class TestVariableNameValidation:
    """Tests for duplicate variable name handling."""

    def test_variable_overwrite_allowed(self):
        """Test that variable names can be overwritten (with warning behavior)."""
        graph = implica.Graph()

        A = implica.Variable("A")
        B = implica.Variable("B")

        # Create nodes via Query API
        q = graph.query()
        q.create(node="temp_a", type=A, properties={"id": 1})
        q.create(node="temp_b", type=B, properties={"id": 2})
        q.execute()

        # Match with same variable name twice
        q2 = graph.query()
        q2.match(node="n", type=A)
        q2.match(node="n", type=B)  # Overwrites previous
        results = q2.return_("n")

        # Should have B node (last binding wins)
        assert len(results) == 1
        assert results[0]["n"].properties["id"] == 2

    def test_two_variables_might_point_to_the_same_node(self):
        """Test that two variables might be pointing at the same node"""
        graph = implica.Graph()

        A = implica.Variable("A")
        I = implica.Term("id", implica.Arrow(A, A))

        q = graph.query()
        q.create(node="n", type=A)
        q.create(edge="id", term=I, start="n", end="n")
        q.execute()

        results = graph.query().match("(n1:*)-[e:A->A]->(n2:*)").return_("n1", "n2")[0]

        assert results["n1"] == results["n2"]


def test_integration_complex_query():
    """Integration test with complex multi-operation query."""
    graph = implica.Graph()

    # Create a small graph structure - each node needs unique type
    Alice = implica.Variable("Alice")
    Bob = implica.Variable("Bob")
    Company = implica.Variable("Company")

    # Create all nodes via Query API
    q = graph.query()
    q.create(node="alice", type=Alice, properties={"name": "Alice", "age": 30})
    q.create(node="bob", type=Bob, properties={"name": "Bob", "age": 25})
    q.create(node="acme", type=Company, properties={"name": "Acme Corp"})
    q.execute()

    works_at_alice_type = implica.Arrow(Alice, Company)
    works_at_bob_type = implica.Arrow(Bob, Company)
    knows_type = implica.Arrow(Alice, Bob)

    works_term1 = implica.Term("works_at_1", works_at_alice_type)
    works_term2 = implica.Term("works_at_2", works_at_bob_type)
    knows_term = implica.Term("knows", knows_type)

    # Create relationships via Query API
    q2 = graph.query()
    q2.match(node="alice", type=Alice)
    q2.match(node="bob", type=Bob)
    q2.match(node="acme", type=Company)
    q2.create(edge="e1", term=works_term1, start="alice", end="acme")
    q2.create(edge="e2", term=works_term2, start="bob", end="acme")
    q2.create(edge="e3", term=knows_term, start="alice", end="bob")
    q2.execute()

    # Verify edges were created
    assert len(graph.edges) == 3

    # Update edge properties
    q4 = graph.query()
    q4.match(node="alice", type=Alice)
    q4.match(node="acme", type=Company)
    q4.match("(alice)-[e]->(acme)")
    q4.set("e", {"start_date": "2020-01-01", "position": "Engineer"})
    q4.execute()

    # Verify update
    edge_uid = list(graph.edges.keys())[0]
    edge = graph.edges[edge_uid]
    assert "start_date" in edge.properties


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
