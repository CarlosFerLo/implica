import pytest
import implica


# =============================================================================
# TEST NODE MATCHING
# =============================================================================


class TestMatchNodeBasic:
    """Tests for basic node pattern matching."""

    def test_empty_match_node_pattern_matches_all_nodes(self):
        """Empty pattern () matches all nodes in the graph."""
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("()").return_()

        assert len(result) == 2

    def test_empty_match_node_pattern_matches_and_captures_all_nodes(self):
        """Pattern (N:) captures all nodes with variable N."""
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("(N:)").return_("N")

        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {"Node(A: {})", "Node(B: {})"}

    def test_match_empty_graph_returns_no_results(self):
        """Matching on empty graph returns empty result."""
        graph = implica.Graph()

        result = graph.query().match("()").return_()

        assert len(result) == 0

    def test_match_same_variable_in_consecutive_matches(self):
        """Consecutive match clauses with same variable reference the same node."""
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("(N:A)").match("(N)").return_("N")

        assert len(result) == 1
        assert str(result[0]["N"]) == "Node(A: {})"


class TestMatchNodeTypeSchema:
    """Tests for node matching with type schemas."""

    def test_match_node_pattern_with_type_schema(self):
        """Pattern (N:A) matches only nodes with type A."""
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("(N:A)").return_("N")

        assert len(result) == 1
        assert "N" in result[0]
        assert isinstance(result[0]["N"], implica.Node)
        assert str(result[0]["N"]) == "Node(A: {})"

    def test_match_node_pattern_with_wildcard_type_schema(self):
        """Pattern (N:*) matches all nodes."""
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()

        result = graph.query().match("(N:*)").return_("N")

        assert len(result) == 3

    def test_match_node_pattern_with_arrow_type_schema(self):
        """Pattern with arrow type (N:A -> B) matches function types."""
        graph = implica.Graph()
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:A -> B)")
            .create("(:A -> C)")
            .create("(:B -> C)")
            .execute()
        )

        result = graph.query().match("(N:A -> B)").return_("N")
        assert len(result) == 1
        assert str(result[0]["N"]) == "Node((A -> B): {})"

    def test_match_node_pattern_with_type_schema_that_matches_many(self):
        """Pattern (N:A -> *) matches all nodes with source type A."""
        graph = implica.Graph()
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:A -> B)")
            .create("(:A -> C)")
            .create("(:B -> C)")
            .execute()
        )

        result = graph.query().match("(N:A -> *)").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {"Node((A -> B): {})", "Node((A -> C): {})"}

    def test_match_node_with_wildcard_to_specific_type(self):
        """Pattern (N:* -> B) matches all function types targeting B."""
        graph = implica.Graph()
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:A -> B)")
            .create("(:C -> B)")
            .create("(:A -> C)")
            .execute()
        )

        result = graph.query().match("(N:* -> B)").return_("N")
        assert len(result) == 2
        assert {str(d["N"]) for d in result} == {"Node((A -> B): {})", "Node((C -> B): {})"}

    def test_match_node_with_nested_arrow_type(self):
        """Pattern with nested arrows (N:(A -> B) -> C)."""
        graph = implica.Graph()
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:C)")
            .create("(:A -> B)")
            .create("(:(A -> B) -> C)")
            .execute()
        )

        result = graph.query().match("(N:(A -> B) -> C)").return_("N")
        assert len(result) == 1
        assert str(result[0]["N"]) == "Node(((A -> B) -> C): {})"

    def test_match_node_with_double_wildcard_arrow(self):
        """Pattern (N:* -> *) matches all arrow types."""
        graph = implica.Graph()
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:A -> B)")
            .create("(:B -> C)")
            .execute()
        )

        result = graph.query().match("(N:* -> *)").return_("N")
        assert len(result) == 2
        assert {str(d["N"]) for d in result} == {"Node((A -> B): {})", "Node((B -> C): {})"}

    def test_match_node_with_type_capture(self):
        """Pattern (N:(X:*) -> (Y:*)) captures type variables."""
        graph = implica.Graph()
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:A -> B)")
            .create("(:C -> D)")
            .execute()
        )

        result = graph.query().match("(N:(X:*) -> (Y:*))").return_("N", "X", "Y")
        assert len(result) == 2

        for r in result:
            assert isinstance(r["N"], implica.Node)
            assert isinstance(r["X"], implica.Type)
            assert isinstance(r["Y"], implica.Type)

    def test_match_node_with_partial_type_capture(self):
        """Pattern (N:(X:A) -> *) captures only matching source type."""
        graph = implica.Graph()
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:A -> B)")
            .create("(:A -> C)")
            .create("(:B -> C)")
            .execute()
        )

        result = graph.query().match("(N:(X:A) -> *)").return_("N", "X")
        assert len(result) == 2
        for r in result:
            assert str(r["X"]) == "A"


class TestMatchNodeTermSchema:
    """Tests for node matching with term schemas."""

    def test_match_node_pattern_with_type_schema_and_term_schema(self):
        """Pattern (N:A:f) matches nodes with type A and term f."""
        graph = implica.Graph(constants=[implica.Constant("f", "A")])
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("(N:A:f)").return_("N")
        assert len(result) == 1
        assert "N" in result[0]
        assert isinstance(result[0]["N"], implica.Node)
        assert str(result[0]["N"]) == "Node(A:f {})"

        result = graph.query().match("(N:B:f)").return_("N")
        assert len(result) == 0

    def test_match_node_pattern_with_type_schema_and_term_schema_that_matches_many(self):
        """Pattern (N:*:*) matches all nodes with any term."""
        graph = implica.Graph(constants=[implica.Constant("f", "A"), implica.Constant("g", "B")])
        graph.query().create("(:A:@f())").create("(:B:@g())").create("(:C)").execute()

        result = graph.query().match("(N:*:*)").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {"Node(A:f {})", "Node(B:g {})"}

    def test_match_node_pattern_with_term_schema_only(self):
        """Pattern (N::f) matches nodes with term matching f."""
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "C -> A")]
        )
        (
            graph.query()
            .create("(:A { foo: 'var' })")
            .create("(:A -> B:@f() { foo: 'var'})")
            .create("(:B)")
            .create("(:C: { foo: 'not' })")
            .create("(:C -> A:@g())")
            .execute()
        )

        result = graph.query().match("(N::f)").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {
            "Node((C -> A):g {})",
            'Node((A -> B):f {foo: "var"})',
        }

    def test_match_node_with_constant_term_pattern(self):
        """Pattern (N::@f()) matches explicit constant application."""
        graph = implica.Graph(constants=[implica.Constant("f", "A")])
        graph.query().create("(:A:@f())").create("(:A)").execute()

        result = graph.query().match("(N::@f())").return_("N")
        assert len(result) == 1
        assert str(result[0]["N"]) == "Node(A:f {})"

    def test_match_node_with_term_application_pattern(self):
        """Pattern with term application f x matches composite terms."""
        graph = implica.Graph(
            constants=[
                implica.Constant("f", "A -> B"),
                implica.Constant("a", "A"),
            ]
        )
        graph.query().create("(:A:@a())").create("(:B:@f() @a())").execute()

        result = graph.query().match("(N::@f() @a())").return_("N")
        assert len(result) == 1
        assert str(result[0]["N"]) == "Node(B:(f a) {})"


class TestMatchNodeProperties:
    """Tests for node matching with property constraints."""

    def test_match_node_pattern_with_properties_that_matches_many(self):
        """Pattern (N { foo: 'var' }) matches nodes with specific property."""
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "C -> A")]
        )
        (
            graph.query()
            .create("(:A { foo: 'var' })")
            .create("(:A -> B:@f() { foo: 'var'})")
            .create("(:B)")
            .create("(:C: { foo: 'not' })")
            .create("(:C -> A:@g())")
            .execute()
        )

        result = graph.query().match("(N { foo: 'var' })").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {
            'Node(A: {foo: "var"})',
            'Node((A -> B):f {foo: "var"})',
        }

    def test_match_node_with_multiple_properties(self):
        """Pattern with multiple property constraints."""
        graph = implica.Graph()
        (
            graph.query()
            .create("(:A { name: 'test', value: 42 })")
            .create("(:A { name: 'test', value: 100 })")
            .create("(:A { name: 'other', value: 42 })")
            .execute()
        )

        result = graph.query().match("(N { name: 'test', value: 42 })").return_("N")
        assert len(result) == 1
        assert str(result[0]["N"]) == 'Node(A: {name: "test", value: 42})'

    def test_match_node_with_integer_property(self):
        """Match nodes by integer property value."""
        graph = implica.Graph()
        graph.query().create("(:A { count: 5 })").create("(:A { count: 10 })").execute()

        result = graph.query().match("(N { count: 5 })").return_("N")
        assert len(result) == 1
        assert isinstance(result[0]["N"], implica.Node)
        assert result[0]["N"].properties()["count"] == 5

    def test_match_node_with_float_property(self):
        """Match nodes by float property value."""
        graph = implica.Graph()
        graph.query().create("(:A { score: 3.14 })").create("(:A { score: 2.71 })").execute()

        result = graph.query().match("(N { score: 3.14 })").return_("N")
        assert len(result) == 1

    def test_match_node_with_boolean_property(self):
        """Match nodes by boolean property value."""
        graph = implica.Graph()
        graph.query().create("(:A { active: true })").create("(:A { active: false })").execute()

        result = graph.query().match("(N { active: true })").return_("N")
        assert len(result) == 1

    def test_match_node_with_nonexistent_property_returns_empty(self):
        """Matching property that doesn't exist returns no results."""
        graph = implica.Graph()
        graph.query().create("(:A { foo: 'bar' })").execute()

        result = graph.query().match("(N { baz: 'qux' })").return_("N")
        assert len(result) == 0


class TestMatchNodeCombined:
    """Tests for node matching with combined type, term, and property constraints."""

    def test_match_node_pattern_with_type_schema_and_properties_that_matches_many(self):
        """Pattern (N:* { foo: 'var' }) combines type wildcard with properties."""
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "C -> A")]
        )
        (
            graph.query()
            .create("(:A { foo: 'var' })")
            .create("(:A -> B:@f() { foo: 'var'})")
            .create("(:B)")
            .create("(:C: { foo: 'not' })")
            .create("(:C -> A:@g())")
            .execute()
        )

        result = graph.query().match("(N:* { foo: 'var' })").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {
            'Node(A: {foo: "var"})',
            'Node((A -> B):f {foo: "var"})',
        }

    def test_match_node_with_type_term_and_properties(self):
        """Pattern combining type schema, term schema, and properties."""
        graph = implica.Graph(constants=[implica.Constant("f", "A"), implica.Constant("g", "B")])
        (
            graph.query()
            .create("(:A:@f() { name: 'test' })")
            .create("(:B:@g() { name: 'other' })")
            .create("(:C { name: 'test' })")  # No term
            .execute()
        )

        result = graph.query().match("(N:A:f { name: 'test' })").return_("N")
        assert len(result) == 1
        assert str(result[0]["N"]) == 'Node(A:f {name: "test"})'


# =============================================================================
# TEST EDGE MATCHING
# =============================================================================


class TestMatchEdgeBasic:
    """Tests for basic edge pattern matching."""

    def test_empty_match_edge_pattern_matches_all_edges(self):
        """Pattern ()-[]->() matches all edges."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("()-[]->()").return_()
        assert len(result) == 3

    def test_empty_match_edge_pattern_matches_and_captures_all_edges(self):
        """Pattern ()-[E]->() captures all edges with variable E."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("()-[E]->()").return_("E")
        assert len(result) == 3
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])
        assert {str(d["E"]) for d in result} == {
            "Edge((A -> B):f {})",
            "Edge((A -> C):f {})",
            "Edge((B -> C):f {})",
        }

    def test_empty_match_edge_pattern_matches_and_captures_all_edges_and_endpoints(self):
        """Pattern (N)-[E]->(M) captures nodes and edges."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("(N)-[E]->(M)").return_("N", "M", "E")
        assert len(result) == 3
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert all(["M" in d for d in result])
        assert all([isinstance(d["M"], implica.Node) for d in result])

        matches = {(str(d["N"]), str(d["E"]), str(d["M"])) for d in result}
        expected_matches = {
            ("Node(A: {})", "Edge((A -> B):f {})", "Node(B: {})"),
            ("Node(A: {})", "Edge((A -> C):f {})", "Node(C: {})"),
            ("Node(B: {})", "Edge((B -> C):f {})", "Node(C: {})"),
        }
        assert matches == expected_matches

    def test_match_edge_no_edges_in_graph(self):
        """Matching edges on graph with only nodes returns empty."""
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("()-[]->()").return_()
        assert len(result) == 0


class TestMatchEdgeDirection:
    """Tests for edge matching with different directions."""

    def test_match_edge_forward_direction(self):
        """Pattern ()-[E]->() matches forward edges."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()

        result = graph.query().match("()-[E]->()").return_("E")
        assert len(result) == 1

    def test_match_edge_backward_direction(self):
        """Pattern ()<-[E]-() matches backward edges."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()

        result = graph.query().match("()<-[E]-()").return_("E")
        assert len(result) == 1
        assert str(result[0]["E"]) == "Edge((A -> B):f {})"

    def test_match_edge_backward_captures_correct_endpoints(self):
        """Backward edge pattern captures endpoints in reverse order."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()

        # Forward: N=A, M=B
        result_fwd = graph.query().match("(N)-[E]->(M)").return_("N", "M")
        assert len(result_fwd) == 1
        assert str(result_fwd[0]["N"]) == "Node(A: {})"
        assert str(result_fwd[0]["M"]) == "Node(B: {})"

        # Backward: N=B, M=A (reversed perspective)
        result_bwd = graph.query().match("(N)<-[E]-(M)").return_("N", "M")
        assert len(result_bwd) == 1
        assert str(result_bwd[0]["N"]) == "Node(B: {})"
        assert str(result_bwd[0]["M"]) == "Node(A: {})"


class TestMatchEdgeTypeSchema:
    """Tests for edge matching with type schemas."""

    def test_match_edge_pattern_with_type_schema_matches_one_edge(self):
        """Pattern ()-[E:A->B]->() matches edge with exact type."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("()-[E:A->B]->()").return_("E")
        assert len(result) == 1
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])

        assert str(result[0]["E"]) == "Edge((A -> B):f {})"

    def test_match_edge_pattern_with_type_schema_matches_more_than_one_edge(self):
        """Pattern ()-[E:A->*]->() matches edges with wildcard target."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("()-[E:A->*]->()").return_("E")
        assert len(result) == 2
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])
        assert {str(d["E"]) for d in result} == {"Edge((A -> B):f {})", "Edge((A -> C):f {})"}

    def test_match_edge_with_wildcard_source_type(self):
        """Pattern ()-[E:*->C]->() matches edges with wildcard source."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .create("()-[::@f(A, B)]->()")
            .execute()
        )

        result = graph.query().match("()-[E:*->C]->()").return_("E")
        assert len(result) == 2
        assert {str(d["E"]) for d in result} == {"Edge((A -> C):f {})", "Edge((B -> C):f {})"}

    def test_match_edge_with_double_wildcard_type(self):
        """Pattern ()-[E:*->*]->() matches all edges."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (graph.query().create("()-[::@f(A, B)]->()").create("()-[::@f(B, C)]->()").execute())

        result = graph.query().match("()-[E:*->*]->()").return_("E")
        assert len(result) == 2

    def test_match_edge_with_type_capture(self):
        """Pattern ()-[E:(X:*)->(Y:*)]->() captures type variables."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()

        result = graph.query().match("()-[E:(X:*)->(Y:*)]->()").return_("E", "X", "Y")
        assert len(result) == 1
        assert isinstance(result[0]["E"], implica.Edge)
        assert isinstance(result[0]["X"], implica.Type)
        assert isinstance(result[0]["Y"], implica.Type)
        assert str(result[0]["X"]) == "A"
        assert str(result[0]["Y"]) == "B"


class TestMatchEdgeEndpoints:
    """Tests for edge matching with endpoint constraints."""

    def test_match_edge_pattern_with_type_schema_on_endpoint(self):
        """Pattern (:A)-[E]->() filters by source node type."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("(:A)-[E]->()").return_("E")
        assert len(result) == 2
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])
        assert {str(d["E"]) for d in result} == {"Edge((A -> B):f {})", "Edge((A -> C):f {})"}

    def test_match_edge_with_target_endpoint_constraint(self):
        """Pattern ()-[E]->(:C) filters by target node type."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .create("()-[::@f(A, B)]->()")
            .execute()
        )

        result = graph.query().match("()-[E]->(:C)").return_("E")
        assert len(result) == 2
        assert {str(d["E"]) for d in result} == {"Edge((A -> C):f {})", "Edge((B -> C):f {})"}

    def test_match_edge_with_both_endpoint_constraints(self):
        """Pattern (:A)-[E]->(:B) filters by both endpoints."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("(:A)-[E]->(:B)").return_("E")
        assert len(result) == 1
        assert str(result[0]["E"]) == "Edge((A -> B):f {})"

    def test_match_edge_with_endpoint_properties(self):
        """Pattern (N { key: 'val' })-[E]->() filters by source properties."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A { key: 'val' })").create("(:A { key: 'other' })").create(
            "(:B)"
        ).execute()
        (
            graph.query()
            .create("(:A { key: 'val' })-[::@f(A, B)]->(:B)")
            .create("(:A { key: 'other' })-[::@f(A, B)]->(:B)")
            .execute()
        )

        result = graph.query().match("(N { key: 'val' })-[E]->()").return_("N", "E")
        assert len(result) == 1
        assert isinstance(result[0]["N"], implica.Node)
        assert result[0]["N"].properties()["key"] == "val"


class TestMatchEdgeProperties:
    """Tests for edge matching with property constraints."""

    def test_match_edge_with_properties(self):
        """Pattern ()-[E { weight: 10 }]->() matches edge with property."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B) { weight: 10 }]->()").execute()
        graph.query().create("()-[::@f(A, C) { weight: 20 }]->()").execute()
        graph.query().create("()-[::@f(B, C) { weight: 10 }]->()").execute()

        result = graph.query().match("()-[E { weight: 10 }]->()").return_("E")
        assert len(result) == 2
        assert all([isinstance(p["E"], implica.Edge) for p in result])
        assert all([p["E"].properties()["weight"] == 10 for p in result])  # type: ignore

    def test_match_edge_with_string_property(self):
        """Match edges by string property."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B) { label: 'edge1' }]->()").execute()
        graph.query().create("()-[::@f(A, C) { label: 'edge2' }]->()").execute()

        result = graph.query().match("()-[E { label: 'edge1' }]->()").return_("E")
        assert len(result) == 1

    def test_match_edge_with_multiple_properties(self):
        """Match edges by multiple properties."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B) { weight: 10, label: 'test' }]->()").execute()
        graph.query().create("()-[::@f(A, C) { weight: 10, label: 'other' }]->()").execute()

        result = graph.query().match("()-[E { weight: 10, label: 'test' }]->()").return_("E")
        assert len(result) == 1


class TestMatchEdgeTermSchema:
    """Tests for edge matching with term schemas."""

    def test_match_edge_with_wildcard_term(self):
        """Pattern ()-[E:*:*]->() matches edges with any term."""
        graph = implica.Graph(
            constants=[
                implica.Constant("f", "(A:*)->(B:*)"),
                implica.Constant("g", "(A:*)->(B:*)"),
            ]
        )
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()
        graph.query().create("()-[::@g(A, C)]->()").execute()

        result = graph.query().match("()-[E:*:*]->()").return_("E")
        assert len(result) == 2


# =============================================================================
# TEST PATH MATCHING
# =============================================================================


class TestMatchPathBasic:
    """Tests for path pattern matching."""

    def test_match_simple_path(self):
        """Match a simple two-node path."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()

        result = graph.query().match("(N)-[E]->(M)").return_("N", "E", "M")
        assert len(result) == 1
        assert str(result[0]["N"]) == "Node(A: {})"
        assert str(result[0]["M"]) == "Node(B: {})"

    def test_match_longer_path(self):
        """Match a three-node path."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()
        graph.query().create("()-[::@f(B, C)]->()").execute()

        result = graph.query().match("(N)-[E1]->(M)-[E2]->(O)").return_("N", "M", "O")
        assert len(result) == 1
        assert str(result[0]["N"]) == "Node(A: {})"
        assert str(result[0]["M"]) == "Node(B: {})"
        assert str(result[0]["O"]) == "Node(C: {})"

    def test_match_path_with_typed_nodes(self):
        """Path with type constraints on nodes."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()
        graph.query().create("()-[::@f(A, C)]->()").execute()

        result = graph.query().match("(:A)-[E]->(:B)").return_("E")
        assert len(result) == 1
        assert str(result[0]["E"]) == "Edge((A -> B):f {})"


class TestMatchPathMixedDirections:
    """Tests for paths with mixed edge directions."""

    def test_match_path_with_backward_edge(self):
        """Path with backward edge direction."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()
        graph.query().create("()-[::@f(C, B)]->()").execute()

        # From B, go backward to A
        result = graph.query().match("(:B)<-[E]-(:A)").return_("E")
        assert len(result) == 1
        assert str(result[0]["E"]) == "Edge((A -> B):f {})"

    def test_match_path_forward_then_backward(self):
        """Path: A->B, then backward from B to find who points to B."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()
        graph.query().create("()-[::@f(C, B)]->()").execute()

        result = graph.query().match("(N)-[E1]->(M)<-[E2]-(O)").return_("N", "M", "O", "E1", "E2")

        assert {str(p["N"]) for p in result} == {"Node(A: {})", "Node(C: {})"}
        assert {str(p["M"]) for p in result} == {"Node(B: {})"}
        assert {str(p["O"]) for p in result} == {"Node(A: {})", "Node(C: {})"}


# =============================================================================
# TEST CHAINED MATCHES
# =============================================================================


class TestChainedMatches:
    """Tests for chained match operations."""

    def test_chained_match_narrows_results(self):
        """Multiple match clauses narrow down results."""
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()

        result = graph.query().match("(N)").match("(N:A)").return_("N")
        assert len(result) == 1
        assert str(result[0]["N"]) == "Node(A: {})"

    def test_chained_match_with_different_variables(self):
        """Chain matches with different variables."""
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("(N:A)").match("(M:B)").return_("N", "M")
        assert len(result) == 1
        assert str(result[0]["N"]) == "Node(A: {})"
        assert str(result[0]["M"]) == "Node(B: {})"

    def test_chained_match_after_edge_match(self):
        """Chain node match after edge match."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()

        result = graph.query().match("(N)-[E]->(M)").match("(O:C)").return_("N", "M", "O")
        assert len(result) == 1
        assert str(result[0]["O"]) == "Node(C: {})"


# =============================================================================
# TEST VARIABLE REUSE
# =============================================================================


class TestVariableReuse:
    """Tests for variable reuse in patterns."""

    def test_same_variable_in_path_must_match_same_node(self):
        """Using same variable twice in path requires same node."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").execute()
        # Self-loop: A -> A
        graph.query().create("()-[::@f(A, A)]->()").create("()-[::@f(A, B)]->()").execute()

        # N-[E]->N requires same start and end node
        result = graph.query().match("(N)-[E]->(N)").return_("N", "E")
        assert len(result) == 1
        assert str(result[0]["N"]) == "Node(A: {})"

    def test_same_variable_no_self_loop_returns_empty(self):
        """Pattern (N)-[E]->(N) returns empty if no self-loop exists."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()

        result = graph.query().match("(N)-[E]->(N)").return_("N", "E")
        assert len(result) == 0

    def test_variable_from_previous_match_used_in_path(self):
        """Variable from previous match can constrain path."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()
        graph.query().create("()-[::@f(A, C)]->()").execute()
        graph.query().create("()-[::@f(C, B)]->()").execute()

        # First match A, then find edges from A to B specifically
        result = graph.query().match("(N:A)").match("(N)-[E]->(:B)").return_("N", "E")
        assert len(result) == 1
        assert str(result[0]["E"]) == "Edge((A -> B):f {})"


# =============================================================================
# TEST RETURN VARIATIONS
# =============================================================================


class TestReturnVariations:
    """Tests for different return patterns."""

    def test_return_no_variables(self):
        """return_() with no variables returns count of matches."""
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("()").return_()
        assert len(result) == 2
        # Results are empty dicts when no variables specified
        assert all([d == {} for d in result])

    def test_return_single_variable(self):
        """Return single captured variable."""
        graph = implica.Graph()
        graph.query().create("(:A)").execute()

        result = graph.query().match("(N)").return_("N")
        assert len(result) == 1
        assert "N" in result[0]

    def test_return_multiple_variables(self):
        """Return multiple captured variables."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()

        result = graph.query().match("(N)-[E]->(M)").return_("N", "E", "M")
        assert len(result) == 1
        assert "N" in result[0]
        assert "E" in result[0]
        assert "M" in result[0]

    def test_return_subset_of_variables(self):
        """Return only some of the captured variables."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        graph.query().create("(:A)").create("(:B)").execute()
        graph.query().create("()-[::@f(A, B)]->()").execute()

        result = graph.query().match("(N)-[E]->(M)").return_("E")
        assert len(result) == 1
        assert "E" in result[0]
        assert "N" not in result[0]
        assert "M" not in result[0]

    def test_return_type_variable(self):
        """Return captured type variable."""
        graph = implica.Graph()
        graph.query().create("(:A -> B)").execute()

        result = graph.query().match("(N:(X:*) -> *)").return_("N", "X")
        assert len(result) == 1
        assert isinstance(result[0]["N"], implica.Node)
        assert isinstance(result[0]["X"], implica.Type)

    def test_return_term_variable(self):
        """Return captured term variable"""
        graph = implica.Graph(constants=[implica.Constant("f", "A")])
        graph.query().create("(::@f())").execute()

        result = graph.query().match("(N::X)").return_("N", "X")
        assert len(result) == 1
        assert isinstance(result[0]["N"], implica.Node)
        assert isinstance(result[0]["X"], implica.Term)


# =============================================================================
# TEST ERROR CASES
# =============================================================================


class TestMatchErrors:
    """Tests for error handling in match operations."""

    def test_return_undefined_variable_raises_error(self):
        """Returning undefined variable raises error."""
        graph = implica.Graph()
        graph.query().create("(:A)").execute()

        with pytest.raises(KeyError):
            graph.query().match("(N)").return_("M")

    def test_invalid_pattern_syntax_raises_error(self):
        """Invalid pattern syntax raises error."""
        graph = implica.Graph()

        with pytest.raises(ValueError):
            graph.query().match("invalid pattern")

    def test_unbalanced_parentheses_raises_error(self):
        """Unbalanced parentheses in pattern raises error."""
        graph = implica.Graph()

        with pytest.raises(ValueError):
            graph.query().match("((N)")

    def test_empty_pattern_raises_error(self):
        """Empty pattern string raises error."""
        graph = implica.Graph()

        with pytest.raises(ValueError):
            graph.query().match("")


# =============================================================================
# TEST COMPLEX SCENARIOS
# =============================================================================


class TestComplexScenarios:
    """Tests for complex real-world-like scenarios."""

    def test_type_theory_application_types(self):
        """Match function application types in a type-theoretic graph."""
        graph = implica.Graph(
            constants=[
                implica.Constant("id", "(T:*) -> (T -> T)"),
                implica.Constant(
                    "compose", "(A:*) -> (B:*) -> (C:*) -> ((B -> C) -> ((A -> B) -> (A -> C)))"
                ),
            ]
        )

        # Create base types and function types
        (
            graph.query()
            .create("(:Int)")
            .create("(:String)")
            .create("(:Bool)")
            .create("(:Int -> Int)")
            .create("(:String -> Int)")
            .create("(:Int -> Bool)")
            .execute()
        )

        # Find all endofunctions (A -> A)
        result = graph.query().match("(N:(T:*) -> T)").return_("N", "T")
        assert len(result) == 1
        assert str(result[0]["T"]) == "Int"

    def test_diamond_pattern(self):
        """Match diamond-shaped subgraph."""
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        (graph.query().create("(:A)").create("(:B)").create("(:C)").create("(:D)").execute())
        # A -> B, A -> C, B -> D, C -> D
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, D)]->()")
            .create("()-[::@f(C, D)]->()")
            .execute()
        )

        # Find paths from A to D
        result = graph.query().match("(:A)-[E1]->(M)-[E2]->(:D)").return_("M", "E1", "E2")
        assert len(result) == 2
        middle_nodes = {str(r["M"]) for r in result}
        assert middle_nodes == {"Node(B: {})", "Node(C: {})"}


# =============================================================================
# LEGACY TEST CLASSES (kept for backward compatibility)
# =============================================================================


class TestMatchNodeQuery:
    """Legacy test class - kept for backward compatibility."""

    def test_empty_match_node_pattern_matches_all_nodes(self):
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("()").return_()

        assert len(result) == 2

    def test_empty_match_node_pattern_matches_and_captures_all_nodes(self):
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("(N:)").return_("N")

        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {"Node(A: {})", "Node(B: {})"}

    def test_match_node_pattern_with_type_schema(self):
        graph = implica.Graph()
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("(N:A)").return_("N")

        assert len(result) == 1
        assert "N" in result[0]
        assert isinstance(result[0]["N"], implica.Node)
        assert str(result[0]["N"]) == "Node(A: {})"

    def test_match_node_pattern_with_type_schema_and_term_schema(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A")])
        graph.query().create("(:A)").create("(:B)").execute()

        result = graph.query().match("(N:A:f)").return_("N")
        assert len(result) == 1
        assert "N" in result[0]
        assert isinstance(result[0]["N"], implica.Node)
        assert str(result[0]["N"]) == "Node(A:f {})"

        result = graph.query().match("(N:B:f)").return_("N")
        assert len(result) == 0

    def test_match_node_pattern_with_type_schema_that_matches_many(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A")])
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:A -> B)")
            .create("(:A -> C)")
            .create("(:B -> C)")
            .execute()
        )

        result = graph.query().match("(N:A -> *)").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {"Node((A -> B): {})", "Node((A -> C): {})"}

    def test_match_node_pattern_with_type_schema_and_term_schema_that_matches_many(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A"), implica.Constant("g", "B")])
        graph.query().create("(:A:@f())").create("(:B:@g())").create("(:C)").execute()

        result = graph.query().match("(N:*:*)").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {"Node(A:f {})", "Node(B:g {})"}

    def test_match_node_pattern_with_type_schema_and_properties_that_matches_many(self):
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "C -> A")]
        )
        (
            graph.query()
            .create("(:A { foo: 'var' })")
            .create("(:A -> B:@f() { foo: 'var'})")
            .create("(:B)")
            .create("(:C: { foo: 'not' })")
            .create("(:C -> A:@g())")
            .execute()
        )

        result = graph.query().match("(N:* { foo: 'var' })").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {
            'Node(A: {foo: "var"})',
            'Node((A -> B):f {foo: "var"})',
        }

    def test_match_node_pattern_with_term_schema_that_matches_many(self):
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "C -> A")]
        )
        (
            graph.query()
            .create("(:A { foo: 'var' })")
            .create("(:A -> B:@f() { foo: 'var'})")
            .create("(:B)")
            .create("(:C: { foo: 'not' })")
            .create("(:C -> A:@g())")
            .execute()
        )

        result = graph.query().match("(N::f)").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {
            "Node((C -> A):g {})",
            'Node((A -> B):f {foo: "var"})',
        }

    def test_match_node_pattern_with_properties_that_matches_many(self):
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "C -> A")]
        )
        (
            graph.query()
            .create("(:A { foo: 'var' })")
            .create("(:A -> B:@f() { foo: 'var'})")
            .create("(:B)")
            .create("(:C: { foo: 'not' })")
            .create("(:C -> A:@g())")
            .execute()
        )

        result = graph.query().match("(N { foo: 'var' })").return_("N")
        assert len(result) == 2
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {
            'Node(A: {foo: "var"})',
            'Node((A -> B):f {foo: "var"})',
        }


class TestMatchEdgeQuery:
    """Legacy test class - kept for backward compatibility."""

    def test_empty_match_edge_pattern_matches_all_edges(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("()-[]->()").return_()
        assert len(result) == 3

    def test_empty_match_edge_pattern_matches_and_captures_all_edges(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("()-[E]->()").return_("E")
        assert len(result) == 3
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])
        assert {str(d["E"]) for d in result} == {
            "Edge((A -> B):f {})",
            "Edge((A -> C):f {})",
            "Edge((B -> C):f {})",
        }

    def test_empty_match_edge_pattern_matches_and_captures_all_edges_and_endpoints(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("(N)-[E]->(M)").return_("N", "M", "E")
        assert len(result) == 3
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert all(["M" in d for d in result])
        assert all([isinstance(d["M"], implica.Node) for d in result])

        matches = {(str(d["N"]), str(d["E"]), str(d["M"])) for d in result}
        expected_matches = {
            ("Node(A: {})", "Edge((A -> B):f {})", "Node(B: {})"),
            ("Node(A: {})", "Edge((A -> C):f {})", "Node(C: {})"),
            ("Node(B: {})", "Edge((B -> C):f {})", "Node(C: {})"),
        }
        assert matches == expected_matches

    def test_match_edge_pattern_with_type_schema_matches_one_edge(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("()-[E:A->B]->()").return_("E")
        assert len(result) == 1
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])

        assert str(result[0]["E"]) == "Edge((A -> B):f {})"

    def test_match_edge_pattern_with_type_schema_matches_more_than_one_edge(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("()-[E:A->*]->()").return_("E")
        assert len(result) == 2
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])
        assert {str(d["E"]) for d in result} == {"Edge((A -> B):f {})", "Edge((A -> C):f {})"}

    def test_match_edge_pattern_with_type_schema_on_endpoint(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])

        graph.query().create("(:A)").create("(:B)").create("(:C)").execute()
        (
            graph.query()
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .create("()-[::@f(B, C)]->()")
            .execute()
        )

        result = graph.query().match("(:A)-[E]->()").return_("E")
        assert len(result) == 2
        assert all(["E" in d for d in result])
        assert all([isinstance(d["E"], implica.Edge) for d in result])
        assert {str(d["E"]) for d in result} == {"Edge((A -> B):f {})", "Edge((A -> C):f {})"}
