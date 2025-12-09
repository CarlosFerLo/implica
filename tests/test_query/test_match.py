import pytest
import implica


class TestMatchQueryNodes:
    def test_match_node_query_without_constraints(self, graph_with_nodes):
        results = graph_with_nodes.query().match(node="N").return_("N")

        assert len(results) == 4
        for result in results:
            assert "N" in result
            assert isinstance(result["N"], implica.Node)

    def test_match_node_query_with_type_constraint(self, graph_with_nodes, type_a):
        results = graph_with_nodes.query().match(node="N", type=type_a).return_("N")

        assert len(results) == 1
        for result in results:
            assert "N" in result
            assert isinstance(result["N"], implica.Node)
            assert result["N"].type == type_a

    def test_match_node_query_with_nonexistent_type(self, graph_with_nodes):
        fake_type = implica.Variable("NonExistentType")
        results = graph_with_nodes.query().match(node="N", type=fake_type).return_("N")

        assert len(results) == 0

    def test_match_node_query_with_type_schema(self, graph_with_nodes):
        type_schema = implica.TypeSchema("A")
        results = graph_with_nodes.query().match(node="N", type_schema=type_schema).return_("N")

        assert len(results) == 1
        for result in results:
            assert "N" in result
            assert isinstance(result["N"], implica.Node)
            assert result["N"].type.name == "A"

    def test_match_node_query_with_term(self, graph_with_nodes, term_a):
        results = graph_with_nodes.query().match(node="N", term=term_a).return_("N")

        assert len(results) == 1
        for result in results:
            assert "N" in result
            assert isinstance(result["N"], implica.Node)
            assert result["N"].term == term_a

    def test_match_node_query_with_nonexistent_term(self, graph_with_nodes, term_b):
        results = graph_with_nodes.query().match(node="N", term=term_b).return_("N")

        assert len(results) == 0

    def test_match_node_query_with_term_schema(self, graph_with_nodes):
        term_schema = implica.TermSchema("x")
        results = graph_with_nodes.query().match(node="N", term_schema=term_schema).return_("N")

        assert len(results) == 1
        for result in results:
            assert "N" in result
            assert isinstance(result["N"], implica.Node)
            assert result["N"].term is not None

    def test_match_node_query_with_type_and_term_constraints(
        self, graph_with_nodes, type_a, term_a
    ):
        results = graph_with_nodes.query().match(node="N", type=type_a, term=term_a).return_("N")

        assert len(results) == 1
        for result in results:
            assert "N" in result
            assert isinstance(result["N"], implica.Node)
            assert result["N"].type == type_a
            assert result["N"].term == term_a

    def test_match_node_query_with_type_and_type_schema_files_to_build(
        self, graph_with_nodes, type_a
    ):
        type_schema = implica.TypeSchema("B")

        with pytest.raises(ValueError):
            graph_with_nodes.query().match(node="N", type=type_a, type_schema=type_schema)

    def test_match_node_query_with_term_and_term_schema_fails_to_build(
        self, graph_with_nodes, term_a
    ):
        term_schema = implica.TermSchema("y")

        with pytest.raises(ValueError):
            graph_with_nodes.query().match(node="N", term=term_a, term_schema=term_schema)

    def test_match_node_with_type_schema_and_context(self, graph_with_nodes, type_a):
        type_schema = implica.TypeSchema("X")

        results = (
            graph_with_nodes.query()
            .add("X", type=type_a)
            .match(node="N", type_schema=type_schema)
            .return_("N")
        )

        assert len(results) == 1
        for result in results:
            assert "N" in result
            assert isinstance(result["N"], implica.Node)
            assert result["N"].type == type_a

    def test_match_node_with_term_schema_and_context(self, graph_with_nodes, term_a):
        term_schema = implica.TermSchema("y")

        results = (
            graph_with_nodes.query()
            .add("y", term=term_a)
            .match(node="N", term_schema=term_schema)
            .return_("N")
        )

        assert len(results) == 1
        for result in results:
            assert "N" in result
            assert isinstance(result["N"], implica.Node)
            assert result["N"].term == term_a

    def test_match_node_chaining_with_independent_variables(self, graph_with_nodes, type_a, type_b):
        results = (
            graph_with_nodes.query()
            .match(node="N1", type=type_a)
            .match(node="N2", type=type_b)
            .return_("N1", "N2")
        )

        assert len(results) == 1
        for result in results:
            assert "N1" in result
            assert "N2" in result
            assert isinstance(result["N1"], implica.Node)
            assert isinstance(result["N2"], implica.Node)
            assert result["N1"].type == type_a
            assert result["N2"].type == type_b


class TestMatchQueryEdges:
    def test_match_edge_query_without_constraints(self, graph_with_edges):
        results = graph_with_edges.query().match(edge="E").return_("E")

        assert len(results) == 2
        for result in results:
            assert "E" in result
            assert isinstance(result["E"], implica.Edge)

    def test_match_edge_query_without_constraints_capturing_start(self, graph_with_edges):
        results = graph_with_edges.query().match(edge="E", start="S").return_("E", "S")

        assert len(results) == 2
        for result in results:
            assert "E" in result
            assert "S" in result
            assert isinstance(result["E"], implica.Edge)
            assert isinstance(result["S"], implica.Node)

    def test_match_edge_query_without_constraints_capturing_end(self, graph_with_edges):
        results = graph_with_edges.query().match(edge="E", end="T").return_("E", "T")

        assert len(results) == 2
        for result in results:
            assert "E" in result
            assert "T" in result
            assert isinstance(result["E"], implica.Edge)
            assert isinstance(result["T"], implica.Node)

    def test_match_edge_query_without_constraints_capturing_start_and_end(self, graph_with_edges):
        results = (
            graph_with_edges.query().match(edge="E", start="S", end="T").return_("E", "S", "T")
        )

        assert len(results) == 2
        for result in results:
            assert "E" in result
            assert "S" in result
            assert "T" in result
            assert isinstance(result["E"], implica.Edge)
            assert isinstance(result["S"], implica.Node)
            assert isinstance(result["T"], implica.Node)

    def test_match_edge_query_with_type_constraint(self, graph_with_edges, term_ab, arrow_ab):
        results = graph_with_edges.query().match(edge="E", type=arrow_ab).return_("E")

        assert len(results) == 1
        for result in results:
            assert "E" in result
            assert isinstance(result["E"], implica.Edge)
            assert result["E"].term == term_ab

    def test_match_edge_query_with_term_constraint(self, graph_with_edges, term_ab):
        results = graph_with_edges.query().match(edge="E", term=term_ab).return_("E")

        assert len(results) == 1
        for result in results:
            assert "E" in result
            assert isinstance(result["E"], implica.Edge)
            assert result["E"].term == term_ab

    def test_match_edge_query_with_type_and_term_constraints(
        self, graph_with_edges, arrow_ab, term_ab
    ):
        results = graph_with_edges.query().match(edge="E", type=arrow_ab, term=term_ab).return_("E")

        assert len(results) == 1
        for result in results:
            assert "E" in result
            assert isinstance(result["E"], implica.Edge)
            assert result["E"].term == term_ab

    def test_match_edge_query_with_nonexistent_type(self, graph_with_edges):
        fake_type = implica.Variable("NonExistentEdgeType")
        results = graph_with_edges.query().match(edge="E", type=fake_type).return_("E")

        assert len(results) == 0

    def test_match_edge_query_with_nonexistent_term(self, graph_with_edges, arrow_ab):
        fake_term = implica.BasicTerm("nonexistent_term", arrow_ab)
        results = graph_with_edges.query().match(edge="E", term=fake_term).return_("E")
        assert len(results) == 0

    def test_match_edge_query_with_two_independent_edges(
        self, graph_with_edges, arrow_ab, term_ab, arrow_ba, term_ba
    ):
        results = (
            graph_with_edges.query()
            .match(edge="E1", type=arrow_ab, term=term_ab)
            .match(edge="E2", type=arrow_ba, term=term_ba)
            .return_("E1", "E2")
        )

        assert len(results) == 1
        for result in results:
            assert "E1" in result
            assert "E2" in result
            assert isinstance(result["E1"], implica.Edge)
            assert isinstance(result["E2"], implica.Edge)
            assert result["E1"].term == term_ab
            assert result["E2"].term == term_ba


class TestMatchQueryPath:
    def test_match_path_query_simple(self, graph_with_edges, arrow_ab, term_ab):
        results = graph_with_edges.query().match("(N1)-[E]->(N2)").return_("N1", "E", "N2")

        assert len(results) == 2
        for result in results:
            assert "N1" in result
            assert "E" in result
            assert "N2" in result
            assert isinstance(result["N1"], implica.Node)
            assert isinstance(result["E"], implica.Edge)
            assert isinstance(result["N2"], implica.Node)

    def test_match_path_query_with_type_constraint_on_nodes(
        self, graph_with_edges, type_a, type_b, term_ab
    ):
        results = graph_with_edges.query().match("(N1:A)-[E]->(N2:B)").return_("N1", "E", "N2")

        assert len(results) == 1
        for result in results:
            assert "N1" in result
            assert "E" in result
            assert "N2" in result
            assert isinstance(result["N1"], implica.Node)
            assert isinstance(result["E"], implica.Edge)
            assert isinstance(result["N2"], implica.Node)
            assert result["N1"].type == type_a
            assert result["E"].term == term_ab
            assert result["N2"].type == type_b

    def test_match_path_query_with_type_constraint_on_edge(
        self, graph_with_edges, type_a, type_b, term_ab
    ):
        results = graph_with_edges.query().match("(N1)-[E:A->B]->(N2)").return_("N1", "E", "N2")

        assert len(results) == 1
        for result in results:
            assert "N1" in result
            assert "E" in result
            assert "N2" in result
            assert isinstance(result["N1"], implica.Node)
            assert isinstance(result["E"], implica.Edge)
            assert isinstance(result["N2"], implica.Node)
            assert result["N1"].type == type_a
            assert result["E"].term == term_ab
            assert result["N2"].type == type_b

    def test_match_path_query_with_term_constraint_on_edge(
        self, graph_with_edges, type_a, type_b, term_ab
    ):
        results = (
            graph_with_edges.query()
            .add("x", term=term_ab)
            .match("(N1)-[E::x]->(N2)")
            .return_("N1", "E", "N2")
        )

        assert len(results) == 1
        for result in results:
            assert "N1" in result
            assert "E" in result
            assert "N2" in result
            assert isinstance(result["N1"], implica.Node)
            assert isinstance(result["E"], implica.Edge)
            assert isinstance(result["N2"], implica.Node)
            assert result["N1"].type == type_a
            assert result["E"].term == term_ab
            assert result["N2"].type == type_b

    def test_match_path_query_with_backward_edge(self, graph_with_edges, type_a, type_b, term_ba):
        results = graph_with_edges.query().match("(N1: A)<-[E]-(N2: B)").return_("N1", "E", "N2")

        assert len(results) == 1
        for result in results:
            assert "N1" in result
            assert "E" in result
            assert "N2" in result
            assert isinstance(result["N1"], implica.Node)
            assert isinstance(result["E"], implica.Edge)
            assert isinstance(result["N2"], implica.Node)
            assert result["N1"].type == type_a
            assert result["E"].term == term_ba
            assert result["N2"].type == type_b

    def test_match_path_query_with_any_direction_edge(
        self, graph_with_edges, type_a, type_b, term_ab, term_ba
    ):
        results = graph_with_edges.query().match("(N1: A)-[E]-(N2: B)").return_("N1", "E", "N2")

        assert len(results) == 2
        for result in results:
            assert "N1" in result
            assert "E" in result
            assert "N2" in result
            assert isinstance(result["N1"], implica.Node)
            assert isinstance(result["E"], implica.Edge)
            assert isinstance(result["N2"], implica.Node)
            assert result["N1"].type == type_a
            assert result["N2"].type == type_b
            assert result["E"].term in {term_ab, term_ba}

    def test_match_path_query_with_impossible_constraints(self, graph_with_edges, type_a, type_b):
        results = graph_with_edges.query().match("(N1: A)-[E]->(N2: A)").return_("N1", "E", "N2")

        assert len(results) == 0
