import pytest
import implica


class TestGraph:
    def test_graph_init(self):
        graph = implica.Graph()
        assert isinstance(graph, implica.Graph)


class TestGraphAutocompleteNodeTerm:
    def test_graph_autocompletes_node_term_if_it_matches_schema_create_node_with_type(
        self, graph_with_K_S, type_a, type_b
    ):
        graph = graph_with_K_S
        type = implica.Arrow(type_a, implica.Arrow(type_b, type_a))

        graph.query().create(node="N", type=type).execute()

        node = graph.query().match(node="N").return_("N")[0]["N"]

        assert node.type == type
        assert node.term is not None

    def test_graph_autocompletes_node_term_if_it_matches_schema_merge_node_with_type(
        self, graph_with_K_S, type_a, type_b
    ):
        graph = graph_with_K_S
        type = implica.Arrow(type_a, implica.Arrow(type_b, type_a))

        graph.query().merge(node="N", type=type).execute()

        node = graph.query().match(node="N").return_("N")[0]["N"]

        assert node.type == type
        assert node.term is not None

    def test_graph_autocompletes_node_term_if_it_matches_schema_create_node_with_type_schema(
        self, graph_with_K_S
    ):
        graph = graph_with_K_S
        type_schema = implica.TypeSchema("A -> B -> A")

        graph.query().create(node="N", type_schema=type_schema).execute()

        node = graph.query().match(node="N").return_("N")[0]["N"]

        assert node.type == type_schema.as_type()
        assert node.term is not None

    def test_graph_autocompletes_node_term_if_it_matches_schema_merge_node_with_type_schema(
        self, graph_with_K_S
    ):
        graph = graph_with_K_S
        type_schema = implica.TypeSchema("A -> B -> A")

        graph.query().merge(node="N", type_schema=type_schema).execute()

        node = graph.query().match(node="N").return_("N")[0]["N"]

        assert node.type == type_schema.as_type()
        assert node.term is not None

    def test_graph_autocomplete_node_term_with_complex_type(self, graph_with_K_S, type_a, type_b):
        graph = graph_with_K_S
        type = implica.Arrow(
            implica.Arrow(type_a, type_b), implica.Arrow(type_a, implica.Arrow(type_a, type_b))
        )

        graph.query().create(node="N", type=type).execute()

        node = graph.query().match(node="N").return_("N")[0]["N"]

        assert node.type == type
        assert node.term is not None

    def test_graph_autocomplete_node_term_with_create_path_single_node(
        self, graph_with_K_S, type_a, type_b
    ):
        graph = graph_with_K_S
        type = implica.Arrow(type_a, implica.Arrow(type_b, type_a))

        graph.query().create("(N: A -> B -> A)").execute()

        node = graph.query().match(node="N").return_("N")[0]["N"]

        assert node.type == type
        assert node.term is not None

    def test_graph_autocomplete_node_term_with_merge_path_single_node(
        self, graph_with_K_S, type_a, type_b
    ):
        graph = graph_with_K_S
        type = implica.Arrow(type_a, implica.Arrow(type_b, type_a))

        graph.query().merge("(N: A -> B -> A)").execute()

        node = graph.query().match(node="N").return_("N")[0]["N"]

        assert node.type == type
        assert node.term is not None

    def test_graph_autocomplete_node_term_with_create_path_complex(
        self, graph_with_K_S, type_a, type_b
    ):
        graph = graph_with_K_S
        type = implica.Arrow(type_a, implica.Arrow(type_b, type_a))
        term = implica.BasicTerm("x", implica.Arrow(type, type_a))

        graph.query().add("f", term=term).create("(N: A -> B -> A)-[::f]->(:A)").execute()

        node = graph.query().match("(N:A->B->A)").return_("N")[0]["N"]

        assert node.type == type
        assert node.term is not None

    def test_graph_autocomplete_node_term_with_merge_path_complex(
        self, graph_with_K_S, type_a, type_b
    ):
        graph = graph_with_K_S
        type = implica.Arrow(type_a, implica.Arrow(type_b, type_a))
        term = implica.BasicTerm("x", implica.Arrow(type, type_a))

        graph.query().add("f", term=term).merge("(N: A -> B -> A)-[::f]->(:A)").execute()

        node = graph.query().match("(N:A->B->A)").return_("N")[0]["N"]

        assert node.type == type
        assert node.term is not None


class TestGraphAutocompleteTermsForwardEdges:

    def test_graph_autocomplete_term_forward_edge_create(
        self, graph_with_K_S, type_a, type_b, term_a, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a, term=term_a).create(
            node="N2", type=type_b
        ).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is None

        graph.query().match("(N1:A)").match("(N2:B)").create(
            edge="E", start="N1", end="N2", term=term_ab
        ).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is not None

    def test_graph_autocomplete_term_forward_edge_merge(
        self, graph_with_K_S, type_a, type_b, term_a, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a, term=term_a).create(
            node="N2", type=type_b
        ).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is None

        graph.query().match("(N1:A)").match("(N2:B)").merge(
            edge="E", start="N1", end="N2", term=term_ab
        ).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is not None

    def test_graph_autocomplete_term_forward_edge_create_path(
        self, graph_with_K_S, type_a, type_b, term_a, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a, term=term_a).create(
            node="N2", type=type_b
        ).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is None

        graph.query().add("f", term=term_ab).create("(N1:A)-[::f]->(N2:B)").execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is not None

    def test_graph_autocomplete_term_forward_edge_merge_path(
        self, graph_with_K_S, type_a, type_b, term_a, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a, term=term_a).create(
            node="N2", type=type_b
        ).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is None

        graph.query().add("f", term=term_ab).merge("(N1:A)-[::f]->(N2:B)").execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is not None

    def test_graph_autocomplete_term_forward_edge_create_path_multiple_edges(
        self, graph_with_K_S, type_a, type_b, type_c, term_a, term_ab, term_bc
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a, term=term_a).create(
            node="N2", type=type_b
        ).create(node="N3", type=type_c).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is None

        res = graph.query().match("(N:C)").return_("N")
        node_c = res[0]["N"]
        assert node_c.type == type_c
        assert node_c.term is None

        graph.query().add("f", term=term_ab).add("g", term=term_bc).create(
            "(:A)-[::f]->(:B)-[::g]->(:C)"
        ).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is not None
        res = graph.query().match("(N:C)").return_("N")
        node_c = res[0]["N"]
        assert node_c.type == type_c
        assert node_c.term is not None

    def test_graph_autocomplete_term_forward_edge_merge_path_multiple_edges(
        self, graph_with_K_S, type_a, type_b, type_c, term_a, term_ab, term_bc
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a, term=term_a).create(
            node="N2", type=type_b
        ).create(node="N3", type=type_c).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is None

        res = graph.query().match("(N:C)").return_("N")
        node_c = res[0]["N"]
        assert node_c.type == type_c
        assert node_c.term is None

        graph.query().add("f", term=term_ab).add("g", term=term_bc).merge(
            "(:A)-[::f]->(:B)-[::g]->(:C)"
        ).execute()

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is not None
        res = graph.query().match("(N:C)").return_("N")
        node_c = res[0]["N"]
        assert node_c.type == type_c
        assert node_c.term is not None


class TestGraphAutocompleteTermsBackwardEdges:
    def test_graph_autocomplete_term_backward_edge_create(
        self, graph_with_K_S, type_a, type_b, term_a, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(
            node="N2", type=type_b, term=term_ab(term_a)
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is None

        graph.query().match("(N1:A)").match("(N2:B)").create(
            edge="E", start="N1", end="N2", term=term_ab
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is not None

    def test_graph_autocomplete_term_backward_edge_merge(
        self, graph_with_K_S, type_a, type_b, term_a, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(
            node="N2", type=type_b, term=term_ab(term_a)
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is None

        graph.query().match("(N1:A)").match("(N2:B)").merge(
            edge="E", start="N1", end="N2", term=term_ab
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is not None

    def test_graph_autocomplete_term_backward_edge_create_path(
        self, graph_with_K_S, type_a, type_b, term_a, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(
            node="N2", type=type_b, term=term_ab(term_a)
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is None

        graph.query().add("f", term=term_ab).create("(N1:A)-[::f]->(N2:B)").execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is not None

    def test_graph_autocomplete_term_backward_edge_merge_path(
        self, graph_with_K_S, type_a, type_b, term_a, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(
            node="N2", type=type_b, term=term_ab(term_a)
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is None

        graph.query().add("f", term=term_ab).merge("(N1:A)-[::f]->(N2:B)").execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is not None

    def test_graph_autocomplete_term_backward_edge_create_path_multiple_edges(
        self, graph_with_K_S, type_a, type_b, type_c, term_a, term_ab, term_bc
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(node="N2", type=type_b).create(
            node="N3", type=type_c, term=term_bc(term_ab(term_a))
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is None

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is None

        graph.query().add("f", term=term_ab).add("g", term=term_bc).create(
            "(:A)-[::f]->(:B)-[::g]->(:C)"
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is not None
        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is not None

    def test_graph_autocomplete_term_backward_edge_merge_path_multiple_edges(
        self, graph_with_K_S, type_a, type_b, type_c, term_a, term_ab, term_bc
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(node="N2", type=type_b).create(
            node="N3", type=type_c, term=term_bc(term_ab(term_a))
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is None

        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is None

        graph.query().add("f", term=term_ab).add("g", term=term_bc).merge(
            "(:A)-[::f]->(:B)-[::g]->(:C)"
        ).execute()

        res = graph.query().match("(N:A)").return_("N")
        node_a = res[0]["N"]
        assert node_a.type == type_a
        assert node_a.term is not None
        res = graph.query().match("(N:B)").return_("N")
        node_b = res[0]["N"]
        assert node_b.type == type_b
        assert node_b.term is not None


class TestGraphAutocompleteNodeTermEdgeTerm:
    def test_graph_autocomplete_node_term_edge_term_create(
        self, graph_with_K_S, type_a, type_b, arrow_ab, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(node="N2", type=type_b).execute()

        graph.query().create(node="N3", type=arrow_ab, term=term_ab).execute()

        res = graph.query().match("(:A)-[E]->(:B)").return_("E")
        assert len(res) == 1

    def test_graph_autocomplete_node_term_edge_term_merge(
        self, graph_with_K_S, type_a, type_b, arrow_ab, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(node="N2", type=type_b).execute()

        graph.query().merge(node="N3", type=arrow_ab, term=term_ab).execute()

        res = graph.query().match("(:A)-[E]->(:B)").return_("E")
        assert len(res) == 1

    def test_graph_autocomplete_node_term_edge_term_create_path(
        self, graph_with_K_S, type_a, type_b, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(node="N2", type=type_b).execute()

        graph.query().add("f", term=term_ab).create("(N3:A -> B:f)").execute()

        res = graph.query().match("(:A)-[E]->(:B)").return_("E")
        assert len(res) == 1

    def test_graph_autocomplete_node_term_edge_term_merge_path(
        self, graph_with_K_S, type_a, type_b, term_ab
    ):
        graph = graph_with_K_S

        graph.query().create(node="N1", type=type_a).create(node="N2", type=type_b).execute()

        graph.query().add("f", term=term_ab).merge("(N3:A -> B:f)").execute()

        res = graph.query().match("(:A)-[E]->(:B)").return_("E")
        assert len(res) == 1
