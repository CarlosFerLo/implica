import pytest
import implica


class TestMatchCreateQuery:
    def test_match_edge_term_and_create_node(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A->B")])
        graph.query().create("(:A)").create("(:B)").create("()-[::@f()]->()").execute()

        graph.query().match("()-[::g]->()").create("(::g)").execute()

        result = graph.query().match("(N:(A -> B))").return_("N")
        assert len(result) == 1

    def test_match_node_type_and_use_it_as_argument_to_constant(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->A")])
        graph.query().create("(:A)").execute()

        graph.query().match("(:(X:*))").create("()-[::@f(X)]->()").execute()

        edges = graph.edges()
        assert len(edges) == 1
        assert str(edges[0]) == "Edge((A -> A):f {})"

    def test_match_node_types_and_create_edge_and_term(self):
        graph = implica.Graph(constants=[implica.Constant("K", "(A:*) -> (B:*) -> A")])
        graph.query().create("(:A -> A { existed: true })").execute()

        (
            graph.query()
            .match("(N: (X:*) { existed: true })")
            .match("(M: (Y:*) { existed: true })")
            .create("(N)-[::@K(X, Y)]->(:Y -> X { existed: false })")
            .execute()
        )

        nodes = graph.nodes()
        assert len(nodes) == 2
        assert {str(n) for n in nodes} == {
            "Node((A -> A): {existed: true})",
            "Node(((A -> A) -> (A -> A)): {existed: false})",
        }

        edges = graph.edges()
        assert len(edges) == 1
        assert str(edges[0]) == "Edge(((A -> A) -> ((A -> A) -> (A -> A))):K {})"
