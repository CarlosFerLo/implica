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
