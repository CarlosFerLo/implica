import pytest
import implica


class TestMatchNodeQuery:
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


# TODO: Continue testing match
