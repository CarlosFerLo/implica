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

    def test_match_node_pattern_with_no_requirements_matches_many(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A")])
        (graph.query().create("(:A:@f())").create("(:B { some: 4 })").create("(:C)").execute())

        result = graph.query().match("(N)").return_("N")
        assert len(result) == 3
        assert all(["N" in d for d in result])
        assert all([isinstance(d["N"], implica.Node) for d in result])
        assert {str(d["N"]) for d in result} == {
            "Node(A:f {})",
            "Node(B: {some: 4})",
            "Node(C: {})",
        }
