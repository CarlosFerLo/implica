import implica
import pytest


class TestCreateNodeQuery:

    def test_create_query_with_minimal_node_pattern(self):
        graph = implica.Graph()

        graph.query().create("(:A)").execute()

        nodes = graph.nodes()

        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == "Node(A: {})"

    def test_create_query_with_capturing_node_pattern(self):
        graph = implica.Graph()

        graph.query().create("(N:A)").execute()

        nodes = graph.nodes()
        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == "Node(A: {})"

    def test_create_query_with_capturing_node_pattern_captures_node(self):
        graph = implica.Graph()

        result = graph.query().create("(N:A)").return_("N")
        assert len(result) == 1
        assert "N" in result[0]
        assert isinstance(result[0]["N"], implica.Node)
        assert str(result[0]["N"]) == "Node(A: {})"

    def test_create_query_with_node_pattern_with_only_constant_term(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A")])

        graph.query().create("(::@f())").execute()
        nodes = graph.nodes()
        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == "Node(A:f {})"

    def test_create_query_with_node_pattern_with_type_and_constant_term(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A")])

        graph.query().create("(:A:@f())").execute()
        nodes = graph.nodes()
        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == "Node(A:f {})"

    def test_create_query_with_node_pattern_and_properties(self):
        graph = implica.Graph()

        graph.query().create("(:A:{ foo: 'var' })").execute()
        nodes = graph.nodes()
        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == 'Node(A: {foo: "var"})'

    def test_create_query_with_node_pattern_and_more_than_one_property(self):
        graph = implica.Graph()

        graph.query().create(
            "(:A:{ string: 'value', bool: true, integer: 5, float: 0.2, list: [1, 2, 3], dict: { foo: 'var'} })"
        ).execute()
        nodes = graph.nodes()
        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert nodes[0].properties() == {
            "string": "value",
            "bool": True,
            "integer": 5,
            "float": 0.2,
            "list": [1, 2, 3],
            "dict": {"foo": "var"},
        }

    def test_create_query_with_node_pattern_and_parametrized_constant(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*) -> (B:*)")])

        graph.query().create("(::@f(C, D))").execute()
        nodes = graph.nodes()
        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == "Node((C -> D):f {})"

    def test_create_more_than_one_node_in_different_queries(self):
        graph = implica.Graph()

        graph.query().create("(:A)").execute()
        graph.query().create("(:B)").execute()

        nodes = graph.nodes()
        assert len(nodes) == 2
        assert {str(n) for n in nodes} == {"Node(A: {})", "Node(B: {})"}

    def test_create_more_than_one_node_in_the_same_query(self):
        graph = implica.Graph()

        graph.query().create("(:A)").create("(:B)").execute()

        nodes = graph.nodes()
        assert len(nodes) == 2
        assert {str(n) for n in nodes} == {"Node(A: {})", "Node(B: {})"}

    def test_create_query_with_multiple_create_statements_captures_nodes_correctly(self):
        graph = implica.Graph()

        result = graph.query().create("(N:A)").create("(M:B)").return_("N", "M")

        assert len(result) == 1
        assert "M" in result[0]
        assert str(result[0]["M"]) == "Node(B: {})"
        assert "N" in result[0]
        assert str(result[0]["N"]) == "Node(A: {})"

    def test_create_query_fails_if_it_has_incompatible_type_term(self):
        graph = implica.Graph(constants=[implica.Constant("f", "B")])

        with pytest.raises(ValueError):
            graph.query().create("(N:A:@f())").execute()


class TestCreateEdgeQuery:
    def test_create_query_with_edge_pattern(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A -> B")])

        graph.query().create("(:A)").create("(:B)").execute()

        graph.query().create("(:A)-[::@f()]->(:B)").execute()

        edges = graph.edges()

        assert len(edges) == 1
        assert isinstance(edges[0], implica.Edge)
        assert str(edges[0]) == "Edge((A -> B):f {})"

    def test_create_query_with_node_and_edges(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A -> B")])

        graph.query().create("(:A)-[::@f()]->(:B)").execute()

        nodes = graph.nodes()
        assert len(nodes) == 2
        assert {str(n) for n in nodes} == {"Node(A: {})", "Node(B: {})"}

        edges = graph.edges()
        assert len(edges) == 1
        assert isinstance(edges[0], implica.Edge)
        assert str(edges[0]) == "Edge((A -> B):f {})"

    def test_create_query_with_edge_pattern_infers_endpoint_types(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A -> B")])

        graph.query().create("()-[::@f()]->()").execute()

        nodes = graph.nodes()
        assert len(nodes) == 2
        assert {str(n) for n in nodes} == {"Node(A: {})", "Node(B: {})"}

        edges = graph.edges()
        assert len(edges) == 1
        assert isinstance(edges[0], implica.Edge)
        assert str(edges[0]) == "Edge((A -> B):f {})"

    def test_create_query_with_edge_pattern_fails_if_endpoint_types_are_incompatible(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A -> B")])

        with pytest.raises(TypeError):
            graph.query().create("(:B)-[::@f()]->(:A)").execute()

    def test_create_query_with_edge_pattern_fails_if_type_and_term_specified_do_not_match(self):
        graph = implica.Graph(constants=[implica.Constant("f", "C -> D")])

        with pytest.raises(ValueError):
            graph.query().create("(:A)-[:A -> B:@f()]->(:B)").execute()

    def test_create_query_with_forward_edge_pattern_infers_term_for_left_node(self):
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "A")]
        )

        graph.query().create("(:A)-[::@f()]->(:B:@f() @g())").execute()

        nodes = graph.nodes()
        assert len(nodes) == 2
        assert {str(n) for n in nodes} == {"Node(A:g {})", "Node(B:(f g) {})"}

    def test_create_query_with_forward_edge_pattern_infers_term_for_right_node(self):
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "A")]
        )

        graph.query().create("(:A:@g())-[::@f()]->(:B)").execute()

        nodes = graph.nodes()
        assert len(nodes) == 2
        assert {str(n) for n in nodes} == {"Node(A:g {})", "Node(B:(f g) {})"}

    def test_create_query_with_forward_edge_pattern_infers_term_for_edge(self):
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "A")]
        )

        graph.query().create("(::@g())-[]->(::@f() @g())").execute()

        edges = graph.edges()
        assert len(edges) == 1
        assert str(edges[0]) == "Edge((A -> B):f {})"

    def test_create_query_with_backward_edge_pattern_infers_term_for_left_node(self):
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "A")]
        )

        graph.query().create("(:B)<-[::@f()]-(:A:@g())").execute()

        nodes = graph.nodes()
        assert len(nodes) == 2
        assert {str(n) for n in nodes} == {"Node(A:g {})", "Node(B:(f g) {})"}

    def test_create_query_with_backward_edge_pattern_infers_term_for_right_node(self):
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "A")]
        )

        graph.query().create("(:B:@f() @g())<-[::@f()]-(:A)").execute()

        nodes = graph.nodes()
        assert len(nodes) == 2
        assert {str(n) for n in nodes} == {"Node(A:g {})", "Node(B:(f g) {})"}

    def test_create_query_with_backward_edge_pattern_infers_term_for_edge(self):
        graph = implica.Graph(
            constants=[implica.Constant("f", "A -> B"), implica.Constant("g", "A")]
        )

        graph.query().create("(::@f() @g())<-[]-(::@g())").execute()

        edges = graph.edges()
        assert len(edges) == 1
        assert str(edges[0]) == "Edge((A -> B):f {})"
