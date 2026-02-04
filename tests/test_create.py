import implica
import pytest


class TestCreateQuery:

    def test_create_query_with_minimal_node_pattern(self):
        graph = implica.Graph()

        graph.query().create("(:A)").execute()

        nodes = graph.nodes()

        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == "Node(A::{})"

    def test_create_query_with_capturing_node_pattern(self):
        graph = implica.Graph()

        graph.query().create("(N:A)").execute()

        nodes = graph.nodes()
        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == "Node(A::{})"

    def test_create_query_with_capturing_node_pattern_captures_node(self):
        graph = implica.Graph()

        result = graph.query().create("(N:A)").return_("N")
        assert len(result) == 1
        assert "N" in result[0]
        assert isinstance(result[0]["N"], implica.Node)
        assert str(result[0]["N"]) == "Node(A::{})"

    def test_create_query_with_node_pattern_with_only_constant_term(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A")])

        graph.query().create("(::@f())").execute()
        nodes = graph.nodes()
        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == "Node(A:f:{})"

    def test_create_query_with_node_pattern_with_type_and_constant_term(self):
        graph = implica.Graph(constants=[implica.Constant("f", "B -> C")])

        graph.query().create("(:B -> C:@f())")
        nodes = graph.nodes()
        assert len(nodes) == 1
        assert isinstance(nodes[0], implica.Node)
        assert str(nodes[0]) == "Node((B -> C):f:{})"
