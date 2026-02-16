import implica
import pytest


class TestSetQueryNode:
    def test_set_query_on_node_with_no_properties_with_overwrite(self):
        graph = implica.Graph()
        graph.query().create("(:A)").execute()

        graph.query().match("(N)").set("N", {"name": "John Doe"}).execute()

        nodes = graph.nodes()
        assert nodes[0].properties() == {"name": "John Doe"}

    def test_set_query_on_node_with_no_properties_without_overwrite(self):
        graph = implica.Graph()
        graph.query().create("(:A)").execute()

        graph.query().match("(N)").set("N", {"name": "John Doe"}, False).execute()

        nodes = graph.nodes()
        assert nodes[0].properties() == {"name": "John Doe"}

    def test_set_query_on_node_with_existing_properties_and_overwrite(self):
        graph = implica.Graph()
        graph.query().create("(:A { name: 'John Doe' })").execute()

        graph.query().match("(N)").set("N", {"age": 5}).execute()

        nodes = graph.nodes()
        assert nodes[0].properties() == {"age": 5}

    def test_set_query_on_node_with_existing_properties_and_non_overwrite(self):
        graph = implica.Graph()
        graph.query().create("(:A { name: 'John Doe' })").execute()

        graph.query().match("(N)").set("N", {"age": 5}, False).execute()

        nodes = graph.nodes()
        assert nodes[0].properties() == {"name": "John Doe", "age": 5}

    def test_set_query_on_node_with_existing_properties_on_more_than_one_node(self):
        graph = implica.Graph()
        graph.query().create("(:A { name: 'Ferran' })").create("(:B { name: 'Julia' })").execute()

        graph.query().match("(N)").set("N", {"age": 21}, False).execute()

        nodes = graph.nodes()
        assert sorted([n.properties() for n in nodes], key=lambda x: x["name"]) == [
            {"name": "Ferran", "age": 21},
            {"name": "Julia", "age": 21},
        ]


class TestSetQueryEdge:
    def test_set_query_edge_with_no_properties_with_overwrite(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A -> B")])
        graph.query().create("(:A)").create("(:B)").create("()-[::@f()]->()").execute()

        graph.query().match("()-[E]->()").set("E", {"name": "John Doe"}).execute()

        edges = graph.edges()
        assert edges[0].properties() == {"name": "John Doe"}

    def test_set_query_edge_with_no_properties_without_overwrite(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A -> B")])
        graph.query().create("(:A)").create("(:B)").create("()-[::@f()]->()").execute()

        graph.query().match("()-[E]->()").set("E", {"name": "John Doe"}, False).execute()

        edges = graph.edges()
        assert edges[0].properties() == {"name": "John Doe"}

    def test_set_query_edge_with_properties_with_overwrite(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A -> B")])
        graph.query().create("(:A)").create("(:B)").create(
            "()-[::@f() {foo: 'var'} ]->()"
        ).execute()

        graph.query().match("()-[E]->()").set("E", {"number": 1}).execute()

        edges = graph.edges()
        assert edges[0].properties() == {"number": 1}

    def test_set_query_edge_with_properties_without_overwrite(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A -> B")])
        graph.query().create("(:A)").create("(:B)").create(
            "()-[::@f() {foo: 'var'} ]->()"
        ).execute()

        graph.query().match("()-[E]->()").set("E", {"number": 1}, False).execute()

        edges = graph.edges()
        assert edges[0].properties() == {"foo": "var", "number": 1}

    def test_set_query_edge_with_properties_with_many_edges(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*)->(B:*)")])
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:C)")
            .create("()-[::@f(A, B)]->()")
            .create("()-[::@f(A, C)]->()")
            .execute()
        )

        graph.query().match("()-[E]->()").set("E", {"index": 1}).execute()

        edges = graph.edges()
        assert all([e.properties() == {"index": 1} for e in edges])


class TestSetQueryFailure:
    def test_set_query_fails_if_try_to_set_properties_of_a_type(self):
        graph = implica.Graph()
        graph.query().create("(:A)").execute()

        with pytest.raises(ValueError):
            graph.query().match("(:(X:*))").set("X", {"foo": "var"}).execute()

    def test_set_query_fails_if_try_to_set_properties_of_a_term(self):
        graph = implica.Graph(constants=[implica.Constant("f", "A")])
        graph.query().create("(::@f())").execute()

        with pytest.raises(ValueError):
            graph.query().match("(::f)").set("f", {"foo": "var"}).execute()
