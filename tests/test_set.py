import implica
import pytest


class TestSetQuery:
    def test_set_query_on_node_with_no_properties(self):
        graph = implica.Graph()
        graph.query().create("(:A)").execute()

        graph.query().match("(N)").set("N", {"name": "John Doe"}).execute()

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
