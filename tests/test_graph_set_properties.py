import pytest
import implica


class TestGraphSetNodeProperties:
    def test_graph_set_node_properties_with_overwrite(self):
        graph = implica.Graph()
        graph.query().create("(:A {foo: 'var'})").create("(:B {foo: 'var'})").create(
            "(:C {foo: 'var'})"
        ).execute()

        nodes = graph.nodes()
        mapping = dict(map(lambda x: (x.uid(), {"number": 1.3}), nodes))

        graph.set_node_properties(mapping)

        nodes = graph.nodes()
        assert len(nodes) == 3
        assert all([n.properties() == {"number": 1.3} for n in nodes])

    def test_graph_set_node_properties_without_overwrite(self):
        graph = implica.Graph()
        graph.query().create("(:A {foo: 'var'})").create("(:B {foo: 'var'})").create(
            "(:C {foo: 'var'})"
        ).execute()

        nodes = graph.nodes()
        mapping = dict(map(lambda x: (x.uid(), {"number": 1.3}), nodes))

        graph.set_node_properties(mapping, False)

        nodes = graph.nodes()
        assert len(nodes) == 3
        assert all([n.properties() == {"foo": "var", "number": 1.3} for n in nodes])

    def test_graph_set_node_properties_partial_dict(self):
        graph = implica.Graph()
        (
            graph.query()
            .create("(:A->B {foo: 'var'})")
            .create("(:B->C {foo: 'var'})")
            .create("(:C {foo: 'var'})")
            .execute()
        )

        nodes = graph.query().match("(N:*->*)").return_("N")
        mapping = dict(map(lambda x: (x["N"].uid(), {"number": 1.3}), nodes))

        graph.set_node_properties(mapping, False)  # type: ignore

        nodes = graph.nodes()
        assert len(nodes) == 3
        assert sum([n.properties() == {"foo": "var", "number": 1.3} for n in nodes]) == 2
        assert sum([n.properties() == {"foo": "var"} for n in nodes]) == 1

    def test_graph_set_edge_properties_with_overwrite(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*) -> (B:*)")])
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:C)")
            .create("()-[::@f(A, B) { foo: 'var' }]->()")
            .create("()-[::@f(A, C) { foo: 'var' }]->()")
            .create("()-[::@f(B, C) { foo: 'var' }]->()")
            .execute()
        )

        edges = graph.edges()
        mapping = dict(map(lambda x: (x.uid(), {"number": 0.3}), edges))

        graph.set_edge_properties(mapping)

        edges = graph.edges()
        assert len(edges) == 3
        assert all([e.properties() == {"number": 0.3} for e in edges])

    def test_graph_set_edge_properties_without_overwrite(self):
        graph = implica.Graph(constants=[implica.Constant("f", "(A:*) -> (B:*)")])
        (
            graph.query()
            .create("(:A)")
            .create("(:B)")
            .create("(:C)")
            .create("()-[::@f(A, B) { foo: 'var' }]->()")
            .create("()-[::@f(A, C) { foo: 'var' }]->()")
            .create("()-[::@f(B, C) { foo: 'var' }]->()")
            .execute()
        )

        edges = graph.edges()
        mapping = dict(map(lambda x: (x.uid(), {"number": 0.3}), edges))

        graph.set_edge_properties(mapping, False)

        edges = graph.edges()
        assert len(edges) == 3
        assert all([e.properties() == {"foo": "var", "number": 0.3} for e in edges])
