import pytest
import implica


class TestCreateQueryNode:
    def test_create_query_with_one_node(self, graph_empty, type_a):
        graph_empty.query().create(
            node="N",
            type=type_a,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a

    def test_create_query_with_two_nodes(self, graph_empty, type_a, type_b):
        graph_empty.query().create(
            node="N1",
            type=type_a,
        ).create(
            node="N2",
            type=type_b,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 2
        types = {node.type for node in nodes}
        assert types == {type_a, type_b}

    def test_create_query_does_not_overwrite_previous_nodes(self, graph_empty, type_a, type_b):
        graph_empty.query().create(
            node="N1",
            type=type_a,
        ).execute()

        graph_empty.query().create(
            node="N2",
            type=type_b,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 2
        types = {node.type for node in nodes}
        assert types == {type_a, type_b}

    def test_create_query_with_node_term(self, graph_empty, type_a, term_a):
        graph_empty.query().create(
            node="N",
            type=type_a,
            term=term_a,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a
        assert node.term == term_a

    def test_create_query_with_node_properties(self, graph_empty, type_a):
        properties = {
            "name": "Node1",
            "value": 42,
        }
        graph_empty.query().create(
            node="N",
            type=type_a,
            properties=properties,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a
        assert node.properties == properties

    def test_create_query_with_type_schema(self, graph_empty, type_a):
        type_schema = implica.TypeSchema(pattern="A")

        graph_empty.query().create(
            node="N",
            type_schema=type_schema,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a

    def test_create_query_with_type_schema_and_context(self, graph_empty, type_b):
        type_schema = implica.TypeSchema(pattern="A")

        graph_empty.query().add("A", type=type_b).create(
            node="N",
            type_schema=type_schema,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_b

    def test_create_query_with_term_schema_and_no_context_fails(self, graph_empty):
        term_schema = implica.TermSchema(pattern="f")

        with pytest.raises(NameError):
            graph_empty.query().create(
                node="N",
                term_schema=term_schema,
            ).execute()

    def test_create_query_with_term_schema_and_context(self, graph_empty, type_a, term_a):
        term_schema = implica.TermSchema(pattern="f")

        graph_empty.query().add("f", term=term_a).create(
            node="N",
            term_schema=term_schema,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a
        assert node.term == term_a

    def test_create_query_with_type_and_term(self, graph_empty, type_a, term_a):
        graph_empty.query().create(
            node="N",
            type=type_a,
            term=term_a,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a
        assert node.term == term_a

    def test_create_query_with_type_and_term_schema(self, graph_empty, type_a, term_a):
        term_schema = implica.TermSchema(pattern="f")

        graph_empty.query().add("f", term=term_a).create(
            node="N",
            type=type_a,
            term_schema=term_schema,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a
        assert node.term == term_a

    def test_create_query_with_type_schema_and_term(self, graph_empty, type_a, term_a):
        type_schema = implica.TypeSchema(pattern="A")

        graph_empty.query().create(
            node="N",
            type_schema=type_schema,
            term=term_a,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a
        assert node.term == term_a

    def test_create_query_with_type_schema_and_term_schema(self, graph_empty, type_a, term_a):
        type_schema = implica.TypeSchema(pattern="A")
        term_schema = implica.TermSchema(pattern="f")

        graph_empty.query().add("f", term=term_a).create(
            node="N",
            type_schema=type_schema,
            term_schema=term_schema,
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a
        assert node.term == term_a

    def test_create_query_fails_with_type_and_type_schema(self, graph_empty, type_a):
        type_schema = implica.TypeSchema(pattern="A")

        with pytest.raises(ValueError):
            graph_empty.query().create(
                node="N",
                type=type_a,
                type_schema=type_schema,
            )

    def test_create_query_fails_with_term_and_term_schema(self, graph_empty, term_a):
        term_schema = implica.TermSchema(pattern="f")

        with pytest.raises(ValueError):
            graph_empty.query().create(
                node="N",
                term=term_a,
                term_schema=term_schema,
            )

    def test_create_query_fails_with_conflicting_type_and_term(self, graph_empty, type_a, term_b):
        with pytest.raises(TypeError):
            graph_empty.query().create(
                node="N",
                type=type_a,
                term=term_b,
            ).execute()

    def test_create_query_fails_if_node_already_exists(self, graph_empty, type_a):
        graph_empty.query().create(
            node="N",
            type=type_a,
        ).execute()

        with pytest.raises(ValueError):
            graph_empty.query().create(
                node="N",
                type=type_a,
            ).execute()


class TestCreateQueryEdge:
    def test_create_query_with_one_edge(self, graph_empty, type_a, type_b, term_ab):
        graph_empty.query().create(node="N1", type=type_a).create(node="N2", type=type_b).create(
            edge="E",
            start="N1",
            end="N2",
            term=term_ab,
        ).execute()

        edges = graph_empty._get_all_edges()
        assert len(edges) == 1
        edge = edges[0]
        assert edge.term == term_ab
        assert edge.start.type == type_a
        assert edge.end.type == type_b

    def test_create_query_with_edge_properties(self, graph_empty, type_a, type_b, term_ab):
        properties = {
            "weight": 3.14,
            "label": "connects",
        }
        graph_empty.query().create(node="N1", type=type_a).create(node="N2", type=type_b).create(
            edge="E",
            start="N1",
            end="N2",
            term=term_ab,
            properties=properties,
        ).execute()

        edges = graph_empty._get_all_edges()
        assert len(edges) == 1
        edge = edges[0]
        assert edge.term == term_ab
        assert edge.start.type == type_a
        assert edge.end.type == type_b
        assert edge.properties == properties

    def test_create_query_fails_with_conflicting_start_node(self, graph_empty, type_b, term_ab):

        graph_empty.query().create(node="N2", type=type_b).execute()

        with pytest.raises(ValueError):
            graph_empty.query().create(
                edge="E",
                start="N3",  # N3 does not exist
                end="N2",
                term=term_ab,
            ).execute()

    def test_create_query_fails_with_conflicting_end_node(self, graph_empty, type_a, term_ab):

        graph_empty.query().create(node="N1", type=type_a).execute()

        with pytest.raises(ValueError):
            graph_empty.query().create(
                edge="E",
                start="N1",
                end="N3",  # N3 does not exist
                term=term_ab,
            ).execute()

    def test_create_query_fails_with_missing_start_node(self, graph_empty, type_b, term_ab):

        graph_empty.query().create(node="N2", type=type_b).execute()

        with pytest.raises(ValueError):
            graph_empty.query().create(
                edge="E",
                end="N2",
                term=term_ab,
            )

    def test_create_query_fails_with_missing_end_node(self, graph_empty, type_a, term_ab):

        graph_empty.query().create(node="N1", type=type_a).execute()

        with pytest.raises(ValueError):
            graph_empty.query().create(
                edge="E",
                start="N1",
                term=term_ab,
            )

    def test_create_query_fails_if_edge_already_exists(self, graph_empty, type_a, type_b, term_ab):
        graph_empty.query().create(node="N1", type=type_a).create(node="N2", type=type_b).create(
            edge="E",
            start="N1",
            end="N2",
            term=term_ab,
        ).execute()

        q = graph_empty.query().match(node="N1", type=type_a).match(node="N2", type=type_b)

        with pytest.raises(ValueError):
            q.create(
                edge="NewE",
                start="N1",
                end="N2",
                term=term_ab,
            ).execute()

    def test_create_query_fails_if_type_conflict_in_start_node(
        self, graph_empty, type_a, type_b, term_aa
    ):
        graph_empty.query().create(node="N1", type=type_a).create(node="N2", type=type_b).execute()

        q = graph_empty.query().match(node="N1", type=type_b).match(node="N2", type=type_a)
        with pytest.raises(TypeError):
            q.create(
                edge="E",
                start="N1",
                end="N2",
                term=term_aa,
            ).execute()

    def test_create_query_fails_if_type_conflict_in_end_node(
        self, graph_empty, type_a, type_b, term_aa
    ):
        graph_empty.query().create(node="N1", type=type_a).create(node="N2", type=type_b).execute()

        q = graph_empty.query().match(node="N1", type=type_a).match(node="N2", type=type_b)
        with pytest.raises(TypeError):
            q.create(
                edge="E",
                start="N2",
                end="N1",
                term=term_aa,
            ).execute()


class TestCreateQueryPath:
    def test_create_query_path_create_a_node(self, graph_empty, type_a):
        graph_empty.query().create("(:A)").execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert node.type == type_a

    def test_create_query_path_create_two_nodes_and_an_edge_fully_explicit(
        self, graph_empty, type_a, type_b, term_ab
    ):
        graph_empty.query().add("x", term=term_ab).create("(:A)-[:A->B:x]->(:B)").execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 2
        types = {node.type for node in nodes}
        assert types == {type_a, type_b}

        edges = graph_empty._get_all_edges()
        assert len(edges) == 1
        edge = edges[0]
        assert edge.term == term_ab

    def test_create_query_path_create_two_nodes_and_an_edge_with_partial_context(
        self, graph_empty, type_a, type_b, term_ab
    ):
        graph_empty.query().add("x", term=term_ab).create("()-[::x]->()").execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 2
        types = {node.type for node in nodes}
        assert types == {type_a, type_b}

        edges = graph_empty._get_all_edges()
        assert len(edges) == 1
        edge = edges[0]
        assert edge.term == term_ab

    def test_create_query_path_create_two_nodes_and_an_edge_backwards(
        self, graph_empty, type_a, type_b, term_ab
    ):
        graph_empty.query().add("x", term=term_ab).create("(:B)<-[:A->B:x]-(:A)").execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 2
        types = {node.type for node in nodes}
        assert types == {type_a, type_b}

        edges = graph_empty._get_all_edges()
        assert len(edges) == 1
        edge = edges[0]
        assert edge.term == term_ab

    def test_create_query_path_create_two_nodes_and_an_edge_backwards_partial_context(
        self, graph_empty, type_a, type_b, term_ab
    ):
        graph_empty.query().add("x", term=term_ab).create("()<-[::x]-()").execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 2
        types = {node.type for node in nodes}
        assert types == {type_a, type_b}

        edges = graph_empty._get_all_edges()
        assert len(edges) == 1
        edge = edges[0]
        assert edge.term == term_ab

    def test_create_query_path_fails_with_conflicting_node_types(self, graph_empty, term_ab):
        with pytest.raises(TypeError):
            graph_empty.query().add("x", term=term_ab).create("(:B)-[:A->B:x]->(:A)").execute()

    def test_create_query_path_with_term_inference(
        self, graph_empty, type_a, type_b, term_a, term_ab
    ):
        graph_empty.query().add("x", term=term_a).add("f", term=term_ab).create(
            "(:A:x)-[:A->B]->(:B:f x)"
        ).execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 2
        types = {node.type for node in nodes}
        assert types == {type_a, type_b}

        edges = graph_empty._get_all_edges()
        assert len(edges) == 1
        edge = edges[0]
        assert edge.term == term_ab

    def test_create_query_path_fails_with_missing_term_inference(self, graph_empty, term_a, term_b):

        with pytest.raises(ValueError):
            graph_empty.query().add("x", term=term_a).add("y", term=term_b).create(
                "(:A:x)-[:A->B]->(:B:y)"
            ).execute()

    def test_create_query_path_fails_with_conflicting_edge_term(self, graph_empty, term_ab):

        with pytest.raises(TypeError):
            graph_empty.query().add("x", term=term_ab).create("(:A)-[::x]->(:A)").execute()

    def test_create_query_path_fails_if_edge_already_exists(self, graph_empty, term_ab):
        graph_empty.query().add("x", term=term_ab).create("(:A)-[:A->B:x]->(:B)").execute()

        with pytest.raises(ValueError):
            graph_empty.query().add("x", term=term_ab).create("(:A)-[:A->B:x]->(:B)").execute()

    def test_create_query_path_fails_if_node_already_exists(self, graph_empty):
        graph_empty.query().create("(:A)").execute()

        with pytest.raises(ValueError):
            graph_empty.query().create("(:A)").execute()

    def test_create_query_path_continues_when_one_of_the_nodes_already_exists(
        self, graph_empty, type_a, type_b, term_ab
    ):
        graph_empty.query().create("(:A)").execute()

        graph_empty.query().add("x", term=term_ab).create("(:A)-[:A->B:x]->(:B)").execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 2
        types = {node.type for node in nodes}
        assert types == {type_a, type_b}

        edges = graph_empty._get_all_edges()
        assert len(edges) == 1
        edge = edges[0]
        assert edge.term == term_ab

    def test_create_query_path_handles_already_matched_nodes(
        self, graph_empty, type_a, type_b, term_a, term_ab
    ):
        graph_empty.query().add("x", term=term_a).create("(:A:x)").execute()
        graph_empty.query().add("y", term=term_ab(term_a)).create("(:B:y)").execute()

        graph_empty.query().match("(N:A)").match("(M:B)").create(" (N)-[:A->B]->(M) ").execute()

        nodes = graph_empty._get_all_nodes()
        assert len(nodes) == 2
        types = {node.type for node in nodes}
        assert types == {type_a, type_b}

        edges = graph_empty._get_all_edges()
        assert len(edges) == 1
        edge = edges[0]
        assert edge.term == term_ab
