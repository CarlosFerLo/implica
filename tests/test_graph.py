import pytest
import implica


# ============================================================================
# NODE TESTS
# ============================================================================


class TestNodeCreation:
    """Test suite for Node creation"""

    def test_node_creation_with_variable(self, var_a):
        """Test creating a node with a Variable type"""
        node = implica.Node(var_a)
        assert str(node) == "Node(A)"
        assert repr(node) == "Node(A)"
        assert node.type == var_a

    def test_node_creation_with_application(self, app_ab):
        """Test creating a node with an Application type"""
        node = implica.Node(app_ab)
        assert str(node) == "Node((A -> B))"
        assert node.type == app_ab

    def test_node_creation_with_properties(self, var_a):
        """Test creating nodes with properties"""
        node = implica.Node(var_a, {"value": 1, "name": "test", "flag": True})
        assert node.properties["value"] == 1
        assert node.properties["name"] == "test"
        assert node.properties["flag"] is True

    def test_node_creation_without_properties(self, var_a):
        """Test creating a node without explicit properties"""
        node = implica.Node(var_a)
        assert isinstance(node.properties, dict)
        assert len(node.properties) == 0

    def test_node_creation_with_empty_properties(self, var_a):
        """Test creating a node with empty properties dict"""
        node = implica.Node(var_a, {})
        assert isinstance(node.properties, dict)
        assert len(node.properties) == 0


class TestNodeUID:
    """Test suite for Node UID generation"""

    def test_node_uid_format(self, var_a):
        """Test that node UID is a SHA256 hash"""
        node = implica.Node(var_a)
        uid = node.uid()
        # UID is a SHA256 hash (64 hex characters)
        assert len(uid) == 64
        assert all(c in "0123456789abcdef" for c in uid)

    def test_node_uid_consistency(self, var_a):
        """Test that calling uid() multiple times returns the same value"""
        node = implica.Node(var_a)
        uid1 = node.uid()
        uid2 = node.uid()
        uid3 = node.uid()
        assert uid1 == uid2 == uid3

    def test_nodes_same_type_same_uid(self, var_a):
        """Test that nodes with the same type have the same UID"""
        node1 = implica.Node(var_a, {"prop1": "value1"})
        node2 = implica.Node(var_a, {"prop2": "value2"})
        assert node1.uid() == node2.uid()

    def test_nodes_different_types_different_uids(self, var_a, var_b):
        """Test that nodes with different types have different UIDs"""
        node1 = implica.Node(var_a)
        node2 = implica.Node(var_b)
        assert node1.uid() != node2.uid()

    def test_nodes_with_application_types_have_unique_uids(self, app_ab, app_ac):
        """Test that nodes with different application types have different UIDs"""
        node1 = implica.Node(app_ab)
        node2 = implica.Node(app_ac)
        assert node1.uid() != node2.uid()

    def test_nodes_with_application_types_but_with_inverse_order_have_unique_ids(
        self, app_ab, app_ba
    ):
        """Test that node with application types in inverse order have different UIDs"""
        node1 = implica.Node(app_ab)
        node2 = implica.Node(app_ba)
        assert node1.uid() != node2.uid()


class TestNodeProperties:
    """Test suite for Node properties"""

    def test_node_properties_are_mutable(self, var_a):
        """Test that node properties can be modified"""
        node = implica.Node(var_a, {"value": 1})
        assert node.properties["value"] == 1

        node.properties["value"] = 2
        assert node.properties["value"] == 2

    def test_node_properties_can_be_added(self, var_a):
        """Test that new properties can be added to a node"""
        node = implica.Node(var_a, {"value": 1})
        node.properties["new_prop"] = "new_value"
        assert node.properties["new_prop"] == "new_value"
        assert node.properties["value"] == 1

    def test_node_properties_can_be_deleted(self, var_a):
        """Test that properties can be deleted from a node"""
        node = implica.Node(var_a, {"value": 1, "name": "test"})
        del node.properties["value"]
        assert "value" not in node.properties
        assert node.properties["name"] == "test"

    def test_node_properties_support_various_types(self, var_a):
        """Test that node properties can hold various Python types"""
        node = implica.Node(
            var_a,
            {
                "int": 42,
                "float": 3.14,
                "str": "hello",
                "bool": True,
                "list": [1, 2, 3],
                "dict": {"nested": "value"},
                "none": None,
            },
        )
        assert node.properties["int"] == 42
        assert node.properties["float"] == 3.14
        assert node.properties["str"] == "hello"
        assert node.properties["bool"] is True
        assert node.properties["list"] == [1, 2, 3]
        assert node.properties["dict"]["nested"] == "value"
        assert node.properties["none"] is None

    def test_node_uid_remains_cached_after_property_mutation(self, var_a):
        """UID remains the same even if node properties change (cached UID)."""
        node = implica.Node(var_a, {"name": "Alice", "age": 30})

        uid_before = node.uid()

        # Modify properties
        node.properties["age"] = 31
        node.properties["city"] = "New York"

        uid_after = node.uid()

        assert uid_before == uid_after


class TestNodeType:
    """Test suite for Node type attribute"""

    def test_node_type_is_accessible(self, var_a):
        """Test that node type can be accessed"""
        node = implica.Node(var_a)
        assert node.type == var_a

    def test_node_type_is_immutable(self, var_a, var_b):
        """Test that node type cannot be modified"""
        node = implica.Node(var_a)
        with pytest.raises(AttributeError):
            node.type = var_b
        assert node.type == var_a


class TestNodeStringRepresentations:
    """Test suite for Node string representations"""

    def test_node_str_with_variable(self, var_a):
        """Test __str__ method with Variable type"""
        node = implica.Node(var_a)
        assert str(node) == "Node(A)"

    def test_node_str_with_application(self, app_ab):
        """Test __str__ method with Application type"""
        node = implica.Node(app_ab)
        assert str(node) == "Node((A -> B))"

    def test_node_repr_with_variable(self, var_a):
        """Test __repr__ method with Variable type"""
        node = implica.Node(var_a)
        assert repr(node) == "Node(A)"

    def test_node_repr_with_application(self, app_ab):
        """Test __repr__ method with Application type"""
        node = implica.Node(app_ab)
        assert repr(node) == "Node((A -> B))"


# ============================================================================
# EDGE TESTS
# ============================================================================


class TestEdgeCreation:
    """Test suite for Edge creation"""

    def test_edge_creation_basic(self, var_a, var_b, app_ab):
        """Test creating a basic edge"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)

        edge = implica.Edge(term, node_a, node_b)
        assert edge.term.name == "f"
        assert edge.start.type == var_a
        assert edge.end.type == var_b

    def test_edge_creation_with_properties(self, var_a, var_b, app_ab):
        """Test creating edges with properties"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)

        edge = implica.Edge(term, node_a, node_b, {"weight": 1.5, "label": "test"})
        assert edge.properties["weight"] == 1.5
        assert edge.properties["label"] == "test"

    def test_edge_creation_without_properties(self, var_a, var_b, app_ab):
        """Test creating an edge without explicit properties"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)

        edge = implica.Edge(term, node_a, node_b)
        assert isinstance(edge.properties, dict)
        assert len(edge.properties) == 0

    def test_edge_with_same_start_and_end_nodes(self, var_a, app_ab):
        """Test creating an edge where start and end are the same type"""
        node_a = implica.Node(var_a)
        # Create a term with type A -> A
        app_aa = implica.Application(var_a, var_a)
        term = implica.Term("identity", app_aa)

        edge = implica.Edge(term, node_a, node_a)
        assert edge.start.type == edge.end.type


class TestEdgeUID:
    """Test suite for Edge UID generation"""

    def test_edge_uid_format(self, var_a, var_b, app_ab):
        """Test that edge UID is a SHA256 hash"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        uid = edge.uid()
        # UID is a SHA256 hash (64 hex characters)
        assert len(uid) == 64
        assert all(c in "0123456789abcdef" for c in uid)

    def test_edge_uid_consistency(self, var_a, var_b, app_ab):
        """Test that calling uid() multiple times returns the same value"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        uid1 = edge.uid()
        uid2 = edge.uid()
        uid3 = edge.uid()
        assert uid1 == uid2 == uid3

    def test_edges_same_term_same_uid(self, var_a, var_b, app_ab):
        """Test that edges with the same term have the same UID"""
        node1_a = implica.Node(var_a)
        node1_b = implica.Node(var_b)
        node2_a = implica.Node(var_a)
        node2_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)

        edge1 = implica.Edge(term, node1_a, node1_b, {"prop": "val1"})
        edge2 = implica.Edge(term, node2_a, node2_b, {"prop": "val2"})
        assert edge1.uid() == edge2.uid()

    def test_edges_different_terms_different_uids(self, var_a, var_b, app_ab):
        """Test that edges with different terms have different UIDs"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term1 = implica.Term("f", app_ab)
        term2 = implica.Term("g", app_ab)

        edge1 = implica.Edge(term1, node_a, node_b)
        edge2 = implica.Edge(term2, node_a, node_b)
        assert edge1.uid() != edge2.uid()


class TestEdgeProperties:
    """Test suite for Edge properties"""

    def test_edge_properties_are_mutable(self, var_a, var_b, app_ab):
        """Test that edge properties can be modified"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b, {"weight": 1.0})

        assert edge.properties["weight"] == 1.0
        edge.properties["weight"] = 2.0
        assert edge.properties["weight"] == 2.0

    def test_edge_properties_can_be_added(self, var_a, var_b, app_ab):
        """Test that new properties can be added to an edge"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b, {"weight": 1.0})

        edge.properties["label"] = "new_label"
        assert edge.properties["label"] == "new_label"
        assert edge.properties["weight"] == 1.0

    def test_edge_properties_can_be_deleted(self, var_a, var_b, app_ab):
        """Test that properties can be deleted from an edge"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b, {"weight": 1.0, "label": "test"})

        del edge.properties["weight"]
        assert "weight" not in edge.properties
        assert edge.properties["label"] == "test"

    def test_edge_properties_support_various_types(self, var_a, var_b, app_ab):
        """Test that edge properties can hold various Python types"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(
            term,
            node_a,
            node_b,
            {
                "int": 42,
                "float": 3.14,
                "str": "hello",
                "bool": False,
                "list": [1, 2, 3],
                "dict": {"nested": "value"},
                "none": None,
            },
        )

        assert edge.properties["int"] == 42
        assert edge.properties["float"] == 3.14
        assert edge.properties["str"] == "hello"
        assert edge.properties["bool"] is False
        assert edge.properties["list"] == [1, 2, 3]
        assert edge.properties["dict"]["nested"] == "value"
        assert edge.properties["none"] is None


class TestEdgeAttributes:
    """Test suite for Edge attributes"""

    def test_edge_term_is_accessible(self, var_a, var_b, app_ab):
        """Test that edge term can be accessed"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        assert edge.term.name == "f"
        assert edge.term.type == app_ab

    def test_edge_start_is_accessible(self, var_a, var_b, app_ab):
        """Test that edge start node can be accessed"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        assert edge.start.type == var_a

    def test_edge_end_is_accessible(self, var_a, var_b, app_ab):
        """Test that edge end node can be accessed"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        assert edge.end.type == var_b

    def test_edge_term_is_immutable(self, var_a, var_b, app_ab, app_ac):
        """Test that edge term cannot be modified"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        new_term = implica.Term("g", app_ac)
        with pytest.raises(AttributeError):
            edge.term = new_term

    def test_edge_start_is_immutable(self, var_a, var_b, var_c, app_ab):
        """Test that edge start cannot be modified"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        node_c = implica.Node(var_c)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        with pytest.raises(AttributeError):
            edge.start = node_c

    def test_edge_end_is_immutable(self, var_a, var_b, var_c, app_ab):
        """Test that edge end cannot be modified"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        node_c = implica.Node(var_c)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        with pytest.raises(AttributeError):
            edge.end = node_c


class TestTermUID:
    """Test suite for Term UID generation and consistency"""

    def test_term_uid_consistency(self, app_ab):
        """Test that calling uid() multiple times on a Term returns the same value"""
        term = implica.Term("x", app_ab)

        uid1 = term.uid()
        uid2 = term.uid()
        uid3 = term.uid()

        assert uid1 == uid2 == uid3


class TestEdgeStringRepresentations:
    """Test suite for Edge string representations"""

    def test_edge_str_format(self, var_a, var_b, app_ab):
        """Test __str__ method format"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        assert str(edge) == "Edge(f: A -> B)"

    def test_edge_repr_format(self, var_a, var_b, app_ab):
        """Test __repr__ method format"""
        node_a = implica.Node(var_a)
        node_b = implica.Node(var_b)
        term = implica.Term("f", app_ab)
        edge = implica.Edge(term, node_a, node_b)

        assert repr(edge) == "Edge(f: A -> B)"

    def test_edge_str_with_application_types(self, app_ab, app_ac):
        """Test __str__ with Application types in nodes"""
        node1 = implica.Node(app_ab)
        node2 = implica.Node(app_ac)
        # Create a term with type (A -> B) -> (A -> C)
        term_type = implica.Application(app_ab, app_ac)
        term = implica.Term("transform", term_type)
        edge = implica.Edge(term, node1, node2)

        assert "transform:" in str(edge)
        assert "->" in str(edge)


# ============================================================================
# GRAPH TESTS
# ============================================================================


class TestGraphCreation:
    """Test suite for Graph creation"""

    def test_graph_creation(self):
        """Test creating an empty graph"""
        graph = implica.Graph()
        assert str(graph) == "Graph(0 nodes, 0 edges)"
        assert repr(graph) == "Graph(0 nodes, 0 edges)"

    def test_graph_nodes_is_dict(self, graph):
        """Test that graph.nodes is a dictionary"""
        assert isinstance(graph.nodes, dict)
        assert len(graph.nodes) == 0

    def test_graph_edges_is_dict(self, graph):
        """Test that graph.edges is a dictionary"""
        assert isinstance(graph.edges, dict)
        assert len(graph.edges) == 0

    def test_multiple_graphs_are_independent(self):
        """Test that multiple graph instances are independent"""
        graph1 = implica.Graph()
        graph2 = implica.Graph()

        assert graph1 is not graph2
        assert graph1.nodes is not graph2.nodes
        assert graph1.edges is not graph2.edges

    def test_graph_has_query_method(self, graph):
        """Test that graph has a query method"""
        assert hasattr(graph, "query")
        assert callable(graph.query)
