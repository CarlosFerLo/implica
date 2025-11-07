import implica


def test_graph_creation(graph):
    """Test creating an empty graph"""
    assert str(graph) == "Graph(0 nodes, 0 edges)"


def test_node_creation(var_a, var_b):
    """Test creating nodes with properties"""
    node_a = implica.Node(var_a, {"value": 1})
    node_b = implica.Node(var_b, {"value": 2})

    assert str(node_a) == "Node(A)"
    assert node_a.properties["value"] == 1
    # UID is now a SHA256 hash (64 hex characters)
    assert len(node_a.uid()) == 64
    assert all(c in "0123456789abcdef" for c in node_a.uid())

    assert str(node_b) == "Node(B)"
    assert node_b.properties["value"] == 2


def test_edge_creation(var_a, var_b, app_ab):
    """Test creating edges with properties"""
    node_a = implica.Node(var_a, {"value": 1})
    node_b = implica.Node(var_b, {"value": 2})
    term = implica.Term("f", app_ab)

    edge = implica.Edge(term, node_a, node_b, {"weight": 1.5})
    assert edge.properties["weight"] == 1.5
    assert str(edge).startswith("Edge(f:")
