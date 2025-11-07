import implica
import pytest


def test_node_pattern_simple():
    """Test simple NodePattern creation"""
    pattern1 = implica.NodePattern(variable="n")
    assert pattern1.variable == "n"


def test_node_pattern_with_type(var_a):
    """Test NodePattern with type"""
    pattern2 = implica.NodePattern(variable="n", type=var_a)
    assert pattern2.variable == "n"


def test_path_pattern_simple_node():
    """Test PathPattern with simple node"""
    path1 = implica.PathPattern("(n)")
    assert len(path1.nodes) == 1
    assert path1.nodes[0].variable == "n"
    assert len(path1.edges) == 0


def test_path_pattern_typed_node():
    """Test PathPattern with typed node"""
    path2 = implica.PathPattern("(n:A)")
    assert len(path2.nodes) == 1
    assert path2.nodes[0].variable == "n"
    assert len(path2.edges) == 0


def test_path_pattern_anonymous_node():
    """Test PathPattern with anonymous node"""
    path = implica.PathPattern("()")
    assert len(path.nodes) == 1
    assert path.nodes[0].variable is None


def test_path_pattern_typed_anonymous_node():
    """Test PathPattern with typed anonymous node"""
    path = implica.PathPattern("(:Person)")
    assert len(path.nodes) == 1
    assert path.nodes[0].variable is None


def test_path_pattern_with_edge():
    """Test PathPattern with edges"""
    path3 = implica.PathPattern("(n:A)-[e]->(m:B)")
    assert len(path3.nodes) == 2
    assert len(path3.edges) == 1
    assert path3.nodes[0].variable == "n"
    assert path3.nodes[1].variable == "m"
    assert path3.edges[0].variable == "e"
    assert path3.edges[0].direction == "forward"


def test_path_pattern_complex():
    """Test complex PathPattern with multiple nodes and edges"""
    path = implica.PathPattern("(a:A)-[e1:term]->(b:B)-[e2]->(c:C)")
    assert len(path.nodes) == 3
    assert len(path.edges) == 2
    assert path.nodes[0].variable == "a"
    assert path.nodes[1].variable == "b"
    assert path.nodes[2].variable == "c"
    assert path.edges[0].variable == "e1"
    assert path.edges[1].variable == "e2"


def test_path_pattern_backward_edge():
    """Test PathPattern with backward edge"""
    path = implica.PathPattern("(n)<-[e]-(m)")
    assert len(path.nodes) == 2
    assert len(path.edges) == 1
    assert path.edges[0].direction == "backward"


def test_path_pattern_bidirectional_edge():
    """Test PathPattern with bidirectional edge"""
    path = implica.PathPattern("(n)-[e]-(m)")
    assert len(path.nodes) == 2
    assert len(path.edges) == 1
    assert path.edges[0].direction == "any"


def test_path_pattern_empty_fails():
    """Test that empty pattern fails"""
    with pytest.raises(Exception):
        implica.PathPattern("")


def test_path_pattern_unmatched_parens_fails():
    """Test that unmatched parentheses fails"""
    with pytest.raises(Exception):
        implica.PathPattern("(n")


def test_path_pattern_unmatched_brackets_fails():
    """Test that unmatched brackets fails"""
    with pytest.raises(Exception):
        implica.PathPattern("(n)-[e->(m)")


def test_path_pattern_schema():
    """Test PathPattern with schema patterns"""
    path = implica.PathPattern("(n:$A -> B$)")
    assert len(path.nodes) == 1
    assert path.nodes[0].variable == "n"
