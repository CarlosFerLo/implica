import pytest
import implica


@pytest.fixture
def type_a():
    return implica.Variable("A")


@pytest.fixture
def type_b():
    return implica.Variable("B")


@pytest.fixture
def term_a(type_a):
    return implica.BasicTerm("a", type_a)


@pytest.fixture
def term_b(type_b):
    return implica.BasicTerm("b", type_b)


@pytest.fixture
def arrow_ab(type_a, type_b):
    return implica.Arrow(type_a, type_b)


@pytest.fixture
def arrow_ba(type_b, type_a):
    return implica.Arrow(type_b, type_a)


@pytest.fixture
def term_ab(arrow_ab):
    return implica.BasicTerm("f", arrow_ab)


@pytest.fixture
def term_ba(arrow_ba):
    return implica.BasicTerm("g", arrow_ba)


@pytest.fixture
def node_a(type_a):
    return implica.Node(type_a)


@pytest.fixture
def node_b(type_b):
    return implica.Node(type_b)


@pytest.fixture
def node_a_with_term(type_a, term_a):
    return implica.Node(type_a, term_a)


@pytest.fixture
def node_b_with_term(type_b, term_b):
    return implica.Node(type_b, term_b)


@pytest.fixture
def edge_ab(term_ab, node_a, node_b):
    return implica.Edge(term_ab, node_a, node_b)


@pytest.fixture
def edge_ba(term_ba, node_b, node_a):
    return implica.Edge(term_ba, node_b, node_a)
