import pytest
import implica


@pytest.fixture
def type_a():
    return implica.Variable("A")


@pytest.fixture
def type_b():
    return implica.Variable("B")


@pytest.fixture
def type_c():
    return implica.Variable("C")


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
def arrow_aa(type_a):
    return implica.Arrow(type_a, type_a)


@pytest.fixture
def term_ab(arrow_ab):
    return implica.BasicTerm("f", arrow_ab)


@pytest.fixture
def term_ba(arrow_ba):
    return implica.BasicTerm("g", arrow_ba)


@pytest.fixture
def term_aa(arrow_aa):
    return implica.BasicTerm("h", arrow_aa)


@pytest.fixture
def K():
    return implica.Constant(
        "K",
        implica.TypeSchema("(A:*) -> (B:*) -> A"),
        lambda A, B: implica.BasicTerm("K", implica.Arrow(A, implica.Arrow(B, A))),
    )


@pytest.fixture
def S():
    return implica.Constant(
        "S",
        implica.TypeSchema("((A:*) -> (B:*) -> (C:*)) -> (A -> B) -> A -> C"),
        lambda A, B, C: implica.BasicTerm(
            "S",
            implica.Arrow(
                implica.Arrow(A, implica.Arrow(B, C)),
                implica.Arrow(implica.Arrow(A, B), implica.Arrow(A, C)),
            ),
        ),
    )


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


@pytest.fixture
def edge_aa(term_aa, node_a):
    return implica.Edge(term_aa, node_a, node_a)


@pytest.fixture
def graph_empty():
    return implica.Graph()


@pytest.fixture
def graph_with_nodes(type_a, term_a, type_b, arrow_ab, arrow_ba):
    graph = implica.Graph()
    graph.query().create(node="N1", type=type_a, term=term_a).create(node="N2", type=type_b).create(
        node="N3", type=arrow_ab
    ).create(node="N4", type=arrow_ba).execute()
    return graph


@pytest.fixture
def graph_with_edges(
    type_a,
    type_b,
    arrow_ab,
    term_ab,
    arrow_ba,
    term_ba,
):
    graph = implica.Graph()
    graph.query().create(node="N1", type=type_a).create(node="N2", type=type_b).create(
        edge="E1", start="N1", end="N2", type=arrow_ab, term=term_ab
    ).create(edge="E2", start="N2", end="N1", type=arrow_ba, term=term_ba).execute()
    return graph


@pytest.fixture
def graph_with_K_S(K, S):
    graph = implica.Graph(constants=[K, S])
    return graph
