import pytest
import implica


# Fixtures
@pytest.fixture
def var_a():
    """Fixture for Variable A"""
    return implica.Variable("A")


@pytest.fixture
def var_b():
    """Fixture for Variable B"""
    return implica.Variable("B")


@pytest.fixture
def var_c():
    """Fixture for Variable C"""
    return implica.Variable("C")


@pytest.fixture
def app_ab(var_a, var_b):
    """Fixture for Application(A -> B)"""
    return implica.Application(var_a, var_b)


@pytest.fixture
def app_ac(var_a, var_c):
    """Fixture for Application (A -> C)"""
    return implica.Application(var_a, var_c)


@pytest.fixture
def app_ba(var_a, var_b):
    """Fixture for Application (B -> A)"""
    return implica.Application(var_b, var_a)


@pytest.fixture
def graph():
    """Fixture for a fresh Graph instance"""
    return implica.Graph()
