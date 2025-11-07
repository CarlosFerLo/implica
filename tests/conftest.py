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
def app_ab(var_a, var_b):
    """Fixture for Application(A -> B)"""
    return implica.Application(var_a, var_b)


@pytest.fixture
def graph():
    """Fixture for a fresh Graph instance"""
    return implica.Graph()
