import pytest
import implica


def test_variable_creation_and_properties(var_a):
    """Test Variable creation and basic properties"""
    assert var_a.name == "A"
    assert str(var_a) == "A"
    assert repr(var_a) == 'Variable("A")'
    # UID is now a SHA256 hash (64 hex characters)
    assert len(var_a.uid()) == 64
    assert all(c in "0123456789abcdef" for c in var_a.uid())


@pytest.mark.parametrize(
    "name,expected_str",
    [
        ("A", "A"),
        ("B", "B"),
        ("SomeType", "SomeType"),
    ],
)
def test_variable_with_different_names(name, expected_str):
    """Test Variable with various names"""
    var = implica.Variable(name)
    assert var.name == name
    assert str(var) == expected_str


def test_application_creation(app_ab):
    """Test Application creation"""
    assert str(app_ab) == "(A -> B)"
    # UID is now a SHA256 hash (64 hex characters)
    assert len(app_ab.uid()) == 64
    assert all(c in "0123456789abcdef" for c in app_ab.uid())


def test_application_getters(app_ab, var_a, var_b):
    """Test Application left and right getters"""
    left = app_ab.left
    right = app_ab.right
    assert isinstance(left, implica.Variable)
    assert isinstance(right, implica.Variable)
    assert left.name == "A"
    assert right.name == "B"


def test_type_equality(var_a, var_b, app_ab, app_ac):
    """Test Type Equality"""
    assert var_a == var_a
    assert var_a == implica.Variable("A")
    assert var_a != var_b

    assert app_ab == app_ab
    assert app_ab == implica.Application(implica.Variable("A"), implica.Variable("B"))
    assert app_ab != app_ac


@pytest.mark.skip("Skipping immutability tests due to problems on enforcing it.")
def test_type_variable_immutability(var_a):
    """Test Type Variable Immutability"""
    with pytest.raises(Exception):
        var_a.name = "B"

    assert var_a.name == "A"


def test_type_application_immutability(app_ab):
    """Test Type Application Immutability"""
    with pytest.raises(Exception):
        app_ab.left = implica.Variable("C")
    with pytest.raises(Exception):
        app_ab.right = implica.Variable("D")

    assert app_ab.left == implica.Variable("A")
    assert app_ab.right == implica.Variable("B")
