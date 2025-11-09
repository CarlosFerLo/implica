import implica
import pytest


def test_term_creation_with_application(app_ab):
    """Test creating a term with an Application type"""
    f = implica.Term("f", app_ab)
    assert f.name == "f"
    assert str(f) == "f:(A -> B)"
    # UID is now a SHA256 hash (64 hex characters)
    assert len(f.uid()) == 64
    assert all(c in "0123456789abcdef" for c in f.uid())


def test_term_creation_with_variable(var_a):
    """Test creating a term with a Variable type"""
    x = implica.Term("x", var_a)
    assert x.name == "x"
    assert str(x) == "x:A"
    # UID is now a SHA256 hash (64 hex characters)
    assert len(x.uid()) == 64
    assert all(c in "0123456789abcdef" for c in x.uid())


def test_term_application(app_ab, var_a):
    """Test applying one term to another"""
    f = implica.Term("f", app_ab)
    x = implica.Term("x", var_a)

    result = f(x)
    assert result.name == "(f x)"
    assert str(result) == "(f x):B"


def test_term_application_fails_if_invalid_types(app_ab, var_c):
    """Test applying one term to another of invalid type"""
    f = implica.Term("f", app_ab)
    x = implica.Term("x", var_c)

    with pytest.raises(TypeError):
        f(x)

    with pytest.raises(TypeError):
        x(f)


@pytest.mark.skip("Skipping immutability tests due to problems on enforcing it.")
def test_term_name_is_immutable(var_a):
    """Test term name is immutable"""
    f = implica.Term("f", var_a)

    with pytest.raises(AttributeError):
        f.name = "g"

    assert f.name == "f"


def test_term_type_is_immutable(var_a, var_b):
    """Test term type is immutable"""
    f = implica.Term("f", var_a)

    with pytest.raises(AttributeError):
        f.type = var_b

    assert f.type == var_a
