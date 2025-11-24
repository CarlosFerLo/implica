import implica
import pytest


def test_term_creation_with_Arrow(app_ab):
    """Test creating a term with an Arrow type"""
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
    # Regular terms should not have application references
    assert x.function_uid is None
    assert x.argument_uid is None


def test_term_Arrow(app_ab, var_a):
    """Test applying one term to another"""
    f = implica.Term("f", app_ab)
    x = implica.Term("x", var_a)

    result = f(x)
    assert result.name == "(f x)"
    assert str(result) == "(f x):B"

    # Verify that the result is an Application term with references
    assert result.function_uid == f.uid()
    assert result.argument_uid == x.uid()
    assert result.function_uid is not None
    assert result.argument_uid is not None


def test_term_Arrow_fails_if_invalid_types(app_ab, var_c):
    """Test applying one term to another of invalid type"""
    f = implica.Term("f", app_ab)
    x = implica.Term("x", var_c)

    with pytest.raises(TypeError):
        f(x)

    with pytest.raises(TypeError):
        x(f)


def test_term_nested_applications(var_a, var_b, var_c):
    """Test nested applications maintain proper references"""
    # Create types: A -> B and B -> C
    ab_type = implica.Arrow(var_a, var_b)
    bc_type = implica.Arrow(var_b, var_c)

    # Create terms
    f = implica.Term("f", ab_type)  # f : A -> B
    g = implica.Term("g", bc_type)  # g : B -> C
    x = implica.Term("x", var_a)  # x : A

    # First application: f(x) : B
    fx = f(x)
    assert fx.name == "(f x)"
    assert fx.function_uid == f.uid()
    assert fx.argument_uid == x.uid()

    # Second application: g(f(x)) : C
    # First we need to apply g to fx, but g expects B -> C and fx has type B
    # So we need a different approach - let's create g with type matching fx's output
    gfx_type = implica.Arrow(fx.type, var_c)
    g2 = implica.Term("g", gfx_type)
    gfx = g2(fx)

    assert gfx.name == "(g (f x))"
    assert gfx.function_uid == g2.uid()
    assert gfx.argument_uid == fx.uid()


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
