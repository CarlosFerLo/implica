import implica


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
    assert str(result).startswith("(f x):")
