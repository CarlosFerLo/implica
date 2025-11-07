import implica


def test_wildcard_matches_all(var_a, var_b, app_ab):
    """Test that wildcard schema matches all types"""
    schema_wildcard = implica.TypeSchema("$*$")
    assert schema_wildcard.matches(var_a)
    assert schema_wildcard.matches(var_b)
    assert schema_wildcard.matches(app_ab)


def test_specific_variable_schema(var_a, var_b):
    """Test schema matching specific variable"""
    schema_a = implica.TypeSchema("$A$")
    assert schema_a.matches(var_a)
    assert not schema_a.matches(var_b)


def test_application_pattern_exact(app_ab):
    """Test exact application pattern matching"""
    schema_app = implica.TypeSchema("$A -> B$")
    assert schema_app.matches(app_ab)


def test_application_pattern_with_wildcard(app_ab):
    """Test application pattern with wildcard"""
    schema_app_wildcard = implica.TypeSchema("$A -> *$")
    assert schema_app_wildcard.matches(app_ab)
