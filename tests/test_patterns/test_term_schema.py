import pytest
import implica


class TestTermSchemaInit:
    """Test TermSchema initialization with valid and invalid patterns."""

    def test_term_schema_init_with_wildcard_pattern(self):
        pattern = "*"
        schema = implica.TermSchema(pattern)

        assert isinstance(schema, implica.TermSchema)
        assert schema.pattern == pattern

    def test_term_schema_init_with_variable_pattern(self):
        pattern = "t"
        schema = implica.TermSchema(pattern)

        assert isinstance(schema, implica.TermSchema)
        assert schema.pattern == pattern

    def test_term_schema_init_with_application_pattern(self):
        pattern = "f x"
        schema = implica.TermSchema(pattern)

        assert isinstance(schema, implica.TermSchema)
        assert schema.pattern == pattern

    def test_term_schema_init_with_complex_pattern(self):
        pattern = "f x y"
        schema = implica.TermSchema(pattern)

        assert isinstance(schema, implica.TermSchema)
        assert schema.pattern == pattern

    def test_term_schema_init_with_nested_pattern(self):
        pattern = "f * y"
        schema = implica.TermSchema(pattern)

        assert isinstance(schema, implica.TermSchema)
        assert schema.pattern == pattern

    def test_term_schema_init_with_multiple_wildcards(self):
        pattern = "* * *"
        schema = implica.TermSchema(pattern)

        assert isinstance(schema, implica.TermSchema)
        assert schema.pattern == pattern

    def test_term_schema_init_with_whitespace(self):
        pattern = "  f   x  "
        schema = implica.TermSchema(pattern)

        assert isinstance(schema, implica.TermSchema)
        assert schema.pattern == pattern

    def test_term_schema_init_with_empty_pattern(self):
        pattern = ""
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)

    def test_term_schema_init_with_invalid_variable_name(self):
        pattern = "123invalid"
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)

    def test_term_schema_init_with_special_characters(self):
        pattern = "f@x"
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)

    def test_term_schema_init_with_only_spaces(self):
        pattern = "   "
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)


class TestTermSchemaSimpleMatches:
    """Test basic pattern matching without captures."""

    def test_term_schema_matches_with_wildcard(self):
        pattern = "*"
        schema = implica.TermSchema(pattern)

        # Create a basic term
        term_type = implica.Variable("Int")
        term = implica.BasicTerm("x", term_type)

        assert schema.matches(term) is True

    def test_term_schema_matches_wildcard_with_application(self):
        pattern = "*"
        schema = implica.TermSchema(pattern)

        # Create an application term
        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)

        assert schema.matches(app) is True

    def test_term_schema_matches_with_variable(self):
        pattern = "x"
        schema = implica.TermSchema(pattern)

        term_type = implica.Variable("Int")
        term = implica.BasicTerm("x", term_type)

        assert schema.matches(term) is True

    def test_term_schema_variable_captures_any_term(self):
        """A variable pattern captures any term (not name-based matching)."""
        pattern = "x"
        schema = implica.TermSchema(pattern)

        term_type = implica.Variable("Int")
        # Pattern variable 'x' will capture term named 'y'
        term = implica.BasicTerm("y", term_type)

        context = {}
        assert schema.matches(term, context) is True
        assert context["x"] == term

    def test_term_schema_variable_captures_application_too(self):
        """A variable can capture an application term."""
        pattern = "t"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)

        context = {}
        assert schema.matches(app, context) is True
        assert context["t"] == app

    def test_term_schema_matches_simple_application(self):
        pattern = "f x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)
        context = {}

        assert schema.matches(app, context) is True
        assert context["f"] == f
        assert context["x"] == x

    def test_term_schema_application_with_wildcard_function(self):
        pattern = "* x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)

        assert schema.matches(app) is True

    def test_term_schema_application_with_wildcard_argument(self):
        pattern = "f *"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)

        assert schema.matches(app) is True

    def test_term_schema_application_with_both_wildcards(self):
        pattern = "* *"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)

        assert schema.matches(app) is True

    def test_term_schema_application_does_not_match_variable(self):
        """Matching fails when pattern expects application but term is variable."""
        pattern = "f x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        term = implica.BasicTerm("f", int_type)

        assert schema.matches(term) is False

    def test_term_schema_does_not_match_wrong_function(self):
        """Matching fails when context has a different term for the variable."""
        pattern = "g x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        wrong_func = implica.BasicTerm("wrong", f_type)
        app = f(x)

        # Pre-populate context with different term for 'g'
        context = {"g": wrong_func}
        assert schema.matches(app, context) is False

    def test_term_schema_does_not_match_wrong_argument(self):
        """Matching fails when context has a different term for the argument variable."""
        pattern = "f y"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        wrong_arg = implica.BasicTerm("wrong", int_type)
        app = f(x)

        # Pre-populate context with different term for 'y'
        context = {"y": wrong_arg}
        assert schema.matches(app, context) is False


class TestTermSchemaLeftAssociativity:
    """Test left-associativity of application: f x y = (f x) y."""

    def test_application_left_associativity_basic(self):
        """Test that 'f x y' matches ((f x) y) not (f (x y))."""
        pattern = "f x y"
        schema = implica.TermSchema(pattern)

        # Create types
        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))

        # Create terms
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)

        # Left-associative: (f x) y
        app = f(x)(y)

        assert schema.matches(app) is True

    def test_application_left_associativity_with_wildcards(self):
        """Test left-associativity with wildcards."""
        pattern = "* * *"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))

        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)

        # Should match (f x) y
        app = f(x)(y)

        assert schema.matches(app) is True

    def test_application_left_associativity_four_terms(self):
        """Test left-associativity with four terms: f x y z = ((f x) y) z."""
        pattern = "f x y z"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, implica.Arrow(int_type, int_type)))

        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)
        z = implica.BasicTerm("z", int_type)

        # Left-associative: ((f x) y) z
        app = f(x)(y)(z)

        assert schema.matches(app) is True

    def test_application_left_associativity_partial_match(self):
        """Test that pattern variables can capture partial applications."""
        pattern = "g z"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))

        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)

        # (f x) y - g captures (f x), z captures y
        app = f(x)(y)

        context = {}
        assert schema.matches(app, context) is True
        assert context["g"] == f(x)
        assert context["z"] == y

    def test_application_left_associativity_nested_pattern(self):
        """Test that 'f x y' pattern expects left-associative structure."""
        pattern = "g f x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        g_type = implica.Arrow(implica.Arrow(int_type, int_type), implica.Arrow(int_type, int_type))
        f_type = implica.Arrow(int_type, int_type)

        g = implica.BasicTerm("g", g_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)

        # (g f) x
        app = g(f)(x)

        assert schema.matches(app) is True


class TestTermSchemaMatchesWithContext:
    """Test pattern matching with automatic captures and context."""

    def test_term_schema_auto_capture_simple_variable(self):
        """A variable automatically captures a term if not in context."""
        pattern = "t"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        term = implica.BasicTerm("x", int_type)
        context = {}

        assert schema.matches(term, context) is True
        assert "t" in context
        assert context["t"] == term

    def test_term_schema_wildcard_no_capture(self):
        """Wildcards match any term but do not capture."""
        pattern = "*"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        term = implica.BasicTerm("x", int_type)
        context = {}

        # Wildcard matches but doesn't capture
        assert schema.matches(term, context) is True
        assert len(context) == 0

    def test_term_schema_auto_capture_in_application(self):
        """Variables in applications automatically capture matched terms."""
        pattern = "f x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f_term = implica.BasicTerm("g", f_type)
        x_term = implica.BasicTerm("y", int_type)
        app = f_term(x_term)

        context = {}

        assert schema.matches(app, context) is True
        assert "f" in context
        assert "x" in context
        assert context["f"] == f_term
        assert context["x"] == x_term

    def test_term_schema_variable_reuse_same_term(self):
        """A variable used twice matches only if both positions have the same term."""
        pattern = "f t t"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)

        # (f x) x - both arguments are the same
        app = f(x)(x)

        context = {}
        assert schema.matches(app, context) is True
        assert context["t"] == x
        # First occurrence captures, second verifies

    def test_term_schema_variable_reuse_different_term_fails(self):
        """A variable used twice fails if positions have different terms."""
        pattern = "f t t"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)

        # (f x) y - arguments are different
        app = f(x)(y)

        assert schema.matches(app) is False

    def test_term_schema_matches_with_existing_context(self):
        """Variables match against existing context values."""
        pattern = "f t"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)

        # Pre-populate context with expected value
        context = {"t": x}

        assert schema.matches(app, context) is True
        # f gets captured, t matches existing
        assert context["f"] == f
        assert context["t"] == x

    def test_term_schema_fails_with_conflicting_context(self):
        """Match fails if context has different value for variable."""
        pattern = "f t"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)
        app = f(x)

        # Context says t should be y, but pattern tries to match x
        context = {"t": y}

        assert schema.matches(app, context) is False
        # Context unchanged after failed match
        assert context["t"] == y

    def test_term_schema_multiple_auto_captures(self):
        """Multiple variables each capture their matched terms."""
        pattern = "f g"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("func1", f_type)
        g = implica.BasicTerm("func2", int_type)
        app = f(g)

        context = {}

        assert schema.matches(app, context) is True
        assert "f" in context
        assert "g" in context
        assert context["f"] == f
        assert context["g"] == g

    def test_term_schema_nested_auto_captures(self):
        """Variables in nested applications each capture their terms."""
        pattern = "f g x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))

        f = implica.BasicTerm("f", f_type)
        g = implica.BasicTerm("g", int_type)
        x = implica.BasicTerm("x", int_type)

        # (f g) x
        app = f(g)(x)

        context = {}
        assert schema.matches(app, context) is True
        assert context["f"] == f
        assert context["g"] == g
        assert context["x"] == x

    def test_term_schema_wildcard_with_variable(self):
        """Wildcards don't capture, variables do."""
        pattern = "* t *"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))

        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)

        # (f x) y
        app = f(x)(y)

        context = {}
        assert schema.matches(app, context) is True
        assert "t" in context
        assert context["t"] == x
        assert len(context) == 1  # Only 't' is captured, wildcards don't capture


class TestTermSchemaEdgeCases:
    """Test edge cases and special scenarios."""

    def test_term_schema_deeply_nested_application(self):
        """Test matching deeply nested applications."""
        pattern = "a b c d"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        a_type = implica.Arrow(int_type, implica.Arrow(int_type, implica.Arrow(int_type, int_type)))

        a = implica.BasicTerm("a", a_type)
        b = implica.BasicTerm("b", int_type)
        c = implica.BasicTerm("c", int_type)
        d = implica.BasicTerm("d", int_type)

        # (((a b) c) d)
        app = a(b)(c)(d)

        assert schema.matches(app) is True

    def test_term_schema_single_term_in_context(self):
        """Test that single term patterns capture correctly."""
        pattern = "x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        term = implica.BasicTerm("myTerm", int_type)

        context = {}
        assert schema.matches(term, context) is True
        assert context["x"] == term

    def test_term_schema_application_as_function(self):
        """Test matching when the function is itself an application."""
        pattern = "f g x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))

        f = implica.BasicTerm("f", f_type)
        g = implica.BasicTerm("g", int_type)
        x = implica.BasicTerm("x", int_type)

        app = f(g)(x)

        assert schema.matches(app) is True

    def test_term_schema_all_wildcards(self):
        """Test pattern with all wildcards."""
        pattern = "* * * *"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, implica.Arrow(int_type, int_type)))

        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)
        z = implica.BasicTerm("z", int_type)

        app = f(x)(y)(z)

        assert schema.matches(app) is True

    def test_term_schema_whitespace_tolerance(self):
        """Test that extra whitespace doesn't affect matching."""
        pattern1 = "f x"
        pattern2 = "  f   x  "
        schema1 = implica.TermSchema(pattern1)
        schema2 = implica.TermSchema(pattern2)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)

        assert schema1.matches(app) is True
        assert schema2.matches(app) is True

    def test_term_schema_complex_nested_captures(self):
        """Test complex nested capture scenarios."""
        pattern = "f x y"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))

        f = implica.BasicTerm("myFunc", f_type)
        x = implica.BasicTerm("arg1", int_type)
        y = implica.BasicTerm("arg2", int_type)

        app = f(x)(y)

        context = {}
        assert schema.matches(app, context) is True
        assert context["f"] == f
        assert context["x"] == x
        assert context["y"] == y


class TestTermSchemaComplexScenarios:
    """Test complex real-world scenarios."""

    def test_term_schema_function_composition_pattern(self):
        """Test matching function composition patterns."""
        pattern = "compose f g"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        compose_type = implica.Arrow(
            implica.Arrow(int_type, int_type),
            implica.Arrow(implica.Arrow(int_type, int_type), implica.Arrow(int_type, int_type)),
        )
        f_type = implica.Arrow(int_type, int_type)

        compose = implica.BasicTerm("compose", compose_type)
        f = implica.BasicTerm("f", f_type)
        g = implica.BasicTerm("g", f_type)

        app = compose(f)(g)

        context = {}
        assert schema.matches(app, context) is True
        assert context["f"] == f
        assert context["g"] == g

    def test_term_schema_curried_function_full_application(self):
        """Test matching fully applied curried functions."""
        pattern = "curry x y z"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        curry_type = implica.Arrow(
            int_type, implica.Arrow(int_type, implica.Arrow(int_type, int_type))
        )

        curry = implica.BasicTerm("curry", curry_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)
        z = implica.BasicTerm("z", int_type)

        app = curry(x)(y)(z)

        assert schema.matches(app) is True

    def test_term_schema_partial_application_pattern(self):
        """Test matching partial applications."""
        pattern = "map f"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        list_type = implica.Variable("List")
        map_type = implica.Arrow(
            implica.Arrow(int_type, int_type), implica.Arrow(list_type, list_type)
        )
        f_type = implica.Arrow(int_type, int_type)

        map_term = implica.BasicTerm("map", map_type)
        f = implica.BasicTerm("f", f_type)

        app = map_term(f)

        assert schema.matches(app) is True

    def test_term_schema_identity_pattern(self):
        """Test matching identity function application."""
        pattern = "id x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        id_type = implica.Arrow(int_type, int_type)

        id_term = implica.BasicTerm("id", id_type)
        x = implica.BasicTerm("x", int_type)

        app = id_term(x)

        assert schema.matches(app) is True

    def test_term_schema_higher_order_function(self):
        """Test matching higher-order function patterns."""
        pattern = "apply f x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        apply_type = implica.Arrow(
            implica.Arrow(int_type, int_type), implica.Arrow(int_type, int_type)
        )
        f_type = implica.Arrow(int_type, int_type)

        apply = implica.BasicTerm("apply", apply_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)

        app = apply(f)(x)

        assert schema.matches(app) is True

    def test_term_schema_nested_application_with_same_function(self):
        """Test pattern where variable f appears twice and must match same term."""
        pattern = "g f f"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        g_type = implica.Arrow(int_type, implica.Arrow(int_type, int_type))

        g = implica.BasicTerm("g", g_type)
        x = implica.BasicTerm("x", int_type)

        # (g x) x - same term x used twice
        app = g(x)(x)

        context = {}
        assert schema.matches(app, context) is True
        assert context["g"] == g
        assert context["f"] == x

    def test_term_schema_with_mixed_wildcards_and_variables(self):
        """Test complex pattern with wildcards and variables mixed."""
        pattern = "* f * g *"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        h_type = implica.Arrow(
            int_type,
            implica.Arrow(int_type, implica.Arrow(int_type, implica.Arrow(int_type, int_type))),
        )

        h = implica.BasicTerm("h", h_type)
        f = implica.BasicTerm("f", int_type)
        g = implica.BasicTerm("g", int_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)

        # ((((h f) x) g) y)
        app = h(f)(x)(g)(y)

        context = {}
        assert schema.matches(app, context) is True
        assert context["f"] == f
        assert context["g"] == g


class TestTermSchemaStringRepresentation:
    """Test string representation methods."""

    def test_term_schema_str_simple(self):
        pattern = "f x"
        schema = implica.TermSchema(pattern)
        assert str(schema) == f"TermSchema('{pattern}')"

    def test_term_schema_repr_simple(self):
        pattern = "f x"
        schema = implica.TermSchema(pattern)
        assert repr(schema) == f"TermSchema('{pattern}')"

    def test_term_schema_str_complex(self):
        pattern = "f x y z"
        schema = implica.TermSchema(pattern)
        assert str(schema) == f"TermSchema('{pattern}')"

    def test_term_schema_repr_complex(self):
        pattern = "f x y z"
        schema = implica.TermSchema(pattern)
        assert repr(schema) == f"TermSchema('{pattern}')"

    def test_term_schema_pattern_property(self):
        pattern = "compose f g x"
        schema = implica.TermSchema(pattern)
        assert schema.pattern == pattern


class TestTermSchemaInvalidPatterns:
    """Test invalid pattern detection and error handling."""

    def test_term_schema_pattern_with_leading_space_only(self):
        """Test that patterns with only leading spaces fail."""
        pattern = " "
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)

    def test_term_schema_pattern_with_invalid_identifier(self):
        """Test that invalid identifiers are rejected."""
        pattern = "f-x"
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)

    def test_term_schema_pattern_with_number_start(self):
        """Test that variables starting with numbers are rejected."""
        pattern = "123x"
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)

    def test_term_schema_pattern_with_special_char(self):
        """Test that special characters are rejected."""
        pattern = "f$x"
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)

    def test_term_schema_empty_after_trim(self):
        """Test that patterns empty after trimming are rejected."""
        pattern = "   "
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)

    def test_term_schema_pattern_with_tabs(self):
        """Test that tabs in identifiers are rejected."""
        pattern = "f\tx"
        # Tabs within identifier names are invalid
        with pytest.raises(ValueError):
            implica.TermSchema(pattern)


class TestTermSchemaContextInteraction:
    """Test interaction with context objects."""

    def test_term_schema_context_not_modified_on_failure(self):
        """Test that context is not modified when match fails."""
        pattern = "f x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        term = implica.BasicTerm("y", int_type)

        context = {}
        result = schema.matches(term, context)

        assert result is False
        assert len(context) == 0

    def test_term_schema_context_preserves_existing_on_failure(self):
        """Test that existing context entries are preserved on failure."""
        pattern = "f y"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        y = implica.BasicTerm("y", int_type)
        z = implica.BasicTerm("z", int_type)
        app = f(x)

        # Pre-populate context where 'y' is already set to a different term
        context = {"y": z, "existing": y}
        result = schema.matches(app, context)

        # Match should fail because context says 'y' should be 'z' but pattern tries to match 'x'
        assert result is False
        # Existing context should remain unchanged
        assert "existing" in context
        assert context["existing"] == y
        assert context["y"] == z  # y still refers to z

    def test_term_schema_context_accumulates_across_matches(self):
        """Test that context accumulates information across multiple matches."""
        pattern1 = "f x"
        pattern2 = "g x"

        schema1 = implica.TermSchema(pattern1)
        schema2 = implica.TermSchema(pattern2)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        g = implica.BasicTerm("g", f_type)
        x = implica.BasicTerm("x", int_type)

        app1 = f(x)
        app2 = g(x)

        context = {}

        assert schema1.matches(app1, context) is True
        assert "f" in context
        assert "x" in context

        # Second match should see 'x' already bound
        assert schema2.matches(app2, context) is True
        assert "g" in context
        # 'x' should still be the same
        assert context["x"] == x

    def test_term_schema_none_context_works(self):
        """Test that passing None as context works (internal context used)."""
        pattern = "f x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)

        # Should work without passing context
        assert schema.matches(app, None) is True
        assert schema.matches(app) is True


class TestTermSchemaErrorCases:
    """Test error handling in various scenarios."""

    def test_term_schema_matches_with_invalid_term_type(self):
        """Test that passing invalid type to matches raises error."""
        pattern = "f x"
        schema = implica.TermSchema(pattern)

        # Passing a string instead of a term
        with pytest.raises((TypeError, ValueError, RuntimeError)):
            schema.matches("not a term")

    def test_term_schema_matches_with_none_term(self):
        """Test that passing None as term raises error."""
        pattern = "f x"
        schema = implica.TermSchema(pattern)

        with pytest.raises((TypeError, ValueError, RuntimeError)):
            schema.matches(None)

    def test_term_schema_context_type_conflict(self):
        """Test error when context contains types instead of terms."""
        pattern = "f x"
        schema = implica.TermSchema(pattern)

        int_type = implica.Variable("Int")
        f_type = implica.Arrow(int_type, int_type)
        f = implica.BasicTerm("f", f_type)
        x = implica.BasicTerm("x", int_type)
        app = f(x)

        # Put a type in the context where a term is expected
        context = {"f": int_type}  # This is a type, not a term

        # Should raise an error about context conflict
        with pytest.raises((TypeError, ValueError)):
            schema.matches(app, context)
