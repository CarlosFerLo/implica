import pytest
import implica


class TestTypeSchemaInit:
    """Test TypeSchema initialization with valid and invalid patterns."""

    def test_type_schema_init_with_wildcard_pattern(self):
        pattern = "*"
        schema = implica.TypeSchema(pattern)

        assert isinstance(schema, implica.TypeSchema)
        assert schema.pattern == pattern

    def test_type_schema_init_with_variable_pattern(self):
        pattern = "a"
        schema = implica.TypeSchema(pattern)

        assert isinstance(schema, implica.TypeSchema)
        assert schema.pattern == pattern

    def test_type_schema_init_with_arrow_pattern(self):
        pattern = "a -> b"
        schema = implica.TypeSchema(pattern)

        assert isinstance(schema, implica.TypeSchema)
        assert schema.pattern == pattern

    def test_type_schema_init_with_complex_pattern(self):
        pattern = "(a -> *) -> c"
        schema = implica.TypeSchema(pattern)

        assert isinstance(schema, implica.TypeSchema)
        assert schema.pattern == pattern

    def test_type_schema_init_with_capture_pattern(self):
        pattern = "(a: *)->b"
        schema = implica.TypeSchema(pattern)

        assert isinstance(schema, implica.TypeSchema)
        assert schema.pattern == pattern

    def test_type_schema_init_with_multiple_captures(self):
        pattern = "(x: *) -> (y: *) -> x"
        schema = implica.TypeSchema(pattern)

        assert isinstance(schema, implica.TypeSchema)
        assert schema.pattern == pattern

    def test_type_schema_init_with_nested_arrows(self):
        pattern = "((a -> b) -> c) -> d"
        schema = implica.TypeSchema(pattern)

        assert isinstance(schema, implica.TypeSchema)
        assert schema.pattern == pattern

    def test_type_schema_init_with_whitespace(self):
        pattern = "  a   ->   b  "
        schema = implica.TypeSchema(pattern)

        assert isinstance(schema, implica.TypeSchema)
        assert schema.pattern == pattern

    def test_type_schema_init_with_invalid_pattern(self):
        pattern = "invalid pattern"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_init_with_empty_pattern(self):
        pattern = ""
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_init_with_unbalanced_parentheses_left(self):
        pattern = "((a -> b"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_init_with_unbalanced_parentheses_right(self):
        pattern = "a -> b))"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_init_with_invalid_variable_name(self):
        pattern = "123invalid"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_init_with_structural_constraint_no_capture(self):
        """Test (:pattern) syntax - structural constraint without capture."""
        pattern = "(:*) -> a"
        schema = implica.TypeSchema(pattern)

        assert isinstance(schema, implica.TypeSchema)
        assert schema.pattern == pattern


class TestTypeSchemaSimpleMatches:
    """Test basic pattern matching without captures."""

    def test_type_schema_matches_with_wildcard(self):
        pattern = "*"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("a")

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_with_wildcard_arrow(self):
        pattern = "*"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_with_wildcard_complex(self):
        pattern = "*"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Arrow(implica.Variable("a"), implica.Variable("b")), implica.Variable("c")
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_with_variable(self):
        pattern = "a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("a")

        assert schema.matches(type_instance) is True

    def test_type_schema_does_not_match_different_variable(self):
        pattern = "a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("b")

        assert schema.matches(type_instance) is False

    def test_type_schema_variable_does_not_match_arrow(self):
        pattern = "a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))

        assert schema.matches(type_instance) is False

    def test_type_schema_matches_with_arrow(self):
        pattern = "a -> b"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_arrow_with_wildcard_left(self):
        pattern = "* -> b"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_arrow_with_wildcard_right(self):
        pattern = "a -> *"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_arrow_with_wildcards_both(self):
        pattern = "* -> *"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_with_complex_type(self):
        pattern = "(a -> *) -> c"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Arrow(implica.Variable("a"), implica.Variable("d")), implica.Variable("c")
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_nested_arrows_left(self):
        pattern = "(a -> b) -> c"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Arrow(implica.Variable("a"), implica.Variable("b")), implica.Variable("c")
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_nested_arrows_right(self):
        pattern = "a -> (b -> c)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("a"), implica.Arrow(implica.Variable("b"), implica.Variable("c"))
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_deeply_nested(self):
        pattern = "((a -> b) -> c) -> d"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Arrow(
                implica.Arrow(implica.Variable("a"), implica.Variable("b")), implica.Variable("c")
            ),
            implica.Variable("d"),
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_does_not_match_invalid_type(self):
        pattern = "a -> b"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("a")

        assert schema.matches(type_instance) is False

    def test_type_schema_does_not_match_isomorphic_type(self):
        pattern = "a -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))

        assert schema.matches(type_instance) is False

    def test_type_schema_does_not_match_wrong_structure(self):
        pattern = "(a -> b) -> c"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("a"), implica.Arrow(implica.Variable("b"), implica.Variable("c"))
        )

        assert schema.matches(type_instance) is False

    def test_type_schema_matches_with_parenthesized_variable(self):
        """Test that (a) is equivalent to a."""
        pattern = "(a)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("a")

        assert schema.matches(type_instance) is True

    def test_type_schema_matches_with_extra_parentheses(self):
        """Test that ((a -> b)) is equivalent to a -> b."""
        pattern = "((a -> b))"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))

        assert schema.matches(type_instance) is True


class TestTypeSchemaMatchesWithContext:
    """Test pattern matching with captures and context."""

    def test_type_schema_matches_with_variable_capture(self):
        pattern = "(a: *) -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("x"), implica.Variable("x"))

        assert schema.matches(type_instance) is True

    def test_type_schema_does_not_match_with_variable_capture_conflict(self):
        pattern = "(a:*)-> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("x"), implica.Variable("y"))

        assert schema.matches(type_instance) is False

    def test_type_schema_capture_with_wildcard(self):
        pattern = "(x: *)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("anyType")
        context = {}

        assert schema.matches(type_instance, context) is True
        assert "x" in context
        assert context["x"] == implica.Variable("anyType")

    def test_type_schema_capture_with_arrow(self):
        pattern = "(f: a -> b)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))
        context = {}

        assert schema.matches(type_instance, context) is True
        assert "f" in context
        assert context["f"] == type_instance

    def test_type_schema_capture_with_specific_variable(self):
        pattern = "(x: Int)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("Int")
        context = {}

        assert schema.matches(type_instance, context) is True
        assert "x" in context
        assert context["x"] == implica.Variable("Int")

    def test_type_schema_matches_with_multiple_captures(self):
        pattern = "(a: *) -> (b: a) -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("x"), implica.Arrow(implica.Variable("x"), implica.Variable("x"))
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_multiple_independent_captures(self):
        pattern = "(x: *) -> (y: *)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("Int"), implica.Variable("String"))
        context = {}

        assert schema.matches(type_instance, context) is True
        assert "x" in context
        assert "y" in context
        assert context["x"] == implica.Variable("Int")
        assert context["y"] == implica.Variable("String")

    def test_type_schema_multiple_same_captures(self):
        pattern = "(x: *) -> (y: *) -> (z: x)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("Int"),
            implica.Arrow(implica.Variable("String"), implica.Variable("Int")),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["x"] == implica.Variable("Int")
        assert context["y"] == implica.Variable("String")
        assert context["z"] == implica.Variable("Int")

    def test_type_schema_does_not_match_with_multiple_captures_conflict(self):
        pattern = "(a: *) -> (b: a) -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("x"), implica.Arrow(implica.Variable("y"), implica.Variable("x"))
        )

        assert schema.matches(type_instance) is False

    def test_type_schema_matches_with_nested_captures(self):
        pattern = "(a: *) -> (b: (c: a) -> a) -> c"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("x"),
            implica.Arrow(
                implica.Arrow(implica.Variable("x"), implica.Variable("x")), implica.Variable("x")
            ),
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_nested_captures_independence(self):
        pattern = "(outer: (inner: *) -> inner)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("T"), implica.Variable("T"))
        context = {}

        assert schema.matches(type_instance, context) is True
        assert "inner" in context
        assert "outer" in context
        assert context["inner"] == implica.Variable("T")
        assert context["outer"] == type_instance

    def test_type_schema_does_not_match_with_nested_captures_conflict(self):
        pattern = "(a: *) -> (b: (c: a) -> a) -> c"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("x"),
            implica.Arrow(
                implica.Arrow(implica.Variable("y"), implica.Variable("x")), implica.Variable("x")
            ),
        )

        assert schema.matches(type_instance) is False

    def test_type_schema_capture_reuse_same_type(self):
        """Test that a capture can be reused if it matches the same type."""
        pattern = "(t: *) -> t -> t"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("Int"), implica.Arrow(implica.Variable("Int"), implica.Variable("Int"))
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_capture_reuse_different_type_fails(self):
        """Test that a capture cannot be reused with a different type."""
        pattern = "(t: *) -> t -> t"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("Int"),
            implica.Arrow(implica.Variable("String"), implica.Variable("Int")),
        )

        assert schema.matches(type_instance) is False

    def test_type_schema_matches_with_external_context(self):
        pattern = "b -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("Int"), implica.Variable("String"))
        context = {"a": implica.Variable("String"), "b": implica.Variable("Int")}

        assert schema.matches(type_instance, context) is True

    def test_type_schema_matches_with_context_in_capture(self):
        """Test using context variables in capture patterns."""
        pattern = "(x: a) -> x"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("Int"), implica.Variable("Int"))
        context = {"a": implica.Variable("Int")}

        assert schema.matches(type_instance, context) is True
        assert context["x"] == implica.Variable("Int")

    def test_type_schema_does_not_match_with_conflicting_external_context(self):
        pattern = "b -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("Int"), implica.Variable("String"))
        context = {"a": implica.Variable("Int"), "b": implica.Variable("Int")}

        assert schema.matches(type_instance, context) is False

    def test_type_schema_external_context_with_capture_conflict(self):
        """Test that captures cannot override existing context."""
        pattern = "(a: *)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("NewType")
        context = {"a": implica.Variable("ExistingType")}

        # Should fail because 'a' already exists in context with different value
        assert schema.matches(type_instance, context) is False

    def test_type_schema_external_context_with_capture_match(self):
        """Test that captures match when context already has the same value."""
        pattern = "(a: *)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("SameType")
        context = {"a": implica.Variable("SameType")}

        assert schema.matches(type_instance, context) is True

    def test_type_schema_captures_are_stored_in_context(self):
        pattern = "(a: *) -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("X"), implica.Variable("X"))
        context = {}

        assert schema.matches(type_instance, context) is True
        assert "a" in context
        assert context["a"] == implica.Variable("X")

    def test_type_schema_captures_with_external_context(self):
        pattern = "(a: *) -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("Y"), implica.Variable("Y"))
        context = {"b": implica.Variable("Z")}

        assert schema.matches(type_instance, context) is True
        assert "a" in context
        assert context["a"] == implica.Variable("Y")
        assert "b" in context
        assert context["b"] == implica.Variable("Z")

    def test_type_schema_captures_complex_arrow_types(self):
        pattern = "(f: (a: *) -> (b: *)) -> a -> b"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Arrow(implica.Variable("Int"), implica.Variable("String")),
            implica.Arrow(implica.Variable("Int"), implica.Variable("String")),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["a"] == implica.Variable("Int")
        assert context["b"] == implica.Variable("String")
        assert context["f"] == implica.Arrow(implica.Variable("Int"), implica.Variable("String"))

    def test_type_schema_structural_constraint_without_capture(self):
        """Test (:pattern) syntax - structural constraint without capture."""
        pattern = "(:*) -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("AnyType"), implica.Variable("a"))
        context = {}

        assert schema.matches(type_instance, context) is True
        # No variable should be captured
        assert len(context) == 0


class TestTypeSchemaEdgeCases:
    """Test edge cases and special scenarios."""

    def test_type_schema_does_not_match_different_variable_names(self):
        """Test that variable patterns match by name literally."""
        pattern = "a -> a"
        schema = implica.TypeSchema(pattern)
        # Even though both sides are "T", the pattern expects Variable("a")
        type_instance = implica.Arrow(implica.Variable("T"), implica.Variable("T"))

        assert schema.matches(type_instance) is False

    def test_type_schema_curried_function(self):
        """Test matching curried function types."""
        pattern = "a -> b -> c"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("a"), implica.Arrow(implica.Variable("b"), implica.Variable("c"))
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_triple_curried_function(self):
        """Test matching triple curried function."""
        pattern = "a -> b -> c -> d"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("a"),
            implica.Arrow(
                implica.Variable("b"), implica.Arrow(implica.Variable("c"), implica.Variable("d"))
            ),
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_nested_wildcard_captures(self):
        """Test nested wildcard captures."""
        pattern = "(f: (* -> *)) -> f"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Arrow(implica.Variable("Int"), implica.Variable("String")),
            implica.Arrow(implica.Variable("Int"), implica.Variable("String")),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["f"] == implica.Arrow(implica.Variable("Int"), implica.Variable("String"))

    def test_type_schema_deeply_nested_structure(self):
        """Test deeply nested arrow structures."""
        pattern = "(((a -> b) -> c) -> d) -> e"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Arrow(
                implica.Arrow(
                    implica.Arrow(implica.Variable("a"), implica.Variable("b")),
                    implica.Variable("c"),
                ),
                implica.Variable("d"),
            ),
            implica.Variable("e"),
        )

        assert schema.matches(type_instance) is True

    def test_type_schema_multiple_wildcards_in_pattern(self):
        """Test multiple wildcards match independently."""
        pattern = "* -> * -> *"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("Int"),
            implica.Arrow(implica.Variable("String"), implica.Variable("Bool")),
        )

        assert schema.matches(type_instance) is True

        type_instance2 = implica.Arrow(
            implica.Arrow(implica.Variable("A"), implica.Variable("B")), implica.Variable("C")
        )

        assert schema.matches(type_instance2) is False

    def test_type_schema_captures_arrow_type(self):
        """Test capturing an arrow type itself."""
        pattern = "(arr: a -> b)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["arr"] == type_instance

    def test_type_schema_variable_reference_before_capture(self):
        """Test that variable patterns previous to capture are matched by variable references."""
        pattern = "a -> (a: *)"
        schema = implica.TypeSchema(pattern)
        # Left is Variable("a"), right will be captured as 'a' which is Variable("a") - should match
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["a"] == implica.Variable("b")

        type_instance2 = implica.Arrow(implica.Variable("x"), implica.Variable("x"))
        assert schema.matches(type_instance2, context) is False

    def test_type_schema_complex_constraint_propagation(self):
        """Test complex constraint propagation through nested captures."""
        pattern = "(x: *) -> (y: x -> x) -> y"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("Int"),
            implica.Arrow(
                implica.Arrow(implica.Variable("Int"), implica.Variable("Int")),
                implica.Arrow(implica.Variable("Int"), implica.Variable("Int")),
            ),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["x"] == implica.Variable("Int")
        assert context["y"] == implica.Arrow(implica.Variable("Int"), implica.Variable("Int"))

    def test_type_schema_whitespace_tolerance(self):
        """Test that patterns handle various whitespace correctly."""
        patterns = ["a->b", "a -> b", "a  ->  b", "  a -> b  ", "a\t->\tb"]
        type_instance = implica.Arrow(implica.Variable("a"), implica.Variable("b"))

        for pattern in patterns:
            schema = implica.TypeSchema(pattern)
            assert schema.matches(type_instance) is True


class TestTypeSchemaInvalidPatterns:
    """Test invalid pattern detection and error handling."""

    def test_type_schema_empty_capture_name_allowed(self):
        """Test that (:*) is valid (structural constraint without capture)."""
        pattern = "(:*)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Variable("anything")

        assert schema.matches(type_instance) is True

    def test_type_schema_arrow_without_right_side(self):
        """Test that incomplete arrow patterns are detected."""
        pattern = "a ->"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_arrow_without_left_side(self):
        """Test that incomplete arrow patterns are detected."""
        pattern = "-> b"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_capture_without_pattern(self):
        """Test that capture without pattern is invalid."""
        pattern = "(a:)"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_nested_unbalanced_left(self):
        """Test nested unbalanced parentheses."""
        pattern = "((a -> b) -> c"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_nested_unbalanced_right(self):
        """Test nested unbalanced parentheses."""
        pattern = "(a -> b)) -> c"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)

    def test_type_schema_invalid_special_characters(self):
        """Test that invalid characters are rejected."""
        invalid_patterns = [
            "a -> b!",
            "a@b",
            "a#->b",
            "a$->$b",
        ]
        for pattern in invalid_patterns:
            with pytest.raises(ValueError):
                implica.TypeSchema(pattern)

    def test_type_schema_number_start_variable(self):
        """Test that variable names starting with numbers are invalid."""
        pattern = "123abc"
        with pytest.raises(ValueError):
            implica.TypeSchema(pattern)


class TestTypeSchemaComplexScenarios:
    """Test complex real-world scenarios."""

    def test_type_schema_monad_bind_simplified(self):
        """Test monad bind type simplified.

        Pattern '(a: *) -> (a -> b) -> b' captures 'a', then uses it in 'a -> b'.
        The 'b' is a literal variable name, so we need Variable("b") in the type.
        """
        pattern = "(a: *) -> (a -> b) -> b"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("Maybe"),
            implica.Arrow(
                implica.Arrow(implica.Variable("Maybe"), implica.Variable("b")),
                implica.Variable("b"),
            ),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["a"] == implica.Variable("Maybe")

    def test_type_schema_higher_order_function_with_captures(self):
        """Test higher-order function with multiple captures."""
        pattern = "(pred: a -> Bool) -> (list: *) -> *"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Arrow(implica.Variable("a"), implica.Variable("Bool")),
            implica.Arrow(implica.Variable("List"), implica.Variable("randomType")),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["pred"] == implica.Arrow(implica.Variable("a"), implica.Variable("Bool"))
        assert context["list"] == implica.Variable("List")

    def test_type_schema_callback_pattern(self):
        """Test callback pattern."""
        pattern = "((result: *) -> *) -> *"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Arrow(implica.Variable("Data"), implica.Variable("Unit")),
            implica.Variable("Unit"),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["result"] == implica.Variable("Data")

    def test_type_schema_continuation_passing_style(self):
        """Test continuation-passing style type."""
        pattern = "(a: *) -> (a -> r) -> r"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("Int"),
            implica.Arrow(
                implica.Arrow(implica.Variable("Int"), implica.Variable("r")), implica.Variable("r")
            ),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["a"] == implica.Variable("Int")

    def test_type_schema_polymorphic_identity_with_capture(self):
        """Test polymorphic identity with explicit capture."""
        pattern = "(id: (t: *) -> t)"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(implica.Variable("X"), implica.Variable("X"))
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["t"] == implica.Variable("X")
        assert context["id"] == type_instance

    def test_type_schema_chain_of_transformations(self):
        """Test chain of type transformations."""
        pattern = "(a: *) -> (b: *) -> (c: *) -> a"
        schema = implica.TypeSchema(pattern)
        type_instance = implica.Arrow(
            implica.Variable("Int"),
            implica.Arrow(
                implica.Variable("String"),
                implica.Arrow(implica.Variable("Bool"), implica.Variable("Int")),
            ),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["a"] == implica.Variable("Int")
        assert context["b"] == implica.Variable("String")
        assert context["c"] == implica.Variable("Bool")

    def test_type_schema_multi_level_nesting_with_mixed_patterns(self):
        """Test multi-level nesting with wildcards, captures, and variables."""
        pattern = "(f: (x: *) -> (y: *) -> x) -> * -> (z: f)"
        schema = implica.TypeSchema(pattern)
        func_type = implica.Arrow(
            implica.Variable("A"), implica.Arrow(implica.Variable("B"), implica.Variable("A"))
        )
        type_instance = implica.Arrow(func_type, implica.Arrow(implica.Variable("Any"), func_type))
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["f"] == func_type
        assert context["x"] == implica.Variable("A")
        assert context["y"] == implica.Variable("B")
        assert context["z"] == func_type

    def test_type_schema_recursive_pattern_reference(self):
        """Test pattern where captures are used in subsequent positions."""
        pattern = "(t: * -> *) -> t -> t -> Bool"
        schema = implica.TypeSchema(pattern)
        arrow_type = implica.Arrow(implica.Variable("Int"), implica.Variable("String"))
        type_instance = implica.Arrow(
            arrow_type,
            implica.Arrow(arrow_type, implica.Arrow(arrow_type, implica.Variable("Bool"))),
        )
        context = {}

        assert schema.matches(type_instance, context) is True
        assert context["t"] == arrow_type


class TestTypeSchemaAssociativity:
    """Test right-associativity of arrow types: a -> b -> c = a -> (b -> c)."""

    def test_arrow_right_associativity_basic(self):
        """Test that a -> b -> c is parsed as a -> (b -> c)."""
        pattern1 = "a -> b -> c"
        pattern2 = "a -> (b -> c)"

        schema1 = implica.TypeSchema(pattern1)
        schema2 = implica.TypeSchema(pattern2)

        # Both should match the same type
        type_instance = implica.Arrow(
            implica.Variable("a"), implica.Arrow(implica.Variable("b"), implica.Variable("c"))
        )

        assert schema1.matches(type_instance) is True
        assert schema2.matches(type_instance) is True

    def test_arrow_right_associativity_not_left(self):
        """Test that a -> b -> c does NOT match (a -> b) -> c."""
        pattern = "a -> b -> c"
        schema = implica.TypeSchema(pattern)

        # This is left-associative, should not match
        type_instance = implica.Arrow(
            implica.Arrow(implica.Variable("a"), implica.Variable("b")), implica.Variable("c")
        )

        assert schema.matches(type_instance) is False

    def test_arrow_right_associativity_with_wildcards(self):
        """Test right-associativity with wildcards."""
        pattern1 = "* -> * -> *"
        pattern2 = "* -> (* -> *)"

        schema1 = implica.TypeSchema(pattern1)
        schema2 = implica.TypeSchema(pattern2)

        type_instance = implica.Arrow(
            implica.Variable("Int"),
            implica.Arrow(implica.Variable("String"), implica.Variable("Bool")),
        )

        assert schema1.matches(type_instance) is True
        assert schema2.matches(type_instance) is True

    def test_arrow_right_associativity_with_captures(self):
        """Test right-associativity with captures."""
        pattern1 = "(a: *) -> (b: *) -> (c: *)"
        pattern2 = "(a: *) -> ((b: *) -> (c: *))"

        schema1 = implica.TypeSchema(pattern1)
        schema2 = implica.TypeSchema(pattern2)

        type_instance = implica.Arrow(
            implica.Variable("Int"),
            implica.Arrow(implica.Variable("String"), implica.Variable("Bool")),
        )

        context1 = {}
        context2 = {}

        assert schema1.matches(type_instance, context1) is True
        assert schema2.matches(type_instance, context2) is True
        assert context1 == context2

    def test_arrow_right_associativity_four_levels(self):
        """Test right-associativity with four levels: a -> b -> c -> d."""
        pattern1 = "a -> b -> c -> d"
        pattern2 = "a -> (b -> (c -> d))"

        schema1 = implica.TypeSchema(pattern1)
        schema2 = implica.TypeSchema(pattern2)

        type_instance = implica.Arrow(
            implica.Variable("a"),
            implica.Arrow(
                implica.Variable("b"), implica.Arrow(implica.Variable("c"), implica.Variable("d"))
            ),
        )

        assert schema1.matches(type_instance) is True
        assert schema2.matches(type_instance) is True

    def test_arrow_left_grouping_explicit(self):
        """Test that we can explicitly override right-associativity with parentheses."""
        pattern = "(a -> b) -> c"
        schema = implica.TypeSchema(pattern)

        # Left-associative type
        type_instance = implica.Arrow(
            implica.Arrow(implica.Variable("a"), implica.Variable("b")), implica.Variable("c")
        )

        assert schema.matches(type_instance) is True

        # Right-associative type should NOT match
        type_instance_right = implica.Arrow(
            implica.Variable("a"), implica.Arrow(implica.Variable("b"), implica.Variable("c"))
        )

        assert schema.matches(type_instance_right) is False


class TestTypeSchemaStringRepresentation:
    """Test string representation methods."""

    def test_type_schema_str_simple(self):
        pattern = "a -> b"
        schema = implica.TypeSchema(pattern)

        assert str(schema) == f"TypeSchema('{pattern}')"

    def test_type_schema_repr_simple(self):
        pattern = "a -> b"
        schema = implica.TypeSchema(pattern)

        assert repr(schema) == f"TypeSchema('{pattern}')"

    def test_type_schema_str_complex(self):
        pattern = "(x: *) -> (y: x -> *) -> y"
        schema = implica.TypeSchema(pattern)

        assert str(schema) == f"TypeSchema('{pattern}')"

    def test_type_schema_repr_complex(self):
        pattern = "(x: *) -> (y: x -> *) -> y"
        schema = implica.TypeSchema(pattern)

        assert repr(schema) == f"TypeSchema('{pattern}')"


class TestTypeSchemaEquality:
    """Test equality comparisons between TypeSchema instances."""

    def test_type_schema_equality_same_pattern(self):
        pattern = "a -> b"
        schema1 = implica.TypeSchema(pattern)
        schema2 = implica.TypeSchema(pattern)

        assert schema1 == schema2

    def test_type_schema_inequality_different_pattern(self):
        pattern1 = "a -> b"
        pattern2 = "a -> c"
        schema1 = implica.TypeSchema(pattern1)
        schema2 = implica.TypeSchema(pattern2)

        assert schema1 != schema2

    def test_type_schema_inequality_different_type(self):
        pattern = "a -> b"
        schema = implica.TypeSchema(pattern)
        other = "NotATypeSchema"

        assert schema != other

    def test_type_schema_equality_complex_pattern(self):
        pattern = "(x: *) -> (y: x -> *) -> y"
        schema1 = implica.TypeSchema(pattern)
        schema2 = implica.TypeSchema(pattern)

        assert schema1 == schema2

    def test_type_schema_inequality_similar_but_different(self):
        pattern1 = "(a: *) -> a"
        pattern2 = "(b: *) -> b"
        schema1 = implica.TypeSchema(pattern1)
        schema2 = implica.TypeSchema(pattern2)

        assert schema1 != schema2

    def test_type_schema_equality_with_whitespace_variation(self):
        pattern1 = "a -> b"
        pattern2 = "  a  ->  b  "
        schema1 = implica.TypeSchema(pattern1)
        schema2 = implica.TypeSchema(pattern2)

        assert schema1 == schema2
