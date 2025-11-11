import implica
import pytest


# ============================================================================
# SECTION 1: BASIC PATTERN MATCHING TESTS
# ============================================================================


class TestBasicPatterns:
    """Test basic pattern matching with wildcards, variables, and simple applications."""

    def test_wildcard_matches_all_types(self, var_a, var_b, app_ab):
        """Test that wildcard schema matches all types"""
        schema = implica.TypeSchema("*")
        assert schema.matches(var_a)
        assert schema.matches(var_b)
        assert schema.matches(app_ab)

    def test_wildcard_matches_nested_applications(self):
        """Test wildcard matches deeply nested applications"""
        # Create A -> (B -> C)
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")
        inner = implica.Application(b, c)
        outer = implica.Application(a, inner)

        schema = implica.TypeSchema("*")
        assert schema.matches(outer)

    def test_specific_variable_exact_match(self, var_a, var_b):
        """Test schema matching specific variable"""
        schema_a = implica.TypeSchema("A")
        assert schema_a.matches(var_a)
        assert not schema_a.matches(var_b)

    def test_specific_variable_case_sensitive(self):
        """Test that variable matching is case-sensitive"""
        var_lower = implica.Variable("a")
        var_upper = implica.Variable("A")

        schema = implica.TypeSchema("A")
        assert schema.matches(var_upper)
        assert not schema.matches(var_lower)

    def test_different_variable_names_no_match(self):
        """Test that different variable names don't match"""
        var_x = implica.Variable("X")
        var_y = implica.Variable("Y")
        var_z = implica.Variable("Z")

        schema = implica.TypeSchema("X")
        assert schema.matches(var_x)
        assert not schema.matches(var_y)
        assert not schema.matches(var_z)

    def test_simple_application_exact_match(self, app_ab):
        """Test exact application pattern matching"""
        schema = implica.TypeSchema("A -> B")
        assert schema.matches(app_ab)

    def test_simple_application_wrong_order_no_match(self, app_ab, app_ba):
        """Test that A -> B doesn't match B -> A"""
        schema = implica.TypeSchema("A -> B")
        assert schema.matches(app_ab)
        assert not schema.matches(app_ba)

    def test_application_left_wildcard(self, app_ab):
        """Test application pattern with left wildcard"""
        schema = implica.TypeSchema("* -> B")
        assert schema.matches(app_ab)

    def test_application_right_wildcard(self, app_ab):
        """Test application pattern with right wildcard"""
        schema = implica.TypeSchema("A -> *")
        assert schema.matches(app_ab)

    def test_application_both_wildcards(self):
        """Test application pattern with wildcards on both sides"""
        var_x = implica.Variable("X")
        var_y = implica.Variable("Y")
        app = implica.Application(var_x, var_y)

        schema = implica.TypeSchema("* -> *")
        assert schema.matches(app)

    def test_variable_does_not_match_application(self, var_a, app_ab):
        """Test that variable pattern doesn't match application"""
        schema = implica.TypeSchema("A")
        assert schema.matches(var_a)
        assert not schema.matches(app_ab)

    def test_application_does_not_match_variable(self, var_a):
        """Test that application pattern doesn't match variable"""
        schema = implica.TypeSchema("A -> B")
        assert not schema.matches(var_a)


# ============================================================================
# SECTION 2: NESTED APPLICATION TESTS
# ============================================================================


class TestNestedApplications:
    """Test deeply nested applications and right-associativity."""

    def test_right_associative_parsing_three_types(self):
        """Test that A -> B -> C is parsed as A -> (B -> C)"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        # Create A -> (B -> C) - right-associative
        bc = implica.Application(b, c)
        abc_right = implica.Application(a, bc)

        # Create (A -> B) -> C - left-associative
        ab = implica.Application(a, b)
        abc_left = implica.Application(ab, c)

        # Pattern should match right-associative structure
        schema = implica.TypeSchema("A -> B -> C")
        assert schema.matches(abc_right), "Should match A -> (B -> C)"
        assert not schema.matches(abc_left), "Should not match (A -> B) -> C"

    def test_explicit_left_associative_with_parens(self):
        """Test explicit left-associative with parentheses: (A -> B) -> C"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        ab = implica.Application(a, b)
        abc_left = implica.Application(ab, c)

        schema = implica.TypeSchema("(A -> B) -> C")
        assert schema.matches(abc_left)

    def test_deeply_nested_right_associative(self):
        """Test deeply nested right-associative: A -> B -> C -> D"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")
        d = implica.Variable("D")

        # Build A -> (B -> (C -> D))
        cd = implica.Application(c, d)
        bcd = implica.Application(b, cd)
        abcd = implica.Application(a, bcd)

        schema = implica.TypeSchema("A -> B -> C -> D")
        assert schema.matches(abcd)

    def test_nested_applications_with_wildcards(self):
        """Test nested applications with wildcard patterns"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        bc = implica.Application(b, c)
        abc = implica.Application(a, bc)

        # Test various wildcard patterns
        assert implica.TypeSchema("* -> * -> *").matches(abc)
        assert implica.TypeSchema("A -> * -> *").matches(abc)
        assert implica.TypeSchema("* -> B -> *").matches(abc)
        assert implica.TypeSchema("* -> * -> C").matches(abc)
        assert implica.TypeSchema("A -> B -> *").matches(abc)
        assert implica.TypeSchema("A -> * -> C").matches(abc)

    def test_complex_nested_structure(self):
        """Test complex nested structure: ((A -> B) -> C) -> D"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")
        d = implica.Variable("D")

        ab = implica.Application(a, b)
        abc = implica.Application(ab, c)
        abcd = implica.Application(abc, d)

        schema = implica.TypeSchema("((A -> B) -> C) -> D")
        assert schema.matches(abcd)

    def test_mixed_nesting_levels(self):
        """Test mixed nesting: (A -> (B -> C)) -> D"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")
        d = implica.Variable("D")

        bc = implica.Application(b, c)
        abc = implica.Application(a, bc)
        abcd = implica.Application(abc, d)

        schema = implica.TypeSchema("(A -> (B -> C)) -> D")
        assert schema.matches(abcd)

        # Also test with simplified notation (right-associative)
        schema2 = implica.TypeSchema("(A -> B -> C) -> D")
        assert schema2.matches(abcd)

    def test_triple_nesting_left_side(self):
        """Test triple nesting on left side: ((A -> B) -> C) -> D"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")
        d = implica.Variable("D")

        ab = implica.Application(a, b)
        ab_c = implica.Application(ab, c)
        result = implica.Application(ab_c, d)

        schema = implica.TypeSchema("((A -> B) -> C) -> D")
        assert schema.matches(result)


# ============================================================================
# SECTION 3: CAPTURE MECHANISM TESTS
# ============================================================================


class TestCaptureMechanism:
    """Test named captures, structural constraints, and multi-level captures."""

    def test_simple_capture_variable(self):
        """Test capturing a simple variable"""
        var_a = implica.Variable("A")
        schema = implica.TypeSchema("(x:A)")

        captures = schema.capture(var_a)
        assert "x" in captures
        assert captures["x"] == var_a

    def test_simple_capture_wildcard(self):
        """Test capturing with wildcard"""
        var_a = implica.Variable("A")
        schema = implica.TypeSchema("(x:*)")

        captures = schema.capture(var_a)
        assert "x" in captures
        assert captures["x"] == var_a

    def test_capture_application_parts(self, app_ab):
        """Test capturing parts of an application: (in:*) -> (out:*)"""
        schema = implica.TypeSchema("(in:*) -> (out:*)")

        captures = schema.capture(app_ab)
        assert "in" in captures
        assert "out" in captures
        assert captures["in"] == implica.Variable("A")
        assert captures["out"] == implica.Variable("B")

    def test_capture_specific_and_wildcard(self, app_ab):
        """Test mixing specific types with wildcard captures"""
        schema = implica.TypeSchema("A -> (out:*)")

        captures = schema.capture(app_ab)
        assert "out" in captures
        assert captures["out"] == implica.Variable("B")

    def test_capture_middle_type_in_chain(self):
        """Test capturing middle type: A -> (mid:*) -> C"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        bc = implica.Application(b, c)
        abc = implica.Application(a, bc)

        schema = implica.TypeSchema("A -> (mid:*) -> C")
        captures = schema.capture(abc)

        assert "mid" in captures
        assert captures["mid"] == b

    def test_multiple_captures_nested(self):
        """Test multiple captures at different nesting levels"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        inner = implica.Application(a, b)
        outer = implica.Application(inner, c)

        schema = implica.TypeSchema("((left:A) -> (right:*)) -> (result:C)")
        captures = schema.capture(outer)

        assert "left" in captures
        assert "right" in captures
        assert "result" in captures
        assert captures["left"] == a
        assert captures["right"] == b
        assert captures["result"] == c

    def test_capture_entire_application(self):
        """Test capturing an entire application type"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        app = implica.Application(a, b)

        schema = implica.TypeSchema("(func:A -> B)")
        captures = schema.capture(app)

        assert "func" in captures
        assert captures["func"] == app

    def test_nested_captures_with_wildcards(self):
        """Test nested captures with wildcard patterns"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        bc = implica.Application(b, c)
        abc = implica.Application(a, bc)

        schema = implica.TypeSchema("(outer:(input:*) -> (inner:(x:*) -> (y:*)))")
        captures = schema.capture(abc)

        assert "outer" in captures
        assert "input" in captures
        assert "inner" in captures
        assert "x" in captures
        assert "y" in captures
        assert captures["outer"] == abc
        assert captures["input"] == a
        assert captures["inner"] == bc
        assert captures["x"] == b
        assert captures["y"] == c

    def test_structural_constraint_no_capture(self):
        """Test structural constraint without name: (:* -> *) -> B"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        ac = implica.Application(a, c)
        result = implica.Application(ac, b)

        schema = implica.TypeSchema("(:* -> *) -> B")

        assert schema.matches(result)
        captures = schema.capture(result)
        assert len(captures) == 0

    def test_mixed_captures_and_constraints(self):
        """Test mixing named captures with structural constraints"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        ab = implica.Application(a, b)
        result = implica.Application(ab, c)

        schema = implica.TypeSchema("(:(captured:A) -> *) -> (result:C)")
        captures = schema.capture(result)

        assert "captured" in captures
        assert "result" in captures
        assert captures["captured"] == a
        assert captures["result"] == c

    def test_capture_with_specific_type_constraint(self):
        """Test capture with specific type constraint in pattern"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        app = implica.Application(a, b)

        schema = implica.TypeSchema("(x:A) -> B")
        captures = schema.capture(app)

        assert "x" in captures
        assert captures["x"] == a

    def test_no_captures_returns_empty_dict(self):
        """Test that patterns without captures return empty dict"""
        a = implica.Variable("A")
        schema = implica.TypeSchema("A")

        captures = schema.capture(a)
        assert isinstance(captures, dict)
        assert len(captures) == 0


# ============================================================================
# SECTION 4: DUPLICATE CAPTURE REFERENCE TESTS
# ============================================================================


class TestDuplicateCaptureReferences:
    """Test same capture name used multiple times with equality checking."""

    def test_duplicate_capture_equal_variables(self):
        """Test that duplicate captures match when values are equal"""
        a1 = implica.Variable("A")
        a2 = implica.Variable("A")
        app = implica.Application(a1, a2)

        # Same capture name twice - should check for equality
        schema = implica.TypeSchema("(x:*) -> (x:*)")
        assert schema.matches(app)

    def test_duplicate_capture_unequal_variables(self):
        """Test that duplicate captures don't match when values differ"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        app = implica.Application(a, b)

        schema = implica.TypeSchema("(x:*) -> (x:*)")
        assert not schema.matches(app)

    def test_duplicate_capture_in_chain(self):
        """Test duplicate captures in longer chain: (x:*) -> B -> (x:*)"""
        a1 = implica.Variable("A")
        a2 = implica.Variable("A")
        b = implica.Variable("B")

        # A -> (B -> A)
        ba = implica.Application(b, a2)
        aba = implica.Application(a1, ba)

        schema = implica.TypeSchema("(x:*) -> B -> (x:*)")
        assert schema.matches(aba)

    def test_duplicate_capture_chain_unequal(self):
        """Test duplicate captures fail with unequal values in chain"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        # A -> (B -> C)
        bc = implica.Application(b, c)
        abc = implica.Application(a, bc)

        schema = implica.TypeSchema("(x:*) -> B -> (x:*)")
        assert not schema.matches(abc)

    def test_duplicate_capture_complex_applications(self):
        """Test duplicate captures with complex application types"""
        a = implica.Variable("A")
        b = implica.Variable("B")

        # Create (A -> B) on both sides
        ab1 = implica.Application(a, b)
        ab2 = implica.Application(a, b)
        app = implica.Application(ab1, ab2)

        schema = implica.TypeSchema("(func:A -> B) -> (func:A -> B)")
        assert schema.matches(app)

    def test_duplicate_capture_complex_applications_unequal(self):
        """Test duplicate captures fail with different applications"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        ab = implica.Application(a, b)
        ac = implica.Application(a, c)
        app = implica.Application(ab, ac)

        schema = implica.TypeSchema("(func:A -> *) -> (func:A -> *)")
        assert not schema.matches(app)

    def test_triple_duplicate_capture(self):
        """Test same capture name used three times"""
        a1 = implica.Variable("A")
        a2 = implica.Variable("A")
        a3 = implica.Variable("A")

        # A -> (A -> A)
        aa = implica.Application(a2, a3)
        aaa = implica.Application(a1, aa)

        schema = implica.TypeSchema("(x:*) -> (x:*) -> (x:*)")
        assert schema.matches(aaa)

    def test_triple_duplicate_capture_one_different(self):
        """Test triple duplicate fails if one value differs"""
        a1 = implica.Variable("A")
        a2 = implica.Variable("A")
        b = implica.Variable("B")

        # A -> (A -> B)
        ab = implica.Application(a2, b)
        aab = implica.Application(a1, ab)

        schema = implica.TypeSchema("(x:*) -> (x:*) -> (x:*)")
        assert not schema.matches(aab)

    def test_capture_returns_last_occurrence(self):
        """Test that capture dict contains the matched value"""
        a = implica.Variable("A")
        app = implica.Application(a, a)

        schema = implica.TypeSchema("(x:A) -> (x:A)")
        captures = schema.capture(app)

        assert "x" in captures
        assert captures["x"] == a


# ============================================================================
# SECTION 5: CHARACTER VALIDATION TESTS
# ============================================================================


class TestCharacterValidation:
    """Test valid/invalid characters in variable names and patterns."""

    def test_alphanumeric_variable_names(self):
        """Test that alphanumeric names work correctly"""
        var = implica.Variable("Variable123")
        schema = implica.TypeSchema("Variable123")
        assert schema.matches(var)

    def test_underscore_in_variable_names(self):
        """Test that underscores are allowed in variable names"""
        var = implica.Variable("my_variable")
        schema = implica.TypeSchema("my_variable")
        assert schema.matches(var)

    def test_uppercase_lowercase_mix(self):
        """Test mixed case variable names"""
        var = implica.Variable("MyVariableType")
        schema = implica.TypeSchema("MyVariableType")
        assert schema.matches(var)

    def test_numbers_in_variable_names(self):
        """Test numbers in variable names"""
        var1 = implica.Variable("Type1")
        var2 = implica.Variable("Type2")
        schema1 = implica.TypeSchema("Type1")
        schema2 = implica.TypeSchema("Type2")

        assert schema1.matches(var1)
        assert not schema1.matches(var2)
        assert schema2.matches(var2)

    def test_whitespace_handling_in_patterns(self):
        """Test that whitespace is handled correctly"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        app = implica.Application(a, b)

        # All these should work with various whitespace
        assert implica.TypeSchema("A->B").matches(app)
        assert implica.TypeSchema("A -> B").matches(app)
        assert implica.TypeSchema("A  ->  B").matches(app)
        assert implica.TypeSchema("  A -> B  ").matches(app)

    def test_whitespace_in_captures(self):
        """Test whitespace handling in capture patterns"""
        a = implica.Variable("A")

        schema1 = implica.TypeSchema("(x:A)")
        schema2 = implica.TypeSchema("( x : A )")
        schema3 = implica.TypeSchema("(  x  :  A  )")

        assert schema1.matches(a)
        assert schema2.matches(a)
        assert schema3.matches(a)

    def test_special_characters_in_variable_names(self):
        """Test that some special characters work in variable names"""
        # Test apostrophe
        var_prime = implica.Variable("A'")
        schema_prime = implica.TypeSchema("A'")
        assert schema_prime.matches(var_prime)

        # Test double prime
        var_double_prime = implica.Variable("A''")
        schema_double_prime = implica.TypeSchema("A''")
        assert schema_double_prime.matches(var_double_prime)

    def test_dot_in_variable_names(self):
        """Test dot notation in variable names"""
        var = implica.Variable("Module.Type")
        schema = implica.TypeSchema("Module.Type")
        assert schema.matches(var)

    def test_reserved_characters_in_context(self):
        """Test that reserved characters work correctly in their contexts"""
        # Colon in capture names
        schema = implica.TypeSchema("(x:A)")
        assert schema.matches(implica.Variable("A"))

        # Arrow in applications
        a = implica.Variable("A")
        b = implica.Variable("B")
        app = implica.Application(a, b)
        schema2 = implica.TypeSchema("A -> B")
        assert schema2.matches(app)

        # Parentheses for grouping
        schema3 = implica.TypeSchema("(A)")
        assert schema3.matches(a)


# ============================================================================
# SECTION 6: ERROR HANDLING TESTS
# ============================================================================


class TestErrorHandling:
    """Test invalid patterns and error conditions."""

    def test_empty_pattern_error(self):
        """Test that empty pattern raises error"""
        with pytest.raises(Exception):  # Should raise ValueError or similar
            implica.TypeSchema("")

    def test_only_whitespace_pattern_error(self):
        """Test that whitespace-only pattern raises error"""
        with pytest.raises(Exception):
            implica.TypeSchema("   ")

    def test_unbalanced_parentheses_left(self):
        """Test unbalanced parentheses - too many left"""
        with pytest.raises(Exception):
            implica.TypeSchema("((A -> B)")

    def test_unbalanced_parentheses_right(self):
        """Test unbalanced parentheses - too many right"""
        with pytest.raises(Exception):
            implica.TypeSchema("(A -> B))")

    def test_missing_arrow_right_side(self):
        """Test pattern with arrow but missing right side"""
        with pytest.raises(Exception):
            implica.TypeSchema("A ->")

    def test_missing_arrow_left_side(self):
        """Test pattern with arrow but missing left side"""
        with pytest.raises(Exception):
            implica.TypeSchema("-> B")

    def test_empty_capture_pattern(self):
        """Test capture with empty pattern"""
        with pytest.raises(Exception):
            implica.TypeSchema("(x:)")

    def test_capture_without_colon(self):
        """Test that (A) is treated as grouping, not capture"""
        var_a = implica.Variable("A")
        schema = implica.TypeSchema("(A)")
        assert schema.matches(var_a)

    def test_multiple_arrows_without_parens(self):
        """Test that multiple arrows work (right-associative)"""
        # This should work: A -> B -> C means A -> (B -> C)
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")
        bc = implica.Application(b, c)
        abc = implica.Application(a, bc)

        schema = implica.TypeSchema("A -> B -> C")
        assert schema.matches(abc)

    def test_nested_empty_parens(self):
        """Test that empty parentheses are invalid"""
        with pytest.raises(Exception):
            implica.TypeSchema("()")

    def test_only_arrow_error(self):
        """Test that arrow alone is invalid"""
        with pytest.raises(Exception):
            implica.TypeSchema("->")

    def test_double_arrow_error(self):
        """Test that double arrow is invalid"""
        with pytest.raises(Exception):
            implica.TypeSchema("A -> -> B")

    def test_capture_with_empty_name(self):
        """Test structural constraint with colon but no name: (:A)"""
        var_a = implica.Variable("A")
        schema = implica.TypeSchema("(:A)")
        # This should work as structural constraint
        assert schema.matches(var_a)


# ============================================================================
# SECTION 7: EDGE CASE TESTS
# ============================================================================


class TestEdgeCases:
    """Test extreme cases and corner conditions."""

    def test_very_deep_nesting(self):
        """Test extremely deep nesting levels"""
        # Build type: A -> (B -> (C -> (D -> E)))
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")
        d = implica.Variable("D")
        e = implica.Variable("E")

        de = implica.Application(d, e)
        cde = implica.Application(c, de)
        bcde = implica.Application(b, cde)
        abcde = implica.Application(a, bcde)

        schema = implica.TypeSchema("A -> B -> C -> D -> E")
        assert schema.matches(abcde)

    def test_very_long_variable_name(self):
        """Test very long variable names"""
        long_name = "A" * 100
        var = implica.Variable(long_name)
        schema = implica.TypeSchema(long_name)
        assert schema.matches(var)

    def test_complex_nested_captures(self):
        """Test deeply nested capture patterns"""
        a = implica.Variable("A")
        b = implica.Variable("B")

        ab = implica.Application(a, b)
        ab_ab = implica.Application(ab, ab)

        schema = implica.TypeSchema("((a:A) -> (b:B)) -> ((c:A) -> (d:B))")
        captures = schema.capture(ab_ab)

        assert len(captures) == 4
        assert all(k in captures for k in ["a", "b", "c", "d"])

    def test_wildcard_only_capture(self):
        """Test capturing just a wildcard"""
        var = implica.Variable("AnythingAtAll")
        schema = implica.TypeSchema("(x:*)")

        captures = schema.capture(var)
        assert "x" in captures
        assert captures["x"] == var

    def test_redundant_parentheses(self):
        """Test that redundant parentheses don't break matching"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        app = implica.Application(a, b)

        # All should match
        assert implica.TypeSchema("A -> B").matches(app)
        assert implica.TypeSchema("(A) -> (B)").matches(app)
        assert implica.TypeSchema("((A)) -> ((B))").matches(app)
        assert implica.TypeSchema("(A -> B)").matches(app)
        assert implica.TypeSchema("((A -> B))").matches(app)

    def test_complex_wildcard_combinations(self):
        """Test complex combinations of wildcards"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        ab = implica.Application(a, b)
        ab_c = implica.Application(ab, c)

        # All should match
        assert implica.TypeSchema("* -> *").matches(ab_c)
        assert implica.TypeSchema("(* -> *) -> *").matches(ab_c)
        assert implica.TypeSchema("(* -> *) -> C").matches(ab_c)
        assert implica.TypeSchema("(A -> *) -> *").matches(ab_c)
        assert implica.TypeSchema("(A -> B) -> *").matches(ab_c)

    def test_alternating_specific_and_wildcard(self):
        """Test alternating specific types and wildcards"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")
        d = implica.Variable("D")

        cd = implica.Application(c, d)
        bcd = implica.Application(b, cd)
        abcd = implica.Application(a, bcd)

        assert implica.TypeSchema("A -> * -> C -> *").matches(abcd)
        assert implica.TypeSchema("* -> B -> * -> D").matches(abcd)

    def test_single_character_variable_names(self):
        """Test single character variable names"""
        x = implica.Variable("X")
        y = implica.Variable("Y")

        schema_x = implica.TypeSchema("X")
        schema_y = implica.TypeSchema("Y")

        assert schema_x.matches(x)
        assert not schema_x.matches(y)
        assert schema_y.matches(y)

    def test_matching_no_match_returns_empty_captures(self):
        """Test that failed match returns empty dict"""
        a = implica.Variable("A")
        b = implica.Variable("B")

        schema = implica.TypeSchema("(x:B)")
        captures = schema.capture(a)

        assert isinstance(captures, dict)
        assert len(captures) == 0

    def test_pattern_with_many_captures(self):
        """Test pattern with many different capture names"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        bc = implica.Application(b, c)
        abc = implica.Application(a, bc)

        schema = implica.TypeSchema("(v1:*) -> ((v2:*) -> (v3:*))")
        captures = schema.capture(abc)

        assert len(captures) == 3
        assert captures["v1"] == a
        assert captures["v2"] == b
        assert captures["v3"] == c


# ============================================================================
# SECTION 8: COMPREHENSIVE INTEGRATION TESTS
# ============================================================================


class TestComprehensiveIntegration:
    """Test realistic complex patterns combining all features."""

    def test_curried_function_pattern(self):
        """Test pattern for curried functions: A -> B -> C"""
        person = implica.Variable("Person")
        number = implica.Variable("Number")
        string = implica.Variable("String")

        # Person -> (Number -> String)
        num_str = implica.Application(number, string)
        full_type = implica.Application(person, num_str)

        schema = implica.TypeSchema("(input:*) -> (param:*) -> (output:*)")
        captures = schema.capture(full_type)

        assert captures["input"] == person
        assert captures["param"] == number
        assert captures["output"] == string

    def test_higher_order_function_pattern(self):
        """Test pattern for higher-order functions: (A -> B) -> C"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        ab = implica.Application(a, b)
        func_type = implica.Application(ab, c)

        schema = implica.TypeSchema("((in:*) -> (out:*)) -> (result:*)")
        captures = schema.capture(func_type)

        assert captures["in"] == a
        assert captures["out"] == b
        assert captures["result"] == c

    def test_functor_map_pattern(self):
        """Test pattern like: (A -> B) -> F A -> F B"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        fa = implica.Variable("FA")
        fb = implica.Variable("FB")

        # (A -> B) -> (FA -> FB)
        ab = implica.Application(a, b)
        fa_fb = implica.Application(fa, fb)
        full = implica.Application(ab, fa_fb)

        schema = implica.TypeSchema("((x:*) -> (y:*)) -> (* -> *)")
        captures = schema.capture(full)

        assert "x" in captures
        assert "y" in captures
        assert captures["x"] == a
        assert captures["y"] == b

    def test_monad_bind_pattern(self):
        """Test monadic bind pattern: M A -> (A -> M B) -> M B"""
        ma = implica.Variable("MA")
        a = implica.Variable("A")
        mb1 = implica.Variable("MB")
        mb2 = implica.Variable("MB")

        # A -> MB
        a_mb = implica.Application(a, mb1)
        # MA -> (A -> MB)
        ma_to_func = implica.Application(ma, a_mb)
        # (MA -> (A -> MB)) -> MB
        full = implica.Application(ma_to_func, mb2)

        schema = implica.TypeSchema("(* -> (inp:*) -> *) -> (out:*)")
        captures = schema.capture(full)

        assert "inp" in captures
        assert "out" in captures

    def test_complex_equality_check_pattern(self):
        """Test complex pattern with multiple equality checks"""
        a = implica.Variable("A")
        b = implica.Variable("B")

        # (A -> B) -> (A -> B)  # Same function type twice
        ab1 = implica.Application(a, b)
        ab2 = implica.Application(a, b)
        full = implica.Application(ab1, ab2)

        schema = implica.TypeSchema("((x:*) -> (y:*)) -> ((x:*) -> (y:*))")
        assert schema.matches(full)

    def test_church_numeral_pattern(self):
        """Test Church numeral pattern: (A -> A) -> (A -> A)"""
        a = implica.Variable("A")

        # A -> A
        aa1 = implica.Application(a, a)
        aa2 = implica.Application(a, a)
        # (A -> A) -> (A -> A)
        full = implica.Application(aa1, aa2)

        schema = implica.TypeSchema("((t:*) -> (t:*)) -> ((t:*) -> (t:*))")
        assert schema.matches(full)

    def test_contravariant_functor_pattern(self):
        """Test contravariant functor: (B -> A) -> F A -> F B"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        fa = implica.Variable("FA")
        fb = implica.Variable("FB")

        # (B -> A)
        ba = implica.Application(b, a)
        # FA -> FB
        fa_fb = implica.Application(fa, fb)
        # (B -> A) -> (FA -> FB)
        full = implica.Application(ba, fa_fb)

        schema = implica.TypeSchema("(* -> *) -> * -> *")
        assert schema.matches(full)

    def test_deep_type_constructor_pattern(self):
        """Test deeply nested type constructor applications"""
        # Build: Maybe (Either A B)
        a = implica.Variable("A")
        b = implica.Variable("B")
        either = implica.Variable("Either")
        maybe = implica.Variable("Maybe")

        # Either A B would be represented as applications
        # For this test, let's use simpler nesting

        ab = implica.Application(a, b)
        result = implica.Application(ab, maybe)

        schema = implica.TypeSchema("(* -> *) -> *")
        assert schema.matches(result)

    def test_realistic_dependent_pattern(self):
        """Test realistic pattern with dependencies and captures"""
        # Simulate: forall a. a -> List a -> List a
        a1 = implica.Variable("a")
        a2 = implica.Variable("a")
        list_a1 = implica.Variable("List_a")
        list_a2 = implica.Variable("List_a")

        # a -> List_a
        a_to_list = implica.Application(a1, list_a1)
        # (a -> List_a) -> List_a
        full = implica.Application(a_to_list, list_a2)

        # Schema should capture the inner application
        schema = implica.TypeSchema("((elem:*) -> *) -> *")
        captures = schema.capture(full)

        assert "elem" in captures
        assert captures["elem"] == a1

    def test_combinator_s_pattern(self):
        """Test S combinator pattern: (A -> B -> C) -> (A -> B) -> A -> C"""
        a = implica.Variable("A")
        b = implica.Variable("B")
        c = implica.Variable("C")

        # Build from right: ((A -> (B -> C)) -> ((A -> B) -> (A -> C)))
        bc = implica.Application(b, c)
        abc = implica.Application(a, bc)
        ab = implica.Application(a, b)
        ac = implica.Application(a, c)
        ab_ac = implica.Application(ab, ac)
        full = implica.Application(abc, ab_ac)

        # Test with wildcard pattern
        schema = implica.TypeSchema("(* -> * -> *) -> (* -> *) -> * -> *")
        assert schema.matches(full)

    def test_lens_pattern(self):
        """Test lens-like pattern: (S -> A) -> (S -> B -> T) -> Lens S T A B"""
        s = implica.Variable("S")
        a = implica.Variable("A")
        b = implica.Variable("B")
        t = implica.Variable("T")

        # S -> A
        sa = implica.Application(s, a)
        # B -> T
        bt = implica.Application(b, t)
        # S -> (B -> T)
        s_bt = implica.Application(s, bt)
        # (S -> A) -> (S -> (B -> T))
        full = implica.Application(sa, s_bt)

        schema = implica.TypeSchema("(* -> (get:*)) -> (* -> * -> *)")
        captures = schema.capture(full)

        assert "get" in captures
        assert captures["get"] == a
