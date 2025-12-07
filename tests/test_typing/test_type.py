import pytest
import implica


class TestVariable:
    """Tests for the Variable type class."""

    def test_variable_creation(self):
        """Test creating a Variable with a valid name."""
        var = implica.Variable("x")
        assert var.name == "x"

    def test_variable_with_longer_name(self):
        """Test creating a Variable with a multi-character name."""
        var = implica.Variable("myVar")
        assert var.name == "myVar"

    def test_variable_uid_is_string(self):
        """Test that uid() returns a string."""
        var = implica.Variable("x")
        uid = var.uid()
        assert isinstance(uid, str)
        assert len(uid) > 0

    def test_variable_uid_consistency(self):
        """Test that calling uid() multiple times returns the same value."""
        var = implica.Variable("x")
        uid1 = var.uid()
        uid2 = var.uid()
        assert uid1 == uid2

    def test_variable_uid_uniqueness(self):
        """Test that different variables have different UIDs."""
        var1 = implica.Variable("x")
        var2 = implica.Variable("y")
        assert var1.uid() != var2.uid()

    def test_variable_same_name_same_uid(self):
        """Test that variables with the same name have the same UID."""
        var1 = implica.Variable("x")
        var2 = implica.Variable("x")
        assert var1.uid() == var2.uid()

    def test_variable_str(self):
        """Test the string representation of a Variable."""
        var = implica.Variable("x")
        assert str(var) == "x"

    def test_variable_repr(self):
        """Test the repr representation of a Variable."""
        var = implica.Variable("myVar")
        assert repr(var) == 'Variable("myVar")'

    def test_variable_equality(self):
        """Test that variables with the same name are equal."""
        var1 = implica.Variable("x")
        var2 = implica.Variable("x")
        assert var1 == var2

    def test_variable_inequality(self):
        """Test that variables with different names are not equal."""
        var1 = implica.Variable("x")
        var2 = implica.Variable("y")
        assert var1 != var2

    def test_variable_hash_consistency(self):
        """Test that the same variable produces the same hash."""
        var = implica.Variable("x")
        hash1 = hash(var)
        hash2 = hash(var)
        assert hash1 == hash2

    def test_variable_hash_equality(self):
        """Test that equal variables have the same hash."""
        var1 = implica.Variable("x")
        var2 = implica.Variable("x")
        assert hash(var1) == hash(var2)

    def test_variable_in_set(self):
        """Test that variables can be used in sets."""
        var1 = implica.Variable("x")
        var2 = implica.Variable("x")
        var3 = implica.Variable("y")

        var_set = {var1, var2, var3}
        assert len(var_set) == 2  # var1 and var2 should be considered the same

    def test_variable_in_dict(self):
        """Test that variables can be used as dictionary keys."""
        var1 = implica.Variable("x")
        var2 = implica.Variable("x")

        d = {var1: "value1"}
        d[var2] = "value2"

        assert len(d) == 1  # var1 and var2 should be the same key
        assert d[var1] == "value2"

    def test_variable_empty_name_raises_error(self):
        """Test that creating a Variable with an empty name raises an error."""
        with pytest.raises(Exception):  # Should raise ValueError or similar
            implica.Variable("")

    def test_variable_whitespace_name_raises_error(self):
        """Test that creating a Variable with only whitespace raises an error."""
        with pytest.raises(Exception):
            implica.Variable("   ")

    def test_variable_special_characters(self):
        """Test variables with special characters in names."""
        var = implica.Variable("var_1")
        assert var.name == "var_1"

    def test_variable_name_immutability(self):
        """Test that the name property cannot be modified after creation."""
        var = implica.Variable("x")

        with pytest.raises(AttributeError):
            var.name = "y"

    def test_variable_name_remains_unchanged(self):
        """Test that the name property remains constant throughout the variable's lifetime."""
        var = implica.Variable("original")
        original_name = var.name

        # Try multiple operations that might affect the variable
        _ = var.uid()
        _ = str(var)
        _ = repr(var)
        _ = hash(var)

        # Name should still be the same
        assert var.name == original_name
        assert var.name == "original"


class TestArrow:
    """Tests for the Arrow type class."""

    def test_arrow_creation(self):
        """Test creating an Arrow type."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        assert arrow.left == var_x
        assert arrow.right == var_y

    def test_arrow_left_getter(self):
        """Test the left property of an Arrow."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        assert arrow.left == var_x

    def test_arrow_right_getter(self):
        """Test the right property of an Arrow."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        assert arrow.right == var_y

    def test_arrow_uid_is_string(self):
        """Test that uid() returns a string."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        uid = arrow.uid()
        assert isinstance(uid, str)
        assert len(uid) > 0

    def test_arrow_uid_consistency(self):
        """Test that calling uid() multiple times returns the same value."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        uid1 = arrow.uid()
        uid2 = arrow.uid()
        assert uid1 == uid2

    def test_arrow_uid_uniqueness(self):
        """Test that different arrows have different UIDs."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        arrow1 = implica.Arrow(var_x, var_y)
        arrow2 = implica.Arrow(var_x, var_z)

        assert arrow1.uid() != arrow2.uid()

    def test_arrow_same_types_same_uid(self):
        """Test that arrows with the same types have the same UID."""
        var_x1 = implica.Variable("x")
        var_y1 = implica.Variable("y")
        arrow1 = implica.Arrow(var_x1, var_y1)

        var_x2 = implica.Variable("x")
        var_y2 = implica.Variable("y")
        arrow2 = implica.Arrow(var_x2, var_y2)

        assert arrow1.uid() == arrow2.uid()

    def test_arrow_str(self):
        """Test the string representation of an Arrow."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        assert str(arrow) == "(x -> y)"

    def test_arrow_repr(self):
        """Test the repr representation of an Arrow."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        assert repr(arrow) == "Arrow(x, y)"

    def test_arrow_equality(self):
        """Test that arrows with the same types are equal."""
        var_x1 = implica.Variable("x")
        var_y1 = implica.Variable("y")
        arrow1 = implica.Arrow(var_x1, var_y1)

        var_x2 = implica.Variable("x")
        var_y2 = implica.Variable("y")
        arrow2 = implica.Arrow(var_x2, var_y2)

        assert arrow1 == arrow2

    def test_arrow_inequality(self):
        """Test that arrows with different types are not equal."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        arrow1 = implica.Arrow(var_x, var_y)
        arrow2 = implica.Arrow(var_x, var_z)

        assert arrow1 != arrow2

    def test_arrow_hash_consistency(self):
        """Test that the same arrow produces the same hash."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        hash1 = hash(arrow)
        hash2 = hash(arrow)
        assert hash1 == hash2

    def test_arrow_hash_equality(self):
        """Test that equal arrows have the same hash."""
        var_x1 = implica.Variable("x")
        var_y1 = implica.Variable("y")
        arrow1 = implica.Arrow(var_x1, var_y1)

        var_x2 = implica.Variable("x")
        var_y2 = implica.Variable("y")
        arrow2 = implica.Arrow(var_x2, var_y2)

        assert hash(arrow1) == hash(arrow2)

    def test_arrow_in_set(self):
        """Test that arrows can be used in sets."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        arrow1 = implica.Arrow(var_x, var_y)
        arrow2 = implica.Arrow(var_x, var_y)
        arrow3 = implica.Arrow(var_x, var_z)

        arrow_set = {arrow1, arrow2, arrow3}
        assert len(arrow_set) == 2  # arrow1 and arrow2 should be considered the same

    def test_arrow_in_dict(self):
        """Test that arrows can be used as dictionary keys."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")

        arrow1 = implica.Arrow(var_x, var_y)
        arrow2 = implica.Arrow(var_x, var_y)

        d = {arrow1: "value1"}
        d[arrow2] = "value2"

        assert len(d) == 1  # arrow1 and arrow2 should be the same key
        assert d[arrow1] == "value2"

    def test_nested_arrow_left(self):
        """Test creating nested arrows (arrow as left argument)."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        inner_arrow = implica.Arrow(var_x, var_y)
        outer_arrow = implica.Arrow(inner_arrow, var_z)

        assert outer_arrow.left == inner_arrow
        assert outer_arrow.right == var_z

    def test_nested_arrow_right(self):
        """Test creating nested arrows (arrow as right argument)."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        inner_arrow = implica.Arrow(var_y, var_z)
        outer_arrow = implica.Arrow(var_x, inner_arrow)

        assert outer_arrow.left == var_x
        assert outer_arrow.right == inner_arrow

    def test_nested_arrow_str(self):
        """Test string representation of nested arrows."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        inner_arrow = implica.Arrow(var_x, var_y)
        outer_arrow = implica.Arrow(inner_arrow, var_z)

        # Should produce ((x -> y) -> z)
        assert str(outer_arrow) == "((x -> y) -> z)"

    def test_deeply_nested_arrows(self):
        """Test deeply nested arrow types."""
        var_a = implica.Variable("a")
        var_b = implica.Variable("b")
        var_c = implica.Variable("c")
        var_d = implica.Variable("d")

        # ((a -> b) -> (c -> d))
        left_arrow = implica.Arrow(var_a, var_b)
        right_arrow = implica.Arrow(var_c, var_d)
        outer_arrow = implica.Arrow(left_arrow, right_arrow)

        assert str(outer_arrow) == "((a -> b) -> (c -> d))"

    def test_arrow_left_immutability(self):
        """Test that the left property cannot be modified after creation."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")
        arrow = implica.Arrow(var_x, var_y)

        with pytest.raises(AttributeError):
            arrow.left = var_z

    def test_arrow_right_immutability(self):
        """Test that the right property cannot be modified after creation."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")
        arrow = implica.Arrow(var_x, var_y)

        with pytest.raises(AttributeError):
            arrow.right = var_z

    def test_arrow_properties_remain_unchanged(self):
        """Test that left and right properties remain constant throughout the arrow's lifetime."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        original_left = arrow.left
        original_right = arrow.right

        # Try multiple operations that might affect the arrow
        _ = arrow.uid()
        _ = str(arrow)
        _ = repr(arrow)
        _ = hash(arrow)

        # Properties should still be the same
        assert arrow.left == original_left
        assert arrow.right == original_right
        assert arrow.left == var_x
        assert arrow.right == var_y

    def test_arrow_nested_immutability(self):
        """Test that nested arrows maintain immutability of their components."""
        var_a = implica.Variable("a")
        var_b = implica.Variable("b")
        var_c = implica.Variable("c")

        inner_arrow = implica.Arrow(var_a, var_b)
        outer_arrow = implica.Arrow(inner_arrow, var_c)

        # Cannot modify outer arrow properties
        with pytest.raises(AttributeError):
            outer_arrow.left = var_c

        with pytest.raises(AttributeError):
            outer_arrow.right = var_a

        # Verify the structure remains intact
        assert outer_arrow.left == inner_arrow
        assert outer_arrow.right == var_c


class TestTypeInteractions:
    """Tests for interactions between Variable and Arrow types."""

    def test_variable_not_equal_to_arrow(self):
        """Test that Variables and Arrows are never equal."""
        var = implica.Variable("x")
        arrow = implica.Arrow(implica.Variable("x"), implica.Variable("y"))

        assert var != arrow

    def test_mixed_types_in_set(self):
        """Test that Variables and Arrows can coexist in sets."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        type_set = {var_x, var_y, arrow}
        assert len(type_set) == 3

    def test_arrow_order_matters(self):
        """Test that arrow direction matters for equality."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")

        arrow1 = implica.Arrow(var_x, var_y)  # x -> y
        arrow2 = implica.Arrow(var_y, var_x)  # y -> x

        assert arrow1 != arrow2
        assert str(arrow1) == "(x -> y)"
        assert str(arrow2) == "(y -> x)"


class TestVariableValidation:
    """Comprehensive tests for variable name validation based on validation.rs rules.

    Tests cover all validation rules from src/utils/validation.rs:
    - Length constraints (1-255 characters)
    - Whitespace detection (no whitespace allowed)
    - Valid characters (alphanumeric and underscore only)
    - Starting character (must be letter or underscore)
    - Reserved names (None, True, False)
    """

    # ===== Valid Names Tests =====

    def test_variable_single_character_names(self):
        """Test that single character names are valid."""
        assert implica.Variable("x").name == "x"
        assert implica.Variable("a").name == "a"
        assert implica.Variable("Z").name == "Z"
        assert implica.Variable("_").name == "_"

    def test_variable_basic_names(self):
        """Test that basic valid names are accepted."""
        assert implica.Variable("variable").name == "variable"
        assert implica.Variable("myVar").name == "myVar"
        assert implica.Variable("data").name == "data"
        assert implica.Variable("result").name == "result"

    def test_variable_underscore_prefix(self):
        """Test that names starting with underscore are valid."""
        assert implica.Variable("_private").name == "_private"
        assert implica.Variable("_var").name == "_var"
        assert implica.Variable("__double").name == "__double"
        assert implica.Variable("___triple").name == "___triple"

    def test_variable_with_numbers(self):
        """Test that names with numbers (not at start) are valid."""
        assert implica.Variable("var1").name == "var1"
        assert implica.Variable("var_123").name == "var_123"
        assert implica.Variable("test2var").name == "test2var"
        assert implica.Variable("value123").name == "value123"

    def test_variable_with_underscores(self):
        """Test that names with underscores in various positions are valid."""
        assert implica.Variable("my_variable").name == "my_variable"
        assert implica.Variable("var_name_test").name == "var_name_test"
        assert implica.Variable("_private_var").name == "_private_var"
        assert implica.Variable("test_").name == "test_"
        assert implica.Variable("a_b_c_d").name == "a_b_c_d"

    def test_variable_mixed_case(self):
        """Test that mixed case names are valid."""
        assert implica.Variable("MyVariable").name == "MyVariable"
        assert implica.Variable("camelCase").name == "camelCase"
        assert implica.Variable("PascalCase").name == "PascalCase"
        assert implica.Variable("UPPERCASE").name == "UPPERCASE"
        assert implica.Variable("lowercase").name == "lowercase"

    def test_variable_alphanumeric_combinations(self):
        """Test various valid alphanumeric combinations."""
        assert implica.Variable("var123abc").name == "var123abc"
        assert implica.Variable("test_1_2_3").name == "test_1_2_3"
        assert implica.Variable("ABC123def").name == "ABC123def"
        assert implica.Variable("_123abc").name == "_123abc"

    def test_variable_max_valid_length(self):
        """Test that names at max length (255 chars) are valid."""
        max_name = "x" * 255
        assert implica.Variable(max_name).name == max_name

        # Test with varied characters
        varied_name = "a" * 200 + "_" + "b" * 54
        assert len(varied_name) == 255
        assert implica.Variable(varied_name).name == varied_name

    def test_variable_near_reserved_but_valid(self):
        """Test that names similar to reserved words but different are valid."""
        assert implica.Variable("none").name == "none"  # lowercase
        assert implica.Variable("true").name == "true"  # lowercase
        assert implica.Variable("false").name == "false"  # lowercase
        assert implica.Variable("None_").name == "None_"
        assert implica.Variable("_True").name == "_True"
        assert implica.Variable("False1").name == "False1"
        assert implica.Variable("MyNone").name == "MyNone"

    # ===== Empty and Length Tests =====

    def test_variable_empty_name_raises_error(self):
        """Test that empty names are rejected."""
        with pytest.raises(Exception) as exc_info:
            implica.Variable("")
        # Verify error message mentions length constraint
        assert "character" in str(exc_info.value).lower() or "empty" in str(exc_info.value).lower()

    def test_variable_max_length_exceeded_by_one(self):
        """Test that names exceeding max length by 1 are rejected."""
        long_name = "x" * 256
        with pytest.raises(Exception) as exc_info:
            implica.Variable(long_name)
        assert "character" in str(exc_info.value).lower()

    def test_variable_max_length_exceeded_significantly(self):
        """Test that very long names are rejected."""
        very_long_name = "x" * 1000
        with pytest.raises(Exception):
            implica.Variable(very_long_name)

    def test_variable_max_length_boundary(self):
        """Test boundary around max length."""
        # 254 should work
        name_254 = "a" * 254
        assert implica.Variable(name_254).name == name_254

        # 255 should work (max)
        name_255 = "b" * 255
        assert implica.Variable(name_255).name == name_255

        # 256 should fail
        name_256 = "c" * 256
        with pytest.raises(Exception):
            implica.Variable(name_256)

    # ===== Whitespace Tests =====

    def test_variable_leading_whitespace_raises_error(self):
        """Test that leading whitespace is rejected."""
        with pytest.raises(Exception) as exc_info:
            implica.Variable(" var")
        assert "whitespace" in str(exc_info.value).lower()

        with pytest.raises(Exception):
            implica.Variable("  var")

        with pytest.raises(Exception):
            implica.Variable("\tvar")

    def test_variable_trailing_whitespace_raises_error(self):
        """Test that trailing whitespace is rejected."""
        with pytest.raises(Exception) as exc_info:
            implica.Variable("var ")
        assert "whitespace" in str(exc_info.value).lower()

        with pytest.raises(Exception):
            implica.Variable("var  ")

        with pytest.raises(Exception):
            implica.Variable("var\t")

    def test_variable_internal_whitespace_raises_error(self):
        """Test that internal whitespace is rejected."""
        with pytest.raises(Exception) as exc_info:
            implica.Variable("var name")
        assert "whitespace" in str(exc_info.value).lower()

        with pytest.raises(Exception):
            implica.Variable("my variable")

        with pytest.raises(Exception):
            implica.Variable("test\tvar")

    def test_variable_only_whitespace_raises_error(self):
        """Test that names containing only whitespace are rejected."""
        with pytest.raises(Exception):
            implica.Variable(" ")

        with pytest.raises(Exception):
            implica.Variable("   ")

        with pytest.raises(Exception):
            implica.Variable("\t")

        with pytest.raises(Exception):
            implica.Variable("\n")

    def test_variable_mixed_whitespace_raises_error(self):
        """Test that various whitespace characters are rejected."""
        with pytest.raises(Exception):
            implica.Variable("var\nname")

        with pytest.raises(Exception):
            implica.Variable("var\rname")

        with pytest.raises(Exception):
            implica.Variable("var\r\nname")

        with pytest.raises(Exception):
            implica.Variable("test \t var")

    # ===== Invalid Characters Tests =====

    def test_variable_hyphen_raises_error(self):
        """Test that hyphens are rejected."""
        with pytest.raises(Exception) as exc_info:
            implica.Variable("var-name")
        assert (
            "alphanumeric" in str(exc_info.value).lower()
            or "character" in str(exc_info.value).lower()
        )

        with pytest.raises(Exception):
            implica.Variable("my-variable")

        with pytest.raises(Exception):
            implica.Variable("-var")

    def test_variable_special_characters_raise_error(self):
        """Test that special characters are rejected."""
        special_chars = [
            "!",
            "@",
            "#",
            "$",
            "%",
            "^",
            "&",
            "*",
            "(",
            ")",
            "+",
            "=",
            "{",
            "}",
            "[",
            "]",
            "|",
            "\\",
            ":",
            ";",
            "'",
            '"',
            "<",
            ">",
            ",",
            ".",
            "?",
            "/",
            "~",
            "`",
        ]

        for char in special_chars:
            with pytest.raises(Exception):
                implica.Variable(f"var{char}name")

    def test_variable_dot_raises_error(self):
        """Test that dots are rejected."""
        with pytest.raises(Exception):
            implica.Variable("var.name")

        with pytest.raises(Exception):
            implica.Variable("my.variable")

    def test_variable_unicode_alphanumeric_characters_are_valid(self):
        """Test that Unicode alphanumeric characters are accepted.

        Note: Rust's is_alphanumeric() accepts Unicode alphanumeric characters,
        not just ASCII. This is the expected behavior.
        """
        # These are valid because they are alphanumeric in Unicode
        assert implica.Variable("varñame").name == "varñame"
        assert implica.Variable("café").name == "café"
        # Note: Some languages' characters might work depending on Unicode properties

    def test_variable_emoji_raises_error(self):
        """Test that emojis are rejected (not alphanumeric)."""
        with pytest.raises(Exception):
            implica.Variable("var😀name")

        with pytest.raises(Exception):
            implica.Variable("🚀variable")

    # ===== Starting Character Tests =====

    def test_variable_starting_with_number_raises_error(self):
        """Test that names starting with numbers are rejected."""
        with pytest.raises(Exception) as exc_info:
            implica.Variable("123var")
        assert "start" in str(exc_info.value).lower() or "letter" in str(exc_info.value).lower()

        with pytest.raises(Exception):
            implica.Variable("1variable")

        with pytest.raises(Exception):
            implica.Variable("0test")

        with pytest.raises(Exception):
            implica.Variable("9_var")

    def test_variable_only_numbers_raise_error(self):
        """Test that names containing only numbers are rejected."""
        with pytest.raises(Exception):
            implica.Variable("123")

        with pytest.raises(Exception):
            implica.Variable("456789")

        with pytest.raises(Exception):
            implica.Variable("0")

    def test_variable_starting_with_special_char_raises_error(self):
        """Test that names starting with special characters are rejected."""
        with pytest.raises(Exception):
            implica.Variable("-var")

        with pytest.raises(Exception):
            implica.Variable("$variable")

        with pytest.raises(Exception):
            implica.Variable("@test")

    # ===== Reserved Names Tests =====

    def test_variable_reserved_none_raises_error(self):
        """Test that 'None' is rejected as reserved."""
        with pytest.raises(Exception) as exc_info:
            implica.Variable("None")
        assert "reserved" in str(exc_info.value).lower()

    def test_variable_reserved_true_raises_error(self):
        """Test that 'True' is rejected as reserved."""
        with pytest.raises(Exception) as exc_info:
            implica.Variable("True")
        assert "reserved" in str(exc_info.value).lower()

    def test_variable_reserved_false_raises_error(self):
        """Test that 'False' is rejected as reserved."""
        with pytest.raises(Exception) as exc_info:
            implica.Variable("False")
        assert "reserved" in str(exc_info.value).lower()

    def test_variable_reserved_case_sensitive(self):
        """Test that reserved name checking is case-sensitive."""
        # These should be valid (different case)
        assert implica.Variable("none").name == "none"
        assert implica.Variable("true").name == "true"
        assert implica.Variable("false").name == "false"
        assert implica.Variable("NONE").name == "NONE"
        assert implica.Variable("TRUE").name == "TRUE"
        assert implica.Variable("FALSE").name == "FALSE"

        # These should fail (exact match)
        with pytest.raises(Exception):
            implica.Variable("None")
        with pytest.raises(Exception):
            implica.Variable("True")
        with pytest.raises(Exception):
            implica.Variable("False")

    # ===== Edge Cases and Combinations =====

    def test_variable_multiple_underscores_consecutive(self):
        """Test that multiple consecutive underscores are valid."""
        assert implica.Variable("var__name").name == "var__name"
        assert implica.Variable("test___var").name == "test___var"
        assert implica.Variable("____").name == "____"

    def test_variable_underscore_number_combinations(self):
        """Test various underscore and number combinations."""
        assert implica.Variable("_1").name == "_1"
        assert implica.Variable("_123").name == "_123"
        assert implica.Variable("__1__2__3").name == "__1__2__3"
        assert implica.Variable("_var_1_2_3").name == "_var_1_2_3"

    def test_variable_all_caps_with_underscores(self):
        """Test constant-style names (all caps with underscores)."""
        assert implica.Variable("MY_CONSTANT").name == "MY_CONSTANT"
        assert implica.Variable("MAX_VALUE").name == "MAX_VALUE"
        assert implica.Variable("DEFAULT_TIMEOUT").name == "DEFAULT_TIMEOUT"

    def test_variable_error_messages_are_informative(self):
        """Test that error messages contain helpful information."""
        # Empty name
        try:
            implica.Variable("")
        except Exception as e:
            error_msg = str(e).lower()
            assert any(word in error_msg for word in ["character", "length", "empty"])

        # Invalid character
        try:
            implica.Variable("var-name")
        except Exception as e:
            error_msg = str(e).lower()
            assert any(word in error_msg for word in ["character", "alphanumeric", "underscore"])

        # Starting with number
        try:
            implica.Variable("123var")
        except Exception as e:
            error_msg = str(e).lower()
            assert any(word in error_msg for word in ["start", "letter", "underscore"])

        # Reserved name
        try:
            implica.Variable("None")
        except Exception as e:
            error_msg = str(e).lower()
            assert "reserved" in error_msg

    def test_variable_validation_consistency(self):
        """Test that validation is consistent across multiple calls."""
        # Valid name should always work
        for _ in range(10):
            assert implica.Variable("valid_name").name == "valid_name"

        # Invalid name should always fail
        for _ in range(10):
            with pytest.raises(Exception):
                implica.Variable("invalid-name")

    def test_variable_boundary_valid_characters(self):
        """Test boundary cases for valid character sets."""
        # Test all lowercase letters
        assert implica.Variable("abcdefghijklmnopqrstuvwxyz").name == "abcdefghijklmnopqrstuvwxyz"

        # Test all uppercase letters
        assert implica.Variable("ABCDEFGHIJKLMNOPQRSTUVWXYZ").name == "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

        # Test all digits (after valid start)
        assert implica.Variable("a0123456789").name == "a0123456789"

        # Test multiple underscores
        assert implica.Variable("a_b_c_d_e_f").name == "a_b_c_d_e_f"


class TestArrowEdgeCases:
    """Tests for Arrow edge cases and complex scenarios."""

    def test_arrow_with_same_variable_both_sides(self):
        """Test creating an arrow with the same variable on both sides."""
        var_x = implica.Variable("x")
        arrow = implica.Arrow(var_x, var_x)

        assert str(arrow) == "(x -> x)"
        assert arrow.left == var_x
        assert arrow.right == var_x

    def test_arrow_with_identical_nested_structures(self):
        """Test arrows with identical nested structures."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")

        inner1 = implica.Arrow(var_x, var_y)
        inner2 = implica.Arrow(implica.Variable("x"), implica.Variable("y"))

        # Both should be equal since they have the same structure
        assert inner1 == inner2

    def test_arrow_complex_nesting(self):
        """Test complex arrow nesting scenarios."""
        var_a = implica.Variable("a")
        var_b = implica.Variable("b")
        var_c = implica.Variable("c")

        # (a -> (b -> c))
        inner = implica.Arrow(var_b, var_c)
        outer = implica.Arrow(var_a, inner)

        assert str(outer) == "(a -> (b -> c))"

    def test_arrow_associativity_left_vs_right(self):
        """Test that arrow associativity matters."""
        var_a = implica.Variable("a")
        var_b = implica.Variable("b")
        var_c = implica.Variable("c")

        # ((a -> b) -> c) vs (a -> (b -> c))
        left_assoc = implica.Arrow(implica.Arrow(var_a, var_b), var_c)
        right_assoc = implica.Arrow(
            var_a, implica.Arrow(implica.Variable("b"), implica.Variable("c"))
        )

        assert left_assoc != right_assoc
        assert str(left_assoc) == "((a -> b) -> c)"
        assert str(right_assoc) == "(a -> (b -> c))"

    def test_arrow_triple_nesting(self):
        """Test triple nested arrows."""
        var_a = implica.Variable("a")
        var_b = implica.Variable("b")
        var_c = implica.Variable("c")
        var_d = implica.Variable("d")

        # (((a -> b) -> c) -> d)
        inner1 = implica.Arrow(var_a, var_b)
        inner2 = implica.Arrow(inner1, var_c)
        outer = implica.Arrow(inner2, var_d)

        assert str(outer) == "(((a -> b) -> c) -> d)"

    def test_arrow_uid_length(self):
        """Test that arrow UIDs are properly formatted."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        uid = arrow.uid()
        # SHA256 hex should be 64 characters
        assert len(uid) == 64
        # Should only contain hex characters
        assert all(c in "0123456789abcdef" for c in uid)

    def test_variable_uid_length(self):
        """Test that variable UIDs are properly formatted."""
        var = implica.Variable("test")
        uid = var.uid()

        # SHA256 hex should be 64 characters
        assert len(uid) == 64
        # Should only contain hex characters
        assert all(c in "0123456789abcdef" for c in uid)


class TestTypeComparison:
    """Tests for type comparison edge cases."""

    def test_variable_comparison_with_none(self):
        """Test that variables don't equal None."""
        var = implica.Variable("x")
        assert var != None

    def test_arrow_comparison_with_none(self):
        """Test that arrows don't equal None."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)
        assert arrow != None

    def test_deeply_nested_arrow_equality(self):
        """Test equality of deeply nested arrows."""
        # Create same structure twice
        var_a1 = implica.Variable("a")
        var_b1 = implica.Variable("b")
        var_c1 = implica.Variable("c")
        inner1 = implica.Arrow(var_a1, var_b1)
        outer1 = implica.Arrow(inner1, var_c1)

        var_a2 = implica.Variable("a")
        var_b2 = implica.Variable("b")
        var_c2 = implica.Variable("c")
        inner2 = implica.Arrow(var_a2, var_b2)
        outer2 = implica.Arrow(inner2, var_c2)

        assert outer1 == outer2
        assert hash(outer1) == hash(outer2)

    def test_arrow_with_different_nesting_levels(self):
        """Test that arrows with different nesting are not equal."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        # (x -> y) -> z
        arrow1 = implica.Arrow(implica.Arrow(var_x, var_y), var_z)

        # x -> (y -> z)
        arrow2 = implica.Arrow(
            implica.Variable("x"), implica.Arrow(implica.Variable("y"), implica.Variable("z"))
        )

        assert arrow1 != arrow2
