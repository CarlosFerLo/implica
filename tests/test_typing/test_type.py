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
