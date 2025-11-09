import pytest
import implica


# ==================== Variable Tests ====================


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
        ("X", "X"),
        ("LongTypeName", "LongTypeName"),
        ("Type123", "Type123"),
        ("_underscore", "_underscore"),
    ],
)
def test_variable_with_different_names(name, expected_str):
    """Test Variable with various names"""
    var = implica.Variable(name)
    assert var.name == name
    assert str(var) == expected_str


def test_variable_repr_format():
    """Test Variable repr format"""
    var = implica.Variable("TestVar")
    assert repr(var) == 'Variable("TestVar")'


def test_variable_uid_consistency():
    """Test that Variable UID is consistent across instances with same name"""
    var1 = implica.Variable("A")
    var2 = implica.Variable("A")
    assert var1.uid() == var2.uid()

    var3 = implica.Variable("B")
    assert var1.uid() != var3.uid()


def test_variable_uid_caching():
    """Test that Variable UID is cached and returns same value"""
    var = implica.Variable("A")
    uid1 = var.uid()
    uid2 = var.uid()
    assert uid1 == uid2
    # UIDs should be identical strings
    assert len(uid1) == 64
    assert uid1 == uid2


def test_variable_hash():
    """Test Variable hashing for use in sets and dicts"""
    var1 = implica.Variable("A")
    var2 = implica.Variable("A")
    var3 = implica.Variable("B")

    # Same name should have same hash
    assert hash(var1) == hash(var2)

    # Can be used in sets
    var_set = {var1, var2, var3}
    assert len(var_set) == 2  # var1 and var2 are duplicates

    # Can be used as dict keys
    var_dict = {var1: "value1", var3: "value2"}
    assert var_dict[var2] == "value1"  # var2 should access var1's value


def test_variable_equality():
    """Test Variable equality comparison"""
    var1 = implica.Variable("A")
    var2 = implica.Variable("A")
    var3 = implica.Variable("B")

    # Reflexive: x == x
    assert var1 == var1

    # Symmetric: if x == y, then y == x
    assert var1 == var2
    assert var2 == var1

    # Transitive: if x == y and y == z, then x == z
    var4 = implica.Variable("A")
    assert var1 == var2 and var2 == var4
    assert var1 == var4

    # Inequality
    assert var1 != var3
    assert not (var1 == var3)


def test_variable_inequality_with_different_types():
    """Test Variable inequality with non-Variable types"""
    var = implica.Variable("A")

    assert var != "A"
    assert var != 1
    assert var != None
    assert var != []
    assert var != {}


def test_variable_with_empty_name_raises_error():
    """Test Variable with empty string name raises error"""
    with pytest.raises(ValueError):
        implica.Variable("")

    with pytest.raises(ValueError):
        implica.Variable("  ")


def test_variable_with_special_characters():
    """Test Variable with special characters in name"""
    special_names = [
        "Type-With-Dash",
        "Type.With.Dot",
        "Type_With_Underscore",
        "Type123",
    ]
    for name in special_names:
        var = implica.Variable(name)
        assert var.name == name
        assert str(var) == name


@pytest.mark.skip("Skipping immutability tests due to problems on enforcing it.")
def test_type_variable_immutability(var_a):
    """Test Type Variable Immutability"""
    with pytest.raises(Exception):
        var_a.name = "B"

    assert var_a.name == "A"


# ==================== Application Tests ====================


def test_application_creation(app_ab):
    """Test Application creation"""
    assert str(app_ab) == "(A -> B)"
    # UID is now a SHA256 hash (64 hex characters)
    assert len(app_ab.uid()) == 64
    assert all(c in "0123456789abcdef" for c in app_ab.uid())


def test_application_getters(app_ab):
    """Test Application left and right getters"""
    left = app_ab.left
    right = app_ab.right
    assert isinstance(left, implica.Variable)
    assert isinstance(right, implica.Variable)
    assert left.name == "A"
    assert right.name == "B"


def test_application_with_variable_types():
    """Test Application with Variable types"""
    var_x = implica.Variable("X")
    var_y = implica.Variable("Y")
    app = implica.Application(var_x, var_y)

    assert app.left == var_x
    assert app.right == var_y
    assert str(app) == "(X -> Y)"


def test_application_with_nested_applications(var_a, var_b, var_c):
    """Test Application with nested Application types"""
    # (A -> B) -> C
    inner_app = implica.Application(var_a, var_b)
    outer_app = implica.Application(inner_app, var_c)

    assert str(outer_app) == "((A -> B) -> C)"
    assert outer_app.left == inner_app
    assert outer_app.right == var_c


def test_application_deeply_nested():
    """Test deeply nested Applications"""
    var_a = implica.Variable("A")
    var_b = implica.Variable("B")
    var_c = implica.Variable("C")
    var_d = implica.Variable("D")

    # ((A -> B) -> C) -> D
    app1 = implica.Application(var_a, var_b)
    app2 = implica.Application(app1, var_c)
    app3 = implica.Application(app2, var_d)

    assert str(app3) == "(((A -> B) -> C) -> D)"
    assert isinstance(app3.left, implica.Application)
    assert isinstance(app3.left.left, implica.Application)
    assert isinstance(app3.left.left.left, implica.Variable)


def test_application_right_nested(var_a, var_b, var_c):
    """Test Application with right-nested types"""

    # A -> (B -> C)
    inner_app = implica.Application(var_b, var_c)
    outer_app = implica.Application(var_a, inner_app)

    assert str(outer_app) == "(A -> (B -> C))"
    assert outer_app.left == var_a
    assert outer_app.right == inner_app


def test_application_repr(app_ab):
    """Test Application repr format"""

    repr_str = repr(app_ab)
    assert "Application" in repr_str


def test_application_uid_consistency():
    """Test that Application UID is consistent across instances with same structure"""
    var_a1 = implica.Variable("A")
    var_b1 = implica.Variable("B")
    app1 = implica.Application(var_a1, var_b1)

    var_a2 = implica.Variable("A")
    var_b2 = implica.Variable("B")
    app2 = implica.Application(var_a2, var_b2)

    assert app1.uid() == app2.uid()

    # Different structure should have different UID
    var_c = implica.Variable("C")
    app3 = implica.Application(var_a1, var_c)
    assert app1.uid() != app3.uid()


def test_application_uid_caching(app_ab):
    """Test that Application UID is cached"""

    uid1 = app_ab.uid()
    uid2 = app_ab.uid()
    assert uid1 == uid2
    # UIDs should be identical strings
    assert len(uid1) == 64
    assert uid1 == uid2


def test_application_hash(var_a, var_b, var_c):
    """Test Application hashing for use in sets and dicts"""

    app1 = implica.Application(var_a, var_b)
    app2 = implica.Application(var_a, var_b)
    app3 = implica.Application(var_a, var_c)

    # Same structure should have same hash
    assert hash(app1) == hash(app2)

    # Can be used in sets
    app_set = {app1, app2, app3}
    assert len(app_set) == 2

    # Can be used as dict keys
    app_dict = {app1: "value1", app3: "value2"}
    assert app_dict[app2] == "value1"


def test_application_equality(var_a, var_b, var_c):
    """Test Application equality comparison"""

    app1 = implica.Application(var_a, var_b)
    app2 = implica.Application(var_a, var_b)
    app3 = implica.Application(var_a, var_c)

    # Reflexive
    assert app1 == app1

    # Symmetric
    assert app1 == app2
    assert app2 == app1

    # Transitive
    app4 = implica.Application(implica.Variable("A"), implica.Variable("B"))
    assert app1 == app2 and app2 == app4
    assert app1 == app4

    # Inequality
    assert app1 != app3
    assert not (app1 == app3)


def test_application_inequality_with_different_types(var_a, var_b, app_ab):
    """Test Application inequality with non-Application types"""

    assert app_ab != var_a
    assert app_ab != "(A -> B)"
    assert app_ab != None
    assert app_ab != []


def test_application_with_same_left_and_right(var_a):
    """Test Application with same Variable on both sides"""
    app = implica.Application(var_a, var_a)

    assert str(app) == "(A -> A)"
    assert app.left == app.right
    assert app.left == var_a


def test_application_equality_with_nested_structures():
    """Test equality of Applications with nested structures"""
    var_a = implica.Variable("A")
    var_b = implica.Variable("B")
    var_c = implica.Variable("C")

    # Build same structure in two ways
    app1 = implica.Application(implica.Application(var_a, var_b), var_c)
    app2 = implica.Application(implica.Application(var_a, var_b), var_c)

    assert app1 == app2


def test_application_immutability(app_ab):
    """Test Type Application Immutability"""
    with pytest.raises(Exception):
        app_ab.left = implica.Variable("C")
    with pytest.raises(Exception):
        app_ab.right = implica.Variable("D")

    assert app_ab.left == implica.Variable("A")
    assert app_ab.right == implica.Variable("B")


# ==================== Mixed Type Tests ====================


def test_type_equality_mixed(var_a, var_b, app_ab, app_ac):
    """Test Type Equality between Variables and Applications"""
    assert var_a == var_a
    assert var_a == implica.Variable("A")
    assert var_a != var_b

    assert app_ab == app_ab
    assert app_ab == implica.Application(implica.Variable("A"), implica.Variable("B"))
    assert app_ab != app_ac

    # Variable should not equal Application even if names match
    assert var_a != app_ab


def test_different_type_combinations():
    """Test various combinations of Variables and Applications"""
    var_a = implica.Variable("A")
    var_b = implica.Variable("B")
    var_c = implica.Variable("C")

    app_ab = implica.Application(var_a, var_b)
    app_bc = implica.Application(var_b, var_c)
    app_nested = implica.Application(app_ab, var_c)

    # All should be unique
    types = [var_a, var_b, var_c, app_ab, app_bc, app_nested]
    for i, t1 in enumerate(types):
        for j, t2 in enumerate(types):
            if i == j:
                assert t1 == t2
            else:
                assert t1 != t2, f"Expected {t1} != {t2} but they were equal"


def test_type_uid_uniqueness(var_a, var_b, app_ab, app_ba):
    """Test that different types have unique UIDs"""

    uids = {var_a.uid(), var_b.uid(), app_ab.uid(), app_ba.uid()}
    assert len(uids) == 4  # All unique


def test_type_str_representation(var_a, var_b, var_c):
    """Test string representation of various type structures"""

    assert str(var_a) == "A"

    app1 = implica.Application(var_a, var_b)
    assert str(app1) == "(A -> B)"

    app2 = implica.Application(app1, var_c)
    assert str(app2) == "((A -> B) -> C)"

    app3 = implica.Application(var_a, app1)
    assert str(app3) == "(A -> (A -> B))"


def test_complex_type_structure():
    """Test building and verifying complex type structures"""
    # Build: ((A -> B) -> C) -> (D -> E)
    var_a = implica.Variable("A")
    var_b = implica.Variable("B")
    var_c = implica.Variable("C")
    var_d = implica.Variable("D")
    var_e = implica.Variable("E")

    left_part = implica.Application(implica.Application(var_a, var_b), var_c)
    right_part = implica.Application(var_d, var_e)
    complex_type = implica.Application(left_part, right_part)

    assert str(complex_type) == "(((A -> B) -> C) -> (D -> E))"
    assert isinstance(complex_type.left, implica.Application)
    assert isinstance(complex_type.right, implica.Application)


def test_type_comparison_comprehensive(var_b):
    """Comprehensive test of type comparisons"""
    var_a1 = implica.Variable("A")
    var_a2 = implica.Variable("A")

    app1 = implica.Application(var_a1, var_b)
    app2 = implica.Application(var_a2, var_b)
    app3 = implica.Application(var_b, var_a1)

    # Equal Variables
    assert var_a1 == var_a2

    # Equal Applications
    assert app1 == app2

    # Different order matters
    assert app1 != app3

    # Cross-type comparisons
    assert var_a1 != app1
    assert var_b != app1


# ==================== BaseType Interface Tests ====================


def test_base_type_interface_variable(var_a):
    """Test that Variable implements BaseType interface"""

    # Should have uid method
    assert hasattr(var_a, "uid")
    assert callable(var_a.uid)

    # Should have __str__ method
    assert hasattr(var_a, "__str__")

    # Should have __repr__ method
    assert hasattr(var_a, "__repr__")


def test_base_type_interface_application(app_ab):
    """Test that Application implements BaseType interface"""

    # Should have uid method
    assert hasattr(app_ab, "uid")
    assert callable(app_ab.uid)

    # Should have __str__ method
    assert hasattr(app_ab, "__str__")

    # Should have __repr__ method
    assert hasattr(app_ab, "__repr__")


def test_type_in_collections(var_a, var_b, app_ab):
    """Test using types in various collection types"""

    # In list
    type_list = [var_a, var_b, app_ab]
    assert len(type_list) == 3
    assert var_a in type_list

    # In tuple
    type_tuple = (var_a, var_b, app_ab)
    assert len(type_tuple) == 3

    # In set
    type_set = {var_a, implica.Variable("A"), var_b}
    assert len(type_set) == 2  # Duplicate A

    # In dict as keys
    type_dict = {var_a: 1, var_b: 2, app_ab: 3}
    assert type_dict[implica.Variable("A")] == 1


# ==================== Edge Cases and Error Handling ====================


def test_application_with_deeply_nested_same_structure(var_a, var_b):
    """Test equality with deeply nested identical structures"""

    # Build: (((A -> A) -> B) -> B)
    app1 = implica.Application(var_a, var_a)
    app2 = implica.Application(app1, var_b)
    app3 = implica.Application(app2, var_b)

    # Build same structure again
    app1_copy = implica.Application(var_a, var_a)
    app2_copy = implica.Application(app1_copy, var_b)
    app3_copy = implica.Application(app2_copy, var_b)

    assert app3 == app3_copy
    assert app3.uid() == app3_copy.uid()


def test_variable_name_with_unicode():
    """Test Variable with unicode characters"""
    var = implica.Variable("Τύπος")  # Greek letters
    assert var.name == "Τύπος"
    assert str(var) == "Τύπος"


def test_application_symmetry(app_ab, app_ba):
    """Test that Application(A, B) != Application(B, A)"""

    assert app_ab != app_ba
    assert str(app_ab) == "(A -> B)"
    assert str(app_ba) == "(B -> A)"
    assert app_ab.uid() != app_ba.uid()


def test_large_type_structure():
    """Test creating and comparing large type structures"""
    vars = [implica.Variable(f"T{i}") for i in range(10)]

    # Build a chain: T0 -> T1 -> T2 -> ... -> T9
    current = vars[0]
    for var in vars[1:]:
        current = implica.Application(current, var)

    # Verify structure
    assert isinstance(current, implica.Application)
    temp = current
    for i in range(8, -1, -1):
        assert isinstance(temp, implica.Application)
        assert temp.right == vars[i + 1]
        temp = temp.left
    assert temp == vars[0]
