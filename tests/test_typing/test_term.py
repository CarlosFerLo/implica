import pytest
import implica


class TestBasicTerm:
    """Tests for the BasicTerm class."""

    def test_basic_term_creation(self):
        """Test creating a BasicTerm with a valid name and type."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("f", var_x)
        assert term.name == "f"

    def test_basic_term_with_longer_name(self):
        """Test creating a BasicTerm with a multi-character name."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("myFunction", var_x)
        assert term.name == "myFunction"

    def test_basic_term_type_property(self):
        """Test that the type property returns the correct type."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("f", var_x)
        assert term.type() == var_x

    def test_basic_term_with_arrow_type(self):
        """Test creating a BasicTerm with an Arrow type."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)
        term = implica.BasicTerm("g", arrow)
        assert term.type() == arrow

    def test_basic_term_uid_is_string(self):
        """Test that uid() returns a string."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("f", var_x)
        uid = term.uid()
        assert isinstance(uid, str)
        assert len(uid) > 0

    def test_basic_term_uid_consistency(self):
        """Test that calling uid() multiple times returns the same value."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("f", var_x)
        uid1 = term.uid()
        uid2 = term.uid()
        assert uid1 == uid2

    def test_basic_term_uid_uniqueness(self):
        """Test that different terms have different UIDs."""
        var_x = implica.Variable("x")
        term1 = implica.BasicTerm("f", var_x)
        term2 = implica.BasicTerm("g", var_x)
        assert term1.uid() != term2.uid()

    def test_basic_term_same_name_same_and_type_uid(self):
        """Test that terms with the same name and type have the same UID."""
        var_x = implica.Variable("x")
        term1 = implica.BasicTerm("f", var_x)
        term2 = implica.BasicTerm("f", var_x)
        assert term1.uid() == term2.uid()

    def test_basic_term_same_name_dif_type_dif_uid(self):
        """Test that terms with the same name but different type gave different UID."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        term_1 = implica.BasicTerm("f", var_x)
        term_2 = implica.BasicTerm("f", var_y)
        assert term_1.uid() != term_2.uid()

    def test_basic_term_str(self):
        """Test the string representation of a BasicTerm."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("f", var_x)
        assert str(term) == "f"

    def test_basic_term_repr(self):
        """Test the repr representation of a BasicTerm."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("myTerm", var_x)
        assert repr(term) == 'BasicTerm("myTerm")'

    def test_basic_term_equality(self):
        """Test that terms with the same name and type are equal."""
        var_x = implica.Variable("x")
        term1 = implica.BasicTerm("f", var_x)
        term2 = implica.BasicTerm("f", var_x)
        assert term1 == term2

    def test_basic_term_inequality_different_names(self):
        """Test that terms with different names are not equal."""
        var_x = implica.Variable("x")
        term1 = implica.BasicTerm("f", var_x)
        term2 = implica.BasicTerm("g", var_x)
        assert term1 != term2

    def test_basic_term_inequality_different_types(self):
        """Test that terms with different types are not equal."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        term_1 = implica.BasicTerm("f", var_x)
        term_2 = implica.BasicTerm("f", var_y)
        assert term_1 != term_2

    def test_basic_term_hash_consistency(self):
        """Test that the same term produces the same hash."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("f", var_x)
        hash1 = hash(term)
        hash2 = hash(term)
        assert hash1 == hash2

    def test_basic_term_hash_equality(self):
        """Test that equal terms have the same hash."""
        var_x = implica.Variable("x")
        term1 = implica.BasicTerm("f", var_x)
        term2 = implica.BasicTerm("f", var_x)
        assert hash(term1) == hash(term2)

    def test_basic_term_in_set(self):
        """Test that terms can be used in sets."""
        var_x = implica.Variable("x")
        term1 = implica.BasicTerm("f", var_x)
        term2 = implica.BasicTerm("f", var_x)
        term3 = implica.BasicTerm("g", var_x)

        term_set = {term1, term2, term3}
        assert len(term_set) == 2  # term1 and term2 should be considered the same

    def test_basic_term_in_dict(self):
        """Test that terms can be used as dictionary keys."""
        var_x = implica.Variable("x")
        term1 = implica.BasicTerm("f", var_x)
        term2 = implica.BasicTerm("f", var_x)

        d = {term1: "value1"}
        d[term2] = "value2"

        assert len(d) == 1  # term1 and term2 should be the same key
        assert d[term1] == "value2"

    def test_basic_term_empty_name_raises_error(self):
        """Test that creating a BasicTerm with an empty name raises an error."""
        var_x = implica.Variable("x")
        with pytest.raises(Exception):
            implica.BasicTerm("", var_x)

    def test_basic_term_whitespace_name_raises_error(self):
        """Test that creating a BasicTerm with only whitespace raises an error."""
        var_x = implica.Variable("x")
        with pytest.raises(Exception):
            implica.BasicTerm("   ", var_x)

    def test_basic_term_special_characters(self):
        """Test terms with special characters in names."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("term_1", var_x)
        assert term.name == "term_1"

    def test_basic_term_name_immutability(self):
        """Test that the name property cannot be modified after creation."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("f", var_x)

        with pytest.raises(AttributeError):
            term.name = "g"

    def test_basic_term_name_remains_unchanged(self):
        """Test that the name property remains constant throughout the term's lifetime."""
        var_x = implica.Variable("x")
        term = implica.BasicTerm("original", var_x)
        original_name = term.name

        # Try multiple operations that might affect the term
        _ = term.uid()
        _ = str(term)
        _ = repr(term)
        _ = hash(term)
        _ = term.type()

        # Name should still be the same
        assert term.name == original_name
        assert term.name == "original"


class TestApplication:
    """Tests for the Application class."""

    def test_application_creation(self):
        """Test creating an Application"""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)

        app = implica.Application(func, arg)
        assert isinstance(app, implica.Application)

    def test_application_creation_via_call(self):
        """Test creating an Application using the call operator."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)

        app = func(arg)
        assert isinstance(app, implica.Application)

    def test_application_function_property(self):
        """Test the function property of an Application."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)

        app = func(arg)
        assert app.function == func

    def test_application_argument_property(self):
        """Test the argument property of an Application."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)

        app = func(arg)
        assert app.argument == arg

    def test_application_resulting_type(self):
        """Test that the application has the correct resulting type."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)

        app = func(arg)
        assert app.type() == var_y

    def test_application_type_mismatch_raises_error(self):
        """Test that applying a function with mismatched types raises an error."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_z)  # Wrong type

        with pytest.raises(TypeError):
            func(arg)

    def test_application_on_variable_type_raises_error(self):
        """Test that applying a term with Variable type raises an error."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")

        func = implica.BasicTerm("f", var_x)  # Not an arrow type
        arg = implica.BasicTerm("a", var_y)

        with pytest.raises(TypeError):
            func(arg)

    def test_application_uid_is_string(self):
        """Test that uid() returns a string."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = func(arg)

        uid = app.uid()
        assert isinstance(uid, str)
        assert len(uid) > 0

    def test_application_uid_consistency(self):
        """Test that calling uid() multiple times returns the same value."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = func(arg)

        uid1 = app.uid()
        uid2 = app.uid()
        assert uid1 == uid2

    def test_application_uid_uniqueness(self):
        """Test that different applications have different UIDs."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg1 = implica.BasicTerm("a", var_x)
        arg2 = implica.BasicTerm("b", var_x)

        app1 = func(arg1)
        app2 = func(arg2)

        assert app1.uid() != app2.uid()

    def test_application_same_terms_same_uid(self):
        """Test that applications with the same function and argument have the same UID."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func1 = implica.BasicTerm("f", arrow)
        arg1 = implica.BasicTerm("a", var_x)
        app1 = func1(arg1)

        func2 = implica.BasicTerm("f", arrow)
        arg2 = implica.BasicTerm("a", var_x)
        app2 = func2(arg2)

        assert app1.uid() == app2.uid()

    def test_application_str(self):
        """Test the string representation of an Application."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = func(arg)

        assert str(app) == "(f a)"

    def test_application_repr(self):
        """Test the repr representation of an Application."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = func(arg)

        assert repr(app) == "Application(f, a)"

    def test_application_equality(self):
        """Test that applications with the same function and argument are equal."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func1 = implica.BasicTerm("f", arrow)
        arg1 = implica.BasicTerm("a", var_x)
        app1 = func1(arg1)

        func2 = implica.BasicTerm("f", arrow)
        arg2 = implica.BasicTerm("a", var_x)
        app2 = func2(arg2)

        assert app1 == app2

    def test_application_inequality(self):
        """Test that applications with different arguments are not equal."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg1 = implica.BasicTerm("a", var_x)
        arg2 = implica.BasicTerm("b", var_x)

        app1 = func(arg1)
        app2 = func(arg2)

        assert app1 != app2

    def test_application_creation_method_is_indifferent(self):
        """Test creating an Application using the __init__ method or the call operator gives the same object."""

        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)

        app_1 = implica.Application(func, arg)
        app_2 = func(arg)

        assert app_1 == app_2

    def test_application_hash_consistency(self):
        """Test that the same application produces the same hash."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = func(arg)

        hash1 = hash(app)
        hash2 = hash(app)
        assert hash1 == hash2

    def test_application_hash_equality(self):
        """Test that equal applications have the same hash."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func1 = implica.BasicTerm("f", arrow)
        arg1 = implica.BasicTerm("a", var_x)
        app1 = func1(arg1)

        func2 = implica.BasicTerm("f", arrow)
        arg2 = implica.BasicTerm("a", var_x)
        app2 = func2(arg2)

        assert hash(app1) == hash(app2)

    def test_application_in_set(self):
        """Test that applications can be used in sets."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg1 = implica.BasicTerm("a", var_x)
        arg2 = implica.BasicTerm("b", var_x)

        app1 = func(arg1)
        app2 = func(arg1)  # Same as app1
        app3 = func(arg2)  # Different

        app_set = {app1, app2, app3}
        assert len(app_set) == 2  # app1 and app2 should be considered the same

    def test_application_in_dict(self):
        """Test that applications can be used as dictionary keys."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)

        app1 = func(arg)
        app2 = func(arg)

        d = {app1: "value1"}
        d[app2] = "value2"

        assert len(d) == 1  # app1 and app2 should be the same key
        assert d[app1] == "value2"

    def test_nested_application_left_associative(self):
        """Test nested applications (left associative)."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        # f : x -> (y -> z)
        arrow_yz = implica.Arrow(var_y, var_z)
        arrow_x_yz = implica.Arrow(var_x, arrow_yz)

        func = implica.BasicTerm("f", arrow_x_yz)
        arg1 = implica.BasicTerm("a", var_x)
        arg2 = implica.BasicTerm("b", var_y)

        # (f a) : y -> z
        app1 = func(arg1)
        assert app1.type() == arrow_yz

        # ((f a) b) : z
        app2 = app1(arg2)
        assert app2.type() == var_z

    def test_nested_application_str(self):
        """Test string representation of nested applications."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        arrow_yz = implica.Arrow(var_y, var_z)
        arrow_x_yz = implica.Arrow(var_x, arrow_yz)

        func = implica.BasicTerm("f", arrow_x_yz)
        arg1 = implica.BasicTerm("a", var_x)
        arg2 = implica.BasicTerm("b", var_y)

        app1 = func(arg1)
        app2 = app1(arg2)

        assert str(app2) == "((f a) b)"

    def test_application_as_function(self):
        """Test using an application as a function in another application."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        arrow_yz = implica.Arrow(var_y, var_z)
        arrow_x_yz = implica.Arrow(var_x, arrow_yz)

        func = implica.BasicTerm("f", arrow_x_yz)
        arg1 = implica.BasicTerm("a", var_x)
        arg2 = implica.BasicTerm("b", var_y)

        app1 = func(arg1)
        app2 = app1(arg2)

        assert isinstance(app2, implica.Application)
        assert isinstance(app2.function, implica.Application)
        assert app2.function == app1

    def test_deeply_nested_applications(self):
        """Test deeply nested applications."""
        var_a = implica.Variable("a")
        var_b = implica.Variable("b")
        var_c = implica.Variable("c")
        var_d = implica.Variable("d")

        # f : a -> (b -> (c -> d))
        arrow_cd = implica.Arrow(var_c, var_d)
        arrow_bcd = implica.Arrow(var_b, arrow_cd)
        arrow_abcd = implica.Arrow(var_a, arrow_bcd)

        func = implica.BasicTerm("f", arrow_abcd)
        arg_a = implica.BasicTerm("x", var_a)
        arg_b = implica.BasicTerm("y", var_b)
        arg_c = implica.BasicTerm("z", var_c)

        app1 = func(arg_a)  # f x : b -> (c -> d)
        app2 = app1(arg_b)  # (f x) y : c -> d
        app3 = app2(arg_c)  # ((f x) y) z : d

        assert app3.type() == var_d
        assert str(app3) == "(((f x) y) z)"

    def test_application_function_immutability(self):
        """Test that the function property cannot be modified after creation."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = func(arg)

        with pytest.raises(AttributeError):
            app.function = implica.BasicTerm("g", arrow)

    def test_application_argument_immutability(self):
        """Test that the argument property cannot be modified after creation."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = func(arg)

        with pytest.raises(AttributeError):
            app.argument = implica.BasicTerm("b", var_x)

    def test_application_properties_remain_unchanged(self):
        """Test that function and argument properties remain constant throughout the application's lifetime."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = func(arg)

        original_function = app.function
        original_argument = app.argument

        # Try multiple operations that might affect the application
        _ = app.uid()
        _ = str(app)
        _ = repr(app)
        _ = hash(app)
        _ = app.type()

        # Properties should still be the same
        assert app.function == original_function
        assert app.argument == original_argument
        assert app.function == func
        assert app.argument == arg


class TestTermInteractions:
    """Tests for interactions between BasicTerm and Application."""

    def test_basic_term_not_equal_to_application(self):
        """Test that a BasicTerm is not equal to an Application."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = func(arg)

        basic_term = implica.BasicTerm("f", var_y)

        assert basic_term != app
        assert app != basic_term

    def test_mixed_terms_in_set(self):
        """Test that BasicTerms and Applications can coexist in sets."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        basic = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)
        app = basic(arg)

        term_set = {basic, app}
        assert len(term_set) == 2

    def test_application_order_matters(self):
        """Test that the order of applications matters."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        var_z = implica.Variable("z")

        arrow_xy = implica.Arrow(var_x, var_y)
        arrow_yz = implica.Arrow(var_y, var_z)
        arrow_xz = implica.Arrow(var_x, var_z)

        # g : x -> y, h : y -> z
        g = implica.BasicTerm("g", arrow_xy)
        h = implica.BasicTerm("h", arrow_yz)
        a = implica.BasicTerm("a", var_x)

        # h(g(a)) should work
        app1 = g(a)  # g a : y
        app2 = h(app1)  # h (g a) : z

        assert app2.type() == var_z
        assert str(app2) == "(h (g a))"

    def test_currying_example(self):
        """Test a currying example with multiple applications."""
        var_x = implica.Variable("x")

        # plus : x -> (x -> x)
        arrow_xx = implica.Arrow(var_x, var_x)
        arrow_x_xx = implica.Arrow(var_x, arrow_xx)

        plus = implica.BasicTerm("plus", arrow_x_xx)
        two = implica.BasicTerm("a", var_x)
        three = implica.BasicTerm("b", var_x)

        # plus 2 : x -> x
        plus_two = plus(two)
        assert plus_two.type() == arrow_xx

        # (plus 2) 3 : x
        result = plus_two(three)
        assert result.type() == var_x
        assert str(result) == "((plus a) b)"

    def test_term_type_preservation(self):
        """Test that term types are correctly preserved through operations."""
        var_x = implica.Variable("x")
        var_y = implica.Variable("y")
        arrow = implica.Arrow(var_x, var_y)

        func = implica.BasicTerm("f", arrow)
        arg = implica.BasicTerm("a", var_x)

        # Check types before application
        assert func.type() == arrow
        assert arg.type() == var_x

        # Check type after application
        app = func(arg)
        assert app.type() == var_y

        # Original terms should still have their types
        assert func.type() == arrow
        assert arg.type() == var_x

    def test_complex_type_application_chain(self):
        """Test a complex chain of applications with various types."""
        # Create types: a, b, c
        var_a = implica.Variable("a")
        var_b = implica.Variable("b")
        var_c = implica.Variable("c")

        # f : a -> b
        arrow_ab = implica.Arrow(var_a, var_b)
        f = implica.BasicTerm("f", arrow_ab)

        # g : b -> c
        arrow_bc = implica.Arrow(var_b, var_c)
        g = implica.BasicTerm("g", arrow_bc)

        # x : a
        x = implica.BasicTerm("x", var_a)

        # Apply f to x: f x : b
        fx = f(x)
        assert fx.type() == var_b

        # Apply g to (f x): g (f x) : c
        gfx = g(fx)
        assert gfx.type() == var_c

        assert str(gfx) == "(g (f x))"
