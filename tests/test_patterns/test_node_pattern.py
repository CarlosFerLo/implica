import pytest
import implica


class TestNodePatternInit:
    def test_node_pattern_creation_with_variable(self):
        node_pattern = implica.NodePattern(variable="x")
        assert node_pattern.variable == "x"

    def test_node_pattern_creation_with_type(self, type_a):
        node_pattern = implica.NodePattern(type=type_a)
        assert node_pattern.type == type_a

    def test_node_pattern_creation_with_type_schema(self):
        type_schema = implica.TypeSchema("A")
        node_pattern = implica.NodePattern(type_schema=type_schema)
        assert node_pattern.type_schema == type_schema

    def test_node_pattern_creation_with_term(self, term_a):
        node_pattern = implica.NodePattern(term=term_a)
        assert node_pattern.term == term_a

    def test_node_pattern_creation_with_term_schema(self):
        term_schema = implica.TermSchema("a")
        node_pattern = implica.NodePattern(term_schema=term_schema)
        assert node_pattern.term_schema == term_schema

    def test_node_pattern_creation_with_properties(self):
        properties = {"key": "value"}
        node_pattern = implica.NodePattern(properties=properties)
        assert node_pattern.properties == properties

    def test_node_pattern_creation_with_specific_parameters(self, type_a, term_a):

        properties = {"key": "value"}

        node_pattern = implica.NodePattern(
            variable="x", type=type_a, term=term_a, properties=properties
        )

        assert node_pattern.variable == "x"
        assert node_pattern.type == type_a
        assert node_pattern.type_schema is None
        assert node_pattern.term == term_a
        assert node_pattern.term_schema is None
        assert node_pattern.properties == properties

    def test_node_pattern_creation_with_schemas(self):
        type_schema = implica.TypeSchema("A")
        term_schema = implica.TermSchema("a")

        node_pattern = implica.NodePattern(
            variable="y", type_schema=type_schema, term_schema=term_schema
        )

        assert node_pattern.variable == "y"
        assert node_pattern.type is None
        assert node_pattern.type_schema == type_schema
        assert node_pattern.term is None
        assert node_pattern.term_schema == term_schema
        assert node_pattern.properties == {}

    def test_node_pattern_creation_no_arguments(self):
        node_pattern = implica.NodePattern()

        assert node_pattern.variable is None
        assert node_pattern.type is None
        assert node_pattern.type_schema is None
        assert node_pattern.term is None
        assert node_pattern.term_schema is None
        assert node_pattern.properties == {}

    def test_node_pattern_creation_fails_invalid_variable(self):
        with pytest.raises(ValueError):
            implica.NodePattern(variable="123invalid")

    def test_node_pattern_creation_fails_if_type_and_type_schema_provided(self, type_a):
        type_schema = implica.TypeSchema("A")
        with pytest.raises(ValueError):
            implica.NodePattern(type=type_a, type_schema=type_schema)

    def test_node_pattern_creation_fails_if_term_and_term_schema_provided(self, term_a):
        term_schema = implica.TermSchema("a")
        with pytest.raises(ValueError):
            implica.NodePattern(term=term_a, term_schema=term_schema)

    @pytest.mark.xfail(reason="Type checking not enforced at this level")
    def test_node_pattern_creation_fails_if_term_type_mismatch(self, type_a, term_b):
        with pytest.raises(TypeError):
            implica.NodePattern(type=type_a, term=term_b)

    def test_node_pattern_creation_fails_if_invalid_properties(self, type_a):
        with pytest.raises(TypeError):
            implica.NodePattern(type=type_a, properties="not_a_dict")


class TestNodePatternSimpleMatches:
    def test_node_pattern_matches_node_with_exact_type(self, node_a, type_a):
        node_pattern = implica.NodePattern(type=type_a)
        assert node_pattern.matches(node_a)

    def test_node_pattern_does_not_match_node_with_different_type(self, node_a, type_b):
        node_pattern = implica.NodePattern(type=type_b)
        assert not node_pattern.matches(node_a)

    def test_node_pattern_matches_node_with_exact_term(self, node_a_with_term, term_a):
        node_pattern = implica.NodePattern(term=term_a)
        assert node_pattern.matches(node_a_with_term)

    def test_node_pattern_does_not_match_node_with_different_term(self, node_a_with_term, term_b):
        node_pattern = implica.NodePattern(term=term_b)
        assert not node_pattern.matches(node_a_with_term)

    def test_node_pattern_does_not_match_node_with_different_term_of_same_type(
        self, node_a_with_term, type_a
    ):
        term_different = implica.BasicTerm("different", type_a)
        node_pattern = implica.NodePattern(term=term_different)
        assert not node_pattern.matches(node_a_with_term)

    def test_node_pattern_matches_node_with_type_schema(self, node_a, type_a):
        type_schema = implica.TypeSchema("A")
        node_pattern = implica.NodePattern(type_schema=type_schema)
        assert node_pattern.matches(node_a)

    def test_node_pattern_does_not_match_node_with_non_matching_type_schema(self, node_a, type_b):
        type_schema = implica.TypeSchema("B")
        node_pattern = implica.NodePattern(type_schema=type_schema)
        assert not node_pattern.matches(node_a)

    def test_node_pattern_matches_node_with_term_schema(self, node_a_with_term, term_a):
        term_schema = implica.TermSchema("x")
        node_pattern = implica.NodePattern(term_schema=term_schema)
        assert node_pattern.matches(node_a_with_term)

    def test_node_pattern_does_not_match_node_without_term_if_term_schema(self, node_a):
        term_schema = implica.TermSchema("y")
        node_pattern = implica.NodePattern(term_schema=term_schema)
        assert not node_pattern.matches(node_a)

    def test_node_pattern_does_not_match_node_with_non_matching_term_schema(self, node_a_with_term):
        term_schema = implica.TermSchema("a b")
        node_pattern = implica.NodePattern(term_schema=term_schema)
        assert not node_pattern.matches(node_a_with_term)

    def test_node_pattern_matches_node_with_properties(self, type_a):
        properties = {"color": "red", "weight": 10}
        node = implica.Node(type_a, properties=properties)

        node_pattern = implica.NodePattern(properties={"color": "red"})
        assert node_pattern.matches(node)

    def test_node_pattern_does_not_match_node_with_non_matching_properties(self, type_a):
        properties = {"color": "red", "weight": 10}
        node = implica.Node(type_a, properties=properties)

        node_pattern = implica.NodePattern(properties={"color": "blue"})
        assert not node_pattern.matches(node)

    def test_node_pattern_matches_node_with_extra_properties(self, type_a):
        properties = {"color": "red", "weight": 10, "size": "large"}
        node = implica.Node(type_a, properties=properties)

        node_pattern = implica.NodePattern(properties={"color": "red", "weight": 10})
        assert node_pattern.matches(node)

    def test_node_pattern_does_not_match_node_missing_properties(self, type_a):
        properties = {"color": "red"}
        node = implica.Node(type_a, properties=properties)

        node_pattern = implica.NodePattern(properties={"color": "red", "weight": 10})
        assert not node_pattern.matches(node)

    def test_node_pattern_matches_node_with_all_criteria(self, node_a_with_term, type_a, term_a):
        properties = {"key": "value"}
        node = implica.Node(type_a, term_a, properties=properties)

        node_pattern = implica.NodePattern(type=type_a, term=term_a, properties={"key": "value"})
        assert node_pattern.matches(node)

    def test_node_pattern_does_not_match_node_if_one_criterion_fails(self, type_a, term_a):
        properties = {"key": "value"}
        node = implica.Node(type_a, term_a, properties=properties)

        node_pattern = implica.NodePattern(
            type=type_a, term=term_a, properties={"key": "different_value"}
        )
        assert not node_pattern.matches(node)


class TestNodePatternMatchesWithContext:
    """Test schemas capturing content to context"""

    def test_node_pattern_type_schema_captures_variable_in_context(self, node_a, type_a):
        type_schema = implica.TypeSchema("(X:*)")
        node_pattern = implica.NodePattern(type_schema=type_schema)

        context = {}
        assert node_pattern.matches(node_a, context)
        assert "X" in context
        assert context["X"] == type_a

    def test_node_pattern_term_schema_captures_variable_in_context(self, node_a_with_term, term_a):
        term_schema = implica.TermSchema("Y")
        node_pattern = implica.NodePattern(term_schema=term_schema)

        context = {}
        assert node_pattern.matches(node_a_with_term, context)
        assert "Y" in context
        assert context["Y"] == term_a

    def test_node_pattern_type_schema_uses_existing_variable_in_context(self, node_a, type_a):
        type_schema = implica.TypeSchema("X")
        node_pattern = implica.NodePattern(type_schema=type_schema)

        context = {"X": type_a}
        assert node_pattern.matches(node_a, context)

    def test_node_pattern_type_schema_fails_if_variable_in_context_mismatch(self, node_a, type_b):
        type_schema = implica.TypeSchema("X")
        node_pattern = implica.NodePattern(type_schema=type_schema)

        context = {"X": type_b}
        assert not node_pattern.matches(node_a, context)

    def test_node_pattern_term_schema_uses_existing_variable_in_context(
        self, node_a_with_term, term_a
    ):
        term_schema = implica.TermSchema("Y")
        node_pattern = implica.NodePattern(term_schema=term_schema)

        context = {"Y": term_a}
        assert node_pattern.matches(node_a_with_term, context)

    def test_node_pattern_term_schema_fails_if_variable_in_context_mismatch(
        self, node_a_with_term, term_b
    ):
        term_schema = implica.TermSchema("Y")
        node_pattern = implica.NodePattern(term_schema=term_schema)

        context = {"Y": term_b}
        assert not node_pattern.matches(node_a_with_term, context)

    def test_node_pattern_with_both_schemas_captures_variables_in_context(
        self, node_a_with_term, type_a, term_a
    ):
        type_schema = implica.TypeSchema("(X:*)")
        term_schema = implica.TermSchema("Y")
        node_pattern = implica.NodePattern(type_schema=type_schema, term_schema=term_schema)

        context = {}
        assert node_pattern.matches(node_a_with_term, context)
        assert "X" in context
        assert context["X"] == type_a
        assert "Y" in context
        assert context["Y"] == term_a

    def test_node_pattern_with_both_schemas_uses_existing_variables_in_context(
        self, node_a_with_term, type_a, term_a
    ):
        type_schema = implica.TypeSchema("X")
        term_schema = implica.TermSchema("Y")
        node_pattern = implica.NodePattern(type_schema=type_schema, term_schema=term_schema)

        context = {"X": type_a, "Y": term_a}
        assert node_pattern.matches(node_a_with_term, context)

    def test_node_pattern_with_both_schemas_fails_if_one_variable_mismatch_in_context(
        self, node_a_with_term, type_a, type_b, term_a
    ):
        type_schema = implica.TypeSchema("X")
        term_schema = implica.TermSchema("Y")
        node_pattern = implica.NodePattern(type_schema=type_schema, term_schema=term_schema)

        context = {"X": type_b, "Y": term_a}
        assert not node_pattern.matches(node_a_with_term, context)

        context = {"X": type_a, "Y": implica.BasicTerm("different", type_a)}
        assert not node_pattern.matches(node_a_with_term, context)

    def test_node_pattern_matches_fails_if_captured_variable_used_later_mismatch(
        self, node_a_with_term
    ):
        type_schema = implica.TypeSchema("(X:*)")
        term_schema = implica.TermSchema("X")
        node_pattern = implica.NodePattern(type_schema=type_schema, term_schema=term_schema)

        context = {}
        with pytest.raises(ValueError):
            node_pattern.matches(node_a_with_term, context)

    def test_node_pattern_matches_succeeds_if_captured_variable_used_later_match(
        self, type_a, arrow_aa
    ):
        type_schema = implica.TypeSchema("(X:*) -> X")
        node_pattern = implica.NodePattern(type_schema=type_schema)
        node = implica.Node(arrow_aa)
        context = {}
        assert node_pattern.matches(node, context)
        assert "X" in context
        assert context["X"] == type_a

    def test_node_pattern_with_properties_and_schemas_captures_variable_in_context(
        self, type_a, term_a
    ):
        type_schema = implica.TypeSchema("(X:*)")
        term_schema = implica.TermSchema("Y")
        properties = {"color": "red"}
        node_pattern = implica.NodePattern(
            type_schema=type_schema, term_schema=term_schema, properties=properties
        )
        node = implica.Node(type_a, term_a, properties=properties)

        context = {}
        assert node_pattern.matches(node, context)
        assert "X" in context
        assert context["X"] == type_a
        assert "Y" in context
        assert context["Y"] == term_a

    def test_node_pattern_with_properties_and_schemas_fails_if_property_mismatch(
        self, node_a_with_term
    ):
        type_schema = implica.TypeSchema("(X:*)")
        term_schema = implica.TermSchema("Y")
        properties = {"color": "blue"}
        node_pattern = implica.NodePattern(
            type_schema=type_schema, term_schema=term_schema, properties=properties
        )

        context = {}
        assert not node_pattern.matches(node_a_with_term, context)


class TestNodePatternStringRepresentation:
    def test_node_pattern_str_with_just_variable(self, type_a, term_a):

        node_pattern = implica.NodePattern(variable="x")

        expected_str = "NodePattern(variable='x')"

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_just_variable(self, type_a, term_a):

        node_pattern = implica.NodePattern(variable="x")

        expected_repr = "NodePattern(variable='x')"

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_type(self, type_a):

        node_pattern = implica.NodePattern(type=type_a)

        expected_str = f"NodePattern(type='{type_a}')"

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_type(self, type_a):
        node_pattern = implica.NodePattern(type=type_a)

        expected_repr = f"NodePattern(type='{type_a}')"

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_term(self, term_a):

        node_pattern = implica.NodePattern(term=term_a)

        expected_str = f"NodePattern(term='{term_a}')"

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_term(self, term_a):

        node_pattern = implica.NodePattern(term=term_a)

        expected_repr = f"NodePattern(term='{term_a}')"

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_properties(self):

        properties = {"key": "value"}
        node_pattern = implica.NodePattern(properties=properties)

        expected_str = f"NodePattern(properties={properties})"

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_properties(self):
        properties = {"key": "value"}
        node_pattern = implica.NodePattern(properties=properties)

        expected_repr = f"NodePattern(properties={properties})"

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_type_schema(self):

        type_schema = implica.TypeSchema("X")
        node_pattern = implica.NodePattern(type_schema=type_schema)

        expected_str = f"NodePattern(type_schema={type_schema})"

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_type_schema(self):

        type_schema = implica.TypeSchema("X")
        node_pattern = implica.NodePattern(type_schema=type_schema)

        expected_repr = f"NodePattern(type_schema={type_schema})"

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_term_schema(self):

        term_schema = implica.TermSchema("x")
        node_pattern = implica.NodePattern(term_schema=term_schema)

        expected_str = f"NodePattern(term_schema={term_schema})"

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_term_schema(self):

        term_schema = implica.TermSchema("x")
        node_pattern = implica.NodePattern(term_schema=term_schema)

        expected_repr = f"NodePattern(term_schema={term_schema})"

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_variable_type_term_and_properties(self, type_a, term_a):

        properties = {"key": "value"}
        node_pattern = implica.NodePattern(
            variable="x", type=type_a, term=term_a, properties=properties
        )

        expected_str = (
            f"NodePattern(variable='x', type='{type_a}', term='{term_a}', properties={properties})"
        )

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_variable_type_term_and_properties(self, type_a, term_a):

        properties = {"key": "value"}
        node_pattern = implica.NodePattern(
            variable="x", type=type_a, term=term_a, properties=properties
        )

        expected_repr = (
            f"NodePattern(variable='x', type='{type_a}', term='{term_a}', properties={properties})"
        )

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_variable_type_schema_term_schema_and_properties(self):

        type_schema = implica.TypeSchema("X")
        term_schema = implica.TermSchema("x")
        properties = {"key": "value"}
        node_pattern = implica.NodePattern(
            variable="y", type_schema=type_schema, term_schema=term_schema, properties=properties
        )

        expected_str = (
            f"NodePattern(variable='y', type_schema={type_schema}, "
            f"term_schema={term_schema}, properties={properties})"
        )

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_variable_type_schema_term_schema_and_properties(self):

        type_schema = implica.TypeSchema("X")
        term_schema = implica.TermSchema("x")
        properties = {"key": "value"}
        node_pattern = implica.NodePattern(
            variable="y", type_schema=type_schema, term_schema=term_schema, properties=properties
        )

        expected_repr = (
            f"NodePattern(variable='y', type_schema={type_schema}, "
            f"term_schema={term_schema}, properties={properties})"
        )

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_no_arguments(self):

        node_pattern = implica.NodePattern()

        expected_str = "NodePattern()"

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_no_arguments(self):

        node_pattern = implica.NodePattern()

        expected_repr = "NodePattern()"

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_type_and_term_schema(self, type_a):

        term_schema = implica.TermSchema("x")
        node_pattern = implica.NodePattern(type=type_a, term_schema=term_schema)

        expected_str = f"NodePattern(type='{type_a}', term_schema={term_schema})"

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_type_and_term_schema(self, type_a):

        term_schema = implica.TermSchema("x")
        node_pattern = implica.NodePattern(type=type_a, term_schema=term_schema)

        expected_repr = f"NodePattern(type='{type_a}', term_schema={term_schema})"

        assert repr(node_pattern) == expected_repr

    def test_node_pattern_str_with_type_schema_and_term(self, term_a):

        type_schema = implica.TypeSchema("X")
        node_pattern = implica.NodePattern(type_schema=type_schema, term=term_a)

        expected_str = f"NodePattern(type_schema={type_schema}, term='{term_a}')"

        assert str(node_pattern) == expected_str

    def test_node_pattern_repr_with_type_schema_and_term(self, term_a):

        type_schema = implica.TypeSchema("X")
        node_pattern = implica.NodePattern(type_schema=type_schema, term=term_a)

        expected_repr = f"NodePattern(type_schema={type_schema}, term='{term_a}')"

        assert repr(node_pattern) == expected_repr
