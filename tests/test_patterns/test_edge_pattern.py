import pytest
import implica


class TestEdgePatternInit:
    def test_edge_pattern_creation_with_variable(self):
        edge_pattern = implica.EdgePattern(variable="e1")
        assert edge_pattern.variable == "e1"

    def test_edge_pattern_creation_with_type(self, arrow_ab):
        edge_pattern = implica.EdgePattern(type=arrow_ab)
        assert edge_pattern.type == arrow_ab

    def test_edge_pattern_creation_with_type_schema(self):
        type_schema = implica.TypeSchema("A -> B")
        edge_pattern = implica.EdgePattern(type_schema=type_schema)
        assert edge_pattern.type_schema == type_schema

    def test_edge_pattern_creation_with_term(self, term_a):
        edge_pattern = implica.EdgePattern(term=term_a)
        assert edge_pattern.term == term_a

    def test_edge_pattern_creation_with_term_schema(self):
        term_schema = implica.TermSchema("f")
        edge_pattern = implica.EdgePattern(term_schema=term_schema)
        assert edge_pattern.term_schema == term_schema

    def test_edge_pattern_creation_with_properties(self):
        properties = {"weight": 5, "label": "edge1"}
        edge_pattern = implica.EdgePattern(properties=properties)
        assert edge_pattern.properties["weight"] == 5
        assert edge_pattern.properties["label"] == "edge1"

    def test_edge_pattern_creation_with_direction(self):
        edge_pattern = implica.EdgePattern(direction="backward")
        assert edge_pattern.direction == "backward"

    def test_edge_pattern_creation_defaults(self):
        edge_pattern = implica.EdgePattern()
        assert edge_pattern.variable is None
        assert edge_pattern.type is None
        assert edge_pattern.type_schema is None
        assert edge_pattern.term is None
        assert edge_pattern.term_schema is None
        assert edge_pattern.properties == {}
        assert edge_pattern.direction == "forward"

    def test_edge_pattern_creation_with_term_and_type(self, term_ab, arrow_ab):
        edge_pattern = implica.EdgePattern(term=term_ab, type=arrow_ab)
        assert edge_pattern.term == term_ab
        assert edge_pattern.type == arrow_ab

    def test_edge_pattern_creation_with_term_schema_and_type_schema(self):
        term_schema = implica.TermSchema("f")
        type_schema = implica.TypeSchema("A -> B")
        edge_pattern = implica.EdgePattern(term_schema=term_schema, type_schema=type_schema)
        assert edge_pattern.term_schema == term_schema
        assert edge_pattern.type_schema == type_schema

    def test_edge_pattern_creation_fails_with_invalid_variable_type(self):
        with pytest.raises(ValueError):
            implica.EdgePattern(variable="123invalid")

    def test_edge_pattern_creation_fails_with_type_and_type_schema(self, arrow_ab):
        type_schema = implica.TypeSchema("A -> B")
        with pytest.raises(ValueError):
            implica.EdgePattern(type=arrow_ab, type_schema=type_schema)

    def test_edge_pattern_creation_fails_with_term_and_term_schema(self, term_a):
        term_schema = implica.TermSchema("f")
        with pytest.raises(ValueError):
            implica.EdgePattern(term=term_a, term_schema=term_schema)

    @pytest.mark.xfail(reason="Type checking not enforced at this level")
    def test_edge_pattern_creation_fails_if_term_type_mismatch(self, term_a, arrow_ab):
        with pytest.raises(TypeError):
            implica.EdgePattern(term=term_a, type=arrow_ab)

    def test_edge_pattern_creation_with_invalid_properties(self):
        with pytest.raises(TypeError):
            implica.EdgePattern(properties="not_a_dict")

    def test_edge_pattern_creation_fails_with_invalid_direction(self):
        with pytest.raises(ValueError):
            implica.EdgePattern(direction="sideways")


class TestEdgePatternSimpleMatches:
    def test_node_pattern_matches_edge_with_exact_type(self, edge_ab, arrow_ab):
        edge_pattern = implica.EdgePattern(type=arrow_ab)
        assert edge_pattern.matches(edge_ab)

    def test_node_pattern_does_not_match_edge_with_different_type(self, edge_ab, arrow_ba):
        edge_pattern = implica.EdgePattern(type=arrow_ba)
        assert not edge_pattern.matches(edge_ab)

    def test_edge_pattern_matches_edge_with_exact_term(self, edge_ab, term_ab):
        edge_pattern = implica.EdgePattern(term=term_ab)
        assert edge_pattern.matches(edge_ab)

    def test_edge_pattern_does_not_match_edge_with_different_term(self, edge_ab, term_ba):
        edge_pattern = implica.EdgePattern(term=term_ba)
        assert not edge_pattern.matches(edge_ab)

    def test_edge_pattern_not_match_edge_with_different_term_of_same_type(self, edge_ab, arrow_ab):
        term = implica.BasicTerm("different", arrow_ab)
        edge_pattern = implica.EdgePattern(term=term)
        assert not edge_pattern.matches(edge_ab)

    def test_node_pattern_matches_edge_with_type_schema(self, edge_ab):
        type_schema = implica.TypeSchema("A -> B")
        edge_pattern = implica.EdgePattern(type_schema=type_schema)
        assert edge_pattern.matches(edge_ab)

    def test_node_pattern_does_not_match_edge_with_non_matching_type_schema(self, edge_ab):
        type_schema = implica.TypeSchema("B -> A")
        edge_pattern = implica.EdgePattern(type_schema=type_schema)
        assert not edge_pattern.matches(edge_ab)

    def test_edge_pattern_matches_edge_with_term_schema(self, edge_ab):
        term_schema = implica.TermSchema("f")
        edge_pattern = implica.EdgePattern(term_schema=term_schema)
        assert edge_pattern.matches(edge_ab)

    def test_edge_pattern_does_not_match_edge_with_non_matching_term_schema(self, edge_ab):
        term_schema = implica.TermSchema("* *")
        edge_pattern = implica.EdgePattern(term_schema=term_schema)
        assert not edge_pattern.matches(edge_ab)

    def test_edge_pattern_matches_edge_with_properties(self, node_a, node_b, term_ab):
        properties = {"weight": 10, "label": "edge_ab"}
        edge_ab_with_props = implica.Edge(
            term_ab,
            node_a,
            node_b,
            properties=properties,
        )
        edge_pattern = implica.EdgePattern(properties={"weight": 10})
        assert edge_pattern.matches(edge_ab_with_props)

    def test_edge_pattern_does_not_match_edge_with_non_matching_properties(
        self, node_a, node_b, term_ab
    ):
        properties = {"weight": 10, "label": "edge_ab"}
        edge_ab_with_props = implica.Edge(
            term_ab,
            node_a,
            node_b,
            properties=properties,
        )
        edge_pattern = implica.EdgePattern(properties={"weight": 5})
        assert not edge_pattern.matches(edge_ab_with_props)

    def test_edge_pattern_not_matches_edge_with_missing_properties(self, node_a, node_b, term_ab):
        properties = {"weight": 10, "label": "edge_ab"}
        edge_ab_with_props = implica.Edge(
            term_ab,
            node_a,
            node_b,
            properties=properties,
        )
        edge_pattern = implica.EdgePattern(properties={"color": "red"})
        assert not edge_pattern.matches(edge_ab_with_props)

    def test_edge_pattern_matches_node_with_all_criteria(self, node_a, node_b, term_ab, arrow_ab):
        properties = {"weight": 10, "label": "edge_ab"}
        edge_ab_with_props = implica.Edge(
            term_ab,
            node_a,
            node_b,
            properties=properties,
        )
        edge_pattern = implica.EdgePattern(
            type=arrow_ab,
            term=term_ab,
            properties={"weight": 10},
        )
        assert edge_pattern.matches(edge_ab_with_props)

    def test_edge_pattern_does_not_match_node_when_one_criterion_fails(
        self, node_a, node_b, term_ab, arrow_ab
    ):
        properties = {"weight": 10, "label": "edge_ab"}
        edge_ab_with_props = implica.Edge(
            term_ab,
            node_a,
            node_b,
            properties=properties,
        )
        edge_pattern = implica.EdgePattern(
            type=arrow_ab,
            term=term_ab,
            properties={"weight": 5},  # This will cause the match to fail
        )
        assert not edge_pattern.matches(edge_ab_with_props)


class TestEdgePatternMatchesWithContext:
    """Test schemas capturing content to context"""

    def test_edge_pattern_type_schema_captures_type_in_context(self, edge_ab, arrow_ab):
        type_schema = implica.TypeSchema("(X:*)")
        edge_pattern = implica.EdgePattern(type_schema=type_schema)
        context = {}
        assert edge_pattern.matches(edge_ab, context)
        assert "X" in context
        assert context["X"] == arrow_ab

    def test_edge_pattern_term_schema_captures_term_in_context(self, edge_ab, term_ab):
        term_schema = implica.TermSchema("X")
        edge_pattern = implica.EdgePattern(term_schema=term_schema)
        context = {}
        assert edge_pattern.matches(edge_ab, context)
        assert "X" in context
        assert context["X"] == term_ab

    def test_edge_pattern_type_schema_uses_existing_variable_in_context(self, edge_ab, arrow_ab):
        type_schema = implica.TypeSchema("X")
        edge_pattern = implica.EdgePattern(type_schema=type_schema)
        context = {"X": arrow_ab}
        assert edge_pattern.matches(edge_ab, context)

    def test_edge_pattern_type_schema_fails_if_variable_mismatch_in_context(
        self, edge_ab, arrow_ab, arrow_ba
    ):
        type_schema = implica.TypeSchema("X")
        edge_pattern = implica.EdgePattern(type_schema=type_schema)
        context = {"X": arrow_ba}
        assert not edge_pattern.matches(edge_ab, context)

    def test_edge_pattern_term_schema_uses_existing_variable_in_context(self, edge_ab, term_ab):
        term_schema = implica.TermSchema("X")
        edge_pattern = implica.EdgePattern(term_schema=term_schema)
        context = {"X": term_ab}
        assert edge_pattern.matches(edge_ab, context)

    def test_edge_pattern_term_schema_fails_if_variable_mismatch_in_context(self, edge_ab, term_ba):
        term_schema = implica.TermSchema("X")
        edge_pattern = implica.EdgePattern(term_schema=term_schema)
        context = {"X": term_ba}
        assert not edge_pattern.matches(edge_ab, context)

    def test_edge_pattern_term_schema_fails_if_variable_in_context_mismatch(self, edge_ab, term_ba):
        term_schema = implica.TermSchema("X")
        edge_pattern = implica.EdgePattern(term_schema=term_schema)
        context = {"X": term_ba}
        assert not edge_pattern.matches(edge_ab, context)

    def test_edge_pattern_with_both_schemas_captures_in_context(self, edge_ab, arrow_ab, term_ab):
        type_schema = implica.TypeSchema("(T:*)")
        term_schema = implica.TermSchema("M")
        edge_pattern = implica.EdgePattern(type_schema=type_schema, term_schema=term_schema)
        context = {}
        assert edge_pattern.matches(edge_ab, context)
        assert "T" in context
        assert context["T"] == arrow_ab
        assert "M" in context
        assert context["M"] == term_ab

    def test_edge_pattern_with_both_schemas_uses_existing_context(self, edge_ab, arrow_ab, term_ab):
        type_schema = implica.TypeSchema("T")
        term_schema = implica.TermSchema("M")
        edge_pattern = implica.EdgePattern(type_schema=type_schema, term_schema=term_schema)
        context = {"T": arrow_ab, "M": term_ab}
        assert edge_pattern.matches(edge_ab, context)

    def test_edge_pattern_with_both_schemas_fails_with_context_mismatch(
        self, edge_ab, arrow_ab, term_ab, arrow_ba
    ):
        type_schema = implica.TypeSchema("T")
        term_schema = implica.TermSchema("M")
        edge_pattern = implica.EdgePattern(type_schema=type_schema, term_schema=term_schema)

        context = {"T": arrow_ba, "M": term_ab}  # Mismatch on T
        assert not edge_pattern.matches(edge_ab, context)

        context = {"T": arrow_ab, "M": implica.BasicTerm("different", arrow_ab)}  # Mismatch on M
        assert not edge_pattern.matches(edge_ab, context)

    def test_edge_pattern_matches_fails_if_captured_variable_used_later_mismatches(self, edge_ab):
        type_schema = implica.TypeSchema("(T:*)")
        term_schema = implica.TermSchema("T")  # Reusing T for term
        edge_pattern = implica.EdgePattern(type_schema=type_schema, term_schema=term_schema)
        context = {}
        with pytest.raises(ValueError):
            edge_pattern.matches(edge_ab, context)

    def test_edge_pattern_matches_succeeds_if_captured_variable_used_later_matches(
        self, edge_aa, type_a, term_aa
    ):
        type_schema = implica.TypeSchema("(T:*) -> T")
        term_schema = implica.TermSchema("M")
        edge_pattern = implica.EdgePattern(type_schema=type_schema, term_schema=term_schema)
        context = {}
        assert edge_pattern.matches(edge_aa, context)
        assert "T" in context
        assert context["T"] == type_a
        assert "M" in context
        assert context["M"] == term_aa

    def test_edge_pattern_with_properties_and_schemas_captures_in_context(
        self, node_a, node_b, term_ab, arrow_ab
    ):
        properties = {"weight": 10, "label": "edge_ab"}
        edge_ab_with_props = implica.Edge(
            term_ab,
            node_a,
            node_b,
            properties=properties,
        )
        type_schema = implica.TypeSchema("(T:*)")
        term_schema = implica.TermSchema("M")
        edge_pattern = implica.EdgePattern(
            type_schema=type_schema,
            term_schema=term_schema,
            properties={"weight": 10},
        )
        context = {}
        assert edge_pattern.matches(edge_ab_with_props, context)
        assert "T" in context
        assert context["T"] == arrow_ab
        assert "M" in context
        assert context["M"] == term_ab


class TestEdgePatternStringRepresentation:
    def test_edge_pattern_str_with_variable(self):
        edge_pattern = implica.EdgePattern(variable="e1")
        assert str(edge_pattern) == "EdgePattern(variable='e1', direction='forward')"

    def test_edge_pattern_repr_with_variable(self):
        edge_pattern = implica.EdgePattern(variable="e1")
        assert repr(edge_pattern) == "EdgePattern(variable='e1', direction='forward')"

    def test_edge_pattern_str_with_type(self, arrow_ab):
        edge_pattern = implica.EdgePattern(type=arrow_ab)
        assert str(edge_pattern) == f"EdgePattern(type='{arrow_ab}', direction='forward')"

    def test_edge_pattern_repr_with_type(self, arrow_ab):
        edge_pattern = implica.EdgePattern(type=arrow_ab)
        assert repr(edge_pattern) == f"EdgePattern(type='{arrow_ab}', direction='forward')"

    def test_edge_pattern_str_with_term(self, term_ab):
        edge_pattern = implica.EdgePattern(term=term_ab)
        assert str(edge_pattern) == f"EdgePattern(term='{term_ab}', direction='forward')"

    def test_edge_pattern_repr_with_term(self, term_ab):
        edge_pattern = implica.EdgePattern(term=term_ab)
        assert repr(edge_pattern) == f"EdgePattern(term='{term_ab}', direction='forward')"

    def test_edge_pattern_str_with_properties(self):
        properties = {"weight": 5}
        edge_pattern = implica.EdgePattern(properties=properties)
        assert str(edge_pattern) == "EdgePattern(properties={'weight': 5}, direction='forward')"

    def test_edge_pattern_repr_with_properties(self):
        properties = {"weight": 5}
        edge_pattern = implica.EdgePattern(properties=properties)
        assert repr(edge_pattern) == "EdgePattern(properties={'weight': 5}, direction='forward')"

    def test_edge_pattern_str_with_direction(self):
        edge_pattern = implica.EdgePattern(direction="backward")
        assert str(edge_pattern) == "EdgePattern(direction='backward')"

    def test_edge_pattern_repr_with_direction(self):
        edge_pattern = implica.EdgePattern(direction="backward")
        assert repr(edge_pattern) == "EdgePattern(direction='backward')"

    def test_edge_pattern_str_with_term_and_type(self, arrow_ab, term_ab):
        properties = {"weight": 5}
        edge_pattern = implica.EdgePattern(
            variable="e1",
            type=arrow_ab,
            term=term_ab,
            properties=properties,
            direction="backward",
        )
        expected_str = (
            f"EdgePattern(variable='e1', type='{arrow_ab}', term='{term_ab}', "
            f"properties={{'weight': 5}}, direction='backward')"
        )
        assert str(edge_pattern) == expected_str

    def test_edge_pattern_repr_with_term_and_type(self, arrow_ab, term_ab):
        properties = {"weight": 5}
        edge_pattern = implica.EdgePattern(
            variable="e1",
            type=arrow_ab,
            term=term_ab,
            properties=properties,
            direction="backward",
        )
        expected_repr = (
            f"EdgePattern(variable='e1', type='{arrow_ab}', term='{term_ab}', "
            f"properties={{'weight': 5}}, direction='backward')"
        )
        assert repr(edge_pattern) == expected_repr

    def test_edge_pattern_str_with_term_schema_and_type_schema(self):
        type_schema = implica.TypeSchema("A -> B")
        term_schema = implica.TermSchema("f")
        edge_pattern = implica.EdgePattern(
            type_schema=type_schema,
            term_schema=term_schema,
        )
        expected_str = f"EdgePattern(type_schema={type_schema}, term_schema={term_schema}, direction='forward')"
        assert str(edge_pattern) == expected_str

    def test_edge_pattern_repr_with_term_schema_and_type_schema(self):
        type_schema = implica.TypeSchema("A -> B")
        term_schema = implica.TermSchema("f")
        edge_pattern = implica.EdgePattern(
            type_schema=type_schema,
            term_schema=term_schema,
        )
        expected_repr = f"EdgePattern(type_schema={type_schema}, term_schema={term_schema}, direction='forward')"
        assert repr(edge_pattern) == expected_repr
