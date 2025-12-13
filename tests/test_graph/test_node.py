import pytest
import implica


class TestNodeCreation:
    def test_create_node_with_type_and_term(self, type_a, term_a):
        node = implica.Node(type_a, term_a)

        assert node.type == type_a
        assert node.term == term_a

    def test_create_node_with_only_type(self, type_a):
        node = implica.Node(type_a)

        assert node.type == type_a
        assert node.term is None

    def test_node_properties(self, type_a):
        properties = {"color": "red", "weight": 10}
        node = implica.Node(type_a, properties=properties)

        assert node.type == type_a
        assert node.properties["color"] == "red"
        assert node.properties["weight"] == 10

    def test_node_creation_term_of_different_type_raises(self, type_a, term_b):
        with pytest.raises(TypeError):
            implica.Node(type_a, term_b)


class TestNodeBasicMethods:
    def test_get_type(self, node_a, type_a):
        assert node_a.type == type_a

    def test_get_term(self, node_a_with_term, term_a):
        assert node_a_with_term.term == term_a

    def test_get_properties(self, type_a):
        properties = {"key1": "value1", "key2": 42}
        node = implica.Node(type_a, properties=properties)

        assert node.properties["key1"] == "value1"
        assert node.properties["key2"] == 42

    def test_node_immutable_properties(self, type_a):
        properties = {"immutable_key": "immutable_value"}
        node = implica.Node(type_a, properties=properties)

        with pytest.raises(TypeError):
            node.properties["immutable_key"] = "new_value"

    def test_node_immutable_type(self, node_a, type_b):
        with pytest.raises(AttributeError):
            node_a.type = type_b

    def test_node_immutable_term(self, node_a_with_term, type_a):
        with pytest.raises(AttributeError):
            node_a_with_term.term = implica.BasicTerm("new_term", type_a)

    def test_node_uid_is_string(self, node_a):
        assert isinstance(node_a.uid(), str)

    def test_node_uid_is_unique(self, node_a, node_b):
        assert node_a.uid() != node_b.uid()

    def test_node_uid_is_consistent(self, node_a):
        uid1 = node_a.uid()
        uid2 = node_a.uid()

        assert uid1 == uid2

    def test_node_uid_does_not_vary_with_different_terms(self, type_a):
        term1 = implica.BasicTerm("term1", type_a)
        term2 = implica.BasicTerm("term2", type_a)

        node1 = implica.Node(type_a, term1)
        node2 = implica.Node(type_a, term2)

        assert node1.uid() == node2.uid()

    def test_node_uid_consistent_with_same_term(self, type_a, term_a):
        node1 = implica.Node(type_a, term_a)
        node2 = implica.Node(type_a, term_a)

        assert node1.uid() == node2.uid()

    def test_node_uid_consistent_with_different_properties(self, type_a, term_a):
        properties1 = {"prop1": "value1"}
        properties2 = {"prop2": "value2"}

        node1 = implica.Node(type_a, term_a, properties=properties1)
        node2 = implica.Node(type_a, term_a, properties=properties2)

        assert node1.uid() == node2.uid()

    def test_node_equality_same_type_and_term(self, type_a, term_a):
        node1 = implica.Node(type_a, term_a)
        node2 = implica.Node(type_a, term_a)

        assert node1 == node2

    def test_node_inequality_different_types(self, node_a, node_b):
        assert node_a != node_b

    def test_node_equality_different_terms(self, type_a):
        term1 = implica.BasicTerm("term_one", type_a)
        term2 = implica.BasicTerm("term_two", type_a)

        node1 = implica.Node(type_a, term1)
        node2 = implica.Node(type_a, term2)

        assert node1 == node2

    def test_node_hash_consistency(self, node_a):
        hash1 = hash(node_a)
        hash2 = hash(node_a)

        assert hash1 == hash2

    def test_node_hash_equality_for_equal_nodes(self, type_a, term_a):
        node1 = implica.Node(type_a, term_a)
        node2 = implica.Node(type_a, term_a)

        assert hash(node1) == hash(node2)


class TestNodeRepresentation:
    def test_node_repr_with_term(self, node_a_with_term):
        repr_str = repr(node_a_with_term)
        assert "Node(A, a)" == repr_str

    def test_node_repr_without_term(self, node_a):
        repr_str = repr(node_a)
        assert "Node(A)" == repr_str

    def test_node_str_with_term(self, node_a_with_term):
        str_repr = str(node_a_with_term)
        assert "Node(A, a)" == str_repr

    def test_node_str_without_term(self, node_a):
        str_repr = str(node_a)
        assert "Node(A)" == str_repr


class TestNodePropertiesDeepImmutability:
    def test_nested_dict_immutability(self, type_a):

        properties = {"outer": {"inner": "value"}}
        node = implica.Node(type_a, properties=properties)

        # Verify the properties exist
        assert node.properties["outer"]["inner"] == "value"

        # Try to modify the nested dictionary
        with pytest.raises(TypeError):
            node.properties["outer"]["inner"] = "new_value"

    def test_nested_list_immutability(self, type_a):
        properties = {"items": [1, 2, 3]}
        node = implica.Node(type_a, properties=properties)

        # Verify the list is converted to an immutable tuple
        assert node.properties["items"] == (1, 2, 3)
        assert isinstance(node.properties["items"], tuple)

        # Tuples don't have append method
        with pytest.raises(AttributeError):
            node.properties["items"].append(4)

        # Try to modify the tuple (tuples don't support item assignment)
        with pytest.raises(TypeError):
            node.properties["items"][0] = 99

    def test_deeply_nested_structure_immutability(self, type_a):
        properties = {"level1": {"level2": {"level3": ["a", "b", "c"]}}}
        node = implica.Node(type_a, properties=properties)

        # Verify access works and lists are converted to tuples
        assert node.properties["level1"]["level2"]["level3"][0] == "a"
        assert isinstance(node.properties["level1"]["level2"]["level3"], tuple)

        # Try to modify at various levels
        with pytest.raises(TypeError):
            node.properties["level1"]["level2"]["level3"][0] = "z"

        # Tuples don't have append method
        with pytest.raises(AttributeError):
            node.properties["level1"]["level2"]["level3"].append("d")

    def test_properties_original_dict_modification_doesnt_affect_node(self, type_a):
        original_props = {"mutable": {"key": "original"}, "another": "value"}
        node = implica.Node(type_a, properties=original_props)

        # Modify the original dictionary
        original_props["mutable"]["key"] = "modified"
        original_props["another"] = "changed"

        # Node properties should remain unchanged
        assert node.properties["mutable"]["key"] == "original"
        assert node.properties["another"] == "value"


class TestNodePropertiesEdgeCases:
    def test_empty_properties_dict(self, type_a):
        node = implica.Node(type_a, properties={})

        assert len(node.properties) == 0
        # Properties are returned as MappingProxyType (immutable dict-like)
        from types import MappingProxyType

        assert isinstance(node.properties, MappingProxyType)

    def test_properties_with_none_value(self, type_a):
        properties = {"key_with_none": None}
        node = implica.Node(type_a, properties=properties)

        assert node.properties["key_with_none"] is None

    def test_properties_with_mixed_types(self, type_a):
        properties = {
            "string": "text",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "none": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
        }
        node = implica.Node(type_a, properties=properties)

        assert node.properties["string"] == "text"
        assert node.properties["integer"] == 42
        assert node.properties["float"] == 3.14
        assert node.properties["boolean"] is True
        assert node.properties["none"] is None
        # Lists are converted to tuples for immutability
        assert node.properties["list"] == (1, 2, 3)
        assert isinstance(node.properties["list"], tuple)
        # Dicts are converted to MappingProxyType for immutability
        assert node.properties["dict"]["nested"] == "value"
        from types import MappingProxyType

        assert isinstance(node.properties["dict"], MappingProxyType)

    def test_no_properties_vs_empty_properties(self, type_a):
        node_no_props = implica.Node(type_a)
        node_empty_props = implica.Node(type_a, properties={})

        # Both should behave the same
        assert len(node_no_props.properties) == 0
        assert len(node_empty_props.properties) == 0


class TestNodeCloning:

    def test_node_properties_isolated_between_instances(self, type_a, term_a):
        # Create two nodes with the same type and term but different properties
        node1 = implica.Node(type_a, term_a, properties={"color": "red"})
        node2 = implica.Node(type_a, term_a, properties={"color": "blue"})

        # They should be equal
        assert node1 == node2

        # But have independent properties
        assert node1.properties["color"] == "red"
        assert node2.properties["color"] == "blue"
