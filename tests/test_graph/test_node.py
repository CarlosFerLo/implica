import pytest
import implica


class TestNodeCreation:
    def test_create_node_with_type_and_term(self):
        type = implica.Variable("Int")
        term = implica.BasicTerm("i_42", type)
        node = implica.Node(type, term)

        assert node.type == type
        assert node.term == term

    def test_create_node_with_only_type(self):
        type = implica.Variable("String")
        node = implica.Node(type)

        assert node.type == type
        assert node.term is None

    def test_node_properties(self):
        type = implica.Variable("Bool")
        properties = {"color": "red", "weight": 10}
        node = implica.Node(type, properties=properties)

        assert node.type == type
        assert node.properties["color"] == "red"
        assert node.properties["weight"] == 10

    def test_node_creation_term_of_different_type_raises(self):
        type1 = implica.Variable("Float")
        type2 = implica.Variable("Int")
        term = implica.BasicTerm("f_3", type2)

        with pytest.raises(TypeError):
            implica.Node(type1, term)


class TestNodeBasicMethods:
    def test_get_type(self):
        type = implica.Variable("CustomType")
        node = implica.Node(type)

        assert node.type == type

    def test_get_term(self):
        type = implica.Variable("CustomType")
        term = implica.BasicTerm("custom_term", type)
        node = implica.Node(type, term)

        assert node.term == term

    def test_get_properties(self):
        type = implica.Variable("AnotherType")
        properties = {"key1": "value1", "key2": 42}
        node = implica.Node(type, properties=properties)

        assert node.properties["key1"] == "value1"
        assert node.properties["key2"] == 42

    def test_node_immutable_properties(self):
        type = implica.Variable("ImmutableType")
        properties = {"immutable_key": "immutable_value"}
        node = implica.Node(type, properties=properties)

        with pytest.raises(TypeError):
            node.properties["immutable_key"] = "new_value"

    def test_node_immutable_type(self):
        type = implica.Variable("ImmutableType")
        node = implica.Node(type)

        with pytest.raises(AttributeError):
            node.type = implica.Variable("NewType")

    def test_node_immutable_term(self):
        type = implica.Variable("ImmutableType")
        term = implica.BasicTerm("immutable_term", type)
        node = implica.Node(type, term)

        with pytest.raises(AttributeError):
            node.term = implica.BasicTerm("new_term", type)

    def test_node_uid_is_string(self):
        type = implica.Variable("UIDType")
        node = implica.Node(type)

        assert isinstance(node.uid(), str)

    def test_node_uid_is_unique(self):
        type1 = implica.Variable("Type1")
        type2 = implica.Variable("Type2")
        node1 = implica.Node(type1)
        node2 = implica.Node(type2)

        assert node1.uid() != node2.uid()

    def test_node_uid_is_consistent(self):
        type = implica.Variable("ConsistentType")
        node = implica.Node(type)

        uid1 = node.uid()
        uid2 = node.uid()

        assert uid1 == uid2

    def test_node_uid_varies_with_different_terms(self):
        type = implica.Variable("VaryingType")
        term1 = implica.BasicTerm("term1", type)
        term2 = implica.BasicTerm("term2", type)

        node1 = implica.Node(type, term1)
        node2 = implica.Node(type, term2)

        assert node1.uid() != node2.uid()

    def test_node_uid_consistent_with_same_term(self):
        type = implica.Variable("ConsistentTermType")
        term = implica.BasicTerm("same_term", type)

        node1 = implica.Node(type, term)
        node2 = implica.Node(type, term)

        assert node1.uid() == node2.uid()

    def test_node_uid_consistent_with_different_properties(self):
        type = implica.Variable("PropertyType")
        term = implica.BasicTerm("property_term", type)

        properties1 = {"prop1": "value1"}
        properties2 = {"prop2": "value2"}

        node1 = implica.Node(type, term, properties=properties1)
        node2 = implica.Node(type, term, properties=properties2)

        assert node1.uid() == node2.uid()

    def test_node_equality_same_type_and_term(self):
        type = implica.Variable("EqualityType")
        term = implica.BasicTerm("equality_term", type)

        node1 = implica.Node(type, term)
        node2 = implica.Node(type, term)

        assert node1 == node2

    def test_node_inequality_different_types(self):
        type1 = implica.Variable("TypeA")
        type2 = implica.Variable("TypeB")
        term1 = implica.BasicTerm("common_term", type1)
        term2 = implica.BasicTerm("common_term", type2)

        node1 = implica.Node(type1, term1)
        node2 = implica.Node(type2, term2)

        assert node1 != node2

    def test_node_inequality_different_terms(self):
        type = implica.Variable("InequalityType")
        term1 = implica.BasicTerm("term_one", type)
        term2 = implica.BasicTerm("term_two", type)

        node1 = implica.Node(type, term1)
        node2 = implica.Node(type, term2)

        assert node1 != node2

    def test_node_hash_consistency(self):
        type = implica.Variable("HashType")
        term = implica.BasicTerm("hash_term", type)

        node = implica.Node(type, term)

        hash1 = hash(node)
        hash2 = hash(node)

        assert hash1 == hash2

    def test_node_hash_equality_for_equal_nodes(self):
        type = implica.Variable("HashEqualityType")
        term = implica.BasicTerm("hash_equality_term", type)

        node1 = implica.Node(type, term)
        node2 = implica.Node(type, term)

        assert hash(node1) == hash(node2)


class TestNodeRepresentation:
    def test_node_repr_with_term(self):
        type = implica.Variable("ReprType")
        term = implica.BasicTerm("repr_term", type)
        node = implica.Node(type, term)

        repr_str = repr(node)
        assert "Node(ReprType, repr_term)" == repr_str

    def test_node_repr_without_term(self):
        type = implica.Variable("ReprNoTermType")
        node = implica.Node(type)

        repr_str = repr(node)
        assert "Node(ReprNoTermType)" == repr_str

    def test_node_str_with_term(self):
        type = implica.Variable("StrType")
        term = implica.BasicTerm("str_term", type)
        node = implica.Node(type, term)

        str_repr = str(node)
        assert "Node(StrType, str_term)" == str_repr

    def test_node_str_without_term(self):
        type = implica.Variable("StrNoTermType")
        node = implica.Node(type)

        str_repr = str(node)
        assert "Node(StrNoTermType)" == str_repr


class TestNodePropertiesDeepImmutability:
    def test_nested_dict_immutability(self):
        type = implica.Variable("NestedType")
        properties = {"outer": {"inner": "value"}}
        node = implica.Node(type, properties=properties)

        # Verify the properties exist
        assert node.properties["outer"]["inner"] == "value"

        # Try to modify the nested dictionary
        with pytest.raises(TypeError):
            node.properties["outer"]["inner"] = "new_value"

    def test_nested_list_immutability(self):
        type = implica.Variable("ListType")
        properties = {"items": [1, 2, 3]}
        node = implica.Node(type, properties=properties)

        # Verify the list is converted to an immutable tuple
        assert node.properties["items"] == (1, 2, 3)
        assert isinstance(node.properties["items"], tuple)

        # Tuples don't have append method
        with pytest.raises(AttributeError):
            node.properties["items"].append(4)

        # Try to modify the tuple (tuples don't support item assignment)
        with pytest.raises(TypeError):
            node.properties["items"][0] = 99

    def test_deeply_nested_structure_immutability(self):
        type = implica.Variable("DeeplyNestedType")
        properties = {"level1": {"level2": {"level3": ["a", "b", "c"]}}}
        node = implica.Node(type, properties=properties)

        # Verify access works and lists are converted to tuples
        assert node.properties["level1"]["level2"]["level3"][0] == "a"
        assert isinstance(node.properties["level1"]["level2"]["level3"], tuple)

        # Try to modify at various levels
        with pytest.raises(TypeError):
            node.properties["level1"]["level2"]["level3"][0] = "z"

        # Tuples don't have append method
        with pytest.raises(AttributeError):
            node.properties["level1"]["level2"]["level3"].append("d")

    def test_properties_original_dict_modification_doesnt_affect_node(self):
        type = implica.Variable("IsolationType")
        original_props = {"mutable": {"key": "original"}, "another": "value"}
        node = implica.Node(type, properties=original_props)

        # Modify the original dictionary
        original_props["mutable"]["key"] = "modified"
        original_props["another"] = "changed"

        # Node properties should remain unchanged
        assert node.properties["mutable"]["key"] == "original"
        assert node.properties["another"] == "value"


class TestNodePropertiesEdgeCases:
    def test_empty_properties_dict(self):
        type = implica.Variable("EmptyPropsType")
        node = implica.Node(type, properties={})

        assert len(node.properties) == 0
        # Properties are returned as MappingProxyType (immutable dict-like)
        from types import MappingProxyType

        assert isinstance(node.properties, MappingProxyType)

    def test_properties_with_none_value(self):
        type = implica.Variable("NoneValueType")
        properties = {"key_with_none": None}
        node = implica.Node(type, properties=properties)

        assert node.properties["key_with_none"] is None

    def test_properties_with_mixed_types(self):
        type = implica.Variable("MixedType")
        properties = {
            "string": "text",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "none": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
        }
        node = implica.Node(type, properties=properties)

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

    def test_no_properties_vs_empty_properties(self):
        type = implica.Variable("PropsCompareType")
        node_no_props = implica.Node(type)
        node_empty_props = implica.Node(type, properties={})

        # Both should behave the same
        assert len(node_no_props.properties) == 0
        assert len(node_empty_props.properties) == 0


class TestNodeCloning:
    def test_nodes_with_same_type_and_term_are_equal(self):
        type = implica.Variable("CloneType")
        term = implica.BasicTerm("clone_term", type)

        node1 = implica.Node(type, term)
        node2 = implica.Node(type, term)

        # Should be equal (same uid)
        assert node1 == node2
        assert node1.uid() == node2.uid()

    def test_nodes_with_same_type_term_different_properties_are_equal(self):
        type = implica.Variable("ClonePropsType")
        term = implica.BasicTerm("clone_props_term", type)

        node1 = implica.Node(type, term, properties={"prop1": "value1"})
        node2 = implica.Node(type, term, properties={"prop2": "value2"})

        # Should be equal (properties don't affect uid)
        assert node1 == node2
        assert node1.uid() == node2.uid()

        # But properties should be different
        assert node1.properties != node2.properties

    def test_node_properties_isolated_between_instances(self):
        type = implica.Variable("IsolatedType")
        term = implica.BasicTerm("isolated_term", type)

        # Create two nodes with the same type and term but different properties
        node1 = implica.Node(type, term, properties={"color": "red"})
        node2 = implica.Node(type, term, properties={"color": "blue"})

        # They should be equal
        assert node1 == node2

        # But have independent properties
        assert node1.properties["color"] == "red"
        assert node2.properties["color"] == "blue"
