import pytest
import implica


class TestEdgeCreation:
    def test_create_node(self, node_a, node_b, term_ab):
        edge = implica.Edge(term_ab, node_a, node_b)

        assert edge.start == node_a
        assert edge.end == node_b
        assert edge.term == term_ab

    def test_edge_creation_term_of_different_type_raises(self, node_a, node_b, type_a, type_b):

        term_a = implica.BasicTerm("a", type_a)

        with pytest.raises(TypeError):
            implica.Edge(term_a, node_a, node_b)

        term_aa = implica.BasicTerm("aa", implica.Arrow(type_a, type_a))

        with pytest.raises(TypeError):
            implica.Edge(term_aa, node_a, node_b)


class TestEdgeBasicMethods:
    def test_get_start_end_term(self, edge_ab, node_a, node_b, term_ab):
        assert edge_ab.start == node_a
        assert edge_ab.end == node_b
        assert edge_ab.term == term_ab

    def test_get_properties(self, node_a, node_b, term_ab):
        properties = {"weight": 5, "color": "red"}
        edge = implica.Edge(term_ab, node_a, node_b, properties=properties)

        assert edge.properties["weight"] == 5
        assert edge.properties["color"] == "red"

    def test_edge_immutable_properties(self, node_a, node_b, term_ab):
        properties = {"immutable_key": "immutable_value"}
        edge = implica.Edge(term_ab, node_a, node_b, properties=properties)

        with pytest.raises(TypeError):
            edge.properties["immutable_key"] = "new_value"

    def test_edge_immutable_start(self, edge_ab, node_b):
        with pytest.raises(AttributeError):
            edge_ab.start = node_b

    def test_edge_immutable_end(self, edge_ab, node_a):
        with pytest.raises(AttributeError):
            edge_ab.end = node_a

    def test_edge_immutable_term(self, edge_ab, term_ab):
        with pytest.raises(AttributeError):
            edge_ab.term = term_ab

    def test_edge_uid_is_string(self, edge_ab):
        uid = edge_ab.uid()
        assert isinstance(uid, str)

    def test_edge_uid_is_unique(self, edge_ab, edge_ba):
        assert edge_ab.uid() != edge_ba.uid()

    def test_edge_uid_is_consistent(self, edge_ab):
        uid1 = edge_ab.uid()
        uid2 = edge_ab.uid()
        assert uid1 == uid2

    def test_edge_uid_varies_with_different_terms(self, node_a, node_b, type_a, type_b):
        term1 = implica.BasicTerm("term1", implica.Arrow(type_a, type_b))
        term2 = implica.BasicTerm("term2", implica.Arrow(type_a, type_b))

        edge1 = implica.Edge(term1, node_a, node_b)
        edge2 = implica.Edge(term2, node_a, node_b)

        assert edge1.uid() != edge2.uid()

    def test_edge_uid_consistent_with_same_term(self, node_a, node_b, term_ab):
        edge1 = implica.Edge(term_ab, node_a, node_b)
        edge2 = implica.Edge(term_ab, node_a, node_b)

        assert edge1.uid() == edge2.uid()

    def test_edge_uid_is_consistent_with_different_properties(self, node_a, node_b, term_ab):
        properties1 = {"weight": 5}
        properties2 = {"weight": 10}

        edge1 = implica.Edge(term_ab, node_a, node_b, properties=properties1)
        edge2 = implica.Edge(term_ab, node_a, node_b, properties=properties2)

        assert edge1.uid() == edge2.uid()

    def test_edge_uid_varies_with_different_start_nodes(self, type_a, type_b, term_ab):
        term_a1 = implica.BasicTerm("a1", type_a)
        term_a2 = implica.BasicTerm("a2", type_a)

        node_a1 = implica.Node(type_a, term_a1)
        node_a2 = implica.Node(type_a, term_a2)
        node_b = implica.Node(type_b)

        edge1 = implica.Edge(term_ab, node_a1, node_b)
        edge2 = implica.Edge(term_ab, node_a2, node_b)

        # UIDs should be the same since they only depend on the edge term
        assert edge1.uid() == edge2.uid()

    def test_edge_uid_varies_with_different_end_nodes(self, type_a, type_b, term_ab):
        term_b1 = implica.BasicTerm("b1", type_b)
        term_b2 = implica.BasicTerm("b2", type_b)

        node_a = implica.Node(type_a)
        node_b1 = implica.Node(type_b, term_b1)
        node_b2 = implica.Node(type_b, term_b2)

        edge1 = implica.Edge(term_ab, node_a, node_b1)
        edge2 = implica.Edge(term_ab, node_a, node_b2)

        # UIDs should be the same since they only depend on the edge term
        assert edge1.uid() == edge2.uid()

    def test_edge_equality_same_term_start_end(self, node_a, node_b, term_ab):
        edge1 = implica.Edge(term_ab, node_a, node_b)
        edge2 = implica.Edge(term_ab, node_a, node_b)

        assert edge1 == edge2

    def test_edge_inequality(self, edge_ab, edge_ba):
        assert edge_ab != edge_ba

    def test_edge_inequality_different_terms(self, node_a, node_b, type_a, type_b):
        term1 = implica.BasicTerm("f1", implica.Arrow(type_a, type_b))
        term2 = implica.BasicTerm("f2", implica.Arrow(type_a, type_b))

        edge1 = implica.Edge(term1, node_a, node_b)
        edge2 = implica.Edge(term2, node_a, node_b)

        assert edge1 != edge2

    def test_edge_hash_consistency(self, edge_ab):
        hash1 = hash(edge_ab)
        hash2 = hash(edge_ab)

        assert hash1 == hash2

    def test_edge_hash_equality_for_equal_edges(self, node_a, node_b, term_ab):
        edge1 = implica.Edge(term_ab, node_a, node_b)
        edge2 = implica.Edge(term_ab, node_a, node_b)

        assert hash(edge1) == hash(edge2)


class TestEdgeRepresentation:
    def test_edge_repr(self, edge_ab):
        repr_str = repr(edge_ab)
        assert "Edge(f: A -> B)" == repr_str

    def test_edge_str(self, edge_ab):
        str_repr = str(edge_ab)
        assert "Edge(f: A -> B)" == str_repr


class TestEdgePropertiesDeepImmutability:
    def test_nested_dict_immutability(self, node_a, node_b, term_ab):
        properties = {"outer": {"inner": "value"}}
        edge = implica.Edge(term_ab, node_a, node_b, properties=properties)

        # Verify the properties exist
        assert edge.properties["outer"]["inner"] == "value"

        # Try to modify the nested dictionary
        with pytest.raises(TypeError):
            edge.properties["outer"]["inner"] = "new_value"

    def test_nested_list_immutability(self, node_a, node_b, term_ab):
        properties = {"items": [1, 2, 3]}
        edge = implica.Edge(term_ab, node_a, node_b, properties=properties)

        # Verify the list is converted to an immutable tuple
        assert edge.properties["items"] == (1, 2, 3)
        assert isinstance(edge.properties["items"], tuple)

        # Tuples don't have append method
        with pytest.raises(AttributeError):
            edge.properties["items"].append(4)

        # Try to modify the tuple (tuples don't support item assignment)
        with pytest.raises(TypeError):
            edge.properties["items"][0] = 99

    def test_deeply_nested_structure_immutability(self, node_a, node_b, term_ab):
        properties = {"level1": {"level2": {"level3": ["a", "b", "c"]}}}
        edge = implica.Edge(term_ab, node_a, node_b, properties=properties)

        # Verify access works and lists are converted to tuples
        assert edge.properties["level1"]["level2"]["level3"][0] == "a"
        assert isinstance(edge.properties["level1"]["level2"]["level3"], tuple)

        # Try to modify at various levels
        with pytest.raises(TypeError):
            edge.properties["level1"]["level2"]["level3"][0] = "z"

        # Tuples don't have append method
        with pytest.raises(AttributeError):
            edge.properties["level1"]["level2"]["level3"].append("d")

    def test_properties_original_dict_modification_doesnt_affect_edge(
        self, node_a, node_b, term_ab
    ):
        original_props = {"mutable": {"key": "original"}, "another": "value"}
        edge = implica.Edge(term_ab, node_a, node_b, properties=original_props)

        # Modify the original dictionary
        original_props["mutable"]["key"] = "modified"
        original_props["another"] = "changed"

        # Edge properties should remain unchanged
        assert edge.properties["mutable"]["key"] == "original"
        assert edge.properties["another"] == "value"


class TestEdgePropertiesEdgeCases:
    def test_empty_properties_dict(self, node_a, node_b, term_ab):
        edge = implica.Edge(term_ab, node_a, node_b, properties={})

        assert len(edge.properties) == 0
        # Properties are returned as MappingProxyType (immutable dict-like)
        from types import MappingProxyType

        assert isinstance(edge.properties, MappingProxyType)

    def test_properties_with_none_value(self, node_a, node_b, term_ab):
        properties = {"key_with_none": None}
        edge = implica.Edge(term_ab, node_a, node_b, properties=properties)

        assert edge.properties["key_with_none"] is None

    def test_properties_with_mixed_types(self, node_a, node_b, term_ab):
        properties = {
            "string": "text",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "none": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
        }
        edge = implica.Edge(term_ab, node_a, node_b, properties=properties)

        assert edge.properties["string"] == "text"
        assert edge.properties["integer"] == 42
        assert edge.properties["float"] == 3.14
        assert edge.properties["boolean"] is True
        assert edge.properties["none"] is None
        # Lists are converted to tuples for immutability
        assert edge.properties["list"] == (1, 2, 3)
        assert isinstance(edge.properties["list"], tuple)
        # Dicts are converted to MappingProxyType for immutability
        assert edge.properties["dict"]["nested"] == "value"
        from types import MappingProxyType

        assert isinstance(edge.properties["dict"], MappingProxyType)

    def test_no_properties_vs_empty_properties(self, node_a, node_b, term_ab):
        edge_no_props = implica.Edge(term_ab, node_a, node_b)
        edge_empty_props = implica.Edge(term_ab, node_a, node_b, properties={})

        # Both should behave the same
        assert len(edge_no_props.properties) == 0
        assert len(edge_empty_props.properties) == 0


class TestEdgeIsolation:
    def test_edge_properties_isolated_between_instances(self, node_a, node_b, term_ab):
        # Create two edges with the same term and nodes but different properties
        edge1 = implica.Edge(term_ab, node_a, node_b, properties={"weight": 5})
        edge2 = implica.Edge(term_ab, node_a, node_b, properties={"weight": 10})

        # They should be equal (based on uid)
        assert edge1 == edge2

        # But have independent properties
        assert edge1.properties["weight"] == 5
        assert edge2.properties["weight"] == 10

    def test_edge_node_independence(self, type_a, type_b, term_ab):
        # Create nodes with properties
        node_a1 = implica.Node(type_a, properties={"value": 1})
        node_b1 = implica.Node(type_b, properties={"value": 2})

        edge = implica.Edge(term_ab, node_a1, node_b1)

        # Nodes in edge should have the same properties
        assert edge.start.properties["value"] == 1
        assert edge.end.properties["value"] == 2
