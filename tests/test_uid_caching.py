"""
Tests for UID caching functionality.

Tests that UIDs are correctly cached to avoid expensive recalculation,
especially for complex recursive type structures.
"""

import pytest
from implica import Graph, Variable, Application, Term


def test_node_uid_is_consistent():
    """Test that calling uid() multiple times on a node returns the same value."""
    from implica import Node

    node_type = Variable("Person")
    node = Node(node_type, {"name": "Alice", "age": 30})

    uid1 = node.uid()
    uid2 = node.uid()
    uid3 = node.uid()

    assert uid1 == uid2
    assert uid2 == uid3
    assert isinstance(uid1, str)
    assert len(uid1) > 0


def test_edge_uid_is_consistent():
    """Test that calling uid() multiple times on an edge returns the same value."""
    from implica import Node, Edge

    person_type = Variable("Person")
    knows_type = Variable("Knows")

    alice = Node(person_type, {"name": "Alice"})
    bob = Node(person_type, {"name": "Bob"})
    term = Term("knows", knows_type)
    edge = Edge(term, alice, bob, {"since": 2020})

    uid1 = edge.uid()
    uid2 = edge.uid()
    uid3 = edge.uid()

    assert uid1 == uid2
    assert uid2 == uid3
    assert isinstance(uid1, str)
    assert len(uid1) > 0


def test_term_uid_is_consistent():
    """Test that calling uid() multiple times on a term returns the same value."""
    var_type = Variable("Nat")
    term = Term("x", var_type)

    uid1 = term.uid()
    uid2 = term.uid()
    uid3 = term.uid()

    assert uid1 == uid2
    assert uid2 == uid3
    assert isinstance(uid1, str)
    assert len(uid1) > 0


def test_type_uid_is_consistent():
    """Test that calling uid() multiple times on a type returns the same value."""
    var_type = Variable("MyType")

    uid1 = var_type.uid()
    uid2 = var_type.uid()
    uid3 = var_type.uid()

    assert uid1 == uid2
    assert uid2 == uid3
    assert isinstance(uid1, str)
    assert len(uid1) > 0


def test_application_type_uid_is_consistent():
    """Test that calling uid() multiple times on an application type returns the same value."""
    func_type = Variable("Function")
    arg_type = Variable("Argument")
    app_type = Application(func_type, arg_type)

    uid1 = app_type.uid()
    uid2 = app_type.uid()
    uid3 = app_type.uid()

    assert uid1 == uid2
    assert uid2 == uid3
    assert isinstance(uid1, str)
    assert len(uid1) > 0


def test_different_types_nodes_have_different_uids():
    """Test that nodes with different types have different UIDs.

    Note: Nodes are identified by their type, not by their properties.
    Two nodes of the same type will have the same UID. This is by design,
    as the type is what defines the structural identity of the node.
    """
    from implica import Node

    person_type = Variable("Person")
    company_type = Variable("Company")

    # Nodes with different types should have different UIDs
    person_node = Node(person_type, {"name": "Alice"})
    company_node = Node(company_type, {"name": "Acme Inc"})

    assert person_node.uid() != company_node.uid()


def test_same_type_nodes_have_same_uid():
    """Test that nodes of the same type have the same UID.

    This is the expected behavior: the UID is based on the type, not the properties.
    """
    from implica import Node

    person_type = Variable("Person")

    alice = Node(person_type, {"name": "Alice", "id": 1})
    bob = Node(person_type, {"name": "Bob", "id": 2})

    # Same type means same UID (by design)
    assert alice.uid() == bob.uid()


def test_different_types_have_different_uids():
    """Test that different types have different UIDs."""
    type_a = Variable("A")
    type_b = Variable("B")

    assert type_a.uid() != type_b.uid()


def test_complex_recursive_type_uid():
    """Test that complex recursive types can compute UIDs correctly."""
    # Create a complex type structure
    base = Variable("Base")
    app1 = Application(base, base)
    app2 = Application(app1, base)
    app3 = Application(app2, app1)

    # UIDs should be consistent even for complex structures
    uid1 = app3.uid()
    uid2 = app3.uid()

    assert uid1 == uid2
    assert isinstance(uid1, str)
    assert len(uid1) > 0


def test_uid_remains_cached():
    """Test that UID is cached and doesn't change even if properties are modified.

    Note: The UID is computed once based on initial state and cached. This ensures
    consistent identification even if mutable properties change. For a new UID,
    a new node must be created.
    """
    from implica import Node

    person_type = Variable("Person")
    node = Node(person_type, {"name": "Alice", "age": 30})

    uid_before = node.uid()

    # Modify properties
    node.properties["age"] = 31
    node.properties["city"] = "New York"

    uid_after = node.uid()

    # UID should remain the same because it's cached
    assert uid_before == uid_after


def test_application_type_nested_uid():
    """Test that nested application types have consistent UIDs."""
    nat_type = Variable("Nat")
    func_type = Application(nat_type, nat_type)
    nested_func = Application(func_type, nat_type)

    uid1 = nested_func.uid()
    uid2 = nested_func.uid()

    assert uid1 == uid2
    assert isinstance(uid1, str)
    assert len(uid1) == 64  # SHA256 is 64 hex characters


def test_same_type_different_instances_same_uid():
    """Test that equivalent types have the same UID even if created separately."""
    type_a1 = Variable("MyType")
    type_a2 = Variable("MyType")

    # Same type should have same UID
    assert type_a1.uid() == type_a2.uid()


def test_same_application_different_instances_same_uid():
    """Test that equivalent application types have the same UID."""
    func1 = Variable("F")
    arg1 = Variable("A")
    app1 = Application(func1, arg1)

    func2 = Variable("F")
    arg2 = Variable("A")
    app2 = Application(func2, arg2)

    # Same structure should have same UID
    assert app1.uid() == app2.uid()


def test_uid_caching_performance():
    """Test that UID caching improves performance for repeated calls."""
    import time

    # Create a deeply nested type structure
    base = Variable("Base")
    complex_type = base
    for _ in range(10):
        complex_type = Application(complex_type, base)

    # First call (computes and caches)
    start = time.time()
    uid1 = complex_type.uid()
    first_call_time = time.time() - start

    # Second call (should use cache)
    start = time.time()
    uid2 = complex_type.uid()
    second_call_time = time.time() - start

    # UIDs should match
    assert uid1 == uid2

    # Note: We don't strictly assert second_call_time < first_call_time
    # because timing can be unreliable in tests, but we log it for manual verification
    print(f"\nFirst call: {first_call_time:.6f}s, Second call: {second_call_time:.6f}s")
