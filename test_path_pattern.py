#!/usr/bin/env python3
"""
Test script to verify PathPattern parsing with term schemas
"""

import sys

sys.path.insert(0, "./python")

try:
    import implica

    print("Testing PathPattern with term schemas...\n")

    # Test 1: Simple path with type and term schemas
    print("Test 1: (n:Person:name)-[e]->(m:Person:age)")
    try:
        pattern = implica.PathPattern("(n:Person:name)-[e]->(m:Person:age)")
        print(f"  ✓ Pattern created: {pattern}")
        print(f"    Nodes: {len(pattern.nodes)}")
        print(f"    Edges: {len(pattern.edges)}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test 2: Anonymous nodes with type and term
    print("\nTest 2: (:Employee:salary)-[works_at]->(:Company:revenue)")
    try:
        pattern = implica.PathPattern("(:Employee:salary)-[works_at]->(:Company:revenue)")
        print(f"  ✓ Pattern created: {pattern}")
        print(f"    Nodes: {len(pattern.nodes)}")
        print(f"    Edges: {len(pattern.edges)}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test 3: Only type schema (backward compatibility)
    print("\nTest 3: (n:Person)-[e]->(m:Company)")
    try:
        pattern = implica.PathPattern("(n:Person)-[e]->(m:Company)")
        print(f"  ✓ Pattern created: {pattern}")
        print(f"    Nodes: {len(pattern.nodes)}")
        print(f"    Edges: {len(pattern.edges)}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test 4: Only variable (backward compatibility)
    print("\nTest 4: (n)-[e]->(m)")
    try:
        pattern = implica.PathPattern("(n)-[e]->(m)")
        print(f"  ✓ Pattern created: {pattern}")
        print(f"    Nodes: {len(pattern.nodes)}")
        print(f"    Edges: {len(pattern.edges)}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test 5: Complex path
    print("\nTest 5: (a:A:x)-[r1]->(b:B:y)-[r2]->(c:C:z)")
    try:
        pattern = implica.PathPattern("(a:A:x)-[r1]->(b:B:y)-[r2]->(c:C:z)")
        print(f"  ✓ Pattern created: {pattern}")
        print(f"    Nodes: {len(pattern.nodes)}")
        print(f"    Edges: {len(pattern.edges)}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    # Test 6: Type without term
    print("\nTest 6: (n:Person:)-[e]->(m)")
    try:
        pattern = implica.PathPattern("(n:Person:)-[e]->(m)")
        print(f"  ✓ Pattern created: {pattern}")
        print(f"    Nodes: {len(pattern.nodes)}")
        print(f"    Edges: {len(pattern.edges)}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

    print("\n✓ All tests completed!")

except ImportError as e:
    print(f"Error importing implica: {e}")
    print("Please build the module first with: maturin develop")
    sys.exit(1)
