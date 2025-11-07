#!/usr/bin/env python3
"""
Demostración de las capacidades mejoradas del PathPattern Parser

Este ejemplo muestra cómo el nuevo parser robusto soporta patrones complejos
de grafos tipo Cypher, incluyendo múltiples nodos, aristas con dirección, y
esquemas de tipos.
"""

import implica


def main():
    print("=" * 70)
    print("PathPattern Parser - Capacidades Mejoradas")
    print("=" * 70)
    print()

    # 1. Patrones simples
    print("1. Nodos simples:")
    pattern1 = implica.PathPattern("(n)")
    print(f"   Pattern: (n)")
    print(f"   Nodos: {len(pattern1.nodes)}, Variable: {pattern1.nodes[0].variable}")
    print()

    # 2. Nodos con tipo
    print("2. Nodos con tipo:")
    pattern2 = implica.PathPattern("(person:Person)")
    print(f"   Pattern: (person:Person)")
    print(f"   Nodos: {len(pattern2.nodes)}, Variable: {pattern2.nodes[0].variable}")
    print()

    # 3. Nodos anónimos
    print("3. Nodos anónimos:")
    pattern3 = implica.PathPattern("(:User)")
    print(f"   Pattern: (:User)")
    print(f"   Nodos: {len(pattern3.nodes)}, Variable: {pattern3.nodes[0].variable}")
    print()

    # 4. Patrón con arista simple
    print("4. Patrón con arista:")
    pattern4 = implica.PathPattern("(a)-[r]->(b)")
    print(f"   Pattern: (a)-[r]->(b)")
    print(f"   Nodos: {len(pattern4.nodes)}, Aristas: {len(pattern4.edges)}")
    print(f"   Dirección: {pattern4.edges[0].direction}")
    print()

    # 5. Patrón complejo con tipos
    print("5. Patrón complejo con tipos:")
    pattern5 = implica.PathPattern("(n:A)-[e:term]->(m:B)")
    print(f"   Pattern: (n:A)-[e:term]->(m:B)")
    print(f"   Nodos: {len(pattern5.nodes)}, Aristas: {len(pattern5.edges)}")
    print(f"   Nodo 1: {pattern5.nodes[0].variable}")
    print(f"   Nodo 2: {pattern5.nodes[1].variable}")
    print(f"   Arista: {pattern5.edges[0].variable}, Dirección: {pattern5.edges[0].direction}")
    print()

    # 6. Múltiples aristas
    print("6. Patrón con múltiples aristas:")
    pattern6 = implica.PathPattern("(a:A)-[e1:term]->(b:B)-[e2]->(c:C)")
    print(f"   Pattern: (a:A)-[e1:term]->(b:B)-[e2]->(c:C)")
    print(f"   Nodos: {len(pattern6.nodes)}, Aristas: {len(pattern6.edges)}")
    for i, node in enumerate(pattern6.nodes):
        print(f"   Nodo {i+1}: {node.variable}")
    for i, edge in enumerate(pattern6.edges):
        print(f"   Arista {i+1}: {edge.variable}, Dirección: {edge.direction}")
    print()

    # 7. Aristas bidireccionales
    print("7. Aristas bidireccionales:")
    pattern7 = implica.PathPattern("(n)-[e]-(m)")
    print(f"   Pattern: (n)-[e]-(m)")
    print(f"   Dirección: {pattern7.edges[0].direction}")
    print()

    # 8. Aristas inversas
    print("8. Aristas inversas:")
    pattern8 = implica.PathPattern("(n)<-[e]-(m)")
    print(f"   Pattern: (n)<-[e]-(m)")
    print(f"   Dirección: {pattern8.edges[0].direction}")
    print()

    # 9. Esquemas de tipos complejos
    print("9. Esquemas de tipos complejos:")
    pattern9 = implica.PathPattern("(n:$A -> B$)")
    print(f"   Pattern: (n:$A -> B$)")
    print(f"   Nodos: {len(pattern9.nodes)}")
    print()

    # 10. Validación de errores
    print("10. Validación de errores:")
    try:
        invalid = implica.PathPattern("(n")
        print("   ERROR: Debería haber fallado")
    except Exception as e:
        print(f"   ✓ Error capturado correctamente: {type(e).__name__}")

    try:
        invalid2 = implica.PathPattern("(n)-[e->(m)")
        print("   ERROR: Debería haber fallado")
    except Exception as e:
        print(f"   ✓ Error capturado correctamente: {type(e).__name__}")

    try:
        invalid3 = implica.PathPattern("")
        print("   ERROR: Debería haber fallado")
    except Exception as e:
        print(f"   ✓ Error capturado correctamente: {type(e).__name__}")

    print()
    print("=" * 70)
    print("✅ Parser robusto funcionando correctamente!")
    print("=" * 70)


if __name__ == "__main__":
    main()
