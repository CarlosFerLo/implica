//! # Implica
//!
//! A Python library for type-theoretical graph modeling and querying.
//!
//! This library provides a framework for building and querying graphs based on type theory,
//! with support for Cypher-like query patterns. It combines the power of Rust's performance
//! with Python's ease of use through PyO3 bindings.
//!
//! ## Main Components
//!
//! - **Type System**: Variables and Applications representing type theoretical types
//! - **Terms**: Typed terms in the type theory
//! - **Graph**: Nodes and edges forming the graph structure
//! - **Type Schemas**: Pattern matching for types
//! - **Query System**: Cypher-like queries with patterns for nodes, edges, and paths
//!
//! ## Performance Features
//!
//! - **SHA256 UIDs**: All elements use SHA256 hashes for unique, collision-resistant identification
//! - **Optimized Lookups**: O(1) dictionary-based lookups for nodes and edges by UID
//! - **Type Indexing**: Dynamic indices for efficient type-based queries
//! - **Cached UIDs**: UIDs are computed once and cached to minimize hash operations
//!
//! ## Example Usage
//!
//! ```python
//! import implica
//!
//! # Create a graph
//! graph = implica.Graph()
//!
//! # Query the graph
//! q = graph.query()
//! q.match(node="n", type_schema="Person")
//! results = q.return_(["n"])
//! ```

use pyo3::prelude::*;

pub mod errors;
pub mod graph;
pub mod patterns;
pub mod query;
pub mod term;
pub mod type_schema;
pub mod types;

use graph::{Edge, Graph, Node};
use patterns::{EdgePattern, NodePattern, PathPattern};
use query::Query;
use term::Term;
use type_schema::TypeSchema;
use types::{Application, Variable};

/// A Python module implemented in Rust for type theoretical graph modeling.
///
/// This module exposes all the core classes and functionality of implica to Python,
/// including the type system, graph components, and query mechanisms.
///
/// # Classes Exposed
///
/// - `Variable`: Type variables
/// - `Application`: Application types (A -> B)
/// - `Term`: Typed terms
/// - `Node`: Graph nodes with types
/// - `Edge`: Graph edges with terms
/// - `Graph`: The main graph structure
/// - `TypeSchema`: Type pattern matching
/// - `NodePattern`, `EdgePattern`, `PathPattern`: Query patterns
/// - `Query`: Cypher-like query builder
#[pymodule]
fn implica(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Type system
    m.add_class::<Variable>()?;
    m.add_class::<Application>()?;

    // Terms
    m.add_class::<Term>()?;

    // Graph components
    m.add_class::<Node>()?;
    m.add_class::<Edge>()?;
    m.add_class::<Graph>()?;

    // Query system
    m.add_class::<TypeSchema>()?;
    m.add_class::<NodePattern>()?;
    m.add_class::<EdgePattern>()?;
    m.add_class::<PathPattern>()?;
    m.add_class::<Query>()?;

    Ok(())
}
