use pyo3::prelude::*;

pub mod context;
pub mod errors;
pub mod graph;
pub mod patterns;
pub mod query;
pub mod typing;
pub mod utils;

use graph::{Edge, Graph, Node};
use patterns::{EdgePattern, NodePattern, PathPattern, TypeSchema};
use query::Query;
use typing::{Application, Arrow, BasicTerm, Variable};

/// A Python module implemented in Rust for type theoretical graph modeling.
///
/// This module exposes all the core classes and functionality of implica to Python,
/// including the type system, graph components, and query mechanisms.
///
/// # Classes Exposed
///
/// - `Variable`: Type variables
/// - `Arrow`: Arrow types (A -> B)
/// - `Term`: Typed terms
/// - `Node`: Graph nodes with types
/// - `Edge`: Graph edges with terms
/// - `Graph`: The main graph structure
/// - `IndexConfig`: Configuration for graph optimization (bloom filters)
/// - `TypeSchema`: Type pattern matching
/// - `NodePattern`, `EdgePattern`, `PathPattern`: Query patterns
/// - `Query`: Cypher-like query builder
#[pymodule]
fn implica(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Type system
    m.add_class::<Variable>()?;
    m.add_class::<Arrow>()?;

    // Terms
    m.add_class::<BasicTerm>()?;
    m.add_class::<Application>()?;

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
