use pyo3::prelude::*;

mod constants;
mod errors;
mod graph;
mod macros;
mod matches;
mod patterns;
mod properties;
mod query;
mod typing;
mod utils;

pub use constants::Constant;
pub use graph::PyGraph;
pub use query::references::*;
pub use query::Query;

#[pymodule]
fn implica(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyGraph>()?;

    m.add_class::<Query>()?;

    m.add_class::<EdgeRef>()?;
    m.add_class::<NodeRef>()?;
    m.add_class::<TermRef>()?;
    m.add_class::<TypeRef>()?;

    m.add_class::<Constant>()?;

    Ok(())
}
