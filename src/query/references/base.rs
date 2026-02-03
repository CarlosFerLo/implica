use crate::query::references::{EdgeRef, NodeRef, TermRef, TypeRef};
use pyo3::prelude::*;
use pyo3::IntoPyObject;

#[derive(Debug, PartialEq, Eq)]
pub enum Reference {
    Edge(EdgeRef),
    Node(NodeRef),
    Term(TermRef),
    Type(TypeRef),
}

impl<'py> IntoPyObject<'py> for Reference {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Reference::Type(v) => Ok(v.into_pyobject(py)?.into_any()),
            Reference::Term(v) => Ok(v.into_pyobject(py)?.into_any()),
            Reference::Node(v) => Ok(v.into_pyobject(py)?.into_any()),
            Reference::Edge(v) => Ok(v.into_pyobject(py)?.into_any()),
        }
    }
}
