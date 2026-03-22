use error_stack::ResultExt;
use hex;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::{
    ctx,
    errors::IntoPyResult,
    graph::{Graph, Uid},
};

#[pyclass(name = "Type")]
#[derive(Debug, Clone)]
pub struct TypeRef {
    graph: Arc<Graph>,

    uid: Uid,
}

impl PartialEq for TypeRef {
    fn eq(&self, other: &Self) -> bool {
        self.uid == other.uid
    }
}

impl Eq for TypeRef {}

impl TypeRef {
    pub fn new(graph: Arc<Graph>, uid: Uid) -> Self {
        TypeRef { graph, uid }
    }
}

#[pymethods]
impl TypeRef {
    pub fn uid(&self) -> String {
        hex::encode(self.uid)
    }

    pub fn as_var(&self) -> PyResult<Option<String>> {
        self.graph
            .type_as_var(&self.uid)
            .attach(ctx!("type reference - type as var"))
            .into_py_result()
    }

    pub fn as_arrow(&self) -> PyResult<Option<(TypeRef, TypeRef)>> {
        self.graph
            .type_as_arrow(&self.uid)
            .map(|pair| {
                if let Some((left, right)) = pair {
                    Some((
                        TypeRef::new(self.graph.clone(), left),
                        TypeRef::new(self.graph.clone(), right),
                    ))
                } else {
                    None
                }
            })
            .attach(ctx!("type reference - type as arrow"))
            .into_py_result()
    }

    pub fn __str__(&self) -> PyResult<String> {
        self.graph
            .type_to_string(&self.uid)
            .attach(ctx!("type reference - to string"))
            .into_py_result()
    }

    pub fn __repr__(&self) -> PyResult<String> {
        self.__str__()
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self == other
    }
}
