use hex;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::graph::{Graph, Uid};

#[pyclass(name = "Type")]
#[derive(Debug, Clone)]
pub struct TypeRef {
    graph: Arc<Graph>,

    uid: Uid,
}

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

    pub fn __str__(&self) -> PyResult<String> {
        self.graph.type_to_string(&self.uid).map_err(|e| e.into())
    }
}
