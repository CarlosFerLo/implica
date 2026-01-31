use hex;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::graph::{Graph, Uid};

#[pyclass(name = "Term")]
#[derive(Debug, Clone)]
pub struct TermRef {
    graph: Arc<Graph>,

    uid: Uid,
}

impl TermRef {
    pub fn new(graph: Arc<Graph>, uid: Uid) -> Self {
        TermRef { graph, uid }
    }
}

#[pymethods]
impl TermRef {
    pub fn uid(&self) -> String {
        hex::encode(self.uid)
    }

    pub fn __str__(&self) -> PyResult<String> {
        self.graph.term_to_string(&self.uid).map_err(|e| e.into())
    }
}
