use pyo3::prelude::*;
use std::sync::Arc;

use crate::graph::{Graph, Uid};

#[pyclass(name = "Node")]
#[derive(Debug, Clone)]
pub struct NodeRef {
    graph: Arc<Graph>,

    uid: Uid,
}

impl NodeRef {
    pub fn new(graph: Arc<Graph>, uid: Uid) -> Self {
        NodeRef { graph, uid }
    }
}

#[pymethods]
impl NodeRef {
    pub fn uid(&self) -> String {
        hex::encode(self.uid)
    }

    pub fn properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let map = self.graph.node_properties(&self.uid)?;

        map.into_pyobject(py)
    }

    pub fn __str__(&self) -> PyResult<String> {
        self.graph.node_to_string(&self.uid).map_err(|e| e.into())
    }
}
