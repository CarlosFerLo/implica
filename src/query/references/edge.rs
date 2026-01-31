use hex;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::graph::{Graph, Uid};

#[pyclass(name = "Edge")]
#[derive(Debug, Clone)]
pub struct EdgeRef {
    graph: Arc<Graph>,

    uid: (Uid, Uid),
}

impl EdgeRef {
    pub fn new(graph: Arc<Graph>, uid: (Uid, Uid)) -> Self {
        EdgeRef { graph, uid }
    }
}

#[pymethods]
impl EdgeRef {
    pub fn uid(&self) -> (String, String) {
        (hex::encode(self.uid.0), hex::encode(self.uid.1))
    }

    pub fn properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let map = self.graph.edge_properties(&self.uid)?;
        map.into_pyobject(py)
    }

    pub fn __str__(&self) -> PyResult<String> {
        self.graph.edge_to_string(&self.uid).map_err(|e| e.into())
    }
}
