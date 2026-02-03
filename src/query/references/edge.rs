use error_stack::ResultExt;
use hex;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::ctx;
use crate::errors::IntoPyResult;
use crate::graph::{Graph, Uid};

#[pyclass(name = "Edge")]
#[derive(Debug, Clone)]
pub struct EdgeRef {
    graph: Arc<Graph>,

    uid: (Uid, Uid),
}

impl PartialEq for EdgeRef {
    fn eq(&self, other: &Self) -> bool {
        self.uid == other.uid
    }
}
impl Eq for EdgeRef {}

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
        let map = self
            .graph
            .edge_properties(&self.uid)
            .attach(ctx!("edge reference - get properties"))
            .into_py_result()?;
        map.into_pyobject(py) // TODO: add some kind of attachment
    }

    pub fn __str__(&self) -> PyResult<String> {
        self.graph
            .edge_to_string(&self.uid)
            .attach(ctx!("edge reference - to string"))
            .into_py_result()
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self == other
    }
}
