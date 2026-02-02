use error_stack::ResultExt;
use hex;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::ctx;
use crate::errors::IntoPyResult;
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
        self.graph
            .term_to_string(&self.uid)
            .attach(ctx!("term reference - to string"))
            .into_py_result()
    }
}
