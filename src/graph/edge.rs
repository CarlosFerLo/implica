use pyo3::prelude::*;
use pyo3::types::PyDict;

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, OnceLock, RwLock};

use crate::errors::ImplicaError;
use crate::graph::alias::SharedPropertyMap;
use crate::graph::node::Node;
use crate::typing::{term_to_python, Term};
use crate::utils::clone_property_map;

#[pyclass]
#[derive(Debug)]
pub struct Edge {
    pub term: Arc<Term>,
    pub start: Arc<RwLock<Node>>,
    pub end: Arc<RwLock<Node>>,
    pub properties: SharedPropertyMap,
    /// Cached UID for performance - computed once and reused
    pub(in crate::graph) uid_cache: OnceLock<String>,
}

impl Clone for Edge {
    fn clone(&self) -> Self {
        Edge {
            term: self.term.clone(),
            start: self.start.clone(),
            end: self.end.clone(),
            properties: Arc::new(RwLock::new(clone_property_map(&self.properties).unwrap())),
            uid_cache: OnceLock::new(),
        }
    }
}

impl Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Edge({}: {} -> {})",
            self.term,
            self.start.read().unwrap().r#type,
            self.end.read().unwrap().r#type
        )
    }
}

impl PartialEq for Edge {
    fn eq(&self, other: &Self) -> bool {
        self.uid() == other.uid()
    }
}

impl Eq for Edge {}

#[pymethods]
impl Edge {
    #[getter]
    pub fn term(&self, py: Python) -> PyResult<Py<PyAny>> {
        let term = self.term.clone();
        term_to_python(py, &term)
    }

    #[getter]
    pub fn start(&self, py: Python) -> PyResult<Py<Node>> {
        Py::new(
            py,
            (*self.start)
                .read()
                .map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("get start".to_string()),
                })?
                .clone(),
        )
    }

    #[getter]
    pub fn end(&self, py: Python) -> PyResult<Py<Node>> {
        Py::new(
            py,
            (*self.end)
                .read()
                .map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("get end".to_string()),
                })?
                .clone(),
        )
    }

    #[getter]
    pub fn get_properties(&self, py: Python) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        let props = self
            .properties
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("execute match edge".to_string()),
            })?;
        for (k, v) in props.iter() {
            dict.set_item(k, v.clone_ref(py))?;
        }
        Ok(dict.into())
    }

    #[setter]
    pub fn set_properties(&self, props: HashMap<String, Py<PyAny>>) -> PyResult<()> {
        let mut guard = self
            .properties
            .write()
            .map_err(|e| ImplicaError::LockError {
                rw: "write".to_string(),
                message: e.to_string(),
                context: Some("edge set properties".to_string()),
            })?;
        guard.clear();
        guard.extend(props);
        Ok(())
    }

    pub fn uid(&self) -> &str {
        self.uid_cache.get_or_init(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"edge:");
            hasher.update(self.term.uid().as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on uid
        self == other
    }
}

impl Edge {
    pub fn new(
        term: Arc<Term>,
        start: Arc<RwLock<Node>>,
        end: Arc<RwLock<Node>>,
        properties: Option<SharedPropertyMap>,
    ) -> Self {
        Edge {
            term,
            start,
            end,
            properties: properties.unwrap_or(Arc::new(RwLock::new(HashMap::new()))),
            uid_cache: OnceLock::new(),
        }
    }
}
