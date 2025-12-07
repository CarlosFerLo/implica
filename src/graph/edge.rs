use pyo3::prelude::*;

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, OnceLock, RwLock};

use crate::errors::ImplicaError;
use crate::graph::node::Node;
use crate::graph::property_map::{
    clone_property_map, property_map_to_python, python_to_property_map, SharedPropertyMap,
};
use crate::typing::{python_to_term, term_to_python, Term};

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

impl Edge {
    pub fn new(
        term: Arc<Term>,
        start: Arc<RwLock<Node>>,
        end: Arc<RwLock<Node>>,
        properties: Option<SharedPropertyMap>,
    ) -> Result<Self, ImplicaError> {
        let term_type = term.r#type();
        if let Some(arr) = term_type.as_arrow() {
            let start = start.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("new edge".to_string()),
            })?;

            if arr.left != start.r#type {
                return Err(ImplicaError::TypeMismatch {
                    expected: arr.left.to_string(),
                    got: start.r#type.to_string(),
                    context: Some("new edge".to_string()),
                });
            }

            let end = end.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("new edge".to_string()),
            })?;

            if arr.right != end.r#type {
                return Err(ImplicaError::TypeMismatch {
                    expected: arr.right.to_string(),
                    got: end.r#type.to_string(),
                    context: Some("new edge".to_string()),
                });
            }
        } else {
            return Err(ImplicaError::InvalidType {
                reason: "Edges must contain terms of an application type".to_string(),
            });
        }

        Ok(Edge {
            term,
            start,
            end,
            properties: properties.unwrap_or(Arc::new(RwLock::new(HashMap::new()))),
            uid_cache: OnceLock::new(),
        })
    }
}

#[pymethods]
impl Edge {
    #[new]
    #[pyo3(signature=(term, start, end, properties = None))]
    pub fn py_new(
        py: Python,
        term: Py<PyAny>,
        start: Node,
        end: Node,
        properties: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let term_obj = python_to_term(term.bind(py))?;
        let props_obj = match properties {
            Some(props) => Some(python_to_property_map(props.bind(py))?),
            None => None,
        };

        Ok(Edge::new(
            Arc::new(term_obj),
            Arc::new(RwLock::new(start.clone())),
            Arc::new(RwLock::new(end.clone())),
            props_obj.map(|p| Arc::new(RwLock::new(p))),
        )?)
    }

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
    pub fn get_properties(&self, py: Python) -> PyResult<Py<PyAny>> {
        let props = self
            .properties
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("execute match edge".to_string()),
            })?;
        property_map_to_python(py, &props)
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

    fn __hash__(&self) -> u64 {
        let uid_str = self.uid();
        let truncated = &uid_str[..16];
        u64::from_str_radix(truncated, 16).unwrap_or(0)
    }
}
