use pyo3::prelude::*;
use pyo3::types::PyAny;

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, OnceLock, RwLock};

use crate::errors::ImplicaError;
use crate::graph::property_map::{
    clone_property_map, properaty_map_to_python, python_to_property_map, SharedPropertyMap,
};
use crate::typing::{python_to_term, python_to_type, term_to_python, type_to_python, Term, Type};

#[pyclass]
#[derive(Debug)]
pub struct Node {
    pub r#type: Arc<Type>,
    pub term: Option<Arc<RwLock<Term>>>,
    pub properties: SharedPropertyMap,
    /// Cached UID for performance - computed once and reused
    pub(in crate::graph) uid_cache: OnceLock<String>,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Node {
            r#type: self.r#type.clone(),
            term: self.term.clone(),
            properties: Arc::new(RwLock::new(clone_property_map(&self.properties).unwrap())),
            uid_cache: OnceLock::new(),
        }
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.term {
            Some(term_lock) => {
                let term = term_lock.read().unwrap();
                write!(f, "Node({}, {})", self.r#type, term)
            }
            None => write!(f, "Node({})", self.r#type),
        }
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.uid() == other.uid()
    }
}

impl Eq for Node {}

impl Node {
    pub fn new(
        r#type: Arc<Type>,
        term: Option<Arc<RwLock<Term>>>,
        properties: Option<HashMap<String, Py<PyAny>>>,
    ) -> Result<Self, ImplicaError> {
        if let Some(term_lock) = &term {
            let term = term_lock.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("new node".to_string()),
            })?;

            if term.r#type() != r#type {
                return Err(ImplicaError::TypeMismatch {
                    expected: r#type.to_string(),
                    got: term.r#type().to_string(),
                    context: Some("new node".to_string()),
                });
            }
        }

        Ok(Node {
            r#type,
            term,
            properties: Arc::new(RwLock::new(properties.unwrap_or_default())),
            uid_cache: OnceLock::new(),
        })
    }
}

#[pymethods]
impl Node {
    #[new]
    #[pyo3(signature=(r#type, term = None, properties = None))]
    pub fn py_new(
        py: Python,
        r#type: Py<PyAny>,
        term: Option<Py<PyAny>>,
        properties: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let type_obj = python_to_type(r#type.bind(py))?;
        let term_obj = match term {
            Some(t) => Some(python_to_term(t.bind(py))?),
            None => None,
        };
        let props_obj = match properties {
            Some(props) => Some(python_to_property_map(props.bind(py))?),
            None => None,
        };

        Ok(Node::new(
            Arc::new(type_obj),
            term_obj.map(|t| Arc::new(RwLock::new(t))),
            props_obj,
        )?)
    }

    #[getter]
    pub fn get_type(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.r#type)
    }

    #[getter]
    pub fn get_term(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match &self.term {
            Some(term_lock) => {
                let term = term_lock.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("get term".to_string()),
                })?;
                term_to_python(py, &term).map(Some)
            }
            None => Ok(None),
        }
    }

    #[getter]
    pub fn get_properties(&self, py: Python) -> PyResult<Py<PyAny>> {
        let props = self
            .properties
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("get term".to_string()),
            })?;
        properaty_map_to_python(py, &props)
    }

    pub fn uid(&self) -> &str {
        self.uid_cache.get_or_init(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"node:");
            hasher.update(self.r#type.uid().as_bytes());

            if let Some(ref term_lock) = self.term {
                let term = term_lock.read().unwrap();
                hasher.update(b":");
                hasher.update(term.uid().as_bytes());
            }

            format!("{:x}", hasher.finalize())
        })
    }

    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on uid
        self.uid() == other.uid()
    }

    fn __hash__(&self) -> u64 {
        let uid_str = self.uid();
        let truncated = &uid_str[..16];
        u64::from_str_radix(truncated, 16).unwrap_or(0)
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }
}
