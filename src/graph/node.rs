use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, OnceLock, RwLock};

use crate::errors::ImplicaError;
use crate::graph::alias::{PropertyMap, SharedPropertyMap};
use crate::typing::{term_to_python, type_to_python, Term, Type};
use crate::utils::clone_property_map;

/// Represents a node in the graph (a type in the model).
///
/// Nodes are the vertices of the graph, each representing a type. They can have
/// associated properties stored as a Python dictionary.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Create a node with a type
/// person_type = implica.Variable("Person")
/// node = implica.Node(person_type)
///
/// # Create a node with properties
/// node = implica.Node(person_type, {"name": "John", "age": 30})
/// ```
///
/// # Fields
///
/// * `type` - The type this node represents (accessible via get_type())
/// * `term` - Optional term for this node (accessible via get_term())
/// * `properties` - A dictionary of node properties
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
    ) -> Self {
        Node {
            r#type,
            term,
            properties: Arc::new(RwLock::new(properties.unwrap_or_default())),
            uid_cache: OnceLock::new(),
        }
    }
}

#[pymethods]
impl Node {
    /// Gets the type of this node.
    ///
    /// # Returns
    ///
    /// The type as a Python object (Variable or Arrow)
    #[getter]
    pub fn get_type(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.r#type)
    }

    /// Gets the term of this node.
    ///
    /// # Returns
    ///
    /// The term as a Python object, or None if no term is set
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
    pub fn get_properties(&self, py: Python) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        let props = self
            .properties
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("get term".to_string()),
            })?;
        for (k, v) in props.iter() {
            dict.set_item(k, v.clone_ref(py))?;
        }
        Ok(dict.into())
    }

    #[setter]
    pub fn set_properties(&self, props: PropertyMap) -> PyResult<()> {
        let mut guard = self
            .properties
            .write()
            .map_err(|e| ImplicaError::LockError {
                rw: "write".to_string(),
                message: e.to_string(),
                context: Some("node set properties".to_string()),
            })?;
        guard.clear();
        guard.extend(props);
        Ok(())
    }

    /// Returns a unique identifier for this node.
    ///
    /// The UID is based on the node's type UID using SHA256.
    /// This result is cached to avoid recalculating for complex recursive types.
    ///
    /// # Returns
    ///
    /// A SHA256 hash representing this node uniquely
    pub fn uid(&self) -> &str {
        self.uid_cache.get_or_init(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"node:");
            hasher.update(self.r#type.uid().as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }

    /// Checks if two nodes are equal using UID.
    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on uid
        self == other
    }

    /// Returns a string representation of the node.
    ///
    /// Format: "Node(type)" or "Node(type, term)" if term is present
    fn __str__(&self) -> String {
        self.to_string()
    }

    /// Returns a detailed representation for debugging.
    ///
    /// Format: "Node(type)" or "Node(type, term)" if term is present
    fn __repr__(&self) -> String {
        self.to_string()
    }
}
