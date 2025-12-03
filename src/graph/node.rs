use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, RwLock};

use crate::graph::alias::{PropertyMap, SharedPropertyMap};
use crate::typing::{term_to_python, type_to_python, Term, Type};

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
    pub(in crate::graph) uid_cache: Arc<RwLock<Option<String>>>,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Python::attach(|py| Node {
            r#type: self.r#type.clone(),
            term: self.term.clone(),
            properties: Arc::new(RwLock::new(
                self.properties
                    .read()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| {
                        let new_props = v.clone_ref(py);
                        (k.clone(), new_props)
                    })
                    .collect(),
            )),
            uid_cache: self.uid_cache.clone(),
        })
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
            uid_cache: Arc::new(RwLock::new(None)),
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
                let term = term_lock.read().unwrap();
                term_to_python(py, &term).map(Some)
            }
            None => Ok(None),
        }
    }

    #[getter]
    pub fn get_properties(&self, py: Python) -> Py<PyDict> {
        let dict = PyDict::new(py);
        for (k, v) in self.properties.read().unwrap().iter() {
            dict.set_item(k, v.clone_ref(py)).unwrap();
        }
        dict.into()
    }

    #[setter]
    pub fn set_properties(&self, props: PropertyMap) {
        let mut guard = self.properties.write().unwrap();
        guard.clear();
        guard.extend(props);
    }

    /// Returns a unique identifier for this node.
    ///
    /// The UID is based on the node's type UID using SHA256.
    /// This result is cached to avoid recalculating for complex recursive types.
    ///
    /// # Returns
    ///
    /// A SHA256 hash representing this node uniquely
    pub fn uid(&self) -> String {
        // Check if we have a cached value
        if let Ok(cache) = self.uid_cache.read() {
            if let Some(cached) = cache.as_ref() {
                return cached.clone();
            }
        }

        // Calculate the UID
        let mut hasher = Sha256::new();
        hasher.update(b"node:");
        hasher.update(self.r#type.uid().as_bytes());
        let uid = format!("{:x}", hasher.finalize());

        // Cache it for future use
        if let Ok(mut cache) = self.uid_cache.write() {
            *cache = Some(uid.clone());
        }

        uid
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
