use pyo3::prelude::*;
use pyo3::types::PyDict;

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, RwLock};

use crate::typing::{python_to_term, term_to_python, Term};
use crate::Node;

/// Represents an edge in the graph (a typed term in the model).
///
/// Edges are directed connections between nodes, each representing a term.
/// An edge connects a start node to an end node and has an associated term
/// that must have a type consistent with the node types.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Create types and nodes
/// A = implica.Variable("A")
/// B = implica.Variable("B")
/// node_a = implica.Node(A)
/// node_b = implica.Node(B)
///
/// # Create a term with type A -> B
/// func_type = implica.Arrow(A, B)
/// term = implica.Term("f", func_type)
///
/// # Create an edge
/// edge = implica.Edge(term, node_a, node_b)
/// ```
///
/// # Fields
///
/// * `term` - The term this edge represents (accessible via term())
/// * `start` - The starting node (accessible via start())
/// * `end` - The ending node (accessible via end())
/// * `properties` - A dictionary of edge properties
#[pyclass]
#[derive(Debug)]
pub struct Edge {
    pub term: Arc<RwLock<Term>>,
    pub start: Arc<Node>,
    pub end: Arc<Node>,
    pub properties: Arc<RwLock<HashMap<String, Py<PyAny>>>>,
    /// Cached UID for performance - computed once and reused
    pub(in crate::graph) uid_cache: Arc<RwLock<Option<String>>>,
}

impl Clone for Edge {
    fn clone(&self) -> Self {
        Python::attach(|py| Edge {
            term: self.term.clone(),
            start: self.start.clone(),
            end: self.end.clone(),
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

impl Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Edge({}: {} -> {})",
            self.term.read().unwrap(),
            self.start.r#type,
            self.end.r#type
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
    /// Creates a new edge with the given term, start and end nodes, and optional properties.
    ///
    /// # Arguments
    ///
    /// * `term` - The term for this edge
    /// * `start` - The starting node
    /// * `end` - The ending node
    /// * `properties` - Optional dictionary of properties (default: empty dict)
    ///
    /// # Returns
    ///
    /// A new `Edge` instance
    ///
    /// # Examples
    ///
    /// ```python
    /// edge = implica.Edge(term, start_node, end_node)
    ///
    /// # With properties
    /// edge = implica.Edge(
    ///     term, start_node, end_node,
    ///     {"weight": 1.0, "label": "applies_to"}
    /// )
    /// ```
    #[new]
    #[pyo3(signature = (term, start, end, properties=None))]
    pub fn new(
        term: Py<PyAny>,
        start: Py<PyAny>,
        end: Py<PyAny>,
        properties: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        Python::attach(|py| {
            let term_obj = python_to_term(term.bind(py))?;
            let start_obj = start.bind(py).extract::<Node>()?;
            let end_obj = end.bind(py).extract::<Node>()?;
            let props = properties
                .unwrap_or_else(|| PyDict::new(py).into())
                .bind(py)
                .extract::<HashMap<String, Py<PyAny>>>()?;

            Ok(Edge {
                term: Arc::new(RwLock::new(term_obj)),
                start: Arc::new(start_obj),
                end: Arc::new(end_obj),
                properties: Arc::new(RwLock::new(props)),
                uid_cache: Arc::new(RwLock::new(None)),
            })
        })
    }

    /// Gets the term of this edge.
    ///
    /// # Returns
    ///
    /// The term as a Python object
    #[getter]
    pub fn term(&self, py: Python) -> PyResult<Py<PyAny>> {
        let term = self.term.read().unwrap();
        term_to_python(py, &term)
    }

    /// Gets the starting node of this edge.
    ///
    /// # Returns
    ///
    /// The start node as a Python object
    #[getter]
    pub fn start(&self, py: Python) -> PyResult<Py<Node>> {
        Py::new(py, (*self.start).clone())
    }

    /// Gets the ending node of this edge.
    ///
    /// # Returns
    ///
    /// The end node as a Python object
    #[getter]
    pub fn end(&self, py: Python) -> PyResult<Py<Node>> {
        Py::new(py, (*self.end).clone())
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
    pub fn set_properties(&self, props: HashMap<String, Py<PyAny>>) {
        let mut guard = self.properties.write().unwrap();
        guard.clear();
        guard.extend(props);
    }

    /// Returns a unique identifier for this edge.
    ///
    /// The UID is based on the edge's term UID using SHA256.
    /// This result is cached to avoid recalculating for complex recursive types.
    ///
    /// # Returns
    ///
    /// A SHA256 hash representing this edge uniquely
    pub fn uid(&self) -> String {
        // Check if we have a cached value
        if let Ok(cache) = self.uid_cache.read() {
            if let Some(cached) = cache.as_ref() {
                return cached.clone();
            }
        }

        // Calculate the UID
        let mut hasher = Sha256::new();
        hasher.update(b"edge:");
        hasher.update(self.term.read().unwrap().uid().as_bytes());
        let uid = format!("{:x}", hasher.finalize());

        // Cache it for future use
        if let Ok(mut cache) = self.uid_cache.write() {
            *cache = Some(uid.clone());
        }

        uid
    }

    /// Returns a string representation of the edge.
    ///
    /// Format: "Edge(term_name: start_type -> end_type)"
    fn __str__(&self) -> String {
        self.to_string()
    }

    /// Returns a detailed representation for debugging.
    ///
    /// Format: "Edge(term_name: start_type -> end_type)"
    fn __repr__(&self) -> String {
        self.to_string()
    }

    /// Checks if two nodes are equal using UID.
    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on uid
        self == other
    }
}
