//! Graph structure for type theoretical models.
//!
//! This module provides the core graph components: nodes representing types,
//! edges representing typed terms, and the graph structure itself. The graph
//! serves as the main data structure for modeling type theoretical theories.

use crate::term::Term;
use crate::types::{python_to_type, type_to_python, Type};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

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
/// * `properties` - A dictionary of node properties
#[pyclass]
#[derive(Debug)]
pub struct Node {
    pub r#type: Arc<Type>,
    #[pyo3(get, set)]
    pub properties: Py<PyDict>,
    // Cached uid
    uid_cache: Option<String>,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Python::with_gil(|py| Node {
            r#type: self.r#type.clone(),
            properties: self.properties.clone_ref(py),
            uid_cache: self.uid_cache.clone(),
        })
    }
}

#[pymethods]
impl Node {
    /// Creates a new node with the given type and optional properties.
    ///
    /// # Arguments
    ///
    /// * `type` - The type for this node (Variable or Application)
    /// * `properties` - Optional dictionary of properties (default: empty dict)
    ///
    /// # Returns
    ///
    /// A new `Node` instance
    ///
    /// # Examples
    ///
    /// ```python
    /// # Simple node
    /// node = implica.Node(implica.Variable("Person"))
    ///
    /// # Node with properties
    /// node = implica.Node(
    ///     implica.Variable("Person"),
    ///     {"name": "Alice", "age": 25}
    /// )
    /// ```
    #[new]
    #[pyo3(signature = (r#type, properties=None))]
    pub fn new(r#type: Py<PyAny>, properties: Option<Py<PyDict>>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let type_obj = python_to_type(r#type.bind(py))?;
            let props = properties.unwrap_or_else(|| PyDict::new(py).into());
            Ok(Node {
                r#type: Arc::new(type_obj),
                properties: props,
                uid_cache: None,
            })
        })
    }

    /// Gets the type of this node.
    ///
    /// # Returns
    ///
    /// The type as a Python object (Variable or Application)
    #[getter]
    pub fn get_type(&self, py: Python) -> PyResult<PyObject> {
        type_to_python(py, &self.r#type)
    }

    /// Returns a unique identifier for this node.
    ///
    /// The UID is based on the node's type UID using SHA256.
    ///
    /// # Returns
    ///
    /// A SHA256 hash representing this node uniquely
    pub fn uid(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"node:");
        hasher.update(self.r#type.uid().as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Returns a string representation of the node.
    ///
    /// Format: "Node(type)"
    fn __str__(&self) -> String {
        format!("Node({})", self.r#type)
    }

    /// Returns a detailed representation for debugging.
    ///
    /// Format: "Node(type)"
    fn __repr__(&self) -> String {
        format!("Node({})", self.r#type)
    }
}

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
/// func_type = implica.Application(A, B)
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
    pub term: Arc<Term>,
    pub start: Arc<Node>,
    pub end: Arc<Node>,
    #[pyo3(get, set)]
    pub properties: Py<PyDict>,
    // Cached uid
    uid_cache: Option<String>,
}

impl Clone for Edge {
    fn clone(&self) -> Self {
        Python::with_gil(|py| Edge {
            term: self.term.clone(),
            start: self.start.clone(),
            end: self.end.clone(),
            properties: self.properties.clone_ref(py),
            uid_cache: self.uid_cache.clone(),
        })
    }
}

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
        Python::with_gil(|py| {
            let term_obj = term.bind(py).extract::<Term>()?;
            let start_obj = start.bind(py).extract::<Node>()?;
            let end_obj = end.bind(py).extract::<Node>()?;
            let props = properties.unwrap_or_else(|| PyDict::new(py).into());

            Ok(Edge {
                term: Arc::new(term_obj),
                start: Arc::new(start_obj),
                end: Arc::new(end_obj),
                properties: props,
                uid_cache: None,
            })
        })
    }

    /// Gets the term of this edge.
    ///
    /// # Returns
    ///
    /// The term as a Python object
    #[getter]
    pub fn term(&self, py: Python) -> PyResult<Py<Term>> {
        Py::new(py, (*self.term).clone())
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

    /// Returns a unique identifier for this edge.
    ///
    /// The UID is based on the edge's term UID using SHA256.
    ///
    /// # Returns
    ///
    /// A SHA256 hash representing this edge uniquely
    pub fn uid(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"edge:");
        hasher.update(self.term.uid().as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Returns a string representation of the edge.
    ///
    /// Format: "Edge(term_name: start_type -> end_type)"
    fn __str__(&self) -> String {
        format!(
            "Edge({}: {} -> {})",
            self.term.name, self.start.r#type, self.end.r#type
        )
    }

    /// Returns a detailed representation for debugging.
    ///
    /// Format: "Edge(term_name: start_type -> end_type)"
    fn __repr__(&self) -> String {
        format!(
            "Edge({}: {} -> {})",
            self.term.name, self.start.r#type, self.end.r#type
        )
    }
}

/// Represents a type theoretical theory model as a graph.
///
/// The Graph is the main container for nodes and edges, representing a complete
/// type theoretical model. It stores nodes (types) and edges (terms) and provides
/// querying capabilities through the Query interface.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Create a graph
/// graph = implica.Graph()
///
/// # Query the graph
/// q = graph.query()
/// q.match(node="n", type_schema="$Person$")
/// results = q.return_(["n"])
///
/// print(graph)  # Graph(X nodes, Y edges)
/// ```
///
/// # Fields
///
/// * `nodes` - Dictionary mapping node UIDs to Node objects
/// * `edges` - Dictionary mapping edge UIDs to Edge objects
#[pyclass]
#[derive(Debug)]
pub struct Graph {
    #[pyo3(get)]
    pub nodes: Py<PyDict>, // uid -> Node
    #[pyo3(get)]
    pub edges: Py<PyDict>, // uid -> Edge
}

impl Clone for Graph {
    fn clone(&self) -> Self {
        Python::with_gil(|py| Graph {
            nodes: self.nodes.clone_ref(py),
            edges: self.edges.clone_ref(py),
        })
    }
}

#[pymethods]
impl Graph {
    /// Creates a new empty graph.
    ///
    /// # Returns
    ///
    /// A new `Graph` instance with no nodes or edges
    ///
    /// # Examples
    ///
    /// ```python
    /// graph = implica.Graph()
    /// ```
    #[new]
    pub fn new() -> PyResult<Self> {
        Python::with_gil(|py| {
            Ok(Graph {
                nodes: PyDict::new(py).into(),
                edges: PyDict::new(py).into(),
            })
        })
    }

    /// Creates a new query builder for this graph.
    ///
    /// The query builder provides a Cypher-like interface for querying the graph.
    ///
    /// # Arguments
    ///
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// A new `Query` instance bound to this graph
    ///
    /// # Examples
    ///
    /// ```python
    /// q = graph.query()
    /// q.match(node="n", type_schema="$Person$")
    /// results = q.return_(["n"])
    /// ```
    pub fn query(&self, py: Python) -> PyResult<Py<crate::query::Query>> {
        Py::new(py, crate::query::Query::new(self.clone()))
    }

    /// Returns a string representation of the graph.
    ///
    /// Shows the number of nodes and edges.
    ///
    /// Format: "Graph(X nodes, Y edges)"
    fn __str__(&self, py: Python) -> String {
        let node_count = self.nodes.bind(py).len();
        let edge_count = self.edges.bind(py).len();
        format!("Graph({} nodes, {} edges)", node_count, edge_count)
    }

    fn __repr__(&self, py: Python) -> String {
        self.__str__(py)
    }
}

impl Graph {
    /// Builds an index mapping type UIDs to node UIDs.
    ///
    /// This enables O(1) lookups for nodes by type instead of O(n) iteration.
    ///
    /// # Returns
    ///
    /// A HashMap where keys are type UIDs and values are vectors of node UIDs
    pub fn build_type_index(&self, py: Python) -> PyResult<HashMap<String, Vec<String>>> {
        let mut index: HashMap<String, Vec<String>> = HashMap::new();
        let nodes_dict = self.nodes.bind(py);

        for (uid_obj, node_obj) in nodes_dict.iter() {
            let uid: String = uid_obj.extract()?;
            let node: Node = node_obj.extract()?;
            let type_uid = node.r#type.uid();

            index.entry(type_uid).or_default().push(uid);
        }

        Ok(index)
    }

    /// Gets nodes by type UID using the type index.
    ///
    /// This is an optimized lookup that avoids iterating over all nodes.
    ///
    /// # Arguments
    ///
    /// * `type_uid` - The UID of the type to search for
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// A vector of nodes matching the type
    pub fn get_nodes_by_type(&self, type_uid: &str, py: Python) -> PyResult<Vec<Node>> {
        let index = self.build_type_index(py)?;
        let nodes_dict = self.nodes.bind(py);
        let mut result = Vec::new();

        if let Some(node_uids) = index.get(type_uid) {
            for uid in node_uids {
                if let Some(node_obj) = nodes_dict.get_item(uid)? {
                    let node: Node = node_obj.extract()?;
                    result.push(node);
                }
            }
        }

        Ok(result)
    }

    /// Gets a node by its UID using O(1) dictionary lookup.
    ///
    /// # Arguments
    ///
    /// * `uid` - The UID of the node
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// An Option containing the node if found
    pub fn get_node_by_uid(&self, uid: &str, py: Python) -> PyResult<Option<Node>> {
        let nodes_dict = self.nodes.bind(py);

        if let Some(node_obj) = nodes_dict.get_item(uid)? {
            Ok(Some(node_obj.extract()?))
        } else {
            Ok(None)
        }
    }

    /// Gets an edge by its UID using O(1) dictionary lookup.
    ///
    /// # Arguments
    ///
    /// * `uid` - The UID of the edge
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// An Option containing the edge if found
    pub fn get_edge_by_uid(&self, uid: &str, py: Python) -> PyResult<Option<Edge>> {
        let edges_dict = self.edges.bind(py);

        if let Some(edge_obj) = edges_dict.get_item(uid)? {
            Ok(Some(edge_obj.extract()?))
        } else {
            Ok(None)
        }
    }
}

impl Default for Graph {
    fn default() -> Self {
        Python::with_gil(|py| Graph {
            nodes: PyDict::new(py).into(),
            edges: PyDict::new(py).into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Variable;

    #[test]
    fn test_node_creation() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let var_a = Py::new(py, Variable::new("A".to_string())).unwrap();
            let node = Node::new(var_a.into(), None).unwrap();

            assert_eq!(node.__str__(), "Node(A)");
            assert!(node.uid().starts_with("node_"));
        });
    }

    #[test]
    fn test_graph_creation() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let graph = Graph::new().unwrap();
            assert_eq!(graph.__str__(py), "Graph(0 nodes, 0 edges)");
        });
    }
}
