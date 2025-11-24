//! Graph structure for type theoretical models.
//!
//! This module provides the core graph components: nodes representing types,
//! edges representing typed terms, and the graph structure itself. The graph
//! serves as the main data structure for modeling type theoretical theories.

use crate::term::Term;
use crate::type_index::TypeIndex;
use crate::types::{python_to_type, type_to_python, Type};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sha2::{Digest, Sha256};
use std::sync::{Arc, RwLock};

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
    /// Cached UID for performance - computed once and reused
    uid_cache: Arc<RwLock<Option<String>>>,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Python::attach(|py| Node {
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
    /// * `type` - The type for this node (Variable or Arrow)
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
        Python::attach(|py| {
            let type_obj = python_to_type(r#type.bind(py))?;
            let props = properties.unwrap_or_else(|| PyDict::new(py).into());
            Ok(Node {
                r#type: Arc::new(type_obj),
                properties: props,
                uid_cache: Arc::new(RwLock::new(None)),
            })
        })
    }

    /// Gets the type of this node.
    ///
    /// # Returns
    ///
    /// The type as a Python object (Variable or Arrow)
    #[getter]
    pub fn get_type(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.r#type)
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
    pub term: Arc<Term>,
    pub start: Arc<Node>,
    pub end: Arc<Node>,
    #[pyo3(get, set)]
    pub properties: Py<PyDict>,
    /// Cached UID for performance - computed once and reused
    uid_cache: Arc<RwLock<Option<String>>>,
}

impl Clone for Edge {
    fn clone(&self) -> Self {
        Python::attach(|py| Edge {
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
        Python::attach(|py| {
            let term_obj = term.bind(py).extract::<Term>()?;
            let start_obj = start.bind(py).extract::<Node>()?;
            let end_obj = end.bind(py).extract::<Node>()?;
            let props = properties.unwrap_or_else(|| PyDict::new(py).into());

            Ok(Edge {
                term: Arc::new(term_obj),
                start: Arc::new(start_obj),
                end: Arc::new(end_obj),
                properties: props,
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
        hasher.update(self.term.uid().as_bytes());
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
/// For large graphs (>100K types), Bloom Filters can be enabled via IndexConfig
/// for O(1) pre-filtering and faster queries.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Small graph (default, no bloom filters)
/// graph = implica.Graph()
///
/// # Large graph with bloom filters enabled
/// config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1_000_000)
/// graph = implica.Graph(config)
///
/// # Auto-configure for expected size
/// config = implica.IndexConfig.for_graph_size(500_000)
/// graph = implica.Graph(config)
///
/// # Query the graph
/// q = graph.query()
/// q.match(node="n", type_schema="Person")
/// results = q.return_(["n"])
///
/// print(graph)  # Graph(X nodes, Y edges)
/// ```
///
/// # Fields
///
/// * `nodes` - Dictionary mapping node UIDs to Node objects
/// * `edges` - Dictionary mapping edge UIDs to Edge objects
/// * `node_type_index` - Tree-based index for fast node type lookups (internal)
/// * `edge_type_index` - Tree-based index for fast edge term type lookups (internal)
#[pyclass]
#[derive(Debug)]
pub struct Graph {
    #[pyo3(get)]
    pub nodes: Py<PyDict>, // uid -> Node
    #[pyo3(get)]
    pub edges: Py<PyDict>, // uid -> Edge

    // Type indices for O(log n) lookups (not exposed to Python)
    pub node_type_index: Arc<std::sync::Mutex<TypeIndex<String>>>, // type -> node UIDs
    pub edge_type_index: Arc<std::sync::Mutex<TypeIndex<String>>>, // term type -> edge UIDs
}

impl Clone for Graph {
    fn clone(&self) -> Self {
        Python::attach(|py| Graph {
            nodes: self.nodes.clone_ref(py),
            edges: self.edges.clone_ref(py),
            node_type_index: self.node_type_index.clone(),
            edge_type_index: self.edge_type_index.clone(),
        })
    }
}

#[pymethods]
impl Graph {
    /// Creates a new empty graph with optional index configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Optional IndexConfig for optimization settings (bloom filters)
    ///
    /// # Returns
    ///
    /// A new `Graph` instance with no nodes or edges
    ///
    /// # Examples
    ///
    /// ```python
    /// # Small graph: bloom filters disabled (default)
    /// graph = implica.Graph()
    ///
    /// # Large graph: enable bloom filters
    /// config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1_000_000)
    /// graph = implica.Graph(config)
    ///
    /// # Auto-configure for graph size
    /// config = implica.IndexConfig.for_graph_size(500_000)
    /// graph = implica.Graph(config)
    ///
    /// # Check if bloom filters are enabled
    /// print(f"Bloom filters: {config.has_bloom_filters()}")
    /// ```
    #[new]
    #[pyo3(signature = (config=None))]
    pub fn new(config: Option<crate::type_index::IndexConfig>) -> PyResult<Self> {
        Python::attach(|py| {
            let index_config = config.unwrap_or_default();

            Ok(Graph {
                nodes: PyDict::new(py).into(),
                edges: PyDict::new(py).into(),
                node_type_index: Arc::new(std::sync::Mutex::new(TypeIndex::with_config(
                    index_config.clone(),
                ))),
                edge_type_index: Arc::new(std::sync::Mutex::new(TypeIndex::with_config(
                    index_config,
                ))),
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
    /// Finds nodes that match a given type using the tree-based index.
    ///
    /// This provides O(log n) lookup for nodes by their type structure.
    ///
    /// # Arguments
    ///
    /// * `typ` - The type to match against
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// A vector of nodes matching the type
    pub fn find_nodes_by_type(&self, typ: &Type, py: Python) -> PyResult<Vec<Node>> {
        let nodes_dict = self.nodes.bind(py);
        let mut result = Vec::new();

        // Lock the index for reading
        let index = self.node_type_index.lock().unwrap();

        // Use the type index to find candidate node UIDs
        let node_uids = match typ {
            Type::Variable(var) => index.find_variable(&var.name),
            Type::Arrow(app) => index.find_arrow(&app.left, &app.right),
        };

        // Retrieve the actual nodes
        for uid in node_uids {
            if let Some(node_obj) = nodes_dict.get_item(uid)? {
                let node: Node = node_obj.extract()?;
                result.push(node);
            }
        }

        Ok(result)
    }

    /// Finds nodes that match a wildcard (any type).
    ///
    /// # Arguments
    ///
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// A vector of all nodes
    pub fn find_all_nodes(&self, py: Python) -> PyResult<Vec<Node>> {
        let nodes_dict = self.nodes.bind(py);
        let mut result = Vec::new();

        let index = self.node_type_index.lock().unwrap();
        let node_uids = index.find_all();

        for uid in node_uids {
            if let Some(node_obj) = nodes_dict.get_item(uid)? {
                let node: Node = node_obj.extract()?;
                result.push(node);
            }
        }

        Ok(result)
    }

    /// Finds edges by the type of their term using the tree-based index.
    ///
    /// This provides O(log n) lookup for edges by their term's type structure.
    ///
    /// # Arguments
    ///
    /// * `typ` - The term type to match against
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// A vector of edges whose term has the matching type
    pub fn find_edges_by_term_type(&self, typ: &Type, py: Python) -> PyResult<Vec<Edge>> {
        let edges_dict = self.edges.bind(py);
        let mut result = Vec::new();

        // Lock the index for reading
        let index = self.edge_type_index.lock().unwrap();

        // Use the type index to find candidate edge UIDs
        let edge_uids = match typ {
            Type::Variable(var) => index.find_variable(&var.name),
            Type::Arrow(app) => index.find_arrow(&app.left, &app.right),
        };

        // Retrieve the actual edges
        for uid in edge_uids {
            if let Some(edge_obj) = edges_dict.get_item(uid)? {
                let edge: Edge = edge_obj.extract()?;
                result.push(edge);
            }
        }

        Ok(result)
    }

    /// Finds all edges (for wildcard matching).
    ///
    /// # Arguments
    ///
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// A vector of all edges
    pub fn find_all_edges(&self, py: Python) -> PyResult<Vec<Edge>> {
        let edges_dict = self.edges.bind(py);
        let mut result = Vec::new();

        let index = self.edge_type_index.lock().unwrap();
        let edge_uids = index.find_all();

        for uid in edge_uids {
            if let Some(edge_obj) = edges_dict.get_item(uid)? {
                let edge: Edge = edge_obj.extract()?;
                result.push(edge);
            }
        }

        Ok(result)
    }

    /// Adds a node to the graph and updates the type index.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to add
    /// * `py` - Python context
    pub fn add_node(&self, node: &Node, py: Python) -> PyResult<()> {
        let uid = node.uid();
        let nodes_dict = self.nodes.bind(py);
        nodes_dict.set_item(&uid, Py::new(py, node.clone())?)?;

        // Update the type index
        let mut index = self.node_type_index.lock().unwrap();
        index.insert(&node.r#type, uid);

        Ok(())
    }

    /// Removes a node from the graph and updates the type index.
    ///
    /// # Arguments
    ///
    /// * `node_uid` - The UID of the node to remove
    /// * `py` - Python context
    pub fn remove_node(&self, node_uid: &str, py: Python) -> PyResult<()> {
        let nodes_dict = self.nodes.bind(py);

        // Get the node first to know its type
        if let Some(node_obj) = nodes_dict.get_item(node_uid)? {
            let node: Node = node_obj.extract()?;

            // Remove from dictionary
            nodes_dict.del_item(node_uid)?;

            // Update the type index
            let mut index = self.node_type_index.lock().unwrap();
            index.remove(&node.r#type, |uid| uid == node_uid);
        }

        Ok(())
    }

    /// Adds an edge to the graph and updates the type index.
    ///
    /// # Arguments
    ///
    /// * `edge` - The edge to add
    /// * `py` - Python context
    pub fn add_edge(&self, edge: &Edge, py: Python) -> PyResult<()> {
        let uid = edge.uid();
        let edges_dict = self.edges.bind(py);
        edges_dict.set_item(&uid, Py::new(py, edge.clone())?)?;

        // Update the type index
        let mut index = self.edge_type_index.lock().unwrap();
        index.insert(&edge.term.r#type, uid);

        Ok(())
    }

    /// Removes an edge from the graph and updates the type index.
    ///
    /// # Arguments
    ///
    /// * `edge_uid` - The UID of the edge to remove
    /// * `py` - Python context
    pub fn remove_edge(&self, edge_uid: &str, py: Python) -> PyResult<()> {
        let edges_dict = self.edges.bind(py);

        // Get the edge first to know its term type
        if let Some(edge_obj) = edges_dict.get_item(edge_uid)? {
            let edge: Edge = edge_obj.extract()?;

            // Remove from dictionary
            edges_dict.del_item(edge_uid)?;

            // Update the type index
            let mut index = self.edge_type_index.lock().unwrap();
            index.remove(&edge.term.r#type, |uid| uid == edge_uid);
        }

        Ok(())
    }

    /// Rebuilds the type indices from scratch.
    ///
    /// This can be useful if the indices get out of sync, though
    /// it should not be necessary in normal usage.
    ///
    /// # Arguments
    ///
    /// * `py` - Python context
    pub fn rebuild_indices(&self, py: Python) -> PyResult<()> {
        // Clear existing indices
        {
            let mut node_index = self.node_type_index.lock().unwrap();
            node_index.clear();
        }
        {
            let mut edge_index = self.edge_type_index.lock().unwrap();
            edge_index.clear();
        }

        // Rebuild node index
        let nodes_dict = self.nodes.bind(py);
        for (uid_obj, node_obj) in nodes_dict.iter() {
            let uid: String = uid_obj.extract()?;
            let node: Node = node_obj.extract()?;

            let mut index = self.node_type_index.lock().unwrap();
            index.insert(&node.r#type, uid);
        }

        // Rebuild edge index
        let edges_dict = self.edges.bind(py);
        for (uid_obj, edge_obj) in edges_dict.iter() {
            let uid: String = uid_obj.extract()?;
            let edge: Edge = edge_obj.extract()?;

            let mut index = self.edge_type_index.lock().unwrap();
            index.insert(&edge.term.r#type, uid);
        }

        Ok(())
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
        Python::attach(|py| Graph {
            nodes: PyDict::new(py).into(),
            edges: PyDict::new(py).into(),
            node_type_index: Arc::new(std::sync::Mutex::new(TypeIndex::new())),
            edge_type_index: Arc::new(std::sync::Mutex::new(TypeIndex::new())),
        })
    }
}
