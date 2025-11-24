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
use std::cmp::Ordering;
use std::sync::{Arc, RwLock};

/// Strategy for resolving conflicts when a term already exists.
///
/// When adding or updating a term on a node or edge, if a term already exists,
/// this strategy determines which term to keep.
///
/// # Variants
///
/// * `KeepExisting` - Keep the existing term, ignore the new one
/// * `KeepNew` - Replace with the new term
/// * `KeepSimplest` - Keep the simplest term (fewer applications, shorter name)
#[pyclass(eq, eq_int)]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum KeepTermStrategy {
    KeepExisting,
    KeepNew,
    #[default]
    KeepSimplest,
}

impl KeepTermStrategy {
    /// Chooses between two terms based on this strategy.
    ///
    /// # Arguments
    ///
    /// * `existing` - The existing term
    /// * `new` - The new term
    ///
    /// # Returns
    ///
    /// The term that should be kept according to the strategy
    pub fn choose<'a>(&self, existing: &'a Term, new: &'a Term) -> &'a Term {
        match self {
            KeepTermStrategy::KeepExisting => existing,
            KeepTermStrategy::KeepNew => new,
            KeepTermStrategy::KeepSimplest => {
                // Compare by simplicity: fewer applications, then shorter name
                let existing_complexity = Self::term_complexity(existing);
                let new_complexity = Self::term_complexity(new);

                match existing_complexity.cmp(&new_complexity) {
                    Ordering::Less => existing,
                    Ordering::Greater => new,
                    Ordering::Equal => match existing.name.len().cmp(&new.name.len()) {
                        Ordering::Greater => new,
                        _ => existing,
                    },
                }
            }
        }
    }

    /// Calculates the complexity of a term.
    ///
    /// Complexity is based on the number of applications in the term.
    /// A basic term has complexity 0, each application adds 1.
    ///
    /// # Arguments
    ///
    /// * `term` - The term to analyze
    ///
    /// # Returns
    ///
    /// The complexity score (lower is simpler)
    fn term_complexity(term: &Term) -> usize {
        // Count the number of applications
        // A term is an application if it has both function_uid and argument_uid
        if term.function_uid.is_some() && term.argument_uid.is_some() {
            // This is an application term
            // We can't recursively compute without having the actual terms,
            // so we count by the name structure (number of spaces/parentheses)
            term.name.matches('(').count() + term.name.matches(' ').count()
        } else {
            // Basic term
            0
        }
    }
}

#[pymethods]
impl KeepTermStrategy {
    fn __str__(&self) -> String {
        match self {
            KeepTermStrategy::KeepExisting => "KeepExisting".to_string(),
            KeepTermStrategy::KeepNew => "KeepNew".to_string(),
            KeepTermStrategy::KeepSimplest => "KeepSimplest".to_string(),
        }
    }
}

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
    pub term: Option<Arc<Term>>,
    #[pyo3(get, set)]
    pub properties: Py<PyDict>,
    /// Cached UID for performance - computed once and reused
    uid_cache: Arc<RwLock<Option<String>>>,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Python::attach(|py| Node {
            r#type: self.r#type.clone(),
            term: self.term.clone(),
            properties: self.properties.clone_ref(py),
            uid_cache: self.uid_cache.clone(),
        })
    }
}

#[pymethods]
impl Node {
    /// Creates a new node with the given type, optional term, and optional properties.
    ///
    /// # Arguments
    ///
    /// * `type` - The type for this node (Variable or Arrow)
    /// * `term` - Optional term for this node
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
    /// # Node with term
    /// term = implica.Term("alice", implica.Variable("Person"))
    /// node = implica.Node(implica.Variable("Person"), term)
    ///
    /// # Node with properties
    /// node = implica.Node(
    ///     implica.Variable("Person"),
    ///     None,
    ///     {"name": "Alice", "age": 25}
    /// )
    /// ```
    #[new]
    #[pyo3(signature = (r#type, term=None, properties=None))]
    pub fn new(
        r#type: Py<PyAny>,
        term: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        Python::attach(|py| {
            let type_obj = python_to_type(r#type.bind(py))?;
            let term_obj = if let Some(t) = term {
                let term: Term = t.bind(py).extract()?;
                Some(Arc::new(term))
            } else {
                None
            };
            let props = properties.unwrap_or_else(|| PyDict::new(py).into());
            Ok(Node {
                r#type: Arc::new(type_obj),
                term: term_obj,
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

    /// Gets the term of this node.
    ///
    /// # Returns
    ///
    /// The term as a Python object, or None if no term is set
    #[getter]
    pub fn get_term(&self, py: Python) -> PyResult<Option<Py<Term>>> {
        if let Some(term) = &self.term {
            Ok(Some(Py::new(py, (**term).clone())?))
        } else {
            Ok(None)
        }
    }

    /// Sets the term of this node.
    ///
    /// # Arguments
    ///
    /// * `term` - The term to set, or None to clear the term
    #[setter]
    pub fn set_term(&mut self, py: Python, term: Option<Py<PyAny>>) -> PyResult<()> {
        self.term = if let Some(t) = term {
            let term_obj: Term = t.bind(py).extract()?;
            Some(Arc::new(term_obj))
        } else {
            None
        };
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
        self.uid() == other.uid()
    }

    /// Returns a string representation of the node.
    ///
    /// Format: "Node(type)" or "Node(type, term)" if term is present
    fn __str__(&self) -> String {
        if let Some(term) = &self.term {
            format!("Node({}, {})", self.r#type, term.name)
        } else {
            format!("Node({})", self.r#type)
        }
    }

    /// Returns a detailed representation for debugging.
    ///
    /// Format: "Node(type)" or "Node(type, term)" if term is present
    fn __repr__(&self) -> String {
        if let Some(term) = &self.term {
            format!("Node({}, {})", self.r#type, term.name)
        } else {
            format!("Node({})", self.r#type)
        }
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

    /// Checks if two nodes are equal using UID.
    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on uid
        self.uid() == other.uid()
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
/// * `terms_registry` - Registry for term deduplication (internal)
/// * `keep_term_strategy` - Strategy for resolving term conflicts
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

    // Terms registry for deduplication
    pub terms_registry: Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<Term>>>>, // term UID -> Term

    // Strategy for term conflicts
    #[pyo3(get, set)]
    pub keep_term_strategy: KeepTermStrategy,
}

impl Clone for Graph {
    fn clone(&self) -> Self {
        Python::attach(|py| Graph {
            nodes: self.nodes.clone_ref(py),
            edges: self.edges.clone_ref(py),
            node_type_index: self.node_type_index.clone(),
            edge_type_index: self.edge_type_index.clone(),
            terms_registry: self.terms_registry.clone(),
            keep_term_strategy: self.keep_term_strategy.clone(),
        })
    }
}

#[pymethods]
impl Graph {
    /// Creates a new empty graph with optional index configuration and term strategy.
    ///
    /// # Arguments
    ///
    /// * `config` - Optional IndexConfig for optimization settings (bloom filters)
    /// * `keep_term_strategy` - Optional strategy for resolving term conflicts (default: KeepSimplest)
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
    /// # Graph with custom term strategy
    /// graph = implica.Graph(None, implica.KeepTermStrategy.KeepExisting)
    ///
    /// # Auto-configure for graph size
    /// config = implica.IndexConfig.for_graph_size(500_000)
    /// graph = implica.Graph(config)
    ///
    /// # Check if bloom filters are enabled
    /// print(f"Bloom filters: {config.has_bloom_filters()}")
    /// ```
    #[new]
    #[pyo3(signature = (config=None, keep_term_strategy=None))]
    pub fn new(
        config: Option<crate::type_index::IndexConfig>,
        keep_term_strategy: Option<KeepTermStrategy>,
    ) -> PyResult<Self> {
        Python::attach(|py| {
            let index_config = config.unwrap_or_default();
            let term_strategy = keep_term_strategy.unwrap_or_default();

            Ok(Graph {
                nodes: PyDict::new(py).into(),
                edges: PyDict::new(py).into(),
                node_type_index: Arc::new(std::sync::Mutex::new(TypeIndex::with_config(
                    index_config.clone(),
                ))),
                edge_type_index: Arc::new(std::sync::Mutex::new(TypeIndex::with_config(
                    index_config,
                ))),
                terms_registry: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
                keep_term_strategy: term_strategy,
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
    /// This method also implements automatic term synchronization and edge creation:
    /// - If the node has a term and its type is Arrow (A -> B), an edge is automatically created
    /// - If a node of the same type already exists with a term, the terms are resolved using KeepTermStrategy
    /// - If an edge of the same type exists, terms are synchronized
    ///
    /// # Arguments
    ///
    /// * `node` - The node to add
    /// * `py` - Python context
    pub fn add_node(&self, node: &Node, py: Python) -> PyResult<()> {
        let uid = node.uid();
        let nodes_dict = self.nodes.bind(py);

        // Check if a node with this UID already exists
        let existing_node: Option<Node> = if let Some(existing_obj) = nodes_dict.get_item(&uid)? {
            Some(existing_obj.extract()?)
        } else {
            None
        };

        // Resolve term conflicts if an existing node has a term
        let final_node = if let Some(mut existing) = existing_node {
            if let (Some(existing_term), Some(new_term)) = (&existing.term, &node.term) {
                // Both have terms, apply strategy
                let chosen_term = self.keep_term_strategy.choose(existing_term, new_term);
                existing.term = Some(Arc::new(chosen_term.clone()));
                existing
            } else if node.term.is_some() {
                // Only new node has a term
                existing.term = node.term.clone();
                existing
            } else {
                // Keep existing
                existing
            }
        } else {
            node.clone()
        };

        // Register the term if present
        if let Some(term) = &final_node.term {
            self.get_or_insert_term((**term).clone());
        }

        // Add or update the node
        nodes_dict.set_item(&uid, Py::new(py, final_node.clone())?)?;

        // Update the type index
        let mut index = self.node_type_index.lock().unwrap();
        index.insert(&final_node.r#type, uid.clone());
        drop(index);

        // If the node has an Arrow type and a term, create corresponding edge
        if let Type::Arrow(arrow) = &*final_node.r#type {
            if let Some(term) = &final_node.term {
                self.sync_arrow_node_to_edge(&final_node, arrow, term, py)?;
            }
        }

        // Synchronize with existing edges of the same Arrow type
        if let Type::Arrow(_) = &*final_node.r#type {
            self.sync_edge_terms_to_node(&final_node, py)?;
        }

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
    /// This method also implements automatic term application:
    /// - If the start node has a term 'a' and the edge has term 'f', the end node gets term 'f(a)'
    /// - Terms are resolved using KeepTermStrategy if conflicts arise
    /// - Terms are synchronized with nodes of matching Arrow types
    ///
    /// # Arguments
    ///
    /// * `edge` - The edge to add
    /// * `py` - Python context
    pub fn add_edge(&self, edge: &Edge, py: Python) -> PyResult<()> {
        let uid = edge.uid();
        let edges_dict = self.edges.bind(py);

        // Register the edge's term
        self.get_or_insert_term((*edge.term).clone());

        edges_dict.set_item(&uid, Py::new(py, edge.clone())?)?;

        // Update the type index
        let mut index = self.edge_type_index.lock().unwrap();
        index.insert(&edge.term.r#type, uid.clone());
        drop(index);

        // Automatic term application: if start node has a term, apply it to end node
        self.apply_term_through_edge(edge, py)?;

        // Synchronize with node of same Arrow type if it exists
        self.sync_edge_to_arrow_node(edge, py)?;

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

    /// Gets or inserts a term in the registry, ensuring deduplication.
    ///
    /// If a term with the same UID already exists, returns the existing Arc.
    /// Otherwise, inserts the new term and returns an Arc to it.
    ///
    /// # Arguments
    ///
    /// * `term` - The term to register
    ///
    /// # Returns
    ///
    /// An Arc to the registered term
    pub fn get_or_insert_term(&self, term: Term) -> Arc<Term> {
        let uid = term.uid();
        let mut registry = self.terms_registry.lock().unwrap();

        if let Some(existing_term) = registry.get(&uid) {
            existing_term.clone()
        } else {
            let arc_term = Arc::new(term);
            registry.insert(uid, arc_term.clone());
            arc_term
        }
    }

    /// Gets a term from the registry by its UID.
    ///
    /// # Arguments
    ///
    /// * `uid` - The UID of the term
    ///
    /// # Returns
    ///
    /// An Option containing the term Arc if found
    pub fn get_term_by_uid(&self, uid: &str) -> Option<Arc<Term>> {
        let registry = self.terms_registry.lock().unwrap();
        registry.get(uid).cloned()
    }

    /// Removes a term from the registry.
    ///
    /// # Arguments
    ///
    /// * `uid` - The UID of the term to remove
    pub fn remove_term(&self, uid: &str) {
        let mut registry = self.terms_registry.lock().unwrap();
        registry.remove(uid);
    }

    /// Synchronizes an Arrow-typed node's term to create/update a corresponding edge.
    ///
    /// When a node of type A -> B has a term f, this creates an edge from node A to node B with term f.
    ///
    /// # Arguments
    ///
    /// * `arrow_node` - The node with Arrow type
    /// * `arrow` - The Arrow type structure
    /// * `term` - The term to use for the edge
    /// * `py` - Python context
    fn sync_arrow_node_to_edge(
        &self,
        _arrow_node: &Node,
        arrow: &crate::types::Arrow,
        term: &Arc<Term>,
        py: Python,
    ) -> PyResult<()> {
        // Find or create nodes for the input and output types
        let start_nodes = self.find_nodes_by_type(&arrow.left, py)?;
        let end_nodes = self.find_nodes_by_type(&arrow.right, py)?;

        // Get or create start node
        let start_node = if start_nodes.is_empty() {
            let new_node = Node {
                r#type: arrow.left.clone(),
                term: None,
                properties: PyDict::new(py).into(),
                uid_cache: Arc::new(RwLock::new(None)),
            };
            self.add_node(&new_node, py)?;
            new_node
        } else {
            start_nodes[0].clone()
        };

        // Get or create end node
        let end_node = if end_nodes.is_empty() {
            let new_node = Node {
                r#type: arrow.right.clone(),
                term: None,
                properties: PyDict::new(py).into(),
                uid_cache: Arc::new(RwLock::new(None)),
            };
            self.add_node(&new_node, py)?;
            new_node
        } else {
            end_nodes[0].clone()
        };

        // Create or update the edge
        let edge = Edge {
            term: term.clone(),
            start: Arc::new(start_node),
            end: Arc::new(end_node),
            properties: PyDict::new(py).into(),
            uid_cache: Arc::new(RwLock::new(None)),
        };

        self.add_edge(&edge, py)?;

        Ok(())
    }

    /// Synchronizes terms from edges to an Arrow-typed node.
    ///
    /// If edges exist with the same type as the node, synchronize their terms.
    ///
    /// # Arguments
    ///
    /// * `arrow_node` - The node with Arrow type
    /// * `py` - Python context
    fn sync_edge_terms_to_node(&self, arrow_node: &Node, py: Python) -> PyResult<()> {
        // Find edges with matching term type
        let edges = self.find_edges_by_term_type(&arrow_node.r#type, py)?;

        if edges.is_empty() {
            return Ok(());
        }

        // If the node has no term, take the first edge's term
        if arrow_node.term.is_none() {
            if let Some(first_edge) = edges.first() {
                // Update the node with the edge's term
                let uid = arrow_node.uid();
                let nodes_dict = self.nodes.bind(py);

                let mut updated_node = arrow_node.clone();
                updated_node.term = Some(first_edge.term.clone());

                nodes_dict.set_item(&uid, Py::new(py, updated_node)?)?;
            }
        }

        Ok(())
    }

    /// Applies term through an edge: if start node has term 'a' and edge has term 'f',
    /// computes end node's term as 'f(a)'.
    ///
    /// # Arguments
    ///
    /// * `edge` - The edge through which to apply the term
    /// * `py` - Python context
    fn apply_term_through_edge(&self, edge: &Edge, py: Python) -> PyResult<()> {
        // Check if the start node has a term
        let start_uid = edge.start.uid();
        let nodes_dict = self.nodes.bind(py);

        let start_node: Option<Node> = if let Some(node_obj) = nodes_dict.get_item(&start_uid)? {
            Some(node_obj.extract()?)
        } else {
            None
        };

        if let Some(start) = start_node {
            if let Some(start_term) = &start.term {
                // Apply the edge's term to the start node's term
                match edge.term.apply(start_term) {
                    Ok(applied_term) => {
                        // Get the end node
                        let end_uid = edge.end.uid();
                        let end_node: Option<Node> =
                            if let Some(node_obj) = nodes_dict.get_item(&end_uid)? {
                                Some(node_obj.extract()?)
                            } else {
                                None
                            };

                        if let Some(mut end) = end_node {
                            // Register the new term
                            let applied_arc = self.get_or_insert_term(applied_term);

                            // Resolve term conflict if end node already has a term
                            let final_term = if let Some(existing_term) = &end.term {
                                let chosen =
                                    self.keep_term_strategy.choose(existing_term, &applied_arc);
                                Arc::new(chosen.clone())
                            } else {
                                applied_arc
                            };

                            end.term = Some(final_term);
                            nodes_dict.set_item(&end_uid, Py::new(py, end.clone())?)?;

                            // Propagate: if the end node has outgoing edges, continue applying
                            self.propagate_term_update(&end, py)?;
                        }
                    }
                    Err(_) => {
                        // Term application failed (type mismatch), skip
                    }
                }
            }
        }

        Ok(())
    }

    /// Propagates term updates through the graph.
    ///
    /// When a node's term is updated, this recursively updates all nodes reachable
    /// through edges by applying the edge terms.
    ///
    /// # Arguments
    ///
    /// * `node` - The node whose term was updated
    /// * `py` - Python context
    fn propagate_term_update(&self, node: &Node, py: Python) -> PyResult<()> {
        // Find all edges starting from this node
        let all_edges = self.find_all_edges(py)?;

        for edge in all_edges {
            if edge.start.uid() == node.uid() {
                // This edge starts from our node, apply term through it
                self.apply_term_through_edge(&edge, py)?;
            }
        }

        Ok(())
    }

    /// Synchronizes an edge's term with a node of matching Arrow type.
    ///
    /// If a node exists with the same Arrow type as the edge, synchronize their terms.
    ///
    /// # Arguments
    ///
    /// * `edge` - The edge to synchronize
    /// * `py` - Python context
    fn sync_edge_to_arrow_node(&self, edge: &Edge, py: Python) -> PyResult<()> {
        // Find nodes with the same type as the edge's term
        let arrow_nodes = self.find_nodes_by_type(&edge.term.r#type, py)?;

        if arrow_nodes.is_empty() {
            return Ok(());
        }

        let arrow_node = &arrow_nodes[0];
        let nodes_dict = self.nodes.bind(py);
        let uid = arrow_node.uid();

        // Resolve term conflict
        let final_term = if let Some(existing_term) = &arrow_node.term {
            let chosen = self.keep_term_strategy.choose(existing_term, &edge.term);
            Arc::new(chosen.clone())
        } else {
            edge.term.clone()
        };

        let mut updated_node = arrow_node.clone();
        updated_node.term = Some(final_term);

        nodes_dict.set_item(&uid, Py::new(py, updated_node)?)?;

        Ok(())
    }
}

impl Default for Graph {
    fn default() -> Self {
        Python::attach(|py| Graph {
            nodes: PyDict::new(py).into(),
            edges: PyDict::new(py).into(),
            node_type_index: Arc::new(std::sync::Mutex::new(TypeIndex::new())),
            edge_type_index: Arc::new(std::sync::Mutex::new(TypeIndex::new())),
            terms_registry: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            keep_term_strategy: KeepTermStrategy::default(),
        })
    }
}
