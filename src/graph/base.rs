//! Graph structure for type theoretical models.
//!
//! This module provides the core graph components: nodes representing types,
//! edges representing typed terms, and the graph structure itself. The graph
//! serves as the main data structure for modeling type theoretical theories.

use crate::errors::ImplicaError;

use crate::typing::Type;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{Edge, Node};

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
    pub nodes: Arc<RwLock<HashMap<String, Node>>>, // uid -> Node
    pub edges: Arc<RwLock<HashMap<String, Edge>>>, // uid -> Edge
}

impl Clone for Graph {
    fn clone(&self) -> Self {
        Graph {
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
        }
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
    pub fn new() -> PyResult<Self> {
        Ok(Graph {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
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
    fn __str__(&self) -> String {
        let node_count = self.nodes.read().unwrap().len();
        let edge_count = self.edges.read().unwrap().len();
        format!("Graph({} nodes, {} edges)", node_count, edge_count)
    }

    fn __repr__(&self) -> String {
        self.__str__()
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
    pub fn find_nodes_by_type(&self, typ: &Type) -> PyResult<Node> {
        Ok(self.nodes.read().unwrap()[&typ.uid()].clone())
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
    pub fn find_all_nodes(&self) -> PyResult<Vec<Node>> {
        let mut result = Vec::new();

        for n in self.nodes.read().unwrap().values() {
            result.push(n.clone());
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
    pub fn find_edges_by_term_type(&self, typ: &Type) -> PyResult<Vec<Edge>> {
        let mut result = Vec::new();

        for e in self.edges.read().unwrap().values() {
            let term = e.term.read().unwrap();
            if term.r#type().as_ref() == typ {
                result.push(e.clone());
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
    pub fn find_all_edges(&self) -> PyResult<Vec<Edge>> {
        let mut result = Vec::new();

        for e in self.edges.read().unwrap().values() {
            result.push(e.clone());
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
    pub fn add_node(&mut self, node: &Node) -> PyResult<()> {
        let uid = node.uid();

        if let Some(existing) = self.nodes.read().unwrap().get(&uid) {
            return Err(ImplicaError::NodeAlreadyExists {
                message: "Tried to add a node with a type that already exists.".to_string(),
                existing: existing.clone(),
                new: node.clone(),
            }
            .into());
        }

        self.nodes.write().unwrap().insert(uid, node.clone());

        Ok(())
    }

    /// Removes a node from the graph and updates the type index.
    ///
    /// # Arguments
    ///
    /// * `node_uid` - The UID of the node to remove
    /// * `py` - Python context
    pub fn remove_node(&mut self, node_uid: &str) -> PyResult<()> {
        match self.nodes.write().unwrap().remove(node_uid) {
            Some(_) => Ok(()),
            None => Err(ImplicaError::NodeNotFound {
                uid: node_uid.to_string(),
                context: Some("node deletion".to_string()),
            }
            .into()),
        }
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
    pub fn add_edge(&mut self, edge: &Edge) -> PyResult<()> {
        let uid = edge.uid();

        if let Some(existing) = self.edges.read().unwrap().get(&uid) {
            return Err(ImplicaError::EdgeAlreadyExists {
                message: "Tried to add a node that already exists.".to_string(),
                existing: existing.clone(),
                new: edge.clone(),
            }
            .into());
        }

        self.edges.write().unwrap().insert(uid, edge.clone());

        Ok(())
    }

    /// Removes an edge from the graph and updates the type index.
    ///
    /// # Arguments
    ///
    /// * `edge_uid` - The UID of the edge to remove
    /// * `py` - Python context
    pub fn remove_edge(&mut self, edge_uid: &str) -> PyResult<()> {
        match self.edges.write().unwrap().remove(edge_uid) {
            Some(_) => Ok(()),
            None => Err(ImplicaError::EdgeNotFound {
                uid: edge_uid.to_string(),
                context: Some("edge deletion".to_string()),
            }
            .into()),
        }
    }
}

impl Default for Graph {
    fn default() -> Self {
        Graph {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
