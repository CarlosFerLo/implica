//! Graph structure for type theoretical models.
//!
//! This module provides the core graph components: nodes representing types,
//! edges representing typed terms, and the graph structure itself. The graph
//! serves as the main data structure for modeling type theoretical theories.

use crate::errors::ImplicaError;

use crate::patterns::{EdgePattern, TypeSchema};
use crate::typing::{Term, Type};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::context::Context;
use crate::graph::{alias::SharedPropertyMap, Edge, Node};

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
    pub nodes: Arc<RwLock<HashMap<String, Arc<RwLock<Node>>>>>, // uid -> Node
    pub edges: Arc<RwLock<HashMap<String, Arc<RwLock<Edge>>>>>, // uid -> Edge
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
    pub fn find_node_by_type(&self, typ: &Type) -> PyResult<Arc<RwLock<Node>>> {
        let nodes = self.nodes.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("find node by type".to_string()),
        })?;
        match nodes.get(typ.uid()) {
            Some(node) => Ok(node.clone()),
            None => Err(ImplicaError::NodeNotFound {
                uid: typ.uid().to_string(),
                context: Some("find_node_by_type".to_string()),
            }
            .into()),
        }
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
    pub fn find_all_nodes(&self) -> PyResult<Vec<Arc<RwLock<Node>>>> {
        let mut result = Vec::new();

        let nodes = self.nodes.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("find all nodes".to_string()),
        })?;
        for n in nodes.values() {
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
    pub fn find_edges_by_term_type(&self, typ: &Type) -> PyResult<Vec<Arc<RwLock<Edge>>>> {
        let mut result = Vec::new();

        let edges = self.edges.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("find edges by term type".to_string()),
        })?;
        for e in edges.values() {
            let edge = e.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("find edges by term type".to_string()),
            })?;
            let term = edge.term.clone();
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
    pub fn find_all_edges(&self) -> PyResult<Vec<Arc<RwLock<Edge>>>> {
        let mut result = Vec::new();

        let edges = self.edges.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("find all edges".to_string()),
        })?;
        for e in edges.values() {
            result.push(e.clone());
        }

        Ok(result)
    }

    pub fn add_node(&self, node: &Node) -> Result<(), ImplicaError> {
        let uid = node.uid();

        let mut nodes = self.nodes.write().map_err(|e| ImplicaError::LockError {
            rw: "write".to_string(),
            message: e.to_string(),
            context: Some("add node".to_string()),
        })?;
        if let Some(existing) = nodes.get(uid) {
            return Err(ImplicaError::NodeAlreadyExists {
                message: "Tried to add a node with a type that already exists.".to_string(),
                existing: existing
                    .read()
                    .map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("add node".to_string()),
                    })?
                    .clone(),
                new: node.clone(),
            });
        }

        nodes.insert(uid.to_string(), Arc::new(RwLock::new(node.clone())));

        Ok(())
    }

    pub fn remove_node(&self, node_uid: &str) -> PyResult<()> {
        let mut nodes = self.nodes.write().map_err(|e| ImplicaError::LockError {
            rw: "write".to_string(),
            message: e.to_string(),
            context: Some("remove node".to_string()),
        })?;
        match nodes.remove(node_uid) {
            Some(node_lock) => {
                let node = node_lock.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("remove node".to_string()),
                })?;
                let pattern = EdgePattern::new(
                    None,
                    None,
                    Some(TypeSchema::new(format!("*->{}", node.r#type))?),
                    None,
                    None,
                    None,
                    "forward".to_string(),
                )?;

                self.remove_edges_matching(pattern)?;

                Ok(())
            }
            None => Err(ImplicaError::NodeNotFound {
                uid: node_uid.to_string(),
                context: Some("node deletion".to_string()),
            }
            .into()),
        }
    }

    pub fn add_edge(
        &self,
        term: Arc<Term>,
        start: Node,
        end: Node,
        properties: Option<SharedPropertyMap>,
    ) -> Result<Edge, ImplicaError> {
        let nodes = self.nodes.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("add edge".to_string()),
        })?;
        let start_ptr = match nodes.get(start.uid()) {
            Some(ptr) => ptr.clone(),
            None => {
                return Err(ImplicaError::NodeNotFound {
                    uid: start.uid().to_string(),
                    context: Some("add edge".to_string()),
                });
            }
        };
        let end_ptr = match nodes.get(end.uid()) {
            Some(ptr) => ptr.clone(),
            None => {
                return Err(ImplicaError::NodeNotFound {
                    uid: end.uid().to_string(),
                    context: Some("add edge".to_string()),
                });
            }
        };

        let edge = Edge::new(term, start_ptr, end_ptr, properties);
        let uid = edge.uid();

        let mut edges = self.edges.write().map_err(|e| ImplicaError::LockError {
            rw: "write".to_string(),
            message: e.to_string(),
            context: Some("add edge".to_string()),
        })?;
        if let Some(existing) = edges.get(uid) {
            return Err(ImplicaError::EdgeAlreadyExists {
                message: "Tried to add a node that already exists.".to_string(),
                existing: existing
                    .read()
                    .map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("add edge".to_string()),
                    })?
                    .clone(),
                new: edge.clone(),
            });
        }

        edges.insert(uid.to_string(), Arc::new(RwLock::new(edge.clone())));

        Ok(edge)
    }

    /// Removes an edge from the graph and updates the type index.
    ///
    /// # Arguments
    ///
    /// * `edge_uid` - The UID of the edge to remove
    /// * `py` - Python context
    pub fn remove_edge(&self, edge_uid: &str) -> PyResult<()> {
        let mut edges = self.edges.write().map_err(|e| ImplicaError::LockError {
            rw: "write".to_string(),
            message: e.to_string(),
            context: Some("remove edge".to_string()),
        })?;
        match edges.remove(edge_uid) {
            Some(_) => Ok(()),
            None => Err(ImplicaError::EdgeNotFound {
                uid: edge_uid.to_string(),
                context: Some("edge deletion".to_string()),
            }
            .into()),
        }
    }

    pub fn remove_edges_matching(&self, pattern: EdgePattern) -> PyResult<()> {
        let mut remove_uids = Vec::new();

        let edges = self.edges.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("remove edges matching".to_string()),
        })?;
        for edge_lock in edges.values() {
            let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("remove edges matching".to_string()),
            })?;
            let context = Arc::new(Context::new());
            if pattern.matches(&edge, context)? {
                remove_uids.push(edge.uid().to_string());
            }
        }

        for uid in remove_uids {
            self.remove_edge(&uid)?;
        }

        Ok(())
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
