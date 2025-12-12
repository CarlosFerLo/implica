use crate::errors::ImplicaError;

use crate::patterns::{EdgePattern, TypeSchema};
use crate::typing::{Term, Type};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::context::Context;
use crate::graph::{property_map::SharedPropertyMap, Edge, Node};
use crate::typing::Constant;

#[derive(Debug)]
pub struct Graph {
    pub nodes: Arc<RwLock<HashMap<String, Arc<RwLock<Node>>>>>, // uid -> Node
    pub edges: Arc<RwLock<HashMap<String, Arc<RwLock<Edge>>>>>, // uid -> Edge

    pub constants: Arc<HashMap<String, Constant>>,
}

impl Clone for Graph {
    fn clone(&self) -> Self {
        Graph {
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            constants: self.constants.clone(),
        }
    }
}

impl Graph {
    pub fn new(constants: Option<Arc<HashMap<String, Constant>>>) -> Graph {
        let constants = match constants {
            Some(c) => c,
            None => Arc::new(HashMap::new()),
        };

        Graph {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
            constants,
        }
    }

    pub fn find_node_by_type(&self, typ: &Type) -> Result<Arc<RwLock<Node>>, ImplicaError> {
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
            }),
        }
    }

    pub fn find_all_nodes(&self) -> Result<Vec<Arc<RwLock<Node>>>, ImplicaError> {
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

    pub fn find_edges_by_term_type(
        &self,
        typ: &Type,
    ) -> Result<Vec<Arc<RwLock<Edge>>>, ImplicaError> {
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

    pub fn find_all_edges(&self) -> Result<Vec<Arc<RwLock<Edge>>>, ImplicaError> {
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
                existing: existing.clone(),
                new: Arc::new(RwLock::new(node.clone())),
            });
        }

        nodes.insert(uid.to_string(), Arc::new(RwLock::new(node.clone())));

        Ok(())
    }

    pub fn remove_node(&self, node_uid: &str) -> Result<(), ImplicaError> {
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
            }),
        }
    }

    pub fn set_node_term(&self, node_uid: &str, term: &Term) -> Result<(), ImplicaError> {
        let nodes = self.nodes.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("set node term".to_string()),
        })?;

        match nodes.get(node_uid) {
            Some(node_lock) => {
                let mut node = node_lock.write().map_err(|e| ImplicaError::LockError {
                    rw: "write".to_string(),
                    message: e.to_string(),
                    context: Some("set node term".to_string()),
                })?;

                node.term = Some(Arc::new(RwLock::new(term.clone())));

                Ok(())
            }
            None => Err(ImplicaError::NodeNotFound {
                uid: node_uid.to_string(),
                context: Some("set node term".to_string()),
            }),
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

        let edge = Edge::new(term, start_ptr, end_ptr, properties)?;
        let uid = edge.uid();

        let mut edges = self.edges.write().map_err(|e| ImplicaError::LockError {
            rw: "write".to_string(),
            message: e.to_string(),
            context: Some("add edge".to_string()),
        })?;
        if let Some(existing) = edges.get(uid) {
            return Err(ImplicaError::EdgeAlreadyExists {
                message: "Tried to add a node that already exists.".to_string(),
                existing: existing.clone(),
                new: Arc::new(RwLock::new(edge.clone())),
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
    pub fn remove_edge(&self, edge_uid: &str) -> Result<(), ImplicaError> {
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
            }),
        }
    }

    pub fn remove_edges_matching(&self, pattern: EdgePattern) -> Result<(), ImplicaError> {
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
            let mut context = Context::new();
            if pattern.matches(&edge, &mut context, self.constants.clone())? {
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
            constants: Arc::new(HashMap::new()),
        }
    }
}

#[pyclass(name = "Graph")]
#[derive(Debug, Clone)]
pub struct PyGraph {
    pub(crate) graph: Arc<Graph>,
}

#[pymethods]
impl PyGraph {
    #[new]
    #[pyo3(signature=(constants=None))]
    pub fn new(constants: Option<Vec<Constant>>) -> Self {
        let constants = constants
            .map(|cts| Arc::new(cts.iter().map(|c| (c.name.clone(), c.clone())).collect()));

        PyGraph {
            graph: Arc::new(Graph::new(constants)),
        }
    }

    pub fn query(&self, py: Python) -> PyResult<Py<crate::query::Query>> {
        Py::new(py, crate::query::Query::new(self.graph.clone()))
    }

    fn __str__(&self) -> String {
        let node_count = self.graph.nodes.read().unwrap().len();
        let edge_count = self.graph.edges.read().unwrap().len();
        format!("Graph({} nodes, {} edges)", node_count, edge_count)
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn _get_all_nodes(&self) -> PyResult<Vec<Node>> {
        let nodes = self
            .graph
            .nodes
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("get all nodes".to_string()),
            })?;

        let mut result = Vec::with_capacity(nodes.len());

        for n in nodes.values() {
            let node = n.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("get all nodes".to_string()),
            })?;
            result.push(node.clone());
        }

        Ok(result)
    }

    pub fn _get_all_edges(&self) -> PyResult<Vec<Edge>> {
        let edges = self
            .graph
            .edges
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("get all edges".to_string()),
            })?;

        let mut results = Vec::with_capacity(edges.len());

        for e in edges.values() {
            let edge = e.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("get all edges".to_string()),
            })?;

            results.push(edge.clone());
        }

        Ok(results)
    }
}
