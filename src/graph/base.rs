use crate::errors::ImplicaError;

use crate::patterns::{EdgePattern, TypeSchema};
use crate::typing::{Term, Type};
use pyo3::prelude::*;
use serde_json;
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

    pub fn find_node_by_type(&self, typ: &Type) -> Result<Option<Arc<RwLock<Node>>>, ImplicaError> {
        let nodes = self.nodes.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("find node by type".to_string()),
        })?;
        match nodes.get(typ.uid()) {
            Some(node) => Ok(Some(node.clone())),
            None => Ok(None),
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

        {
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

            let mut node = node.clone();

            if node.term.is_none() {
                for cnst in self.constants.values() {
                    if let Some(term) = cnst.matches(&node.r#type)? {
                        node.term = Some(Arc::new(RwLock::new(term)));
                    }
                }
            }

            nodes.insert(uid.to_string(), Arc::new(RwLock::new(node.clone())));
        }

        self.update_node_terms(node)?;

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

                let prev_term = node.term.clone();
                node.term = Some(Arc::new(RwLock::new(term.clone())));

                if prev_term.is_none() {
                    self.update_node_terms(&node)?;
                }

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

    fn update_node_terms(&self, node: &Node) -> Result<(), ImplicaError> {
        if let Some(term_lock) = node.term.clone() {
            let term = term_lock.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("update node terms".to_string()),
            })?;

            if let Some(r#type) = node.r#type.as_arrow() {
                let mut start = Node::new(r#type.left.clone(), None, None)?;

                match self.add_node(&start) {
                    Ok(_) => {}
                    Err(ImplicaError::NodeAlreadyExists {
                        message: _,
                        existing,
                        new: _,
                    }) => {
                        start = existing
                            .read()
                            .map_err(|e| ImplicaError::LockError {
                                rw: "read".to_string(),
                                message: e.to_string(),
                                context: Some("update node terms".to_string()),
                            })?
                            .clone();
                    }
                    Err(e) => return Err(e),
                }

                let mut end = Node::new(r#type.right.clone(), None, None)?;

                match self.add_node(&end) {
                    Ok(_) => {}
                    Err(ImplicaError::NodeAlreadyExists {
                        message: _,
                        existing,
                        new: _,
                    }) => {
                        end = existing
                            .read()
                            .map_err(|e| ImplicaError::LockError {
                                rw: "read".to_string(),
                                message: e.to_string(),
                                context: Some("update node terms".to_string()),
                            })?
                            .clone();
                    }
                    Err(e) => return Err(e),
                }

                self.add_edge(Arc::new(term.clone()), start.clone(), end.clone(), None)?;
            }

            let edges = self.edges.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("update node terms".to_string()),
            })?;

            for edge_lock in edges.values() {
                let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("update node terms".to_string()),
                })?;

                let start = edge.start.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("update node terms".to_string()),
                })?;

                let end = edge.end.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("update node terms".to_string()),
                })?;

                if &*start == node {
                    self.set_node_term(end.uid(), &edge.term.apply(&term)?)?;
                } else if &*end == node {
                    if let Some(app) = term.as_application() {
                        if app.function == edge.term {
                            self.set_node_term(start.uid(), &app.argument)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn to_dot(&self) -> Result<String, ImplicaError> {
        let mut dot = String::new();
        dot.push_str("digraph G {\n");

        let nodes = self.nodes.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("to dot".to_string()),
        })?;

        for (uid, node_lock) in nodes.iter() {
            let node = node_lock.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("to dot".to_string()),
            })?;

            dot.push_str(&format!(
                " \"{}\" [label=\"{}\", color=\"{}\"];\n",
                uid,
                node.r#type,
                if node.term.is_none() { "blue" } else { "red" }
            ));
        }

        let edges = self.edges.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("to dot".to_string()),
        })?;

        for edge_lock in edges.values() {
            let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("to dot".to_string()),
            })?;

            let start = edge.start.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("to dot".to_string()),
            })?;

            let end = edge.end.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("to dot".to_string()),
            })?;

            dot.push_str(&format!(
                " \"{}\" -> \"{}\" [label=\"{}\"];\n",
                start.uid(),
                end.uid(),
                edge.term
            ));
        }

        dot.push_str("}\n");

        Ok(dot)
    }

    pub fn to_force_graph_json(&self) -> Result<String, ImplicaError> {
        let nodes: Vec<_> = self
            .nodes
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("to force graph json".to_string()),
            })?
            .values()
            .map(|n| -> Result<HashMap<String, String>, ImplicaError> {
                let node = n.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("to force graph json".to_string()),
                })?;

                Ok(HashMap::from([
                    ("id".to_string(), node.uid().to_string()),
                    ("label".to_string(), node.r#type.to_string()),
                    (
                        "group".to_string(),
                        if node.term.is_none() {
                            "no_term".to_string()
                        } else {
                            "term".to_string()
                        },
                    ),
                ]))
            })
            .collect::<Result<_, _>>()?;

        let links: Vec<_> = self
            .edges
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("to force graph json".to_string()),
            })?
            .values()
            .map(|e| -> Result<HashMap<String, String>, ImplicaError> {
                let edge = e.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("to force graph json".to_string()),
                })?;

                let start = edge.start.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("to force graph json".to_string()),
                })?;

                let end = edge.end.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("to force graph json".to_string()),
                })?;

                Ok(HashMap::from([
                    ("source".to_string(), start.uid().to_string()),
                    ("target".to_string(), end.uid().to_string()),
                    ("label".to_string(), edge.term.to_string()),
                ]))
            })
            .collect::<Result<_, _>>()?;

        let graph = HashMap::from([("nodes".to_string(), nodes), ("links".to_string(), links)]);

        serde_json::to_string(&graph).map_err(|e| ImplicaError::SerializationError {
            message: e.to_string(),
            context: Some("to force graph json".to_string()),
        })
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

    pub fn to_dot(&self) -> PyResult<String> {
        self.graph.to_dot().map_err(|e| e.into())
    }

    pub fn to_force_graph_json(&self) -> PyResult<String> {
        self.graph.to_force_graph_json().map_err(|e| e.into())
    }
}
