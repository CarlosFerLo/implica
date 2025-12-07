use std::collections::HashMap;

use pyo3::prelude::*;

use crate::errors::ImplicaError;
use crate::query::base::{Query, QueryResult};

impl Query {
    pub(super) fn execute_set(
        &mut self,
        var: String,
        props: HashMap<String, Py<PyAny>>,
        overwrite: bool,
    ) -> Result<(), ImplicaError> {
        for m in self.matches.iter() {
            if let Some(qr) = m.get(&var) {
                match qr {
                    QueryResult::Node(n) => {
                        let nodes =
                            self.graph
                                .nodes
                                .read()
                                .map_err(|e| ImplicaError::LockError {
                                    rw: "read".to_string(),
                                    message: e.to_string(),
                                    context: Some("execute set".to_string()),
                                })?;
                        if let Some(node_lock) = nodes.get(n.uid()) {
                            let node = node_lock.read().map_err(|e| ImplicaError::LockError {
                                rw: "read".to_string(),
                                message: e.to_string(),
                                context: Some("execute set".to_string()),
                            })?;
                            let mut node_props =
                                node.properties
                                    .write()
                                    .map_err(|e| ImplicaError::LockError {
                                        rw: "write".to_string(),
                                        message: e.to_string(),
                                        context: Some("execute set".to_string()),
                                    })?;

                            if overwrite {
                                node_props.clear();
                            }

                            Python::attach(|py| {
                                for (k, v) in props.iter() {
                                    node_props.insert(k.clone(), v.clone_ref(py));
                                }
                            })
                        } else {
                            return Err(ImplicaError::NodeNotFound {
                                uid: n.uid().to_string(),
                                context: Some("execute set node".to_string()),
                            });
                        }
                    }
                    QueryResult::Edge(e) => {
                        let edges =
                            self.graph
                                .nodes
                                .read()
                                .map_err(|e| ImplicaError::LockError {
                                    rw: "read".to_string(),
                                    message: e.to_string(),
                                    context: Some("execute set".to_string()),
                                })?;
                        if let Some(edge_lock) = edges.get(e.uid()) {
                            let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                                rw: "read".to_string(),
                                message: e.to_string(),
                                context: Some("execute set".to_string()),
                            })?;
                            let mut edge_props =
                                edge.properties
                                    .write()
                                    .map_err(|e| ImplicaError::LockError {
                                        rw: "read".to_string(),
                                        message: e.to_string(),
                                        context: Some("execute set".to_string()),
                                    })?;

                            if overwrite {
                                edge_props.clear();
                            }

                            Python::attach(|py| {
                                for (k, v) in props.iter() {
                                    edge_props.insert(k.clone(), v.clone_ref(py));
                                }
                            });
                        } else {
                            return Err(ImplicaError::EdgeNotFound {
                                uid: e.uid().to_string(),
                                context: Some("execute set edge".to_string()),
                            });
                        }
                    }
                }
            } else {
                return Err(ImplicaError::VariableNotFound {
                    name: var.clone(),
                    context: Some("delete".to_string()),
                });
            }
        }

        Ok(())
    }
}
