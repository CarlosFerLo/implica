use std::collections::HashMap;
use std::iter::zip;
use std::sync::{Arc, RwLock};

use pyo3::prelude::*;

use crate::errors::ImplicaError;
use crate::graph::Node;
use crate::query::base::{CreateOp, Query, QueryResult};
use crate::typing::{Arrow, Type};
use crate::utils::PlaceholderGenerator;

impl Query {
    pub(super) fn execute_create(&mut self, create_op: CreateOp) -> Result<(), ImplicaError> {
        if self.matches.is_empty() {
            self.matches.push(HashMap::new());
        }

        match create_op {
            CreateOp::Node(node_pattern) => {
                for m in self.matches.iter_mut() {
                    if let Some(var) = &node_pattern.variable {
                        if m.contains_key(var) {
                            return Err(ImplicaError::VariableAlreadyExists {
                                name: var.clone(),
                                context: Some("create node".to_string()),
                            });
                        }
                    }

                    let r#type = if let Some(type_obj) = &node_pattern.r#type {
                        type_obj.clone()
                    } else if let Some(type_schema) = &node_pattern.type_schema {
                        Arc::new(type_schema.as_type(self.context.clone())?)
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                            message:
                                "To create a node you must provide either a 'type' or 'type_schema'"
                                    .to_string(),
                            context: Some("create node".to_string()),
                        });
                    };

                    let term = if let Some(term_obj) = &node_pattern.term {
                        Some(term_obj.clone())
                    } else if let Some(term_schema) = &node_pattern.term_schema {
                        Some(Arc::new(term_schema.as_term(self.context.clone())?))
                    } else {
                        None
                    };

                    let mut props = HashMap::new();

                    Python::attach(|py| {
                        for (k, v) in node_pattern.properties.iter() {
                            props.insert(k.clone(), v.clone_ref(py));
                        }
                    });

                    let node = Node::new(
                        r#type,
                        term.map(|t| Arc::new(RwLock::new((*t).clone()))),
                        Some(props),
                    )?;

                    self.graph.add_node(&node)?;

                    if let Some(var) = &node_pattern.variable {
                        m.insert(var.clone(), QueryResult::Node(node));
                    }
                }
            }
            CreateOp::Edge(edge_pattern, start, end) => {
                for m in self.matches.iter_mut() {
                    if let Some(ref var) = edge_pattern.variable {
                        if m.contains_key(var) {
                            return Err(ImplicaError::VariableAlreadyExists {
                                name: var.clone(),
                                context: Some("create edge".to_string()),
                            });
                        }
                    }

                    let start_node = if let Some(qr) = m.get(&start) {
                        match qr {
                            QueryResult::Node(n) => n.clone(),
                            QueryResult::Edge(_) => {
                                return Err(ImplicaError::InvalidQuery {
                                    message: format!(
                                        "start node identifier '{}' matches as an edge.",
                                        &start
                                    ),
                                    context: Some("create_edge".to_string()),
                                });
                            }
                        }
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                            message: format!(
                                "start node identifier '{}' did not appear in the match.",
                                &start
                            ),
                            context: Some("create edge".to_string()),
                        });
                    };

                    let end_node = if let Some(qr) = m.get(&end) {
                        match qr {
                            QueryResult::Node(n) => n.clone(),
                            QueryResult::Edge(_) => {
                                return Err(ImplicaError::InvalidQuery {
                                    message: format!(
                                        "end node identifier '{}' matches as an edge.",
                                        &start
                                    ),
                                    context: Some("create_edge".to_string()),
                                });
                            }
                        }
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                            message: format!(
                                "end node identifier '{}' did not appear in the match.",
                                &start
                            ),
                            context: Some("create edge".to_string()),
                        });
                    };

                    let term = if let Some(term_obj) = &edge_pattern.term {
                        (**term_obj).clone()
                    } else if let Some(term_schema) = &edge_pattern.term_schema {
                        term_schema.as_term(self.context.clone())?
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                        message:
                            "To create an edge you must provide either a 'term' or 'term_schema'"
                                .to_string(),
                        context: Some("create edge".to_string()),
                    });
                    };

                    let mut props = HashMap::new();

                    Python::attach(|py| {
                        for (k, v) in edge_pattern.properties.iter() {
                            props.insert(k.clone(), v.clone_ref(py));
                        }
                    });

                    let edge = self.graph.add_edge(
                        Arc::new(term),
                        start_node,
                        end_node,
                        Some(Arc::new(RwLock::new(props))),
                    )?;

                    if let Some(ref var) = edge_pattern.variable {
                        m.insert(var.clone(), QueryResult::Edge(edge));
                    }
                }
            }
            CreateOp::Path(mut path) => {
                if path.edges.len() != path.nodes.len() + 1 {
                    return Err(ImplicaError::InvalidQuery {
                        message: format!(
                            "Expected number of edges {} for {} nodes, actual number of edges {}",
                            path.nodes.len() + 1,
                            path.nodes.len(),
                            path.edges.len()
                        ),
                        context: Some("create path".to_string()),
                    });
                }

                let nodes_len = path.nodes.len();

                let ph_generator = PlaceholderGenerator::new();

                for np in path.nodes.iter_mut() {
                    if np.variable.is_none() {
                        let var_name = ph_generator.next();
                        np.variable = Some(var_name);
                    }

                    if let Some(ref type_schema) = np.type_schema {
                        np.r#type = Some(Arc::new(type_schema.as_type(self.context.clone())?));
                        np.type_schema = None;
                    }

                    if let Some(ref term_schema) = np.term_schema {
                        np.term = Some(Arc::new(term_schema.as_term(self.context.clone())?));
                        np.term_schema = None;
                    }

                    if np.r#type.is_none() {
                        if let Some(ref term) = np.term {
                            np.r#type = Some(term.r#type().clone());
                        }
                    }
                }
                for ep in path.edges.iter_mut() {
                    if ep.variable.is_none() {
                        let var_name = ph_generator.next();
                        ep.variable = Some(var_name);
                    }

                    if let Some(ref type_schema) = ep.type_schema {
                        ep.r#type = Some(Arc::new(type_schema.as_type(self.context.clone())?));
                        ep.type_schema = None;
                    }

                    if let Some(ref term_schema) = ep.term_schema {
                        ep.r#term = Some(Arc::new(term_schema.as_term(self.context.clone())?));
                        ep.term_schema = None;
                    }

                    if ep.r#type.is_none() {
                        if let Some(ref term) = ep.term {
                            ep.r#type = Some(term.r#type().clone());
                        }
                    }
                }

                for m in self.matches.iter_mut() {
                    for np in path.nodes.iter_mut() {
                        if let Some(ref var) = np.variable {
                            if let Some(qr) = m.get(var) {
                                match qr {
                                    QueryResult::Node(node) => {
                                        np.r#type = Some(node.r#type.clone());
                                        np.term = if let Some(t) = node.term.clone() {
                                            Some(Arc::new(
                                                (t.read().map_err(|e| {
                                                    ImplicaError::LockError {
                                                        rw: "read".to_string(),
                                                        message: e.to_string(),
                                                        context: Some("execute delete".to_string()),
                                                    }
                                                })?)
                                                .clone(),
                                            ))
                                        } else {
                                            None
                                        };
                                    }
                                    QueryResult::Edge(_) => {
                                        return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", var),
                                            context: Some("create path".to_string())
                                        });
                                    }
                                }
                            }
                        }
                    }

                    for ep in path.edges.iter_mut() {
                        if let Some(ref var) = ep.variable {
                            if let Some(qr) = m.get(var) {
                                match qr {
                                    QueryResult::Edge(edge) => {
                                        ep.r#type = Some(edge.term.r#type());
                                        ep.term = Some(edge.term.clone())
                                    }
                                    QueryResult::Node(_) => {
                                        return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                            context: Some("create path".to_string())
                                        });
                                    }
                                }
                            }
                        }
                    }

                    let mut queue: Vec<(usize, bool)> = Vec::new();

                    queue.extend(zip(0..nodes_len, vec![true; nodes_len]));
                    queue.extend(zip(0..(nodes_len - 1), vec![false; nodes_len - 1]));

                    // Process the queue
                    while let Some((idx, is_node)) = queue.pop() {
                        if is_node {
                            // First, collect the values we need from other nodes/edges before mutably borrowing
                            let left_edge_type_update = if idx > 0 {
                                if let Some(left_edge) = path.edges.get(idx - 1) {
                                    if let Some(ref edge_type) = left_edge.r#type {
                                        if let Some(arr) = edge_type.as_arrow() {
                                            Some(arr.right.clone())
                                        } else {
                                            return Err(ImplicaError::InvalidQuery {
                                                message:
                                                    "The type of an edge must be an arrow type."
                                                        .to_string(),
                                                context: Some("create path node".to_string()),
                                            });
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len - 1,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            } else {
                                None
                            };

                            let left_edge_term_update = if idx > 0 {
                                if let Some(left_edge) = path.edges.get(idx - 1) {
                                    if let Some(ref edge_term) = left_edge.term {
                                        if let Some(left_node) = path.nodes.get(idx - 1) {
                                            if let Some(ref left_node_term) = left_node.term {
                                                Some(edge_term.apply(left_node_term)?)
                                            } else {
                                                None
                                            }
                                        } else {
                                            return Err(ImplicaError::IndexOutOfRange {
                                                idx,
                                                length: nodes_len,
                                                context: Some("create path node".to_string()),
                                            });
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len - 1,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            } else {
                                None
                            };

                            let right_edge_type_update = if idx < nodes_len - 1 {
                                if let Some(right_edge) = path.edges.get(idx) {
                                    if let Some(ref edge_type) = right_edge.r#type {
                                        if let Some(arr) = edge_type.as_arrow() {
                                            Some(arr.right.clone())
                                        } else {
                                            return Err(ImplicaError::InvalidQuery {
                                                message:
                                                    "The type of an edge must be an arrow type."
                                                        .to_string(),
                                                context: Some("create path node".to_string()),
                                            });
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len - 1,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            } else {
                                None
                            };

                            let right_edge_term_update = if idx < nodes_len - 1 {
                                if let Some(right_edge) = path.edges.get(idx) {
                                    if let Some(ref edge_term) = right_edge.term {
                                        if let Some(right_node) = path.nodes.get(idx + 1) {
                                            if let Some(ref right_node_term) = right_node.term {
                                                Some(edge_term.apply(right_node_term)?)
                                            } else {
                                                None
                                            }
                                        } else {
                                            return Err(ImplicaError::IndexOutOfRange {
                                                idx,
                                                length: nodes_len,
                                                context: Some("create path node".to_string()),
                                            });
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len - 1,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            } else {
                                None
                            };

                            // Now we can safely borrow the node mutably
                            if let Some(node) = path.nodes.get_mut(idx) {
                                let mut changed = false;

                                if idx > 0 {
                                    // Apply type update
                                    if node.r#type.is_none() {
                                        if let Some(type_result) = left_edge_type_update {
                                            node.r#type = Some(type_result);
                                            changed = true;
                                        }
                                    }

                                    // Apply term update
                                    if node.term.is_none() {
                                        if let Some(term_result) = left_edge_term_update {
                                            node.term = Some(Arc::new(term_result));
                                            changed = true;
                                        }
                                    }
                                }

                                if idx < nodes_len - 1 {
                                    if node.r#type.is_none() {
                                        if let Some(type_result) = right_edge_type_update {
                                            node.r#type = Some(type_result);
                                            changed = true;
                                        }
                                    }

                                    if node.term.is_none() {
                                        if let Some(term_result) = right_edge_term_update {
                                            node.term = Some(Arc::new(term_result));
                                            changed = true;
                                        }
                                    }
                                }

                                if changed {
                                    queue.extend([(idx - 1, false), (idx, false)]);
                                }
                            } else {
                                return Err(ImplicaError::IndexOutOfRange {
                                    idx,
                                    length: nodes_len,
                                    context: Some("create path node".to_string()),
                                });
                            }
                        } else {
                            let left_node = match path.nodes.get(idx) {
                                Some(n) => n,
                                None => {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            };

                            let right_node = match path.nodes.get(idx + 1) {
                                Some(n) => n,
                                None => {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            };

                            let type_update = match (&left_node.r#type, &right_node.r#type) {
                                (Some(left_type), Some(right_type)) => Some(Type::Arrow(
                                    Arrow::new(left_type.clone(), right_type.clone()),
                                )),
                                _ => None,
                            };

                            let term_update = match (&left_node.term, &right_node.term) {
                                (Some(left_term), Some(right_term)) => {
                                    if let Some(right_term) = right_term.as_application() {
                                        if left_term.as_ref() == right_term.argument.as_ref() {
                                            Some(right_term.function.clone())
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            };

                            if let Some(edge) = path.edges.get_mut(idx) {
                                let mut changed = false;
                                if edge.r#type.is_none() {
                                    edge.r#type = type_update.map(Arc::new);
                                    changed = true;
                                }
                                if edge.term.is_none() {
                                    edge.term = term_update;
                                    changed = true;
                                }

                                if changed {
                                    queue.extend([(idx, true), (idx + 1, true)]);
                                }
                            } else {
                                return Err(ImplicaError::IndexOutOfRange {
                                    idx,
                                    length: nodes_len,
                                    context: Some("create path edge".to_string()),
                                });
                            }
                        }
                    }

                    let mut nodes = Vec::new();

                    for np in path.nodes.iter() {
                        if let Some(ref var) = np.variable {
                            if let Some(qr) = m.get(var) {
                                match qr {
                                    QueryResult::Node(n) => {
                                        nodes.push(n.clone());
                                    }
                                    QueryResult::Edge(_) => {
                                        return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", var),
                                            context: Some("create path".to_string())
                                        });
                                    }
                                }

                                continue;
                            }
                        }

                        let r#type = match &np.r#type {
                            Some(t) => t.clone(),
                            None => {
                                return Err(ImplicaError::InvalidQuery {
                                message:
                                    "could not resolve the type of a node from the provided pattern"
                                        .to_string(),
                                context: Some("create path".to_string()),
                            });
                            }
                        };
                        let term = np.term.clone().map(|t| Arc::new(RwLock::new((*t).clone())));

                        let mut props = HashMap::new();

                        Python::attach(|py| {
                            for (k, v) in np.properties.iter() {
                                props.insert(k.clone(), v.clone_ref(py));
                            }
                        });

                        let mut node = Node::new(r#type, term, Some(props))?;

                        match self.graph.add_node(&node) {
                            Ok(()) => (),
                            Err(e) => match e {
                                ImplicaError::NodeAlreadyExists {
                                    message: _,
                                    existing,
                                    new: _,
                                } => node = existing.clone(),
                                _ => {
                                    return Err(e);
                                }
                            },
                        }

                        if let Some(ref var) = np.variable {
                            m.insert(var.clone(), QueryResult::Node(node.clone()));
                            nodes.push(node);
                        }
                    }

                    for (idx, ep) in path.edges.iter().enumerate() {
                        if let Some(ref var) = ep.variable {
                            if m.contains_key(var) {
                                continue;
                            }
                        }

                        let term = match &ep.term {
                            Some(t) => t.clone(),
                            None => {
                                return Err(ImplicaError::InvalidQuery {
                                    message: "could not resolve the term of an edge from the provided pattern".to_string(),
                                    context: Some("create path".to_string())
                                });
                            }
                        };

                        let mut props = HashMap::new();

                        Python::attach(|py| {
                            for (k, v) in ep.properties.iter() {
                                props.insert(k.clone(), v.clone_ref(py));
                            }
                        });

                        let start = match nodes.get(idx) {
                            Some(n) => n.clone(),
                            None => {
                                return Err(ImplicaError::IndexOutOfRange {
                                    idx,
                                    length: nodes_len,
                                    context: Some("create path".to_string()),
                                });
                            }
                        };

                        let end = match nodes.get(idx + 1) {
                            Some(n) => n.clone(),
                            None => {
                                return Err(ImplicaError::IndexOutOfRange {
                                    idx: idx + 1,
                                    length: nodes_len,
                                    context: Some("create path".to_string()),
                                });
                            }
                        };

                        let edge = self.graph.add_edge(
                            term,
                            start,
                            end,
                            Some(Arc::new(RwLock::new(props))),
                        )?;

                        if let Some(ref var) = ep.variable {
                            m.insert(var.clone(), QueryResult::Edge(edge));
                        }
                    }

                    for ph in ph_generator.prev() {
                        m.remove(&ph);
                    }
                }
            }
        }
        Ok(())
    }
}
