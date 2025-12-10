use pyo3::prelude::*;

use std::{
    collections::HashMap,
    iter::zip,
    sync::{Arc, RwLock},
};

use crate::{
    context::Context,
    errors::ImplicaError,
    graph::{Graph, Node},
    patterns::{CompiledDirection, EdgePattern, NodePattern, PathPattern},
    query::base::{Query, QueryResult},
    typing::{Arrow, Type},
    utils::PlaceholderGenerator,
};

impl Query {
    pub(super) fn execute_create_path(
        &mut self,
        mut path: PathPattern,
    ) -> Result<(), ImplicaError> {
        if path.edges.len() != path.nodes.len() - 1 {
            return Err(ImplicaError::InvalidQuery {
                message: format!(
                    "Expected number of edges {} for {} nodes, actual number of edges {}",
                    path.nodes.len() - 1,
                    path.nodes.len(),
                    path.edges.len()
                ),
                context: Some(format!("create path {}", line!())),
            });
        }

        let ph_generator = PlaceholderGenerator::new();

        for (m, ref context) in self.matches.iter_mut() {
            Self::normalize_nodes(&mut path.nodes, &ph_generator, m, context)?;
            Self::normalize_edges(&mut path.edges, &ph_generator, m, context)?;

            Self::infer(&mut path.nodes, &mut path.edges)?;

            let nodes = Self::insert_nodes(&path.nodes, m, &mut self.graph)?;
            Self::insert_edges(&nodes, &path.edges, m, &mut self.graph)?;

            for ph in ph_generator.prev() {
                m.remove(&ph);
            }
        }

        Ok(())
    }

    fn normalize_nodes(
        nodes: &mut [NodePattern],
        ph_generator: &PlaceholderGenerator,
        r#match: &HashMap<String, QueryResult>,
        context: &Context,
    ) -> Result<(), ImplicaError> {
        for np in nodes.iter_mut() {
            if let Some(ref var) = np.variable {
                if let Some(qr) = r#match.get(var) {
                    match qr {
                        QueryResult::Node(node) => {
                            np.r#type = Some(node.r#type.clone());
                            np.term = if let Some(t) = node.term.clone() {
                                Some(Arc::new(
                                    (t.read().map_err(|e| ImplicaError::LockError {
                                        rw: "read".to_string(),
                                        message: e.to_string(),
                                        context: Some(format!("create path {}", line!())),
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
                                            context: Some(format!("create path {}", line!()))
                                        });
                        }
                    }
                }
            } else {
                if np.variable.is_none() {
                    let var_name = ph_generator.next();
                    np.variable = Some(var_name);
                }

                if let Some(ref type_schema) = np.type_schema {
                    np.r#type = Some(Arc::new(type_schema.as_type(context)?));
                    np.type_schema = None;
                }

                if let Some(ref term_schema) = np.term_schema {
                    np.term = Some(Arc::new(term_schema.as_term(context)?));
                    np.term_schema = None;
                }

                if np.r#type.is_none() {
                    if let Some(ref term) = np.term {
                        np.r#type = Some(term.r#type().clone());
                    }
                }
            }
        }
        Ok(())
    }

    fn normalize_edges(
        edges: &mut [EdgePattern],
        ph_generator: &PlaceholderGenerator,
        r#match: &HashMap<String, QueryResult>,
        context: &Context,
    ) -> Result<(), ImplicaError> {
        for ep in edges.iter_mut() {
            if let Some(ref var) = ep.variable {
                if let Some(qr) = r#match.get(var) {
                    match qr {
                        QueryResult::Edge(edge) => {
                            ep.r#type = Some(edge.term.r#type());
                            ep.term = Some(edge.term.clone())
                        }
                        QueryResult::Node(_) => {
                            return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                            context: Some(format!("create path {}", line!()))
                                        });
                        }
                    }
                }
            } else {
                if ep.variable.is_none() {
                    let var_name = ph_generator.next();
                    ep.variable = Some(var_name);
                }

                if let Some(ref type_schema) = ep.type_schema {
                    ep.r#type = Some(Arc::new(type_schema.as_type(context)?));
                    ep.type_schema = None;
                }

                if let Some(ref term_schema) = ep.term_schema {
                    ep.r#term = Some(Arc::new(term_schema.as_term(context)?));
                    ep.term_schema = None;
                }

                if ep.r#type.is_none() {
                    if let Some(ref term) = ep.term {
                        ep.r#type = Some(term.r#type().clone());
                    }
                }
            }
        }

        Ok(())
    }

    fn infer(nodes: &mut [NodePattern], edges: &mut [EdgePattern]) -> Result<(), ImplicaError> {
        let mut queue: Vec<(usize, bool)> = Vec::new();

        queue.extend(zip(0..nodes.len(), vec![true; nodes.len()]));
        queue.extend(zip(0..(edges.len()), vec![false; edges.len()]));

        while let Some((idx, is_node)) = queue.pop() {
            if is_node {
                if Self::infer_node(nodes, edges, idx)? {
                    if idx > 0 && !queue.contains(&(idx - 1, false)) {
                        queue.push((idx - 1, false));
                    }
                    if idx < nodes.len() - 1 {
                        queue.push((idx, false));
                    }
                }
            } else if Self::infer_edge(nodes, edges, idx)? {
                if !queue.contains(&(idx, true)) {
                    queue.push((idx, true));
                }
                if !queue.contains(&(idx + 1, true)) {
                    queue.push((idx + 1, true));
                }
            }
        }

        Ok(())
    }

    fn infer_node(
        nodes: &mut [NodePattern],
        edges: &mut [EdgePattern],
        idx: usize,
    ) -> Result<bool, ImplicaError> {
        let left_edge_type_update = if idx > 0 {
            if let Some(left_edge) = edges.get(idx - 1) {
                if let Some(ref edge_type) = left_edge.r#type {
                    if let Some(arr) = edge_type.as_arrow() {
                        match left_edge.compiled_direction {
                            CompiledDirection::Forward => Some(arr.right.clone()),
                            CompiledDirection::Backward => Some(arr.left.clone()),
                            CompiledDirection::Any => todo!(
                                "the 'any' direction of edges is not supported yet for create."
                            ),
                        }
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                            message: "The type of an edge must be an arrow type.".to_string(),
                            context: Some(format!("create path node {}", line!())),
                        });
                    }
                } else {
                    None
                }
            } else {
                return Err(ImplicaError::IndexOutOfRange {
                    idx,
                    length: edges.len(),
                    context: Some(format!("create path node {}", line!())),
                });
            }
        } else {
            None
        };

        let left_edge_term_update = if idx > 0 {
            if let Some(left_edge) = edges.get(idx - 1) {
                if let Some(ref edge_term) = left_edge.term {
                    if let Some(left_node) = nodes.get(idx - 1) {
                        if let Some(ref left_node_term) = left_node.term {
                            match left_edge.compiled_direction {
                                CompiledDirection::Forward => {
                                    Some(Arc::new(edge_term.apply(left_node_term)?))
                                }
                                CompiledDirection::Backward => {
                                    if let Some(left_node_term) = left_node_term.as_application() {
                                        if left_node_term.function.as_ref() == edge_term.as_ref() {
                                            Some(left_node_term.argument.clone())
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                CompiledDirection::Any => {
                                    todo!(
                                "the 'any' direction of edges is not supported yet for create."
                            )
                                }
                            }
                        } else {
                            None
                        }
                    } else {
                        return Err(ImplicaError::IndexOutOfRange {
                            idx,
                            length: nodes.len(),
                            context: Some(format!("create path node {}", line!())),
                        });
                    }
                } else {
                    None
                }
            } else {
                return Err(ImplicaError::IndexOutOfRange {
                    idx: idx - 1,
                    length: edges.len(),
                    context: Some(format!("create path node {}", line!())),
                });
            }
        } else {
            None
        };

        let right_edge_type_update = if idx < nodes.len() - 1 {
            if let Some(right_edge) = edges.get(idx) {
                if let Some(ref edge_type) = right_edge.r#type {
                    if let Some(arr) = edge_type.as_arrow() {
                        match right_edge.compiled_direction {
                            CompiledDirection::Forward => Some(arr.left.clone()),
                            CompiledDirection::Backward => Some(arr.right.clone()),
                            CompiledDirection::Any => todo!(
                                "the 'any' direction of edges is not supported yet for create."
                            ),
                        }
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                            message: "The type of an edge must be an arrow type.".to_string(),
                            context: Some(format!("create path node {}", line!())),
                        });
                    }
                } else {
                    None
                }
            } else {
                return Err(ImplicaError::IndexOutOfRange {
                    idx,
                    length: edges.len(),
                    context: Some(format!("create path node {}", line!())),
                });
            }
        } else {
            None
        };

        let right_edge_term_update = if idx < nodes.len() - 1 {
            if let Some(right_edge) = edges.get(idx) {
                if let Some(ref edge_term) = right_edge.term {
                    if let Some(right_node) = nodes.get(idx + 1) {
                        if let Some(ref right_node_term) = right_node.term {
                            match right_edge.compiled_direction {
                                CompiledDirection::Forward => {
                                    if let Some(app) = right_node_term.as_application() {
                                        if &app.function == edge_term {
                                            Some(app.argument.clone())
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                CompiledDirection::Backward => {
                                    Some(Arc::new(edge_term.apply(right_node_term)?))
                                }
                                CompiledDirection::Any => todo!(
                                    "the 'any' direction of edges is not supported yet for create."
                                ),
                            }
                        } else {
                            None
                        }
                    } else {
                        return Err(ImplicaError::IndexOutOfRange {
                            idx,
                            length: nodes.len(),
                            context: Some(format!("create path node {}", line!())),
                        });
                    }
                } else {
                    None
                }
            } else {
                return Err(ImplicaError::IndexOutOfRange {
                    idx,
                    length: edges.len(),
                    context: Some(format!("create path node {}", line!())),
                });
            }
        } else {
            None
        };

        if let Some(node) = nodes.get_mut(idx) {
            let mut changed = false;

            if node.r#type.is_none() {
                if let Some(type_result) = left_edge_type_update {
                    node.r#type = Some(type_result);
                    changed = true;
                } else if let Some(type_result) = right_edge_type_update {
                    node.r#type = Some(type_result);
                    changed = true;
                }
            }

            if node.term.is_none() {
                if let Some(term_result) = left_edge_term_update {
                    node.term = Some(term_result);
                    changed = true;
                } else if let Some(term_result) = right_edge_term_update {
                    node.term = Some(term_result);
                    changed = true;
                }
            }

            Ok(changed)
        } else {
            Err(ImplicaError::IndexOutOfRange {
                idx,
                length: nodes.len(),
                context: Some(format!("create path node {}", line!())),
            })
        }
    }

    fn infer_edge(
        nodes: &mut [NodePattern],
        edges: &mut [EdgePattern],
        idx: usize,
    ) -> Result<bool, ImplicaError> {
        let compiled_direction = if let Some(edge) = edges.get(idx) {
            edge.compiled_direction.clone()
        } else {
            return Err(ImplicaError::IndexOutOfRange {
                idx,
                length: nodes.len(),
                context: Some(format!("create path edge {}", line!())),
            });
        };

        let left_node = match nodes.get(idx) {
            Some(n) => n,
            None => {
                return Err(ImplicaError::IndexOutOfRange {
                    idx,
                    length: nodes.len(),
                    context: Some(format!("create path node {}", line!())),
                });
            }
        };

        let right_node = match nodes.get(idx + 1) {
            Some(n) => n,
            None => {
                return Err(ImplicaError::IndexOutOfRange {
                    idx: idx + 1,
                    length: nodes.len(),
                    context: Some(format!("create path node {}", line!())),
                });
            }
        };

        let type_update = match (&left_node.r#type, &right_node.r#type) {
            (Some(left_type), Some(right_type)) => match compiled_direction {
                CompiledDirection::Forward => Some(Arc::new(Type::Arrow(Arrow::new(
                    left_type.clone(),
                    right_type.clone(),
                )))),
                CompiledDirection::Backward => Some(Arc::new(Type::Arrow(Arrow::new(
                    right_type.clone(),
                    left_type.clone(),
                )))),
                CompiledDirection::Any => {
                    todo!("the 'any' direction of edges is not supported yet for create.")
                }
            },
            _ => None,
        };

        let term_update = match (&left_node.term, &right_node.term) {
            (Some(left_term), Some(right_term)) => match compiled_direction {
                CompiledDirection::Forward => {
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
                CompiledDirection::Backward => {
                    if let Some(left_term) = left_term.as_application() {
                        if right_term.as_ref() == left_term.argument.as_ref() {
                            Some(left_term.function.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                CompiledDirection::Any => {
                    todo!("the 'any' direction of edges is not supported yet for create.")
                }
            },
            _ => None,
        };

        if let Some(edge) = edges.get_mut(idx) {
            let mut changed = false;
            if edge.r#type.is_none() {
                edge.r#type = type_update;
                changed = true;
            }
            if edge.term.is_none() {
                edge.term = term_update;
                changed = true;
            }
            Ok(changed)
        } else {
            Err(ImplicaError::IndexOutOfRange {
                idx,
                length: nodes.len(),
                context: Some(format!("create path edge {}", line!())),
            })
        }
    }

    fn insert_nodes(
        nodes: &Vec<NodePattern>,
        r#match: &mut HashMap<String, QueryResult>,
        graph: &mut Graph,
    ) -> Result<Vec<Node>, ImplicaError> {
        let mut new_nodes = Vec::new();

        for np in nodes {
            // -- Check if already in the match ---
            if let Some(ref var) = np.variable {
                if let Some(qr) = r#match.get(var) {
                    match qr {
                        QueryResult::Node(n) => {
                            new_nodes.push(n.clone());
                        }
                        QueryResult::Edge(_) => {
                            return Err(ImplicaError::InvalidQuery {
                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", var),
                                context: Some(format!("create path node {}", line!()))
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
                        message: "could not resolve the type of a node from the provided pattern"
                            .to_string(),
                        context: Some(format!("create path {}", line!())),
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

            match graph.add_node(&node) {
                Ok(()) => (),
                Err(e) => match e {
                    ImplicaError::NodeAlreadyExists {
                        message: _,
                        existing,
                        new: _,
                    } => {
                        let existing = existing.read().map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some(format!("create path {}", line!())),
                        })?;

                        node = existing.clone();
                    }
                    _ => return Err(e),
                },
            }

            if let Some(ref var) = np.variable {
                r#match.insert(var.clone(), QueryResult::Node(node.clone()));
                new_nodes.push(node);
            }
        }

        Ok(new_nodes)
    }

    fn insert_edges(
        nodes: &[Node],
        edges: &[EdgePattern],
        r#match: &mut HashMap<String, QueryResult>,
        graph: &mut Graph,
    ) -> Result<(), ImplicaError> {
        for (idx, ep) in edges.iter().enumerate() {
            if let Some(ref var) = ep.variable {
                if let Some(qr) = r#match.get(var) {
                    match qr {
                        QueryResult::Edge(e) => {
                            continue;
                        }
                        QueryResult::Node(_) => {
                            return Err(ImplicaError::InvalidQuery {
                                message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                context: Some(format!("create path edge {}", line!()))
                            });
                        }
                    }
                }
            }

            let term = match &ep.term {
                Some(t) => t.clone(),
                None => {
                    return Err(ImplicaError::InvalidQuery {
                        message: "could not resolve the term of an edge from the provided pattern"
                            .to_string(),
                        context: Some(format!("create path node {}", line!())),
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
                        length: nodes.len(),
                        context: Some(format!("create path {}", line!())),
                    });
                }
            };

            let end = match nodes.get(idx + 1) {
                Some(n) => n.clone(),
                None => {
                    return Err(ImplicaError::IndexOutOfRange {
                        idx: idx + 1,
                        length: nodes.len(),
                        context: Some(format!("create path {}", line!())),
                    });
                }
            };

            let edge = match ep.compiled_direction {
                CompiledDirection::Forward => {
                    graph.add_edge(term, start, end, Some(Arc::new(RwLock::new(props))))?
                }
                CompiledDirection::Backward => {
                    graph.add_edge(term, end, start, Some(Arc::new(RwLock::new(props))))?
                }
                CompiledDirection::Any => {
                    todo!("the 'any' direction of edges is not supported yet for create.")
                }
            };

            if let Some(ref var) = ep.variable {
                r#match.insert(var.clone(), QueryResult::Edge(edge));
            }
        }

        Ok(())
    }
}
