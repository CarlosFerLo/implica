use pyo3::prelude::*;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::patterns::CompiledDirection;
use crate::{
    errors::ImplicaError,
    graph::Node,
    patterns::EdgePattern,
    query::base::{Query, QueryResult},
};

impl Query {
    pub(super) fn execute_create_edge(
        &mut self,
        edge_pattern: EdgePattern,
        start_var: String,
        end_var: String,
        is_merge: bool,
    ) -> Result<(), ImplicaError> {
        for (m, context) in self.matches.iter_mut() {
            if let Some(ref var) = edge_pattern.variable {
                if m.contains_key(var) {
                    return Err(ImplicaError::VariableAlreadyExists {
                        name: var.clone(),
                        context: Some(format!("create edge {}", line!())),
                    });
                }
            }

            let (start_node, end_node) =
                Self::get_endpoints(start_var.clone(), end_var.clone(), m)?;

            let term = if let Some(term_obj) = &edge_pattern.term {
                (**term_obj).clone()
            } else if let Some(term_schema) = &edge_pattern.term_schema {
                term_schema.as_term(context, self.graph.constants.clone())?
            } else {
                return Err(ImplicaError::InvalidQuery {
                    message: "To create an edge you must provide either a 'term' or 'term_schema'"
                        .to_string(),
                    context: Some(format!("create edge {}", line!())),
                });
            };

            let mut props = HashMap::new();

            Python::attach(|py| {
                for (k, v) in edge_pattern.properties.iter() {
                    props.insert(k.clone(), v.clone_ref(py));
                }
            });

            let edge = match edge_pattern.compiled_direction {
                CompiledDirection::Forward => self.graph.add_edge(
                    Arc::new(term),
                    start_node,
                    end_node,
                    Some(Arc::new(RwLock::new(props))),
                )?,
                CompiledDirection::Backward => self.graph.add_edge(
                    Arc::new(term),
                    end_node,
                    start_node,
                    Some(Arc::new(RwLock::new(props))),
                )?,
                CompiledDirection::Any => {
                    if let Ok(edge) = self.graph.add_edge(
                        Arc::new(term.clone()),
                        start_node.clone(),
                        end_node.clone(),
                        Some(Arc::new(RwLock::new(props))),
                    ) {
                        edge
                    } else {
                        let mut props = HashMap::new();

                        Python::attach(|py| {
                            for (k, v) in edge_pattern.properties.iter() {
                                props.insert(k.clone(), v.clone_ref(py));
                            }
                        });

                        match self.graph.add_edge(
                            Arc::new(term),
                            end_node,
                            start_node,
                            Some(Arc::new(RwLock::new(props))),
                        ) {
                            Ok(edge) => edge,
                            Err(e) => match &e {
                                ImplicaError::EdgeAlreadyExists {
                                    message: _,
                                    existing,
                                    new: _,
                                } => {
                                    if is_merge {
                                        existing
                                            .read()
                                            .map_err(|e| ImplicaError::LockError {
                                                rw: "read".to_string(),
                                                message: e.to_string(),
                                                context: Some(
                                                    "execute create edge (merge mode)".to_string(),
                                                ),
                                            })?
                                            .clone()
                                    } else {
                                        return Err(e);
                                    }
                                }
                                _ => return Err(e),
                            },
                        }
                    }
                }
            };

            if let Some(ref var) = edge_pattern.variable {
                m.insert(var.clone(), QueryResult::Edge(edge));
            }
        }
        Ok(())
    }

    fn get_endpoints(
        start_var: String,
        end_var: String,
        r#match: &HashMap<String, QueryResult>,
    ) -> Result<(Node, Node), ImplicaError> {
        let start_node = if let Some(qr) = r#match.get(&start_var) {
            match qr {
                QueryResult::Node(n) => n.clone(),
                QueryResult::Edge(_) => {
                    return Err(ImplicaError::InvalidQuery {
                        message: format!(
                            "start node identifier '{}' matches as an edge.",
                            &start_var
                        ),
                        context: Some(format!("create edge {}", line!())),
                    });
                }
            }
        } else {
            return Err(ImplicaError::InvalidQuery {
                message: format!(
                    "start node identifier '{}' did not appear in the match.",
                    &start_var
                ),
                context: Some(format!("create edge {}", line!())),
            });
        };

        let end_node = if let Some(qr) = r#match.get(&end_var) {
            match qr {
                QueryResult::Node(n) => n.clone(),
                QueryResult::Edge(_) => {
                    return Err(ImplicaError::InvalidQuery {
                        message: format!("end node identifier '{}' matches as an edge.", &end_var),
                        context: Some(format!("create edge {}", line!())),
                    });
                }
            }
        } else {
            return Err(ImplicaError::InvalidQuery {
                message: format!(
                    "end node identifier '{}' did not appear in the match.",
                    &end_var
                ),
                context: Some(format!("create edge {}", line!())),
            });
        };

        Ok((start_node, end_node))
    }
}
