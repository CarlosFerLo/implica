use std::collections::HashMap;

use crate::context::Context;
use crate::errors::ImplicaError;
use crate::patterns::NodePattern;
use crate::query::base::{Query, QueryResult};

impl Query {
    pub(super) fn execute_match_node(
        &mut self,
        node_pattern: NodePattern,
    ) -> Result<(), ImplicaError> {
        if let Some(ref var) = node_pattern.variable {
            let nodes = self
                .graph
                .nodes
                .read()
                .map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("execute match node".to_string()),
                })?;

            if self.matches.is_empty() {
                for node_lock in nodes.values() {
                    let node = node_lock.read().map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("execute match node".to_string()),
                    })?;

                    let mut context = Context::new();

                    if node_pattern.matches(&node, &mut context, self.graph.constants.clone())? {
                        let dict = HashMap::from([(var.clone(), QueryResult::Node(node.clone()))]);
                        self.matches.push((dict, context));
                    }
                }
            } else {
                let mut results = Vec::new();
                let mut contained = false;

                for (ref m, context) in self.matches.iter_mut() {
                    if let Some(old) = m.get(var) {
                        match old {
                            QueryResult::Node(old_node) => {
                                for new_node_lock in nodes.values() {
                                    let new_node = new_node_lock.read().map_err(|e| {
                                        ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match node".to_string()),
                                        }
                                    })?;

                                    if node_pattern.matches(
                                        &new_node,
                                        context,
                                        self.graph.constants.clone(),
                                    )? && &*new_node == old_node
                                    {
                                        results.push((m.clone(), context.clone()));
                                    }
                                }
                            }
                            QueryResult::Edge(old_edge) => {
                                return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", var),
                                            context: Some("match variable".to_string())
                                        });
                            }
                        }

                        contained = true;
                    }
                }

                if contained {
                    self.matches = results;
                } else {
                    let mut results = Vec::new();
                    for node_lock in nodes.values() {
                        let node = node_lock.read().map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some("execute match node".to_string()),
                        })?;

                        for (m, context) in self.matches.iter() {
                            let mut context = context.clone();
                            if node_pattern.matches(
                                &node,
                                &mut context,
                                self.graph.constants.clone(),
                            )? {
                                let mut dict = m.clone();

                                dict.insert(var.clone(), QueryResult::Node(node.clone()));
                                results.push((dict, context));
                            }
                        }
                    }
                    self.matches = results;
                }
            }

            Ok(())
        } else {
            Err(ImplicaError::InvalidQuery {
                message: "must provide a 'variable' property for the node pattern in match - it usually means providing the 'node' parameter in the match method"
                    .to_string(),
                context: Some("execute match node".to_string()),
            })
        }
    }
}
