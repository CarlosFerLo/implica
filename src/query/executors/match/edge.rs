use std::collections::HashMap;

use crate::context::Context;
use crate::errors::ImplicaError;
use crate::graph::Edge;
use crate::patterns::{CompiledDirection, EdgePattern};
use crate::query::base::{Query, QueryResult};

impl Query {
    pub(super) fn execute_match_edge(
        &mut self,
        edge_pattern: EdgePattern,
        start_var: Option<String>,
        end_var: Option<String>,
    ) -> Result<(), ImplicaError> {
        let edges = self
            .graph
            .edges
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("execute match edge".to_string()),
            })?;

        if self.matches.is_empty() {
            for edge_lock in edges.values() {
                let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("execute match edge".to_string()),
                })?;

                let mut context = Context::new();

                if edge_pattern.matches(&edge, &mut context)? {
                    for dict in Self::generate_full_edge_match_dict(
                        &edge,
                        &edge_pattern,
                        start_var.clone(),
                        end_var.clone(),
                    )? {
                        if !dict.is_empty() {
                            self.matches.push((dict, context.clone()));
                        }
                    }
                }
            }
        } else {
            let mut results = Vec::new();
            let mut contained = false;

            for (ref m, context) in self.matches.iter_mut() {
                for edge_lock in edges.values() {
                    let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("execute match edge".to_string()),
                    })?;

                    if edge_pattern.matches(&edge, context)? {
                        let mut dict = m.clone();

                        if let Some(ref var) = edge_pattern.variable {
                            if let Some(qr) = m.get(var) {
                                contained = true;
                                match qr {
                                    QueryResult::Edge(e) => {
                                        if &*edge != e {
                                            continue;
                                        }
                                    }
                                    QueryResult::Node(_) => {
                                        return Err(ImplicaError::InvalidQuery { message: format!("tried to access variable '{}' as an edge that belongs to a node", var), context: Some("execute match edge".to_string()) });
                                    }
                                }
                            } else {
                                dict.insert(var.clone(), QueryResult::Edge(edge.clone()));
                            }
                        }

                        match edge_pattern.compiled_direction {
                            CompiledDirection::Forward => {
                                if Self::match_adjacent_nodes(
                                    &edge,
                                    start_var.clone(),
                                    end_var.clone(),
                                    &mut contained,
                                    &mut dict,
                                )? {
                                    results.push((dict, context.clone()));
                                }
                            }
                            CompiledDirection::Backward => {
                                if Self::match_adjacent_nodes(
                                    &edge,
                                    end_var.clone(),
                                    start_var.clone(),
                                    &mut contained,
                                    &mut dict,
                                )? {
                                    results.push((dict, context.clone()));
                                }
                            }
                            CompiledDirection::Any => {
                                let mut dict2 = dict.clone();

                                // -- Forward ----
                                if Self::match_adjacent_nodes(
                                    &edge,
                                    start_var.clone(),
                                    end_var.clone(),
                                    &mut contained,
                                    &mut dict,
                                )? {
                                    results.push((dict, context.clone()));
                                }

                                // -- Backward ----
                                if Self::match_adjacent_nodes(
                                    &edge,
                                    end_var.clone(),
                                    start_var.clone(),
                                    &mut contained,
                                    &mut dict2,
                                )? {
                                    results.push((dict2, context.clone()));
                                }
                            }
                        }
                    }
                }
            }

            if contained {
                self.matches = results;
            } else {
                let mut results = Vec::new();

                for (m, context) in self.matches.iter() {
                    for edge_lock in edges.values() {
                        let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some("execute match edge".to_string()),
                        })?;

                        let mut context = context.clone();

                        if edge_pattern.matches(&edge, &mut context)? {
                            for dict in Self::generate_full_edge_match_dict(
                                &edge,
                                &edge_pattern,
                                start_var.clone(),
                                end_var.clone(),
                            )? {
                                let mut dict = dict.clone();

                                for (key, val) in m.iter() {
                                    dict.insert(key.clone(), val.clone());
                                }

                                results.push((dict, context.clone()));
                            }
                        }
                    }
                }

                self.matches = results;
            }
        }
        Ok(())
    }

    fn generate_full_edge_match_dict(
        e: &Edge,
        edge_pattern: &EdgePattern,
        start_var: Option<String>,
        end_var: Option<String>,
    ) -> Result<Vec<HashMap<String, QueryResult>>, ImplicaError> {
        let mut matches = Vec::new();

        let mut dict = HashMap::new();

        if let Some(ref var) = edge_pattern.variable {
            dict.insert(var.clone(), QueryResult::Edge(e.clone()));
        }

        match edge_pattern.compiled_direction {
            CompiledDirection::Forward => {
                Self::populate_start_and_end_var_from_edge(
                    e,
                    start_var.clone(),
                    end_var.clone(),
                    &mut dict,
                )?;

                if !dict.is_empty() {
                    matches.push(dict);
                }
            }
            CompiledDirection::Backward => {
                Self::populate_start_and_end_var_from_edge(
                    e,
                    end_var.clone(),
                    start_var.clone(),
                    &mut dict,
                )?;

                if !dict.is_empty() {
                    matches.push(dict);
                }
            }
            CompiledDirection::Any => {
                let mut dict2 = dict.clone();

                // -- Forward ---
                Self::populate_start_and_end_var_from_edge(
                    e,
                    start_var.clone(),
                    end_var.clone(),
                    &mut dict,
                )?;

                if !dict.is_empty() {
                    matches.push(dict);
                }

                // -- Backward ---
                Self::populate_start_and_end_var_from_edge(
                    e,
                    end_var.clone(),
                    start_var.clone(),
                    &mut dict2,
                )?;

                if !dict2.is_empty() {
                    matches.push(dict2);
                }
            }
        }
        Ok(matches)
    }

    fn populate_start_and_end_var_from_edge(
        edge: &Edge,
        start_var: Option<String>,
        end_var: Option<String>,
        dict: &mut HashMap<String, QueryResult>,
    ) -> Result<(), ImplicaError> {
        if let Some(ref start_var) = start_var {
            let start_node = edge.start.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("execute match edge".to_string()),
            })?;

            dict.insert(start_var.clone(), QueryResult::Node(start_node.clone()));
        }

        if let Some(ref end_var) = end_var {
            let end_node = edge.end.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("execute match edge".to_string()),
            })?;

            dict.insert(end_var.clone(), QueryResult::Node(end_node.clone()));
        }
        Ok(())
    }

    fn match_adjacent_nodes(
        edge: &Edge,
        start_var: Option<String>,
        end_var: Option<String>,
        contained: &mut bool,
        dict: &mut HashMap<String, QueryResult>,
    ) -> Result<bool, ImplicaError> {
        if let Some(ref start_var) = start_var {
            let start_node = edge.start.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("execute match edge".to_string()),
            })?;

            if let Some(ref qr) = dict.get(start_var) {
                *contained = true;
                match qr {
                    QueryResult::Node(node) => {
                        if &*start_node != node {
                            return Ok(false);
                        }
                    }
                    QueryResult::Edge(_) => {
                        return Err(ImplicaError::InvalidQuery {
                            message: format!(
                                "tried to access variable '{}' as an edge that belongs to a node",
                                start_var
                            ),
                            context: Some("execute match edge".to_string()),
                        })?;
                    }
                }
            } else {
                dict.insert(start_var.clone(), QueryResult::Node(start_node.clone()));
            }
        }

        if let Some(ref end_var) = end_var {
            let end_node = edge.end.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("execute match edge".to_string()),
            })?;

            if let Some(qr) = dict.get(end_var) {
                *contained = true;
                match qr {
                    QueryResult::Node(node) => {
                        if &*end_node != node {
                            return Ok(false);
                        }
                    }
                    QueryResult::Edge(_) => {
                        return Err(ImplicaError::InvalidQuery {
                            message: format!(
                                "tried to access variable '{}' as a node that belongs to an edge",
                                end_var
                            ),
                            context: Some("execute match edge".to_string()),
                        })?;
                    }
                }
            } else {
                dict.insert(end_var.clone(), QueryResult::Node(end_node.clone()));
            }
        }

        Ok(true)
    }
}
