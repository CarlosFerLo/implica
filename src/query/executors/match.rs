use std::collections::HashMap;
use std::iter::zip;

use crate::errors::ImplicaError;
use crate::graph::Edge;
use crate::patterns::{EdgePattern, NodePattern, PathPattern};
use crate::query::base::{MatchOp, Query, QueryResult};
use crate::utils::PlaceholderGenerator;

impl Query {
    pub(super) fn execute_match(&mut self, match_op: MatchOp) -> Result<(), ImplicaError> {
        match match_op {
            MatchOp::Node(node_pattern) => self.execute_match_node(node_pattern),
            MatchOp::Edge(edge_pattern, start_var, end_var) => {
                self.execute_match_edge(edge_pattern, start_var, end_var)
            }
            MatchOp::Path(path) => self.execute_match_path(path),
        }
    }

    fn execute_match_node(&mut self, node_pattern: NodePattern) -> Result<(), ImplicaError> {
        let mut new_matches = Vec::new();

        let nodes = self
            .graph
            .nodes
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("execute match node".to_string()),
            })?;

        for node_lock in nodes.values() {
            let node = node_lock.read().map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("execute match node".to_string()),
            })?;

            if node_pattern.matches(&node, self.context.clone())? {
                new_matches.push(node.clone());
            }
        }

        if let Some(ref var) = node_pattern.variable {
            if self.matches.is_empty() {
                for m in new_matches {
                    let dict = HashMap::from([(var.clone(), QueryResult::Node(m))]);
                    self.matches.push(dict);
                }
            } else {
                let mut results = Vec::new();
                let mut preserved = Vec::new();
                let mut contained = false;

                for m in self.matches.iter() {
                    if let Some(old) = m.get(var) {
                        match old {
                            QueryResult::Node(old_node) => {
                                for new_node in new_matches.iter() {
                                    if new_node == old_node {
                                        results.push(m.clone());
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
                    } else {
                        preserved.push(m.clone());
                    }
                }

                if contained {
                    results.append(&mut preserved);
                    self.matches = results;
                } else {
                    for m in new_matches {
                        let dict = HashMap::from([(var.clone(), QueryResult::Node(m))]);
                        self.matches.push(dict);
                    }
                }
            }
        }

        Ok(())
    }

    fn execute_match_edge(
        &mut self,
        edge_pattern: EdgePattern,
        start_var: Option<String>,
        end_var: Option<String>,
    ) -> Result<(), ImplicaError> {
        let mut potential_matches = Vec::new();

        {
            let edges = self
                .graph
                .edges
                .read()
                .map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("execute match edge".to_string()),
                })?;

            for edge_lock in edges.values() {
                let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("execute match edge".to_string()),
                })?;
                if edge_pattern.matches(&edge, self.context.clone())? {
                    potential_matches.push(edge.clone());
                }
            }
        }
        match (start_var, end_var) {
            (Some(start), Some(end)) => self.execute_match_edge_with_start_and_end(
                edge_pattern,
                start,
                end,
                potential_matches,
            ),
            (Some(start), None) => {
                self.execute_match_edge_with_start(edge_pattern, start, potential_matches)
            }
            (None, Some(end)) => {
                self.execute_match_edge_with_end(edge_pattern, end, potential_matches)
            }
            (None, None) => {
                self.execute_match_edge_with_no_endpoints(edge_pattern, potential_matches)
            }
        }
    }

    fn execute_match_edge_with_start_and_end(
        &mut self,
        edge_pattern: EdgePattern,
        start: String,
        end: String,
        potential_matches: Vec<Edge>,
    ) -> Result<(), ImplicaError> {
        if self.matches.is_empty() {
            for m in potential_matches {
                let mut dict = HashMap::from([
                    (
                        start.clone(),
                        QueryResult::Node(
                            (*m.start.read().map_err(|e| ImplicaError::LockError {
                                rw: "read".to_string(),
                                message: e.to_string(),
                                context: Some("execute match edge".to_string()),
                            })?)
                            .clone(),
                        ),
                    ),
                    (
                        end.clone(),
                        QueryResult::Node(
                            (*m.end.read().map_err(|e| ImplicaError::LockError {
                                rw: "read".to_string(),
                                message: e.to_string(),
                                context: Some("execute match edge".to_string()),
                            })?)
                            .clone(),
                        ),
                    ),
                ]);
                if let Some(ref var) = edge_pattern.variable {
                    dict.insert(var.clone(), QueryResult::Edge(m));
                }

                self.matches.push(dict);
            }
        } else {
            let mut results = Vec::new();
            let mut contained = false;

            if let Some(ref var) = edge_pattern.variable {
                for m in self.matches.iter() {
                    match (m.get(var), m.get(&start), m.get(&end)) {
                        (Some(old_var), Some(old_start), Some(old_end)) => {
                            match old_var {
                                QueryResult::Edge(old_var_edge) => match old_start {
                                    QueryResult::Node(old_start_node) => match old_end {
                                        QueryResult::Node(old_end_node) => {
                                            for new in potential_matches.iter() {
                                                let new_start = new.start.read().map_err(|e| {
                                                    ImplicaError::LockError {
                                                        rw: "read".to_string(),
                                                        message: e.to_string(),
                                                        context: Some(
                                                            "execute match edge".to_string(),
                                                        ),
                                                    }
                                                })?;
                                                let new_end = new.end.read().map_err(|e| {
                                                    ImplicaError::LockError {
                                                        rw: "read".to_string(),
                                                        message: e.to_string(),
                                                        context: Some(
                                                            "execute match edge".to_string(),
                                                        ),
                                                    }
                                                })?;
                                                if (new == old_var_edge)
                                                    & (&*new_start == old_start_node)
                                                    & (&*new_end == old_end_node)
                                                {
                                                    results.push(m.clone());
                                                }
                                            }
                                        }
                                        QueryResult::Edge(old_end_edge) => {
                                            return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            });
                                        }
                                    },
                                    QueryResult::Edge(old_start_node) => {
                                        return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                                context: Some("match variable".to_string())
                                                            });
                                    }
                                },
                                QueryResult::Node(old_var_node) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }
                            contained = true;
                        }
                        (Some(old_var), Some(old_start), None) => {
                            match old_var {
                                QueryResult::Edge(old_var_edge) => match old_start {
                                    QueryResult::Node(old_start_node) => {
                                        for new in potential_matches.iter() {
                                            let new_start = new.start.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?;
                                            if (new == old_var_edge)
                                                & (&*new_start == old_start_node)
                                            {
                                                results.push(m.clone());
                                            }
                                        }
                                    }
                                    QueryResult::Edge(old_start_) => {
                                        return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                                context: Some("match variable".to_string())
                                                            });
                                    }
                                },
                                QueryResult::Node(old_var_node) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }
                            contained = true;
                        }
                        (Some(old_var), None, Some(old_end)) => {
                            match old_var {
                                QueryResult::Edge(old_var_edge) => match old_end {
                                    QueryResult::Node(old_end_node) => {
                                        for new in potential_matches.iter() {
                                            let new_end = new.end.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?;
                                            if (new == old_var_edge) & (&*new_end == old_end_node) {
                                                results.push(m.clone());
                                            }
                                        }
                                    }
                                    QueryResult::Edge(old_end_edge) => {
                                        return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            });
                                    }
                                },
                                QueryResult::Node(old_var_node) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match edge".to_string())
                                                    });
                                }
                            }

                            contained = true;
                        }
                        (None, Some(old_start), Some(old_end)) => {
                            match old_start {
                                QueryResult::Node(old_start_node) => match old_end {
                                    QueryResult::Node(old_end_node) => {
                                        for new in potential_matches.iter() {
                                            let new_start = new.start.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?;
                                            let new_end = new.end.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?;
                                            if (&*new_start == old_start_node)
                                                & (&*new_end == old_end_node)
                                            {
                                                let mut dict = m.clone();
                                                dict.insert(
                                                    var.clone(),
                                                    QueryResult::Edge(new.clone()),
                                                );
                                                results.push(dict);
                                            }
                                        }
                                    }
                                    QueryResult::Edge(old_end_edge) => {
                                        return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            });
                                    }
                                },
                                QueryResult::Edge(old_start_edge) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }
                            contained = true;
                        }
                        (Some(old_var), None, None) => {
                            match old_var {
                                QueryResult::Edge(old_var_edge) => {
                                    for new in potential_matches.iter() {
                                        if new == old_var_edge {
                                            let mut dict = m.clone();
                                            dict.insert(
                                                start.clone(),
                                                QueryResult::Node(
                                                    (*new.start.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?)
                                                    .clone(),
                                                ),
                                            );
                                            dict.insert(
                                                end.clone(),
                                                QueryResult::Node(
                                                    (*new.end.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?)
                                                    .clone(),
                                                ),
                                            );
                                            results.push(dict);
                                        }
                                    }
                                }
                                QueryResult::Node(old_var_node) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to a edge", var),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }

                            contained = true;
                        }
                        (None, Some(old_start), None) => {
                            match old_start {
                                QueryResult::Node(old_start_node) => {
                                    for new in potential_matches.iter() {
                                        let new_start = new.start.read().map_err(|e| {
                                            ImplicaError::LockError {
                                                rw: "read".to_string(),
                                                message: e.to_string(),
                                                context: Some("execute match edge".to_string()),
                                            }
                                        })?;
                                        if &*new_start == old_start_node {
                                            let mut dict = m.clone();
                                            dict.insert(
                                                var.clone(),
                                                QueryResult::Edge(new.clone()),
                                            );
                                            dict.insert(
                                                end.clone(),
                                                QueryResult::Node(
                                                    (*new.end.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?)
                                                    .clone(),
                                                ),
                                            );
                                            results.push(dict);
                                        }
                                    }
                                }
                                QueryResult::Edge(old_start_edge) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }

                            contained = true;
                        }
                        (None, None, Some(old_end)) => {
                            match old_end {
                                QueryResult::Node(old_end_node) => {
                                    for new in potential_matches.iter() {
                                        let new_end = new.end.read().map_err(|e| {
                                            ImplicaError::LockError {
                                                rw: "read".to_string(),
                                                message: e.to_string(),
                                                context: Some("execute match edge".to_string()),
                                            }
                                        })?;
                                        if &*new_end == old_end_node {
                                            let mut dict = m.clone();
                                            dict.insert(
                                                var.clone(),
                                                QueryResult::Edge(new.clone()),
                                            );
                                            dict.insert(
                                                start.clone(),
                                                QueryResult::Node(
                                                    (*new.start.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?)
                                                    .clone(),
                                                ),
                                            );
                                            results.push(dict);
                                        }
                                    }
                                }
                                QueryResult::Edge(old_end_edge) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }

                            contained = true;
                        }
                        (None, None, None) => (),
                    }
                }

                if contained {
                    self.matches = results;
                } else {
                    // Cartesian product
                    let mut results = Vec::new();
                    for m in potential_matches {
                        for old_match in self.matches.iter() {
                            let mut dict = old_match.clone();
                            dict.extend([
                                (var.clone(), QueryResult::Edge(m.clone())),
                                (
                                    start.clone(),
                                    QueryResult::Node(
                                        (*m.start.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?)
                                        .clone(),
                                    ),
                                ),
                                (
                                    end.clone(),
                                    QueryResult::Node(
                                        (*m.end.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?)
                                        .clone(),
                                    ),
                                ),
                            ]);
                            results.push(dict);
                        }
                    }
                    self.matches = results;
                }
            } else {
                for m in self.matches.iter() {
                    match (m.get(&start), m.get(&end)) {
                        (Some(old_start), Some(old_end)) => {
                            match old_start {
                                QueryResult::Node(old_start_node) => match old_end {
                                    QueryResult::Node(old_end_node) => {
                                        for new in potential_matches.iter() {
                                            let new_start = new.start.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?;
                                            let new_end = new.end.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?;
                                            if (&*new_start == old_start_node)
                                                & (&*new_end == old_end_node)
                                            {
                                                results.push(m.clone());
                                            }
                                        }
                                    }
                                    QueryResult::Edge(old_end_edge) => {
                                        return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            });
                                    }
                                },
                                QueryResult::Edge(old_start_edge) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }
                            contained = true;
                        }
                        (Some(old_start), None) => {
                            match old_start {
                                QueryResult::Node(old_start_node) => {
                                    for new in potential_matches.iter() {
                                        let new_start = new.start.read().map_err(|e| {
                                            ImplicaError::LockError {
                                                rw: "read".to_string(),
                                                message: e.to_string(),
                                                context: Some("execute match edge".to_string()),
                                            }
                                        })?;
                                        if &*new_start == old_start_node {
                                            let mut dict = m.clone();
                                            dict.insert(
                                                end.clone(),
                                                QueryResult::Node(
                                                    (*new.end.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?)
                                                    .clone(),
                                                ),
                                            );
                                            results.push(dict);
                                        }
                                    }
                                }
                                QueryResult::Edge(old_start_edge) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }
                            contained = true;
                        }
                        (None, Some(old_end)) => match old_end {
                            QueryResult::Node(old_end_node) => {
                                for new in potential_matches.iter() {
                                    let new_end =
                                        new.end.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?;
                                    if &*new_end == old_end_node {
                                        let mut dict = m.clone();
                                        dict.insert(
                                            start.clone(),
                                            QueryResult::Node(
                                                (*new.start.read().map_err(|e| {
                                                    ImplicaError::LockError {
                                                        rw: "read".to_string(),
                                                        message: e.to_string(),
                                                        context: Some(
                                                            "execute match edge".to_string(),
                                                        ),
                                                    }
                                                })?)
                                                .clone(),
                                            ),
                                        );
                                        results.push(dict);
                                    }
                                }
                            }
                            QueryResult::Edge(old_end_edge) => {
                                return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                        context: Some("match variable".to_string())
                                                    });
                            }
                        },
                        (None, None) => (),
                    }
                }

                if contained {
                    self.matches = results;
                } else {
                    // Cartesian Product
                    let mut results = Vec::new();
                    for m in potential_matches {
                        for old_match in self.matches.iter() {
                            let mut dict = old_match.clone();
                            dict.extend([
                                (
                                    start.clone(),
                                    QueryResult::Node(
                                        (*m.start.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?)
                                        .clone(),
                                    ),
                                ),
                                (
                                    end.clone(),
                                    QueryResult::Node(
                                        (*m.end.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?)
                                        .clone(),
                                    ),
                                ),
                            ]);
                            results.push(dict);
                        }
                    }
                    self.matches = results;
                }
            }
        }

        Ok(())
    }

    fn execute_match_edge_with_start(
        &mut self,
        edge_pattern: EdgePattern,
        start: String,
        potential_matches: Vec<Edge>,
    ) -> Result<(), ImplicaError> {
        if self.matches.is_empty() {
            for m in potential_matches {
                let mut dict = HashMap::from([(
                    start.clone(),
                    QueryResult::Node(
                        (*m.start.read().map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some("execute match edge".to_string()),
                        })?)
                        .clone(),
                    ),
                )]);
                if let Some(ref var) = edge_pattern.variable {
                    dict.insert(var.clone(), QueryResult::Edge(m));
                }

                self.matches.push(dict);
            }
        } else {
            let mut results = Vec::new();
            let mut contained = false;

            if let Some(ref var) = edge_pattern.variable {
                for m in self.matches.iter() {
                    match (m.get(var), m.get(&start)) {
                        (Some(old_var), Some(old_start)) => {
                            match old_var {
                                QueryResult::Edge(old_var_edge) => match old_start {
                                    QueryResult::Node(old_start_node) => {
                                        for new in potential_matches.iter() {
                                            let new_start = new.start.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?;
                                            if (new == old_var_edge)
                                                & (&*new_start == old_start_node)
                                            {
                                                results.push(m.clone());
                                            }
                                        }
                                    }
                                    QueryResult::Edge(old_start_edge) => {
                                        return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                                context: Some("match variable".to_string())
                                                            });
                                    }
                                },
                                QueryResult::Node(old_var_node) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }
                            contained = true;
                        }
                        (Some(old_var), None) => {
                            match old_var {
                                QueryResult::Edge(old_var_edge) => {
                                    for new in potential_matches.iter() {
                                        if new == old_var_edge {
                                            let mut dict = m.clone();
                                            dict.insert(
                                                start.clone(),
                                                QueryResult::Node(
                                                    (*new.start.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?)
                                                    .clone(),
                                                ),
                                            );
                                            results.push(dict);
                                        }
                                    }
                                }
                                QueryResult::Node(old_var_node) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                                context: Some("match variable".to_string())
                                                            });
                                }
                            }
                            contained = true;
                        }
                        (None, Some(old_start)) => {
                            match old_start {
                                QueryResult::Node(old_start_node) => {
                                    for new in potential_matches.iter() {
                                        let new_start = new.start.read().map_err(|e| {
                                            ImplicaError::LockError {
                                                rw: "read".to_string(),
                                                message: e.to_string(),
                                                context: Some("execute match edge".to_string()),
                                            }
                                        })?;
                                        if &*new_start == old_start_node {
                                            let mut dict = m.clone();
                                            dict.insert(
                                                var.clone(),
                                                QueryResult::Edge(new.clone()),
                                            );
                                        }
                                    }
                                }
                                QueryResult::Edge(old_start_edge) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }
                            contained = true;
                        }
                        (None, None) => (),
                    }
                }

                if contained {
                    self.matches = results;
                } else {
                    // Cartesian product
                    let mut results = Vec::new();
                    for m in potential_matches {
                        for old_match in self.matches.iter() {
                            let mut dict = old_match.clone();
                            dict.extend([
                                (
                                    start.clone(),
                                    QueryResult::Node(
                                        (*m.start.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?)
                                        .clone(),
                                    ),
                                ),
                                (var.clone(), QueryResult::Edge(m.clone())),
                            ]);
                            results.push(dict);
                        }
                    }
                }
            } else {
                for m in self.matches.iter() {
                    if let Some(old_start) = m.get(&start) {
                        match old_start {
                            QueryResult::Node(old_start_node) => {
                                for new in potential_matches.iter() {
                                    let new_start =
                                        new.start.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?;
                                    if old_start_node == &*new_start {
                                        results.push(m.clone());
                                    }
                                }
                            }
                            QueryResult::Edge(old_start_edge) => {
                                return Err(ImplicaError::InvalidQuery {
                                                    message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
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
                    for m in potential_matches {
                        for old_match in self.matches.iter() {
                            let mut dict = old_match.clone();
                            dict.extend([(
                                start.clone(),
                                QueryResult::Node(
                                    (*m.start.read().map_err(|e| ImplicaError::LockError {
                                        rw: "read".to_string(),
                                        message: e.to_string(),
                                        context: Some("execute match edge".to_string()),
                                    })?)
                                    .clone(),
                                ),
                            )]);
                            results.push(dict);
                        }
                    }
                    self.matches = results;
                }
            }
        }
        Ok(())
    }

    fn execute_match_edge_with_end(
        &mut self,
        edge_pattern: EdgePattern,
        end: String,
        potential_matches: Vec<Edge>,
    ) -> Result<(), ImplicaError> {
        if self.matches.is_empty() {
            for m in potential_matches {
                let mut dict = HashMap::from([(
                    end.clone(),
                    QueryResult::Node(
                        (*m.end.read().map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some("execute match edge".to_string()),
                        })?)
                        .clone(),
                    ),
                )]);
                if let Some(ref var) = edge_pattern.variable {
                    dict.insert(var.clone(), QueryResult::Edge(m));
                }

                self.matches.push(dict);
            }
        } else {
            let mut results = Vec::new();
            let mut contained = false;

            if let Some(ref var) = edge_pattern.variable {
                for m in self.matches.iter() {
                    match (m.get(var), m.get(&end)) {
                        (Some(old_var), Some(old_end)) => {
                            match old_var {
                                QueryResult::Edge(old_var_edge) => match old_end {
                                    QueryResult::Node(old_end_node) => {
                                        for new in potential_matches.iter() {
                                            let new_end = new.end.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?;
                                            if (new == old_var_edge) & (&*new_end == old_end_node) {
                                                results.push(m.clone());
                                            }
                                        }
                                    }
                                    QueryResult::Edge(old_end_edge) => {
                                        return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            });
                                    }
                                },
                                QueryResult::Node(old_var_node) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match edge".to_string())
                                                    });
                                }
                            }

                            contained = true;
                        }
                        (Some(old_var), None) => {
                            match old_var {
                                QueryResult::Edge(old_var_edge) => {
                                    for new in potential_matches.iter() {
                                        if new == old_var_edge {
                                            let mut dict = m.clone();
                                            dict.insert(
                                                end.clone(),
                                                QueryResult::Node(
                                                    (*new.end.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?)
                                                    .clone(),
                                                ),
                                            );
                                            results.push(dict);
                                        }
                                    }
                                }
                                QueryResult::Node(old_var_node) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to a edge", var),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }

                            contained = true;
                        }
                        (None, Some(old_end)) => {
                            match old_end {
                                QueryResult::Node(old_end_node) => {
                                    for new in potential_matches.iter() {
                                        let new_end = new.end.read().map_err(|e| {
                                            ImplicaError::LockError {
                                                rw: "read".to_string(),
                                                message: e.to_string(),
                                                context: Some("execute match edge".to_string()),
                                            }
                                        })?;
                                        if &*new_end == old_end_node {
                                            let mut dict = m.clone();
                                            dict.insert(
                                                var.clone(),
                                                QueryResult::Edge(new.clone()),
                                            );
                                            results.push(dict);
                                        }
                                    }
                                }
                                QueryResult::Edge(old_end_edge) => {
                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                        context: Some("match variable".to_string())
                                                    });
                                }
                            }

                            contained = true;
                        }
                        (None, None) => (),
                    }
                }

                if contained {
                    self.matches = results;
                } else {
                    let mut results = Vec::new();
                    for m in potential_matches {
                        for old_match in self.matches.iter() {
                            let mut dict = old_match.clone();
                            dict.extend([
                                (
                                    end.clone(),
                                    QueryResult::Node(
                                        (*m.end.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?)
                                        .clone(),
                                    ),
                                ),
                                (var.clone(), QueryResult::Edge(m.clone())),
                            ]);
                            results.push(dict);
                        }
                    }
                    self.matches = results;
                }
            } else {
                for m in self.matches.iter() {
                    if let Some(old_end) = m.get(&end) {
                        match old_end {
                            QueryResult::Node(old_end_node) => {
                                for new in potential_matches.iter() {
                                    let new_end =
                                        new.end.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?;
                                    if old_end_node == &*new_end {
                                        results.push(m.clone());
                                    }
                                }
                            }
                            QueryResult::Edge(old_end_edge) => {
                                return Err(ImplicaError::InvalidQuery {
                                                    message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
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
                    for m in potential_matches {
                        for old_match in self.matches.iter() {
                            let mut dict = old_match.clone();
                            dict.extend([(
                                end.clone(),
                                QueryResult::Node(
                                    (*m.end.read().map_err(|e| ImplicaError::LockError {
                                        rw: "read".to_string(),
                                        message: e.to_string(),
                                        context: Some("execute match edge".to_string()),
                                    })?)
                                    .clone(),
                                ),
                            )]);
                            results.push(dict);
                        }
                    }
                    self.matches = results;
                }
            }
        }
        Ok(())
    }

    fn execute_match_edge_with_no_endpoints(
        &mut self,
        edge_pattern: EdgePattern,
        potential_matches: Vec<Edge>,
    ) -> Result<(), ImplicaError> {
        if let Some(ref var) = edge_pattern.variable {
            if self.matches.is_empty() {
                for m in potential_matches {
                    let dict = HashMap::from([(var.clone(), QueryResult::Edge(m))]);
                    self.matches.push(dict);
                }
            } else {
                let mut results = Vec::new();
                let mut contained = false;

                for m in self.matches.iter() {
                    if let Some(old) = m.get(var) {
                        match old {
                            QueryResult::Edge(old_edge) => {
                                for new_edge in potential_matches.iter() {
                                    if new_edge == old_edge {
                                        results.push(m.clone());
                                    }
                                }
                            }
                            QueryResult::Node(old_node) => {
                                return Err(ImplicaError::InvalidQuery {
                                                    message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                    context: Some("match edge".to_string())
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
                    for m in potential_matches {
                        for old_match in self.matches.iter() {
                            let mut dict = old_match.clone();
                            dict.extend([(var.clone(), QueryResult::Edge(m.clone()))]);
                            results.push(dict);
                        }
                    }
                    self.matches = results;
                }
            }
        }
        Ok(())
    }

    fn execute_match_path(&mut self, mut path: PathPattern) -> Result<(), ImplicaError> {
        let ph_generator = PlaceholderGenerator::new();

        for np in path.nodes.iter_mut() {
            if np.variable.is_none() {
                let var_name = ph_generator.next();
                np.variable = Some(var_name);
            }
        }
        for ep in path.edges.iter_mut() {
            if ep.variable.is_none() {
                let var_name = ph_generator.next();
                ep.variable = Some(var_name);
            }
        }

        let mut prev = path.nodes.remove(0);
        self.execute_match(MatchOp::Node(prev.clone()))?;

        for (ep, np) in zip(path.edges, path.nodes) {
            self.execute_match(MatchOp::Node(np.clone()))?;
            self.execute_match(MatchOp::Edge(
                ep,
                prev.variable.clone(),
                np.variable.clone(),
            ))?;
            prev = np;
        }

        for res in self.matches.iter_mut() {
            for ph in ph_generator.prev() {
                res.remove(&ph);
            }
        }

        Ok(())
    }
}
