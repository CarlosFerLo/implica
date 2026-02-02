use error_stack::ResultExt;
use std::iter::zip;
use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use rayon::prelude::*;

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::base::Graph;
use crate::matches::{next_match_id, MatchSet};
use crate::patterns::PathPattern;

impl Graph {
    pub(crate) fn match_path_pattern(
        &self,
        pattern: &PathPattern,
        matches: MatchSet,
    ) -> ImplicaResult<MatchSet> {
        let out_map: MatchSet = Arc::new(DashMap::new());

        pattern
            .validate()
            .attach(ctx!("graph - match path pattern"))?;

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value().clone();

            let mut matches = Arc::new(DashMap::from_iter([(
                next_match_id(),
                (_prev_uid, r#match.clone()),
            )]));

            let mut prev_node_pattern = pattern.nodes.first().unwrap();

            matches = match self.match_node_pattern(prev_node_pattern, matches) {
                Ok(m) => m,
                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match path pattern"))),
            };

            for (node_pattern, edge_pattern) in zip(pattern.nodes[1..].iter(), pattern.edges.iter())
            {
                matches = match self.match_edge_pattern(
                    edge_pattern,
                    prev_node_pattern.variable.clone(),
                    node_pattern.variable.clone(),
                    matches,
                ) {
                    Ok(m) => m,
                    Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match path pattern"))),
                };

                matches = match self.match_node_pattern(node_pattern, matches) {
                    Ok(m) => m,
                    Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match path pattern"))),
                };

                prev_node_pattern = node_pattern;
            }

            matches
                .par_iter()
                .try_for_each(|m| {
                    match out_map.insert(next_match_id(), m.value().clone()) {
                        None => ControlFlow::Continue(()),
                        Some(_) => ControlFlow::Break(ImplicaError::RuntimeError { message: "Unique identifier generator next_match_id created a previously existing id (should not happen)".to_string(), context: Some("match path pattern".to_string()) }.into())
                    }
                })
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }
}
