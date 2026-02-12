use error_stack::{Report, ResultExt};
use std::iter::zip;
use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use rayon::prelude::*;

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::base::Graph;
use crate::matches::{next_match_id, MatchElement, MatchSet};
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

            let node_pattern = pattern.nodes.first().unwrap();

            matches = match self.match_node_pattern(node_pattern, matches) {
                Ok(m) => m,
                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match path pattern"))),
            };

            for (node_pattern, edge_pattern) in zip(pattern.nodes[1..].iter(), pattern.edges.iter())
            {
                matches = match self.match_edge_pattern(
                    edge_pattern,
                    matches,
                ) {
                    Ok(m) => m,
                    Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match path pattern"))),
                };

                let new_matches: MatchSet = Arc::new(DashMap::new());

                let res = matches.par_iter().try_for_each(|entry| -> ControlFlow<Report<ImplicaError>> {
                    let (prev_uid, r#match) = entry.value().clone();

                    let node = match self.nodes.get(&prev_uid) {
                        Some(uid) => *uid.key(),
                        None => return ControlFlow::Break(ImplicaError::IndexCorruption { message: "previously matched node should exist in NodeIndex".to_string(), context: Some(ctx!("checking node matches pattern")) }.into())
                    };

                    let new_match = match self.check_node_matches(&node, node_pattern, r#match) {
                        Ok(Some(m)) => m,
                        Ok(None) => return ControlFlow::Continue(()),
                        Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match path pattern")))
                    };

                    if let Some(ref var) = node_pattern.variable {
                        match new_match.insert(var, MatchElement::Node(node)) {
                            Ok(()) => (),
                            Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match path pattern")))
                        }
                    }

                    new_matches.insert(next_match_id(), (node, new_match));

                    ControlFlow::Continue(())

                });

                matches = match res {
                    ControlFlow::Continue(()) => new_matches,
                    ControlFlow::Break(e) => return ControlFlow::Break(e.attach(ctx!("graph - match path pattern")))
                }
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
