use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use error_stack::Report;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::Uid;
use crate::matches::{next_match_id, Match, MatchElement, MatchSet};
use crate::patterns::CompiledDirection;
use crate::{graph::base::Graph, patterns::EdgePattern};

impl Graph {
    pub(super) fn match_edge_pattern(
        &self,
        pattern: &EdgePattern,
        matches: MatchSet,
    ) -> ImplicaResult<MatchSet> {
        let out_map: MatchSet = Arc::new(DashMap::new());

        let result =
            matches
                .par_iter()
                .try_for_each(|entry| -> ControlFlow<Report<ImplicaError>> {
                    let (prev_uid, r#match) = entry.value().clone();

                    // Check if match already holds the desired edge
                    if let Some(ref var) = pattern.variable {
                        if let Some(old) = r#match.get(var) {
                            let old_edge = match old.as_edge(var, None) {
                                Ok(edge) => edge,
                                Err(e) => {
                                    return ControlFlow::Break(
                                        e.attach(ctx!("graph - match edge pattern")),
                                    )
                                }
                            };

                            match self.check_edge_matches(&prev_uid, &old_edge, pattern, r#match.clone()) {
                                Ok(Some(new_match)) => {
                                    let next_uid = match pattern.compiled_direction {
                                        CompiledDirection::Forward => old_edge.1,
                                        CompiledDirection::Backward => old_edge.0,
                                        CompiledDirection::Any => {
                                            todo!("any direction is not supported yet")
                                        }
                                    };

                                    out_map.insert(next_match_id(), (next_uid, new_match));

                                    return ControlFlow::Continue(());
                                },
                                Ok(None) => return ControlFlow::Continue(()),
                                Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")))
                            }


                        }
                    }

                    // Get possible edges based on prev_uid

                    let possible_edges = match pattern.compiled_direction {
                        CompiledDirection::Forward => {
                            match self.start_to_edge_index.get(&prev_uid) {
                                Some(edges) => edges.value().clone(),
                                None => return ControlFlow::Break(ImplicaError::IndexCorruption { message: "prev_uid should be pointing at a valid node, and it dos not have an entry in the StartToEdgeIndex".to_string(), context: Some("graph - match edge pattern".to_string()) }.into())
                            }
                        }
                        CompiledDirection::Backward => {
                            match self.end_to_edge_index.get(&prev_uid) {
                                Some(edges) => edges.value().clone(),
                                None => return ControlFlow::Break(ImplicaError::IndexCorruption { message: "prev_uid should be pointing at a valid node, and it dos not have an entry in the StartToEdgeIndex".to_string(), context: Some("graph - match edge pattern".to_string()) }.into())
                            }
                        }
                        CompiledDirection::Any => todo!("any direction not supported yet")
                    } ;

                    possible_edges.par_iter().try_for_each(|entry| -> ControlFlow<Report<ImplicaError>> {
                        let edge = *entry.key();

                        match self.check_edge_matches(&prev_uid, &edge, pattern, r#match.clone()) {
                            Ok(Some(new_match)) => {

                                if let Some(ref var) = pattern.variable {
                                    match new_match.insert(var, MatchElement::Edge(edge)) {
                                        Ok(()) => (),
                                        Err(e) => return ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")))
                                    }
                                }

                                let next_uid = match pattern.compiled_direction {
                                    CompiledDirection::Forward => edge.1,
                                    CompiledDirection::Backward => edge.0,
                                    CompiledDirection::Any => {
                                        todo!("any direction is not supported yet")
                                    }
                                };

                                out_map.insert(next_match_id(), (next_uid, new_match));

                                ControlFlow::Continue(())

                            }
                            Ok(None) => ControlFlow::Continue(()),
                            Err(e) => ControlFlow::Break(e.attach(ctx!("graph - match edge pattern")))
                        }


                    })
                });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }

    fn check_edge_matches(
        &self,
        prev_uid: &Uid,
        edge: &(Uid, Uid),
        pattern: &EdgePattern,
        r#match: Arc<Match>,
    ) -> ImplicaResult<Option<Arc<Match>>> {
        // Check endpoint matches
        if !Self::match_endpoint(prev_uid, edge, &pattern.compiled_direction) {
            return Ok(None);
        }

        // Get the type uid of the edge
        let edge_type = match self.edge_to_type_index.get(edge) {
            Some(uid) => *uid.value(),
            None => {
                return Err(ImplicaError::IndexCorruption {
                    message: "missing type for edge in edge_to_type_index".to_string(),
                    context: Some("check edge matches".to_string()),
                }
                .into())
            }
        };

        // Create new match element
        let mut new_match = Arc::new(Match::new(Some(r#match)));

        // Check if its type satisfies the type schema
        if let Some(ref type_schema) = pattern.type_schema {
            new_match = match self.check_type_matches(&edge_type, &type_schema.compiled, new_match)
            {
                Ok(m) => match m {
                    Some(m) => m,
                    None => return Ok(None),
                },
                Err(e) => return Err(e.attach(ctx!("check edge matches"))),
            }
        }

        // Check if its term satisfies the term schema
        if let Some(ref term_schema) = pattern.term_schema {
            new_match = match self.check_term_matches(&edge_type, &term_schema.compiled, new_match)
            {
                Ok(m) => match m {
                    Some(m) => m,
                    None => return Ok(None),
                },
                Err(e) => return Err(e.attach(ctx!("check edge matches"))),
            }
        }

        // Check if properties match
        if let Some(ref properties) = pattern.properties {
            match self.check_edge_matches_properties(edge, properties) {
                Ok(true) => (),
                Ok(false) => return Ok(None),
                Err(e) => return Err(e.attach(ctx!("check edge matches"))),
            }
        }

        Ok(Some(new_match))
    }

    fn match_endpoint(endpoint: &Uid, edge: &(Uid, Uid), direction: &CompiledDirection) -> bool {
        match direction {
            CompiledDirection::Forward => edge.0 == *endpoint,
            CompiledDirection::Backward => edge.1 == *endpoint,
            CompiledDirection::Any => todo!("any direction not supported yet"),
        }
    }
}
