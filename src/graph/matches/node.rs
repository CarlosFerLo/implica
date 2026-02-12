use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use rayon::prelude::*;

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::base::{Graph, Uid};
use crate::matches::{next_match_id, Match, MatchElement, MatchSet};
use crate::patterns::NodePattern;

impl Graph {
    pub(super) fn match_node_pattern(
        &self,
        pattern: &NodePattern,
        matches: MatchSet,
    ) -> ImplicaResult<MatchSet> {
        let out_map: MatchSet = Arc::new(DashMap::new());

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value().clone();

            if let Some(ref var) = pattern.variable {
                if let Some(ref old_element) = r#match.get(var) {
                    let old = match old_element.as_node(var, Some("match node pattern".to_string()))
                    {
                        Ok(uid) => uid,
                        Err(e) => {
                            return ControlFlow::Break(e.attach(ctx!("graph - match node pattern")))
                        }
                    };

                    let mut new_match = r#match.clone();
                    if let Some(ref type_schema) = pattern.type_schema {
                        let res = self.check_type_matches(&old, &type_schema.compiled, new_match);

                        match res {
                            Ok(m) => match m {
                                Some(m) => new_match = m.clone(),
                                None => return ControlFlow::Continue(()),
                            },
                            Err(e) => {
                                return ControlFlow::Break(
                                    e.attach(ctx!("graph - match node pattern")),
                                )
                            }
                        }
                    }
                    if let Some(ref term_schema) = pattern.term_schema {
                        let res = self.check_term_matches(&old, &term_schema.compiled, new_match);

                        match res {
                            Ok(m) => match m {
                                Some(m) => new_match = m.clone(),
                                None => return ControlFlow::Continue(()),
                            },
                            Err(e) => {
                                return ControlFlow::Break(
                                    e.attach(ctx!("graph - match node pattern")),
                                )
                            }
                        }
                    }

                    if let Some(ref properties) = pattern.properties {
                        let res = self.check_node_matches_properties(&old, properties);

                        match res {
                            Ok(true) => (),
                            Ok(false) => return ControlFlow::Continue(()),
                            Err(e) => {
                                return ControlFlow::Break(
                                    e.attach(ctx!("graph - match node pattern")),
                                )
                            }
                        }
                    }

                    out_map.insert(next_match_id(), (old, new_match));

                    return ControlFlow::Continue(());
                }
            }
            let mut match_set: MatchSet = Arc::new(DashMap::new());
            match_set.insert(next_match_id(), (_prev_uid, r#match.clone()));

            if let Some(ref type_schema) = pattern.type_schema {
                match_set = match self.match_type_schema(type_schema, match_set) {
                    Ok(m) => m,
                    Err(e) => {
                        return ControlFlow::Break(e.attach(ctx!("graph - match node pattern")))
                    }
                };

                dbg!(&match_set);

                match_set.par_iter().try_for_each(|entry| {
                    let (prev_uid, original_match) = entry.value().clone();

                    let m = Arc::new(Match::new(Some(original_match)));

                    if let Some(ref term_schema) = pattern.term_schema {
                        match self.check_term_matches(&prev_uid, &term_schema.compiled, m.clone()) {
                            Ok(m) => match m {
                                Some(m) => {
                                    if let Some(ref properties) = pattern.properties {
                                        match self
                                            .check_node_matches_properties(&prev_uid, properties)
                                        {
                                            Ok(true) => (),
                                            Ok(false) => return ControlFlow::Continue(()),
                                            Err(e) => {
                                                return ControlFlow::Break(
                                                    e.attach(ctx!("graph - match node pattern")),
                                                )
                                            }
                                        }
                                    }

                                    if let Some(ref var) = pattern.variable {
                                        match m.insert(var, MatchElement::Node(prev_uid)) {
                                            Ok(_) => (),
                                            Err(e) => {
                                                return ControlFlow::Break(
                                                    e.attach(ctx!("graph - match node pattern")),
                                                )
                                            }
                                        }
                                    }

                                    out_map.insert(next_match_id(), (prev_uid, m.clone()));

                                    ControlFlow::Continue(())
                                }
                                None => ControlFlow::Continue(()),
                            },
                            Err(e) => match e.current_context() {
                                ImplicaError::TermNotFound { .. } => ControlFlow::Continue(()),
                                _ => {
                                    ControlFlow::Break(e.attach(ctx!("graph - match node pattern")))
                                }
                            },
                        }
                    } else {
                        if let Some(ref properties) = pattern.properties {
                            match self.check_node_matches_properties(&prev_uid, properties) {
                                Ok(true) => (),
                                Ok(false) => return ControlFlow::Continue(()),
                                Err(e) => {
                                    return ControlFlow::Break(
                                        e.attach(ctx!("graph - match node pattern")),
                                    )
                                }
                            }
                        }

                        if let Some(ref var) = pattern.variable {
                            match m.insert(var, MatchElement::Node(prev_uid)) {
                                Ok(_) => (),
                                Err(e) => {
                                    return ControlFlow::Break(
                                        e.attach(ctx!("graph - match node pattern")),
                                    )
                                }
                            }
                        }

                        out_map.insert(next_match_id(), (prev_uid, m.clone()));

                        ControlFlow::Continue(())
                    }
                })
            } else if let Some(ref term_schema) = pattern.term_schema {
                match_set = match self.match_term_schema(term_schema, match_set) {
                    Ok(m) => m,
                    Err(e) => {
                        return ControlFlow::Break(e.attach(ctx!("graph - match node pattern")))
                    }
                };

                match_set.par_iter().try_for_each(|entry| {
                    let (prev_uid, m) = entry.value().clone();

                    if let Some(ref properties) = pattern.properties {
                        match self.check_node_matches_properties(&prev_uid, properties) {
                            Ok(true) => (),
                            Ok(false) => return ControlFlow::Continue(()),
                            Err(e) => {
                                return ControlFlow::Break(
                                    e.attach(ctx!("graph - match node pattern")),
                                )
                            }
                        }
                    }

                    if let Some(ref var) = pattern.variable {
                        match m.insert(var, MatchElement::Node(prev_uid)) {
                            Ok(_) => (),
                            Err(e) => {
                                return ControlFlow::Break(
                                    e.attach(ctx!("graph - match node pattern")),
                                )
                            }
                        }
                    }

                    out_map.insert(next_match_id(), (prev_uid, m.clone()));

                    ControlFlow::Continue(())
                })
            } else {
                self.nodes.par_iter().try_for_each(|entry| {
                    let new_uid = *entry.key();

                    if let Some(ref properties) = pattern.properties {
                        match self.check_node_matches_properties(&new_uid, properties) {
                            Ok(true) => (),
                            Ok(false) => return ControlFlow::Continue(()),
                            Err(e) => {
                                return ControlFlow::Break(
                                    e.attach(ctx!("graph - match node pattern")),
                                )
                            }
                        }
                    }

                    let new_matches = Arc::new(Match::new(Some(r#match.clone())));

                    if let Some(ref var) = pattern.variable {
                        match new_matches.insert(var, MatchElement::Node(new_uid)) {
                            Ok(_) => (),
                            Err(e) => {
                                return ControlFlow::Break(
                                    e.attach(ctx!("graph - match node pattern")),
                                )
                            }
                        }
                    }

                    out_map.insert(next_match_id(), (new_uid, new_matches.clone()));

                    ControlFlow::Continue(())
                })
            }
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }

    pub(super) fn check_node_matches(
        &self,
        node: &Uid,
        pattern: &NodePattern,
        r#match: Arc<Match>,
    ) -> ImplicaResult<Option<Arc<Match>>> {
        let mut new_match = Arc::new(Match::new(Some(r#match)));

        // Check node matches type schema
        if let Some(ref type_schema) = pattern.type_schema {
            new_match = match self.check_type_matches(node, &type_schema.compiled, new_match) {
                Ok(m) => match m {
                    Some(m) => m,
                    None => return Ok(None),
                },
                Err(e) => return Err(e.attach(ctx!("check node matches"))),
            };
        }

        // Check node matches term schema
        if let Some(ref term_schema) = pattern.term_schema {
            new_match = match self.check_term_matches(node, &term_schema.compiled, new_match) {
                Ok(m) => match m {
                    Some(m) => m,
                    None => return Ok(None),
                },
                Err(e) => return Err(e.attach(ctx!("check node matches"))),
            }
        }

        // Check properties match
        if let Some(ref properties) = pattern.properties {
            match self.check_node_matches_properties(node, properties) {
                Ok(true) => (),
                Ok(false) => return Ok(None),
                Err(e) => return Err(e.attach(ctx!("check node matches"))),
            }
        }

        Ok(Some(new_match))
    }
}
