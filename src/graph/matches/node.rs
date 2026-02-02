use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use rayon::prelude::*;

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::base::Graph;
use crate::matches::{next_match_id, MatchElement, MatchSet};
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

                match_set.par_iter().try_for_each(|entry| {
                    let (prev_uid, m) = entry.value().clone();

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

                    let new_matches = r#match.clone();

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
}
