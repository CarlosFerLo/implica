use error_stack::ResultExt;
use std::iter::zip;
use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use rayon::prelude::*;

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::base::{Graph, TermRep, Uid};
use crate::matches::{next_match_id, Match, MatchElement, MatchSet};
use crate::patterns::{TermPattern, TermSchema};

impl Graph {
    pub(super) fn match_term_schema(
        &self,
        term_schema: &TermSchema,
        matches: MatchSet,
    ) -> ImplicaResult<MatchSet> {
        self.match_term_pattern(&term_schema.compiled, matches)
            .attach(ctx!("graph - match term schema"))
    }

    fn match_term_pattern(
        &self,
        pattern: &TermPattern,
        matches: MatchSet,
    ) -> ImplicaResult<MatchSet> {
        let out_map: MatchSet = Arc::new(DashMap::new());

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value();
            let r#match = r#match.clone();

            self.term_index.par_iter().try_for_each(|entry| {
                match self.check_term_matches(entry.key(), pattern, r#match.clone()) {
                    Ok(new_match_op) => {
                        if let Some(new_match) = new_match_op {
                            out_map.insert(next_match_id(), (*entry.key(), new_match));
                        }
                        ControlFlow::Continue(())
                    }
                    Err(e) => ControlFlow::Break(e.attach(ctx!("graph - match term pattern"))),
                }
            })
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e.attach(ctx!("graph - match term pattern"))),
        }
    }

    pub(super) fn check_term_matches(
        &self,
        term_uid: &Uid,
        pattern: &TermPattern,
        r#match: Arc<Match>,
    ) -> ImplicaResult<Option<Arc<Match>>> {
        if let Some(term_row) = self.term_index.get(term_uid) {
            match pattern {
                TermPattern::Wildcard => Ok(Some(r#match.clone())),
                TermPattern::Variable(var) => {
                    if let Some(ref old_element) = r#match.get(var) {
                        let old_uid = old_element
                            .as_term(var, Some("check term matches".to_string()))
                            .attach(ctx!("graph - check term matches"))?;

                        if &old_uid == term_uid {
                            Ok(Some(r#match.clone()))
                        } else {
                            Ok(None)
                        }
                    } else {
                        let new_match = Match::new(Some(r#match.clone()));
                        new_match
                            .insert(var, MatchElement::Term(*term_uid))
                            .attach(ctx!("graph - match term pattern"))?;

                        Ok(Some(Arc::new(new_match)))
                    }
                }
                TermPattern::Application { function, argument } => match term_row.value() {
                    TermRep::Application(function_uid, argument_uid) => {
                        if let Some(function_match) = self
                            .check_term_matches(function_uid, function, r#match.clone())
                            .attach(ctx!("graph - match term pattern"))?
                        {
                            self.check_term_matches(argument_uid, argument, function_match)
                                .attach(ctx!("graph - match term pattern"))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Ok(None),
                },
                TermPattern::Constant { name, args } => {
                    let constant = match self.constants.get(name) {
                        Some(c) => c.value().clone(),
                        None => {
                            return Err(ImplicaError::ConstantNotFound {
                                name: name.clone(),
                                context: Some(ctx!("check term matches")),
                            }
                            .into())
                        }
                    };

                    match term_row.value() {
                        TermRep::Base(var) => {
                            if var != name {
                                return Ok(None);
                            }

                            let const_match = match self
                                .check_type_matches(
                                    term_uid,
                                    &constant.type_schema.compiled,
                                    Arc::new(Match::new(None)),
                                )
                                .attach(ctx!("check term matches"))?
                            {
                                Some(m) => m,
                                None => return Ok(None),
                            };

                            let mut new_match = Arc::new(Match::new(Some(r#match)));

                            for (v, arg) in zip(constant.free_variables.iter(), args) {
                                if let Some(element) = const_match.get(v) {
                                    let matched_type =
                                        element.as_type(v, Some(ctx!("check term matches")))?;

                                    new_match = match self
                                        .check_type_matches(&matched_type, &arg.compiled, new_match)
                                        .attach(ctx!("check term matches"))?
                                    {
                                        Some(m) => m,
                                        None => return Ok(None),
                                    };
                                }
                            }

                            Ok(Some(new_match))
                        }
                        TermRep::Application(..) => Ok(None),
                    }
                }
            }
        } else {
            Err(ImplicaError::TermNotFound {
                uid: *term_uid,
                context: Some("check term matches".to_string()),
            }
            .into())
        }
    }
}
