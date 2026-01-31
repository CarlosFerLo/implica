use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use rayon::prelude::*;

use crate::errors::ImplicaError;
use crate::graph::base::{Graph, TermRep, Uid};
use crate::matches::{next_match_id, Match, MatchElement, MatchSet};
use crate::patterns::{TermPattern, TermSchema};

impl Graph {
    pub(super) fn match_term_schema(
        &self,
        term_schema: &TermSchema,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        self.match_term_pattern(&term_schema.compiled, matches)
    }

    fn match_term_pattern(
        &self,
        pattern: &TermPattern,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
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
                    Err(e) => ControlFlow::Break(e),
                }
            })
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }

    pub(super) fn check_term_matches(
        &self,
        term_uid: &Uid,
        pattern: &TermPattern,
        r#match: Arc<Match>,
    ) -> Result<Option<Arc<Match>>, ImplicaError> {
        if let Some(term_row) = self.term_index.get(term_uid) {
            match pattern {
                TermPattern::Wildcard => Ok(Some(r#match.clone())),
                TermPattern::Variable(var) => {
                    if let Some(ref old_element) = r#match.get(var) {
                        let old_uid =
                            old_element.as_term(var, Some("check term matches".to_string()))?;

                        if &old_uid == term_uid {
                            Ok(Some(r#match.clone()))
                        } else {
                            Ok(None)
                        }
                    } else {
                        let new_match = Match::new(Some(r#match.clone()));
                        new_match.insert(var, MatchElement::Term(*term_uid))?;

                        Ok(Some(Arc::new(new_match)))
                    }
                }
                TermPattern::Application { function, argument } => match term_row.value() {
                    TermRep::Application(function_uid, argument_uid) => {
                        if let Some(function_match) =
                            self.check_term_matches(function_uid, function, r#match.clone())?
                        {
                            self.check_term_matches(argument_uid, argument, function_match)
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Ok(None),
                },
                TermPattern::Constant { .. } => todo!("constants not supported yet"),
            }
        } else {
            Err(ImplicaError::TermNotFound {
                uid: *term_uid,
                context: Some("check term matches".to_string()),
            })
        }
    }
}
