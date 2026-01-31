use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use rayon::prelude::*;

use crate::errors::ImplicaError;
use crate::graph::base::{Graph, TypeRep, Uid};
use crate::matches::{next_match_id, Match, MatchElement, MatchSet};
use crate::patterns::{TypePattern, TypeSchema};

impl Graph {
    pub(super) fn match_type_schema(
        &self,
        type_schema: &TypeSchema,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        self.match_type_pattern(&type_schema.compiled, matches)
    }

    fn match_type_pattern(
        &self,
        pattern: &TypePattern,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        let out_map: MatchSet = Arc::new(DashMap::new());

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value();
            let r#match = r#match.clone();

            self.type_index.par_iter().try_for_each(|entry| {
                match self.check_type_matches(entry.key(), pattern, r#match.clone()) {
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

    pub(super) fn check_type_matches(
        &self,
        type_uid: &Uid,
        pattern: &TypePattern,
        r#match: Arc<Match>,
    ) -> Result<Option<Arc<Match>>, ImplicaError> {
        if let Some(type_row) = self.type_index.get(type_uid) {
            match pattern {
                TypePattern::Wildcard => Ok(Some(r#match.clone())),
                TypePattern::Variable(var) => {
                    if let Some(ref old_element) = r#match.get(var) {
                        let old_uid =
                            old_element.as_type(var, Some("check type matches".to_string()))?;

                        if &old_uid == type_row.key() {
                            Ok(Some(r#match.clone()))
                        } else {
                            Ok(None)
                        }
                    } else {
                        match type_row.value() {
                            TypeRep::Variable(type_name) => {
                                if var == type_name {
                                    Ok(Some(r#match.clone()))
                                } else {
                                    Ok(None)
                                }
                            }
                            _ => Ok(None),
                        }
                    }
                }
                TypePattern::Arrow { left, right } => match type_row.value() {
                    TypeRep::Arrow(left_uid, right_uid) => {
                        if let Some(left_match) =
                            self.check_type_matches(left_uid, left, r#match.clone())?
                        {
                            self.check_type_matches(right_uid, right, left_match.clone())
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Ok(None),
                },
                TypePattern::Capture { name, pattern } => {
                    if let Some(capture_match) =
                        self.check_type_matches(type_uid, pattern, r#match.clone())?
                    {
                        let new_match = Match::new(Some(capture_match));
                        new_match.insert(name, MatchElement::Type(*type_uid))?;

                        Ok(Some(Arc::new(new_match)))
                    } else {
                        Ok(None)
                    }
                }
            }
        } else {
            Err(ImplicaError::TypeNotFound {
                uid: *type_uid,
                context: Some("check type matches".to_string()),
            })
        }
    }
}
