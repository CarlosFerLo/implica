use std::{ops::ControlFlow, sync::Arc};

use dashmap::DashMap;
use rayon::prelude::*;

use crate::{
    errors::ImplicaError,
    graph::base::Graph,
    matches::{next_match_id, Match, MatchElement, MatchSet},
    patterns::NodePattern,
};

impl Graph {
    pub fn create_node(
        &self,
        node_pattern: &NodePattern,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        let out_map = Arc::new(DashMap::new());

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value().clone();

            if let Some(ref var) = node_pattern.variable {
                if r#match.contains_key(var) {
                    return ControlFlow::Break(ImplicaError::VariableAlreadyExists {
                        name: var.to_string(),
                        context: Some("create node".to_string()),
                    });
                }
            }

            let node_type = match &node_pattern.type_schema {
                Some(type_schema) => match self.type_schema_to_type(type_schema, r#match.clone()) {
                    Ok(t) => t,
                    Err(e) => return ControlFlow::Break(e),
                },
                None => {
                    return ControlFlow::Break(ImplicaError::InvalidPattern {
                        pattern: format!("{:?}", node_pattern),
                        reason:
                            "To create a node, the node pattern provided must contain a type schema"
                                .to_string(),
                    })
                }
            };

            let node_term = match &node_pattern.term_schema {
                Some(term_schema) => match self.term_schema_to_term(term_schema, r#match.clone()) {
                    Ok(t) => Some(t),
                    Err(e) => return ControlFlow::Break(e),
                },
                None => None,
            };

            match self.add_node(node_type, node_term) {
                Ok(uid) => {
                    let new_match = Arc::new(Match::new(Some(r#match)));

                    if let Some(var) = &node_pattern.variable {
                        match new_match.insert(var, MatchElement::Node(uid)) {
                            Ok(()) => (),
                            Err(e) => return ControlFlow::Break(e),
                        }
                    }

                    out_map.insert(next_match_id(), (_prev_uid, new_match));
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(e),
            }
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }
}
