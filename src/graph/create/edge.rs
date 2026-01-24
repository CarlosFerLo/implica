use std::ops::ControlFlow;
use std::sync::Arc;

use dashmap::DashMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::errors::ImplicaError;
use crate::graph::base::Graph;
use crate::matches::MatchSet;
use crate::patterns::EdgePattern;

impl Graph {
    pub fn create_edge(
        &self,
        edge_pattern: &EdgePattern,
        matches: MatchSet,
    ) -> Result<MatchSet, ImplicaError> {
        let out_map = Arc::new(DashMap::new());

        let result = matches.par_iter().try_for_each(|row| {
            let (_prev_uid, r#match) = row.value().clone();

            if let Some(ref var) = edge_pattern.variable {
                if r#match.contains_key(var) {
                    return ControlFlow::Break(ImplicaError::VariableAlreadyExists {
                        name: var.to_string(),
                        context: Some("create edge".to_string()),
                    });
                }
            }

            let term = match &edge_pattern.term_schema {
                Some(term_schema) => match self.term_schema_to_term(term_schema, r#match) {
                    Ok(t) => t,
                    Err(e) => return  ControlFlow::Break(e)
                },
                None => {
                    return ControlFlow::Break(ImplicaError::InvalidPattern { pattern: format!("{:?}", edge_pattern), reason: "To create an edge, the edge pattern provided must contain a term schema".to_string() });
                }
            };

            match self.add_edge(term) {
                Ok(_) => ControlFlow::Continue(()),
                Err(e) => ControlFlow::Break(e)
            }
        });

        match result {
            ControlFlow::Continue(()) => Ok(out_map),
            ControlFlow::Break(e) => Err(e),
        }
    }
}
