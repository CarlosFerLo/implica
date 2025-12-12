use std::sync::{Arc, RwLock};

use crate::{
    errors::ImplicaError,
    patterns::TermSchema,
    query::base::{Query, QueryResult},
    typing::Term,
};

impl Query {
    pub(super) fn execute_put(
        &mut self,
        variable: String,
        term: Option<Term>,
        term_schema: Option<TermSchema>,
    ) -> Result<(), ImplicaError> {
        for (m, context) in self.matches.iter_mut() {
            let term_obj = if let Some(ref term) = term {
                term.clone()
            } else if let Some(ref schema) = term_schema {
                schema.as_term(context, self.graph.constants.clone())?
            } else {
                return Err(ImplicaError::InvalidQuery {
                    message: "must provide the 'term' or the 'term_schema' in the put query"
                        .to_string(),
                    context: Some("execute put".to_string()),
                });
            };

            let node_uid = if let Some(qr) = m.get_mut(&variable) {
                match qr {
                    QueryResult::Node(node) => {
                        match &node.term {
                            Some(term_lock) => {
                                let mut term =
                                    term_lock.write().map_err(|e| ImplicaError::LockError {
                                        rw: "write".to_string(),
                                        message: e.to_string(),
                                        context: Some("execute put".to_string()),
                                    })?;

                                *term = term_obj.clone();
                            }
                            None => {
                                node.term = Some(Arc::new(RwLock::new(term_obj.clone())));
                            }
                        }

                        node.uid()
                    }
                    QueryResult::Edge(_) => {
                        return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to an edge has been accessed as a node", variable),
                                            context: Some("execute put".to_string())
                                        });
                    }
                }
            } else {
                return Err(ImplicaError::VariableNotFound {
                    name: variable,
                    context: Some("execute put".to_string()),
                });
            };

            self.graph.set_node_term(node_uid, &term_obj)?;
        }

        Ok(())
    }
}
