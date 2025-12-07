use crate::errors::ImplicaError;
use crate::query::base::{Query, QueryResult};

impl Query {
    pub(super) fn execute_delete(&mut self, vars: Vec<String>) -> Result<(), ImplicaError> {
        for m in self.matches.iter_mut() {
            for var in vars.iter() {
                if let Some(qr) = m.remove(var) {
                    match qr {
                        QueryResult::Node(n) => {
                            self.graph.remove_node(n.uid())?;
                        }
                        QueryResult::Edge(e) => {
                            self.graph.remove_edge(e.uid())?;
                        }
                    }
                } else {
                    return Err(ImplicaError::VariableNotFound {
                        name: var.clone(),
                        context: Some("delete".to_string()),
                    });
                }
            }
        }

        Ok(())
    }
}
