use rhai::Scope;

use crate::errors::ImplicaError;
use crate::query::base::{Query, QueryResult};
use crate::utils::{props_as_map, Evaluator};

impl Query {
    pub(super) fn execute_where(&mut self, condition: String) -> Result<(), ImplicaError> {
        let mut results = Vec::new();

        let evaluator = Evaluator::new()?;

        for (m, context) in self.matches.iter() {
            let mut scope = Scope::new();
            for (var, qr) in m.iter() {
                let props = match qr {
                    QueryResult::Node(n) => {
                        n.properties.read().map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some("execute where".to_string()),
                        })?
                    }
                    QueryResult::Edge(e) => {
                        e.properties.read().map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some("execute where".to_string()),
                        })?
                    }
                };

                let map = props_as_map(&props)?;
                scope.push(var.clone(), map);
            }

            if evaluator.eval(&mut scope, &condition)? {
                results.push((m.clone(), context.clone()));
            }
        }

        self.matches = results;

        Ok(())
    }
}
