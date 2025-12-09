use std::collections::HashMap;

use crate::errors::ImplicaError;
use crate::query::base::Query;

impl Query {
    pub(super) fn execute_with(&mut self, vars: Vec<String>) -> Result<(), ImplicaError> {
        for (m, _) in self.matches.iter_mut() {
            let mut dict = HashMap::new();

            for v in vars.iter() {
                match m.get(v) {
                    Some(qr) => {
                        dict.insert(v.clone(), qr.clone());
                    }
                    None => {
                        return Err(ImplicaError::VariableNotFound {
                            name: v.clone(),
                            context: Some("with".to_string()),
                        });
                    }
                }
            }

            *m = dict;
        }

        Ok(())
    }
}
