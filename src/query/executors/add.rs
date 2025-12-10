use std::collections::HashMap;

use crate::{
    context::Context,
    errors::ImplicaError,
    query::base::{AddOp, Query},
};

impl Query {
    pub(super) fn execute_add(&mut self, add_op: AddOp) -> Result<(), ImplicaError> {
        if self.matches.is_empty() {
            self.matches.push((HashMap::new(), Context::new()));
        }

        for (_, context) in self.matches.iter_mut() {
            match &add_op {
                AddOp::Type(var, r#type) => {
                    context.add_type(var.clone(), r#type.clone())?;
                }
                AddOp::Term(var, term) => {
                    context.add_term(var.clone(), term.clone())?;
                }
            }
        }

        Ok(())
    }
}
