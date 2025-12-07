use crate::{
    errors::ImplicaError,
    query::base::{AddOp, Query},
};

impl Query {
    pub(super) fn execute_add(&mut self, add_op: AddOp) -> Result<(), ImplicaError> {
        match add_op {
            AddOp::Type(var, r#type) => {
                self.context.add_type(var, r#type)?;
            }
            AddOp::Term(var, term) => {
                self.context.add_term(var, term)?;
            }
        }

        Ok(())
    }
}
