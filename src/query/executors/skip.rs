use crate::errors::ImplicaError;
use crate::query::base::Query;

impl Query {
    pub(super) fn execute_skip(&mut self, count: usize) -> Result<(), ImplicaError> {
        // Skip the first N results for each variable
        if count < self.matches.len() {
            self.matches.drain(0..count);
        } else {
            self.matches.clear();
        }
        Ok(())
    }
}
