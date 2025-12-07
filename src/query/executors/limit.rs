use crate::errors::ImplicaError;
use crate::query::base::Query;

impl Query {
    pub(super) fn execute_limit(&mut self, count: usize) -> Result<(), ImplicaError> {
        // Limit the number of results for each variable
        self.matches.truncate(count);
        Ok(())
    }
}
