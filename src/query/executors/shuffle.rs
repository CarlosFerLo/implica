use rand::rng;
use rand::seq::SliceRandom;

use crate::{errors::ImplicaError, query::base::Query};

impl Query {
    pub(super) fn execute_shuffle(&mut self) -> Result<(), ImplicaError> {
        let mut rng = rng();

        self.matches.shuffle(&mut rng);

        Ok(())
    }
}
