use std::iter::zip;

use crate::errors::ImplicaError;
use crate::patterns::PathPattern;
use crate::query::base::{MatchOp, Query};
use crate::utils::PlaceholderGenerator;

impl Query {
    pub(super) fn execute_match_path(&mut self, mut path: PathPattern) -> Result<(), ImplicaError> {
        let ph_generator = PlaceholderGenerator::new();

        for np in path.nodes.iter_mut() {
            if np.variable.is_none() {
                let var_name = ph_generator.next();
                np.variable = Some(var_name);
            }
        }
        for ep in path.edges.iter_mut() {
            if ep.variable.is_none() {
                let var_name = ph_generator.next();
                ep.variable = Some(var_name);
            }
        }

        let mut prev = path.nodes.remove(0);
        self.execute_match(MatchOp::Node(prev.clone()))?;

        for (ep, np) in zip(path.edges, path.nodes) {
            self.execute_match(MatchOp::Node(np.clone()))?;
            self.execute_match(MatchOp::Edge(
                ep,
                prev.variable.clone(),
                np.variable.clone(),
            ))?;
            prev = np;
        }

        for (res, _) in self.matches.iter_mut() {
            for ph in ph_generator.prev() {
                res.remove(&ph);
            }
        }

        Ok(())
    }
}
