use crate::errors::ImplicaError;
use crate::query::base::{MatchOp, Query};

mod edge;
mod node;
mod path;

impl Query {
    pub(super) fn execute_match(&mut self, match_op: MatchOp) -> Result<(), ImplicaError> {
        match match_op {
            MatchOp::Node(node_pattern) => self.execute_match_node(node_pattern),
            MatchOp::Edge(edge_pattern, start_var, end_var) => {
                self.execute_match_edge(edge_pattern, start_var, end_var)
            }
            MatchOp::Path(path) => self.execute_match_path(path),
        }
    }
}
