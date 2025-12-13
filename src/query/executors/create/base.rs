use std::collections::HashMap;

use crate::{
    context::Context,
    errors::ImplicaError,
    query::base::{CreateOp, Query, QueryOperation},
};

#[path = "edge.rs"]
mod edge;
#[path = "node.rs"]
mod node;
#[path = "path.rs"]
mod path;

impl Query {
    pub(super) fn execute_create(&mut self, create_op: CreateOp) -> Result<(), ImplicaError> {
        if self.should_add_match() {
            self.matches.push((HashMap::new(), Context::new()));
        }

        match create_op {
            CreateOp::Node(node_pattern, is_merge) => {
                self.execute_create_node(node_pattern, is_merge)
            }
            CreateOp::Edge(edge_pattern, start_var, end_var, is_merge) => {
                self.execute_create_edge(edge_pattern, start_var, end_var, is_merge)
            }
            CreateOp::Path(path, is_merge) => self.execute_create_path(path, is_merge),
        }
    }

    fn should_add_match(&self) -> bool {
        if !self.matches.is_empty() {
            false
        } else {
            for op in self.operations.iter() {
                match op {
                    QueryOperation::Match(_) | QueryOperation::Where(_) => {
                        return false;
                    }
                    _ => {
                        continue;
                    }
                }
            }

            true
        }
    }
}
