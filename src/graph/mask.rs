use std::sync::Arc;

use dashmap::DashSet;

use crate::graph::Uid;

#[derive(Debug, Clone)]
pub(crate) struct Mask {
    pub(crate) nodes: Arc<DashSet<Uid>>,
}

impl Mask {
    pub(crate) fn new(nodes: Arc<DashSet<Uid>>) -> Self {
        Mask { nodes }
    }

    pub(in crate::graph) fn contains(&self, node: &Uid) -> bool {
        self.nodes.contains(node)
    }

    pub(in crate::graph) fn add_node(&self, node: &Uid) {
        self.nodes.insert(*node);
    }

    pub(crate) fn remove_node(&self, node: &Uid) {
        self.nodes.remove(node);
    }
}
