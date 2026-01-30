use crate::errors::ImplicaError;
use crate::graph::base::{Graph, Uid};
use crate::properties::PropertyMap;
use crate::utils::compare_values;

impl Graph {
    pub(super) fn check_node_matches_properties(
        &self,
        node_uid: &Uid,
        properties: &PropertyMap,
    ) -> Result<bool, ImplicaError> {
        if let Some(entry) = self.nodes.get(node_uid) {
            let node_properties = entry.value();

            properties.try_par_compare(|key, value| {
                if let Some(other) = node_properties.get(key)? {
                    Ok(compare_values(value, &other))
                } else {
                    Ok(true)
                }
            })
        } else {
            Err(ImplicaError::NodeNotFound {
                uid: *node_uid,
                context: Some("check node matches properties".to_string()),
            })
        }
    }

    pub(super) fn check_edge_matches_properties(
        &self,
        edge_uid: &(Uid, Uid),
        properties: &PropertyMap,
    ) -> Result<bool, ImplicaError> {
        if let Some(entry) = self.edges.get(edge_uid) {
            let edge_properties = entry.value();

            properties.try_par_compare(|key, value| {
                if let Some(other) = edge_properties.get(key)? {
                    Ok(compare_values(value, &other))
                } else {
                    Ok(true)
                }
            })
        } else {
            Err(ImplicaError::EdgeNotFound {
                uid: *edge_uid,
                context: Some("check edge matches properties".to_string()),
            })
        }
    }
}
