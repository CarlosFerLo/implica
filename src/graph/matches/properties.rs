use error_stack::ResultExt;

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::base::{Graph, Uid};
use crate::properties::PropertyMap;
use crate::utils::compare_values;

impl Graph {
    pub(super) fn check_node_matches_properties(
        &self,
        node_uid: &Uid,
        properties: &PropertyMap,
    ) -> ImplicaResult<bool> {
        if let Some(entry) = self.nodes.get(node_uid) {
            let node_properties = entry.value();

            properties.try_par_compare(|key, value| {
                if let Some(other) = node_properties
                    .get(key)
                    .attach(ctx!("graph - check node matches properties"))?
                {
                    Ok(compare_values(value, &other))
                } else {
                    Ok(false)
                }
            })
        } else {
            Err(ImplicaError::NodeNotFound {
                uid: *node_uid,
                context: Some("check node matches properties".to_string()),
            }
            .into())
        }
    }

    pub(super) fn check_edge_matches_properties(
        &self,
        edge_uid: &(Uid, Uid),
        properties: &PropertyMap,
    ) -> ImplicaResult<bool> {
        if let Some(entry) = self.edges.get(edge_uid) {
            let edge_properties = entry.value();

            properties.try_par_compare(|key, value| {
                if let Some(other) = edge_properties
                    .get(key)
                    .attach(ctx!("graph - check edge matches properties"))?
                {
                    Ok(compare_values(value, &other))
                } else {
                    Ok(true)
                }
            })
        } else {
            Err(ImplicaError::EdgeNotFound {
                uid: *edge_uid,
                context: Some("check edge matches properties".to_string()),
            }
            .into())
        }
    }
}
