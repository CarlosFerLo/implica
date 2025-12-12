use pyo3::prelude::*;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{
    errors::ImplicaError,
    graph::Node,
    patterns::NodePattern,
    query::base::{Query, QueryResult},
};

impl Query {
    pub(super) fn execute_create_node(
        &mut self,
        node_pattern: NodePattern,
        is_merge: bool,
    ) -> Result<(), ImplicaError> {
        for (m, ref context) in self.matches.iter_mut() {
            if let Some(var) = &node_pattern.variable {
                if m.contains_key(var) {
                    return Err(ImplicaError::VariableAlreadyExists {
                        name: var.clone(),
                        context: Some(format!("create node {}", line!())),
                    });
                }
            }

            let term = if let Some(term_obj) = &node_pattern.term {
                Some(term_obj.clone())
            } else if let Some(term_schema) = &node_pattern.term_schema {
                Some(Arc::new(
                    term_schema.as_term(context, self.graph.constants.clone())?,
                ))
            } else {
                None
            };

            let r#type = if let Some(type_obj) = &node_pattern.r#type {
                type_obj.clone()
            } else if let Some(type_schema) = &node_pattern.type_schema {
                Arc::new(type_schema.as_type(context)?)
            } else if let Some(ref trm) = term {
                trm.r#type()
            } else {
                return Err(ImplicaError::InvalidQuery {
                    message: "To create a node you must provide either a 'type' or 'type_schema'"
                        .to_string(),
                    context: Some(format!("create node {}", line!())),
                });
            };

            let mut props = HashMap::new();

            Python::attach(|py| {
                for (k, v) in node_pattern.properties.iter() {
                    props.insert(k.clone(), v.clone_ref(py));
                }
            });

            let mut node = Node::new(
                r#type,
                term.map(|t| Arc::new(RwLock::new((*t).clone()))),
                Some(props),
            )?;

            match self.graph.add_node(&node) {
                Ok(_) => (),
                Err(e) => match &e {
                    ImplicaError::NodeAlreadyExists {
                        message: _,
                        existing,
                        new: _,
                    } => {
                        if is_merge {
                            node = existing
                                .read()
                                .map_err(|e| ImplicaError::LockError {
                                    rw: "read".to_string(),
                                    message: e.to_string(),
                                    context: Some("execute create node (merge mode)".to_string()),
                                })?
                                .clone();
                        } else {
                            return Err(e);
                        }
                    }
                    _ => {
                        return Err(e);
                    }
                },
            }

            if let Some(var) = &node_pattern.variable {
                m.insert(var.clone(), QueryResult::Node(node));
            }
        }
        Ok(())
    }
}
