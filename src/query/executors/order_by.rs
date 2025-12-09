use std::cmp::Ordering;

use pyo3::prelude::*;

use crate::errors::ImplicaError;
use crate::query::base::{Query, QueryResult};
use crate::utils::{compare_values, validate_variable_name};

impl Query {
    pub(super) fn execute_order_by(
        &mut self,
        vars: Vec<String>,
        ascending: bool,
    ) -> Result<(), ImplicaError> {
        let mut props: Vec<(String, String)> = Vec::new();
        for var in &vars {
            let parts: Vec<&str> = var.split(".").collect();

            if parts.len() != 2 {
                return Err(ImplicaError::InvalidQuery {
                    message: format!("Invalid variable provided: {}", var),
                    context: Some("order by".to_string()),
                });
            }

            validate_variable_name(parts[0])?;
            validate_variable_name(parts[1])?;

            props.push((parts[0].to_string(), parts[1].to_string()));
        }

        Python::attach(|py| {
            self.matches.sort_by(|(a, _), (b, _)| {
                for (var, prop) in &props {
                    let val_a = match a.get(var) {
                        Some(qr) => match qr {
                            QueryResult::Node(n) => match n.properties.read() {
                                Ok(dict) => dict.get(prop).map(|v| v.clone_ref(py)),
                                Err(_) => None,
                            },
                            QueryResult::Edge(e) => match e.properties.read() {
                                Ok(dict) => dict.get(prop).map(|v| v.clone_ref(py)),
                                Err(_) => None,
                            },
                        },
                        None => None,
                    };
                    let val_b = match b.get(var) {
                        Some(qr) => match qr {
                            QueryResult::Node(n) => match n.properties.read() {
                                Ok(dict) => dict.get(prop).map(|v| v.clone_ref(py)),
                                Err(_) => None,
                            },
                            QueryResult::Edge(e) => match e.properties.read() {
                                Ok(dict) => dict.get(prop).map(|v| v.clone_ref(py)),
                                Err(_) => None,
                            },
                        },
                        None => None,
                    };

                    let ordering = compare_values(val_a, val_b, py);
                    if ordering != Ordering::Equal {
                        return if ascending {
                            ordering
                        } else {
                            ordering.reverse()
                        };
                    }
                }

                Ordering::Equal
            });
        });
        Ok(())
    }
}
