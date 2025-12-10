#![allow(unused_variables)]

use crate::context::Context;
use crate::errors::ImplicaError;
use crate::graph::{python_to_property_map, Edge, Graph, Node};
use crate::patterns::{EdgePattern, NodePattern, PathPattern, TermSchema, TypeSchema};
use crate::typing::{python_to_term, python_to_type, Term, Type};
use crate::utils::validate_variable_name;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::sync::Arc;

#[path = "executors/add.rs"]
mod add;
#[path = "executors/create/base.rs"]
mod create;
#[path = "executors/delete.rs"]
mod delete;
#[path = "executors/limit.rs"]
mod limit;
#[path = "executors/match/base.rs"]
mod r#match;
#[path = "executors/order_by.rs"]
mod order_by;
#[path = "executors/set.rs"]
mod set;
#[path = "executors/skip.rs"]
mod skip;
#[path = "executors/where.rs"]
mod r#where;
#[path = "executors/with.rs"]
mod with;

#[pyclass]
#[derive(Clone, Debug)]
pub struct Query {
    pub graph: Graph,
    pub matches: Vec<(HashMap<String, QueryResult>, Context)>,
    pub operations: Vec<QueryOperation>,
}

#[derive(Clone, Debug)]
pub enum QueryResult {
    Node(Node),
    Edge(Edge),
}

#[derive(Debug)]
pub enum QueryOperation {
    Match(MatchOp),
    Where(String),
    Create(CreateOp),
    Add(AddOp),
    Set(String, HashMap<String, Py<PyAny>>, bool),
    Delete(Vec<String>),
    With(Vec<String>),
    OrderBy(Vec<String>, bool),
    Limit(usize),
    Skip(usize),
}

impl Clone for QueryOperation {
    fn clone(&self) -> Self {
        Python::attach(|py| match self {
            QueryOperation::Match(m) => QueryOperation::Match(m.clone()),
            QueryOperation::Where(w) => QueryOperation::Where(w.clone()),
            QueryOperation::Create(c) => QueryOperation::Create(c.clone()),
            QueryOperation::Add(a) => QueryOperation::Add(a.clone()),
            QueryOperation::Set(var, dict, overwrite) => {
                let mut new_dict = HashMap::new();

                Python::attach(|py| {
                    for (k, v) in dict.iter() {
                        new_dict.insert(k.clone(), v.clone_ref(py));
                    }
                });

                QueryOperation::Set(var.clone(), new_dict, *overwrite)
            }
            QueryOperation::Delete(vars) => QueryOperation::Delete(vars.clone()),
            QueryOperation::With(w) => QueryOperation::With(w.clone()),
            QueryOperation::OrderBy(v, asc) => QueryOperation::OrderBy(v.clone(), *asc),
            QueryOperation::Limit(l) => QueryOperation::Limit(*l),
            QueryOperation::Skip(s) => QueryOperation::Skip(*s),
        })
    }
}

#[derive(Clone, Debug)]
pub enum MatchOp {
    Node(NodePattern),
    Edge(EdgePattern, Option<String>, Option<String>),
    Path(PathPattern),
}

#[derive(Clone, Debug)]
pub enum CreateOp {
    Node(NodePattern),
    Edge(EdgePattern, String, String),
    Path(PathPattern),
}

#[derive(Clone, Debug)]
pub enum AddOp {
    Type(String, Type),
    Term(String, Term),
}

#[pymethods]
impl Query {
    #[new]
    pub fn new(graph: Graph) -> Self {
        Query {
            graph,
            matches: Vec::new(),
            operations: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature=(pattern=None, node=None, edge=None, start=None, end=None, r#type=None, type_schema=None, term=None, term_schema=None, properties=None))]
    pub fn r#match(
        &mut self,
        py: Python,
        pattern: Option<String>,
        node: Option<String>,
        edge: Option<String>,
        start: Option<String>,
        end: Option<String>,
        r#type: Option<Py<PyAny>>,
        type_schema: Option<TypeSchema>,
        term: Option<Py<PyAny>>,
        term_schema: Option<TermSchema>,
        properties: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        if let Some(p) = pattern {
            // Parse Cypher-like pattern
            let path = PathPattern::parse(p)?;
            self.operations
                .push(QueryOperation::Match(MatchOp::Path(path)));
        } else if node.is_some() {
            // Convert Python types to Rust types
            let type_obj = if let Some(t) = r#type {
                Some(Arc::new(python_to_type(t.bind(py))?))
            } else {
                None
            };

            let term_obj = if let Some(t) = term {
                Some(Arc::new(python_to_term(t.bind(py))?))
            } else {
                None
            };

            let properties_map = if let Some(p) = properties {
                Some(python_to_property_map(p.bind(py))?)
            } else {
                None
            };

            // Match node
            let node_pattern = NodePattern::new(
                node,
                type_obj,
                type_schema,
                term_obj,
                term_schema,
                properties_map,
            )?;
            self.operations
                .push(QueryOperation::Match(MatchOp::Node(node_pattern)));
        } else if edge.is_some() {
            // Convert Python types to Rust types
            let type_obj = if let Some(t) = r#type {
                Some(Arc::new(python_to_type(t.bind(py))?))
            } else {
                None
            };

            let term_obj = if let Some(t) = term {
                Some(Arc::new(python_to_term(t.bind(py))?))
            } else {
                None
            };

            let properties_map = if let Some(props) = properties {
                Some(python_to_property_map(props.bind(py))?)
            } else {
                None
            };

            // Match edge
            let edge_pattern = EdgePattern::new(
                edge.clone(),
                type_obj,
                type_schema,
                term_obj,
                term_schema,
                properties_map,
                "forward".to_string(),
            )?;

            if let Some(ref start_var) = start {
                validate_variable_name(start_var)?;
            }
            if let Some(ref end_var) = end {
                validate_variable_name(end_var)?;
            }

            self.operations.push(QueryOperation::Match(MatchOp::Edge(
                edge_pattern,
                start,
                end,
            )));
        }

        Ok(self.clone())
    }

    pub fn r#where(&mut self, condition: String) -> PyResult<Self> {
        self.operations.push(QueryOperation::Where(condition));
        Ok(self.clone())
    }

    #[pyo3(signature = (*variables))]
    pub fn return_(&mut self, py: Python, variables: Vec<String>) -> PyResult<Vec<Py<PyAny>>> {
        // Execute all operations to build matched_vars
        self.execute_operations()?;

        // Collect results
        let mut results = Vec::new();

        if self.matches.is_empty() {
            return Ok(results);
        }

        for (m, _) in self.matches.iter() {
            let dict = PyDict::new(py);
            for (k, v) in m.iter() {
                if variables.contains(k) {
                    match v {
                        QueryResult::Node(n) => {
                            dict.set_item(k, n.clone())?;
                        }
                        QueryResult::Edge(e) => {
                            dict.set_item(k, e.clone())?;
                        }
                    }
                }
            }
            results.push(dict.into());
        }

        Ok(results)
    }

    pub fn return_count(&mut self, py: Python) -> PyResult<usize> {
        self.execute_operations()?;

        Ok(self.matches.len())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature=(pattern=None, node=None, edge=None, start=None, end=None, r#type=None, type_schema=None, term=None, term_schema=None, properties=None))]
    pub fn create(
        &mut self,
        py: Python,
        pattern: Option<String>,
        node: Option<String>,
        edge: Option<String>,
        start: Option<String>,
        end: Option<String>,
        r#type: Option<Py<PyAny>>,
        type_schema: Option<TypeSchema>,
        term: Option<Py<PyAny>>,
        term_schema: Option<TermSchema>,
        properties: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        if let Some(p) = pattern {
            let path = PathPattern::parse(p)?;
            self.operations
                .push(QueryOperation::Create(CreateOp::Path(path)));
        } else if node.is_some() {
            // Convert Python types to Rust types
            let type_obj = if let Some(t) = r#type {
                Some(Arc::new(python_to_type(t.bind(py))?))
            } else {
                None
            };

            let term_obj = if let Some(t) = term {
                Some(Arc::new(python_to_term(t.bind(py))?))
            } else {
                None
            };

            let properties_map = if let Some(props) = properties {
                Some(python_to_property_map(props.bind(py))?)
            } else {
                None
            };

            let node_pattern = NodePattern::new(
                node,
                type_obj,
                type_schema,
                term_obj,
                term_schema,
                properties_map,
            )?;
            self.operations
                .push(QueryOperation::Create(CreateOp::Node(node_pattern)));
        } else if edge.is_some() {
            // Convert Python types to Rust types
            let type_obj = if let Some(t) = r#type {
                Some(Arc::new(python_to_type(t.bind(py))?))
            } else {
                None
            };

            let term_obj = if let Some(t) = term {
                Some(Arc::new(python_to_term(t.bind(py))?))
            } else {
                None
            };

            let properties_map = if let Some(props) = properties {
                Some(python_to_property_map(props.bind(py))?)
            } else {
                None
            };

            let edge_pattern = EdgePattern::new(
                edge.clone(),
                type_obj,
                type_schema,
                term_obj,
                term_schema,
                properties_map,
                "forward".to_string(),
            )?;

            let start = if let Some(start_var) = start {
                validate_variable_name(&start_var)?;
                start_var
            } else {
                return Err(ImplicaError::InvalidQuery {
                    message: "for creating an edge you must specify a 'start' variable."
                        .to_string(),
                    context: Some("create".to_string()),
                }
                .into());
            };

            let end = if let Some(end_var) = end {
                validate_variable_name(&end_var)?;
                end_var
            } else {
                return Err(ImplicaError::InvalidQuery {
                    message: "for creating an edge you must specify a 'end' variable.".to_string(),
                    context: Some("create".to_string()),
                }
                .into());
            };

            self.operations.push(QueryOperation::Create(CreateOp::Edge(
                edge_pattern,
                start,
                end,
            )));
        }

        Ok(self.clone())
    }

    #[pyo3(signature=(variable, r#type=None, term=None))]
    pub fn add(
        &mut self,
        py: Python,
        variable: String,
        r#type: Option<Py<PyAny>>,
        term: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        validate_variable_name(&variable)?;

        let type_obj = if let Some(t) = r#type {
            Some(python_to_type(t.bind(py))?)
        } else {
            None
        };

        let term_obj = if let Some(t) = term {
            Some(python_to_term(t.bind(py))?)
        } else {
            None
        };

        match (type_obj, term_obj) {
            (Some(typ), Some(trm)) => {
                return Err(ImplicaError::InvalidQuery { message: "cannot include 'term' and 'type' in an add operation - they are mutually exclusive".to_string(), context: Some("add".to_string()) }.into());
            }
            (Some(typ), None) => {
                self.operations
                    .push(QueryOperation::Add(AddOp::Type(variable, typ)));
            }
            (None, Some(trm)) => {
                self.operations
                    .push(QueryOperation::Add(AddOp::Term(variable, trm)));
            }
            (None, None) => {
                return Err(ImplicaError::InvalidQuery {
                    message: "must specify at least one of 'term' or 'type'".to_string(),
                    context: Some("add".to_string()),
                }
                .into());
            }
        }

        Ok(self.clone())
    }

    pub fn set(
        &mut self,
        py: Python,
        variable: String,
        properties: Py<PyAny>,
        overwrite: bool,
    ) -> PyResult<Self> {
        let props = python_to_property_map(properties.bind(py))?;

        self.operations
            .push(QueryOperation::Set(variable, props, overwrite));
        Ok(self.clone())
    }

    #[pyo3(signature = (*variables))]
    pub fn delete(&mut self, variables: Vec<String>) -> PyResult<Self> {
        self.operations.push(QueryOperation::Delete(variables));
        Ok(self.clone())
    }

    #[pyo3(signature = (*variables))]
    pub fn with_(&mut self, variables: Vec<String>) -> PyResult<Self> {
        self.operations.push(QueryOperation::With(variables));
        Ok(self.clone())
    }

    #[pyo3(signature = (*variables, ascending=true))]
    pub fn order_by(&mut self, variables: Vec<String>, ascending: bool) -> PyResult<Self> {
        self.operations
            .push(QueryOperation::OrderBy(variables, ascending));
        Ok(self.clone())
    }

    pub fn limit(&mut self, count: usize) -> PyResult<Self> {
        self.operations.push(QueryOperation::Limit(count));
        Ok(self.clone())
    }

    pub fn skip(&mut self, count: usize) -> PyResult<Self> {
        self.operations.push(QueryOperation::Skip(count));
        Ok(self.clone())
    }

    pub fn execute(&mut self, py: Python) -> PyResult<Self> {
        self.execute_operations()?;
        Ok(self.clone())
    }
}

impl Query {
    fn execute_operations(&mut self) -> Result<(), ImplicaError> {
        for op in self.operations.clone() {
            match op {
                QueryOperation::Match(match_op) => {
                    self.execute_match(match_op)?;
                }
                QueryOperation::Create(create_op) => {
                    self.execute_create(create_op)?;
                }
                QueryOperation::Add(add_op) => {
                    self.execute_add(add_op)?;
                }
                QueryOperation::Delete(vars) => {
                    self.execute_delete(vars)?;
                }
                QueryOperation::Set(var, props, overwrite) => {
                    self.execute_set(var, props, overwrite)?;
                }
                QueryOperation::Where(condition) => {
                    self.execute_where(condition)?;
                }
                QueryOperation::With(vars) => {
                    self.execute_with(vars)?;
                }
                QueryOperation::OrderBy(vars, ascending) => {
                    self.execute_order_by(vars, ascending)?;
                }
                QueryOperation::Limit(count) => {
                    self.execute_limit(count)?;
                }
                QueryOperation::Skip(count) => {
                    self.execute_skip(count)?;
                }
            }
        }
        Ok(())
    }
}
