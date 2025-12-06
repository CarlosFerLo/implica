//! Cypher-like query system for graph querying and manipulation.
//!
//! This module provides the `Query` structure for building and executing
//! Cypher-like queries on graphs. It supports pattern matching, creation,
//! deletion, merging, and other graph operations.

#![allow(unused_variables)]

use crate::context::Context;
use crate::errors::ImplicaError;
use crate::graph::{Edge, Graph, Node};
use crate::patterns::{EdgePattern, NodePattern, PathPattern, TermSchema, TypeSchema};
use crate::typing::{python_to_term, python_to_type, Arrow, Type};
use crate::utils::{compare_values, props_as_map, Evaluator};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rhai::Scope;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::iter::zip;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// Cypher-like query builder for the graph.
///
/// The Query structure provides a fluent interface for building and executing
/// graph queries. It supports pattern matching, node/edge creation, updates,
/// and deletions, similar to Cypher query language.
///
/// # Examples
///
/// ```python
/// import implica
///
/// graph = implica.Graph()
/// q = graph.query()
///
/// # Match nodes
/// q.match(node="n", type_schema="$Person$")
/// results = q.return_(["n"])
///
/// # Create nodes
/// q.create(node="p", type=person_type, properties={"name": "Alice"})
///
/// # Complex queries
/// q.match("(a:Person)-[r:knows]->(b:Person)")
/// q.where("a.age > 25")
/// results = q.return_(["a", "b"])
/// ```
///
/// # Fields
///
/// * `graph` - The graph being queried
/// * `matched_vars` - Variables matched during query execution (internal)
/// * `operations` - Queue of operations to execute (internal)
#[pyclass]
#[derive(Clone, Debug)]
pub struct Query {
    pub graph: Graph,
    pub matches: Vec<HashMap<String, QueryResult>>,
    pub operations: Vec<QueryOperation>,
    pub context: Arc<Context>,
}

/// Result type for query matching (internal).
///
/// Represents either a matched node or a matched edge.
#[derive(Clone, Debug)]
pub enum QueryResult {
    Node(Node),
    Edge(Edge),
}

/// Query operation types (internal).
///
/// Represents the different operations that can be performed in a query.
#[derive(Debug)]
pub enum QueryOperation {
    Match(MatchOp),
    Where(String),
    Create(CreateOp),
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

/// Match operation types (internal).
///
/// Represents different patterns that can be matched.
#[derive(Clone, Debug)]
pub enum MatchOp {
    Node(NodePattern),
    Edge(EdgePattern, Option<String>, Option<String>),
    Path(PathPattern),
}

/// Create operation types (internal).
///
/// Represents different elements that can be created.
#[derive(Clone, Debug)]
pub enum CreateOp {
    Node(NodePattern),
    Edge(EdgePattern, String, String),
    Path(PathPattern),
}

#[pymethods]
impl Query {
    /// Creates a new query for the given graph.
    ///
    /// # Arguments
    ///
    /// * `graph` - The graph to query
    ///
    /// # Returns
    ///
    /// A new `Query` instance
    ///
    /// # Note
    ///
    /// Typically you don't create queries directly but use `graph.query()` instead.
    #[new]
    pub fn new(graph: Graph) -> Self {
        Query {
            graph,
            matches: Vec::new(),
            operations: Vec::new(),
            context: Arc::new(Context::new()),
        }
    }

    /// Matches nodes, edges, or paths in the graph.
    ///
    /// This is the primary method for pattern matching in queries. It supports
    /// multiple forms: pattern strings, explicit node/edge specifications, and more.
    ///
    /// # Arguments
    ///
    /// * `pattern` - Optional Cypher-like pattern string (e.g., "(n:Person)-\[e\]->(m)")
    /// * `node` - Optional variable name for node matching
    /// * `edge` - Optional variable name for edge matching
    /// * `start` - Optional start node for edge matching
    /// * `end` - Optional end node for edge matching
    /// * `type` - Optional specific type to match for nodes
    /// * `type_schema` - Optional type schema pattern for nodes
    /// * `term` - Optional specific term for edges
    /// * `term_type_schema` - Optional type schema for edge terms
    /// * `properties` - Optional dictionary of required properties
    ///
    /// # Returns
    ///
    /// Self (for method chaining)
    ///
    /// # Examples
    ///
    /// ```python
    /// # Match with pattern string
    /// q.match("(n:Person)-\[e:knows\]->(m:Person)")
    ///
    /// # Match node
    /// q.match(node="n", type_schema="$Person$")
    ///
    /// # Match edge
    /// q.match(edge="e", start=start_node, end=end_node)
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn r#match(
        &mut self,
        pattern: Option<String>,
        node: Option<String>,
        edge: Option<String>,
        start: Option<Py<PyAny>>,
        end: Option<Py<PyAny>>,
        r#type: Option<Py<PyAny>>,
        type_schema: Option<Py<PyAny>>,
        term: Option<Py<PyAny>>,
        term_schema: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        if let Some(p) = pattern {
            // Parse Cypher-like pattern
            let path = PathPattern::parse(p)?;
            self.operations
                .push(QueryOperation::Match(MatchOp::Path(path)));
        } else if node.is_some() {
            Python::attach(|py| -> PyResult<()> {
                // Convert Python types to Rust types
                let type_obj = if let Some(t) = r#type {
                    Some(python_to_type(t.bind(py))?)
                } else {
                    None
                };

                let type_schema_obj = if let Some(ts) = type_schema {
                    let schema_str: String = ts.bind(py).extract()?;
                    Some(crate::patterns::TypeSchema::new(schema_str)?)
                } else {
                    None
                };

                let term_obj = if let Some(t) = term {
                    Some(python_to_term(t.bind(py))?)
                } else {
                    None
                };

                let term_schema_obj = if let Some(ts) = term_schema {
                    let schema_str: String = ts.bind(py).extract()?;
                    Some(crate::patterns::TermSchema::new(schema_str)?)
                } else {
                    None
                };

                let properties_map = if let Some(props) = properties {
                    Some(
                        props
                            .bind(py)
                            .extract::<std::collections::HashMap<String, Py<PyAny>>>()?,
                    )
                } else {
                    None
                };

                // Match node
                let node_pattern = NodePattern::new(
                    node,
                    type_obj.map(Arc::new),
                    type_schema_obj,
                    term_obj.map(Arc::new),
                    term_schema_obj,
                    properties_map,
                )?;
                self.operations
                    .push(QueryOperation::Match(MatchOp::Node(node_pattern)));
                Ok(())
            })?;
        } else if edge.is_some() {
            Python::attach(|py| -> PyResult<()> {
                // Convert Python types to Rust types
                let type_obj = if let Some(t) = r#type {
                    Some(Arc::new(python_to_type(t.bind(py))?))
                } else {
                    None
                };

                let type_schema_obj = if let Some(ts) = type_schema {
                    let schema_str: String = ts.bind(py).extract()?;
                    Some(TypeSchema::new(schema_str)?)
                } else {
                    None
                };

                let term_obj = if let Some(t) = term {
                    Some(Arc::new(python_to_term(t.bind(py))?))
                } else {
                    None
                };

                let term_schema_obj = if let Some(ts) = term_schema {
                    let schema_str: String = ts.bind(py).extract()?;
                    Some(TermSchema::new(schema_str)?)
                } else {
                    None
                };

                let properties_map = if let Some(props) = properties {
                    Some(
                        props
                            .bind(py)
                            .extract::<std::collections::HashMap<String, Py<PyAny>>>()?,
                    )
                } else {
                    None
                };

                // Match edge
                let edge_pattern = EdgePattern::new(
                    edge.clone(),
                    type_obj,
                    type_schema_obj,
                    term_obj,
                    term_schema_obj,
                    properties_map,
                    "forward".to_string(),
                )?;
                let start_var = Self::extract_var_or_none(start)?;
                let end_var = Self::extract_var_or_none(end)?;
                self.operations.push(QueryOperation::Match(MatchOp::Edge(
                    edge_pattern,
                    start_var,
                    end_var,
                )));
                Ok(())
            })?;
        }

        Ok(self.clone())
    }

    /// Adds a WHERE clause to filter results (not fully implemented).
    ///
    /// # Arguments
    ///
    /// * `condition` - SQL-like condition string
    ///
    /// # Returns
    ///
    /// Self (for method chaining)
    pub fn r#where(&mut self, condition: String) -> PyResult<Self> {
        self.operations.push(QueryOperation::Where(condition));
        Ok(self.clone())
    }

    /// Returns the specified variables from the query results.
    ///
    /// Executes all operations and returns the matched variables as a list of
    /// dictionaries, where each dictionary maps variable names to their values.
    ///
    /// # Arguments
    ///
    /// * `py` - Python context
    /// * `variables` - List of variable names to return
    ///
    /// # Returns
    ///
    /// A list of dictionaries containing the requested variables
    ///
    /// # Examples
    ///
    /// ```python
    /// q.match(node="n", type_schema="$Person$")
    /// results = q.return_(["n"])
    /// for row in results:
    ///     print(row["n"])
    /// ```
    #[pyo3(signature = (*variables))]
    pub fn return_(&mut self, py: Python, variables: Vec<String>) -> PyResult<Vec<Py<PyAny>>> {
        // Execute all operations to build matched_vars
        self.execute_operations(py)?;

        // Collect results
        let mut results = Vec::new();

        if self.matches.is_empty() {
            return Ok(results);
        }

        for m in self.matches.iter() {
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
        self.execute_operations(py)?;

        Ok(self.matches.len())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        &mut self,
        pattern: Option<String>,
        node: Option<String>,
        edge: Option<String>,
        r#type: Option<Py<PyAny>>,
        type_schema: Option<Py<PyAny>>,
        term: Option<Py<PyAny>>,
        term_schema: Option<Py<PyAny>>,
        start: Option<Py<PyAny>>,
        end: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        if let Some(p) = pattern {
            let path = PathPattern::parse(p)?;
            self.operations
                .push(QueryOperation::Create(CreateOp::Path(path)));
        } else if node.is_some() {
            Python::attach(|py| -> PyResult<()> {
                // Convert Python types to Rust types
                let type_obj = if let Some(t) = r#type {
                    Some(python_to_type(t.bind(py))?)
                } else {
                    None
                };

                let type_schema = if let Some(schema) = type_schema {
                    Some(schema.bind(py).extract::<TypeSchema>()?)
                } else {
                    None
                };

                let term_obj = if let Some(t) = term {
                    Some(python_to_term(t.bind(py))?)
                } else {
                    None
                };

                let term_schema = if let Some(schema) = term_schema {
                    Some(schema.bind(py).extract::<TermSchema>()?)
                } else {
                    None
                };

                let properties_map = if let Some(props) = properties {
                    Some(props.bind(py).extract::<HashMap<String, Py<PyAny>>>()?)
                } else {
                    None
                };

                let node_pattern = NodePattern::new(
                    node,
                    type_obj.map(Arc::new),
                    type_schema,
                    term_obj.map(Arc::new),
                    term_schema,
                    properties_map,
                )?;
                self.operations
                    .push(QueryOperation::Create(CreateOp::Node(node_pattern)));
                Ok(())
            })?;
        } else if edge.is_some() {
            Python::attach(|py| -> PyResult<()> {
                // Convert Python types to Rust types
                let type_obj = if let Some(t) = r#type {
                    Some(Arc::new(python_to_type(t.bind(py))?))
                } else {
                    None
                };

                let type_schema = if let Some(schema) = type_schema {
                    Some(schema.bind(py).extract::<TypeSchema>()?)
                } else {
                    None
                };

                let term_obj = if let Some(t) = term {
                    Some(Arc::new(python_to_term(t.bind(py))?))
                } else {
                    None
                };

                let term_schema = if let Some(schema) = term_schema {
                    Some(schema.bind(py).extract::<TermSchema>()?)
                } else {
                    None
                };

                let properties_map = if let Some(props) = properties {
                    Some(props.bind(py).extract::<HashMap<String, Py<PyAny>>>()?)
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
                let start_var = Self::extract_var(start)?;
                let end_var = Self::extract_var(end)?;
                self.operations.push(QueryOperation::Create(CreateOp::Edge(
                    edge_pattern,
                    start_var,
                    end_var,
                )));
                Ok(())
            })?;
        }

        Ok(self.clone())
    }

    pub fn set(
        &mut self,
        variable: String,
        properties: Py<PyDict>,
        overwrite: bool,
    ) -> PyResult<Self> {
        let mut props = HashMap::new();
        Python::attach(|py| -> PyResult<()> {
            for (k, v) in properties.bind(py) {
                let key = k.extract::<String>()?;
                let val = v.unbind();
                props.insert(key, val);
            }
            Ok(())
        })?;

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
        self.execute_operations(py)?;
        Ok(self.clone())
    }
}

impl Query {
    #[allow(unused_variables)]
    fn extract_var(obj: Option<Py<PyAny>>) -> Result<String, ImplicaError> {
        Python::attach(|py| {
            if let Some(o) = obj {
                if let Ok(s) = o.bind(py).extract::<String>() {
                    Ok(s)
                } else {
                    Err(ImplicaError::InvalidQuery {
                        message: "Expected string variable name".to_string(),
                        context: Some("variable extraction".to_string()),
                    })
                }
            } else {
                Err(ImplicaError::InvalidQuery {
                    message: "variable name required".to_string(),
                    context: Some("extract_var".to_string()),
                })
            }
        })
    }

    fn extract_var_or_none(obj: Option<Py<PyAny>>) -> Result<Option<String>, ImplicaError> {
        Python::attach(|py| {
            if let Some(o) = obj {
                if let Ok(s) = o.bind(py).extract::<String>() {
                    Ok(Some(s))
                } else if let Ok(_node) = o.bind(py).extract::<Node>() {
                    Ok(None) // Node object provided
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        })
    }

    fn execute_operations(&mut self, py: Python) -> PyResult<()> {
        for op in self.operations.clone() {
            match op {
                QueryOperation::Match(match_op) => {
                    self.execute_match(match_op)?;
                }
                QueryOperation::Create(create_op) => {
                    self.execute_create(create_op)?;
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

    fn execute_match(&mut self, match_op: MatchOp) -> PyResult<()> {
        match match_op {
            MatchOp::Node(node_pattern) => {
                let mut new_matches = Vec::new();

                let nodes = self
                    .graph
                    .nodes
                    .read()
                    .map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("execute match node".to_string()),
                    })?;

                for node_lock in nodes.values() {
                    let node = node_lock.read().map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("execute match node".to_string()),
                    })?;

                    if node_pattern.matches(&node, self.context.clone())? {
                        new_matches.push(node.clone());
                    }
                }

                if let Some(ref var) = node_pattern.variable {
                    if self.matches.is_empty() {
                        for m in new_matches {
                            let dict = HashMap::from([(var.clone(), QueryResult::Node(m))]);
                            self.matches.push(dict);
                        }
                    } else {
                        let mut results = Vec::new();
                        let mut preserved = Vec::new();
                        let mut contained = false;

                        for m in self.matches.iter() {
                            if let Some(old) = m.get(var) {
                                match old {
                                    QueryResult::Node(old_node) => {
                                        for new_node in new_matches.iter() {
                                            if new_node == old_node {
                                                results.push(m.clone());
                                            }
                                        }
                                    }
                                    QueryResult::Edge(old_edge) => {
                                        return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", var),
                                            context: Some("match variable".to_string())
                                        }.into());
                                    }
                                }

                                contained = true;
                            } else {
                                preserved.push(m.clone());
                            }
                        }

                        if contained {
                            results.append(&mut preserved);
                            self.matches = results;
                        } else {
                            for m in new_matches {
                                let dict = HashMap::from([(var.clone(), QueryResult::Node(m))]);
                                self.matches.push(dict);
                            }
                        }
                    }
                }
            }
            MatchOp::Edge(edge_pattern, start_var, end_var) => {
                let mut potential_matches = Vec::new();

                let edges = self
                    .graph
                    .edges
                    .read()
                    .map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("execute match edge".to_string()),
                    })?;

                for edge_lock in edges.values() {
                    let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("execute match edge".to_string()),
                    })?;
                    if edge_pattern.matches(&edge, self.context.clone())? {
                        potential_matches.push(edge.clone());
                    }
                }

                match (start_var, end_var) {
                    (Some(start), Some(end)) => {
                        if self.matches.is_empty() {
                            for m in potential_matches {
                                let mut dict = HashMap::from([
                                    (
                                        start.clone(),
                                        QueryResult::Node(
                                            (*m.start.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?)
                                            .clone(),
                                        ),
                                    ),
                                    (
                                        end.clone(),
                                        QueryResult::Node(
                                            (*m.end.read().map_err(|e| {
                                                ImplicaError::LockError {
                                                    rw: "read".to_string(),
                                                    message: e.to_string(),
                                                    context: Some("execute match edge".to_string()),
                                                }
                                            })?)
                                            .clone(),
                                        ),
                                    ),
                                ]);
                                if let Some(ref var) = edge_pattern.variable {
                                    dict.insert(var.clone(), QueryResult::Edge(m));
                                }

                                self.matches.push(dict);
                            }
                        } else {
                            let mut results = Vec::new();
                            let mut contained = false;

                            if let Some(ref var) = edge_pattern.variable {
                                for m in self.matches.iter() {
                                    match (m.get(var), m.get(&start), m.get(&end)) {
                                        (Some(old_var), Some(old_start), Some(old_end)) => {
                                            match old_var {
                                                QueryResult::Edge(old_var_edge) => {
                                                    match old_start {
                                                        QueryResult::Node(old_start_node) => {
                                                            match old_end {
                                                                QueryResult::Node(old_end_node) => {
                                                                    for new in
                                                                        potential_matches.iter()
                                                                    {
                                                                        let new_start = new
                                                                            .start
                                                                            .read()
                                                                            .map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                        let new_end =
                                                                            new.end.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                        if (new == old_var_edge)
                                                                            & (&*new_start
                                                                                == old_start_node)
                                                                            & (&*new_end
                                                                                == old_end_node)
                                                                        {
                                                                            results.push(m.clone());
                                                                        }
                                                                    }
                                                                }
                                                                QueryResult::Edge(old_end_edge) => {
                                                                    return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            }.into());
                                                                }
                                                            }
                                                        }
                                                        QueryResult::Edge(old_start_node) => {
                                                            return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                                context: Some("match variable".to_string())
                                                            }.into());
                                                        }
                                                    }
                                                }
                                                QueryResult::Node(old_var_node) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }
                                            contained = true;
                                        }
                                        (Some(old_var), Some(old_start), None) => {
                                            match old_var {
                                                QueryResult::Edge(old_var_edge) => {
                                                    match old_start {
                                                        QueryResult::Node(old_start_node) => {
                                                            for new in potential_matches.iter() {
                                                                let new_start =
                                                                    new.start.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                if (new == old_var_edge)
                                                                    & (&*new_start
                                                                        == old_start_node)
                                                                {
                                                                    results.push(m.clone());
                                                                }
                                                            }
                                                        }
                                                        QueryResult::Edge(old_start_) => {
                                                            return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                                context: Some("match variable".to_string())
                                                            }.into());
                                                        }
                                                    }
                                                }
                                                QueryResult::Node(old_var_node) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }
                                            contained = true;
                                        }
                                        (Some(old_var), None, Some(old_end)) => {
                                            match old_var {
                                                QueryResult::Edge(old_var_edge) => {
                                                    match old_end {
                                                        QueryResult::Node(old_end_node) => {
                                                            for new in potential_matches.iter() {
                                                                let new_end = new.end.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                if (new == old_var_edge)
                                                                    & (&*new_end == old_end_node)
                                                                {
                                                                    results.push(m.clone());
                                                                }
                                                            }
                                                        }
                                                        QueryResult::Edge(old_end_edge) => {
                                                            return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            }.into());
                                                        }
                                                    }
                                                }
                                                QueryResult::Node(old_var_node) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match edge".to_string())
                                                    }.into());
                                                }
                                            }

                                            contained = true;
                                        }
                                        (None, Some(old_start), Some(old_end)) => {
                                            match old_start {
                                                QueryResult::Node(old_start_node) => {
                                                    match old_end {
                                                        QueryResult::Node(old_end_node) => {
                                                            for new in potential_matches.iter() {
                                                                let new_start =
                                                                    new.start.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                let new_end =
                                                                    new.end.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                if (&*new_start == old_start_node)
                                                                    & (&*new_end == old_end_node)
                                                                {
                                                                    let mut dict = m.clone();
                                                                    dict.insert(
                                                                        var.clone(),
                                                                        QueryResult::Edge(
                                                                            new.clone(),
                                                                        ),
                                                                    );
                                                                    results.push(dict);
                                                                }
                                                            }
                                                        }
                                                        QueryResult::Edge(old_end_edge) => {
                                                            return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            }.into());
                                                        }
                                                    }
                                                }
                                                QueryResult::Edge(old_start_edge) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }
                                            contained = true;
                                        }
                                        (Some(old_var), None, None) => {
                                            match old_var {
                                                QueryResult::Edge(old_var_edge) => {
                                                    for new in potential_matches.iter() {
                                                        if new == old_var_edge {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                start.clone(),
                                                                QueryResult::Node(
                                                                    (*new.start.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?)
                                                                        .clone(),
                                                                ),
                                                            );
                                                            dict.insert(
                                                                end.clone(),
                                                                QueryResult::Node(
                                                                    (*new.end.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?)
                                                                        .clone(),
                                                                ),
                                                            );
                                                            results.push(dict);
                                                        }
                                                    }
                                                }
                                                QueryResult::Node(old_var_node) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to a edge", var),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }

                                            contained = true;
                                        }
                                        (None, Some(old_start), None) => {
                                            match old_start {
                                                QueryResult::Node(old_start_node) => {
                                                    for new in potential_matches.iter() {
                                                        let new_start =
                                                            new.start.read().map_err(|e| {
                                                                ImplicaError::LockError {
                                                                    rw: "read".to_string(),
                                                                    message: e.to_string(),
                                                                    context: Some(
                                                                        "execute match edge"
                                                                            .to_string(),
                                                                    ),
                                                                }
                                                            })?;
                                                        if &*new_start == old_start_node {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                var.clone(),
                                                                QueryResult::Edge(new.clone()),
                                                            );
                                                            dict.insert(
                                                                end.clone(),
                                                                QueryResult::Node(
                                                                    (*new.end.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?)
                                                                        .clone(),
                                                                ),
                                                            );
                                                            results.push(dict);
                                                        }
                                                    }
                                                }
                                                QueryResult::Edge(old_start_edge) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }

                                            contained = true;
                                        }
                                        (None, None, Some(old_end)) => {
                                            match old_end {
                                                QueryResult::Node(old_end_node) => {
                                                    for new in potential_matches.iter() {
                                                        let new_end =
                                                            new.end.read().map_err(|e| {
                                                                ImplicaError::LockError {
                                                                    rw: "read".to_string(),
                                                                    message: e.to_string(),
                                                                    context: Some(
                                                                        "execute match edge"
                                                                            .to_string(),
                                                                    ),
                                                                }
                                                            })?;
                                                        if &*new_end == old_end_node {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                var.clone(),
                                                                QueryResult::Edge(new.clone()),
                                                            );
                                                            dict.insert(
                                                                start.clone(),
                                                                QueryResult::Node(
                                                                    (*new.start.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?)
                                                                        .clone(),
                                                                ),
                                                            );
                                                            results.push(dict);
                                                        }
                                                    }
                                                }
                                                QueryResult::Edge(old_end_edge) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }

                                            contained = true;
                                        }
                                        (None, None, None) => (),
                                    }
                                }

                                if contained {
                                    self.matches = results;
                                } else {
                                    // Cartesian product
                                    let mut results = Vec::new();
                                    for m in potential_matches {
                                        for old_match in self.matches.iter() {
                                            let mut dict = old_match.clone();
                                            dict.extend([
                                                (var.clone(), QueryResult::Edge(m.clone())),
                                                (
                                                    start.clone(),
                                                    QueryResult::Node(
                                                        (*m.start.read().map_err(|e| {
                                                            ImplicaError::LockError {
                                                                rw: "read".to_string(),
                                                                message: e.to_string(),
                                                                context: Some(
                                                                    "execute match edge"
                                                                        .to_string(),
                                                                ),
                                                            }
                                                        })?)
                                                        .clone(),
                                                    ),
                                                ),
                                                (
                                                    end.clone(),
                                                    QueryResult::Node(
                                                        (*m.end.read().map_err(|e| {
                                                            ImplicaError::LockError {
                                                                rw: "read".to_string(),
                                                                message: e.to_string(),
                                                                context: Some(
                                                                    "execute match edge"
                                                                        .to_string(),
                                                                ),
                                                            }
                                                        })?)
                                                        .clone(),
                                                    ),
                                                ),
                                            ]);
                                            results.push(dict);
                                        }
                                    }
                                    self.matches = results;
                                }
                            } else {
                                for m in self.matches.iter() {
                                    match (m.get(&start), m.get(&end)) {
                                        (Some(old_start), Some(old_end)) => {
                                            match old_start {
                                                QueryResult::Node(old_start_node) => {
                                                    match old_end {
                                                        QueryResult::Node(old_end_node) => {
                                                            for new in potential_matches.iter() {
                                                                let new_start =
                                                                    new.start.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                let new_end =
                                                                    new.end.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                if (&*new_start == old_start_node)
                                                                    & (&*new_end == old_end_node)
                                                                {
                                                                    results.push(m.clone());
                                                                }
                                                            }
                                                        }
                                                        QueryResult::Edge(old_end_edge) => {
                                                            return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            }.into());
                                                        }
                                                    }
                                                }
                                                QueryResult::Edge(old_start_edge) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }
                                            contained = true;
                                        }
                                        (Some(old_start), None) => {
                                            match old_start {
                                                QueryResult::Node(old_start_node) => {
                                                    for new in potential_matches.iter() {
                                                        let new_start =
                                                            new.start.read().map_err(|e| {
                                                                ImplicaError::LockError {
                                                                    rw: "read".to_string(),
                                                                    message: e.to_string(),
                                                                    context: Some(
                                                                        "execute match edge"
                                                                            .to_string(),
                                                                    ),
                                                                }
                                                            })?;
                                                        if &*new_start == old_start_node {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                end.clone(),
                                                                QueryResult::Node(
                                                                    (*new.end.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?)
                                                                        .clone(),
                                                                ),
                                                            );
                                                            results.push(dict);
                                                        }
                                                    }
                                                }
                                                QueryResult::Edge(old_start_edge) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }
                                            contained = true;
                                        }
                                        (None, Some(old_end)) => {
                                            match old_end {
                                                QueryResult::Node(old_end_node) => {
                                                    for new in potential_matches.iter() {
                                                        let new_end =
                                                            new.end.read().map_err(|e| {
                                                                ImplicaError::LockError {
                                                                    rw: "read".to_string(),
                                                                    message: e.to_string(),
                                                                    context: Some(
                                                                        "execute match edge"
                                                                            .to_string(),
                                                                    ),
                                                                }
                                                            })?;
                                                        if &*new_end == old_end_node {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                            start.clone(),
                                                            QueryResult::Node(
                                                                (*new.start.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?)
                                                                    .clone(),
                                                            ),
                                                        );
                                                            results.push(dict);
                                                        }
                                                    }
                                                }
                                                QueryResult::Edge(old_end_edge) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }
                                        }
                                        (None, None) => (),
                                    }
                                }

                                if contained {
                                    self.matches = results;
                                } else {
                                    // Cartesian Product
                                    let mut results = Vec::new();
                                    for m in potential_matches {
                                        for old_match in self.matches.iter() {
                                            let mut dict = old_match.clone();
                                            dict.extend([
                                                (
                                                    start.clone(),
                                                    QueryResult::Node(
                                                        (*m.start.read().map_err(|e| {
                                                            ImplicaError::LockError {
                                                                rw: "read".to_string(),
                                                                message: e.to_string(),
                                                                context: Some(
                                                                    "execute match edge"
                                                                        .to_string(),
                                                                ),
                                                            }
                                                        })?)
                                                        .clone(),
                                                    ),
                                                ),
                                                (
                                                    end.clone(),
                                                    QueryResult::Node(
                                                        (*m.end.read().map_err(|e| {
                                                            ImplicaError::LockError {
                                                                rw: "read".to_string(),
                                                                message: e.to_string(),
                                                                context: Some(
                                                                    "execute match edge"
                                                                        .to_string(),
                                                                ),
                                                            }
                                                        })?)
                                                        .clone(),
                                                    ),
                                                ),
                                            ]);
                                            results.push(dict);
                                        }
                                    }
                                    self.matches = results;
                                }
                            }
                        }
                    }
                    (Some(start), None) => {
                        if self.matches.is_empty() {
                            for m in potential_matches {
                                let mut dict = HashMap::from([(
                                    start.clone(),
                                    QueryResult::Node(
                                        (*m.start.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?)
                                        .clone(),
                                    ),
                                )]);
                                if let Some(ref var) = edge_pattern.variable {
                                    dict.insert(var.clone(), QueryResult::Edge(m));
                                }

                                self.matches.push(dict);
                            }
                        } else {
                            let mut results = Vec::new();
                            let mut contained = false;

                            if let Some(ref var) = edge_pattern.variable {
                                for m in self.matches.iter() {
                                    match (m.get(var), m.get(&start)) {
                                        (Some(old_var), Some(old_start)) => {
                                            match old_var {
                                                QueryResult::Edge(old_var_edge) => {
                                                    match old_start {
                                                        QueryResult::Node(old_start_node) => {
                                                            for new in potential_matches.iter() {
                                                                let new_start =
                                                                    new.start.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                if (new == old_var_edge)
                                                                    & (&*new_start
                                                                        == old_start_node)
                                                                {
                                                                    results.push(m.clone());
                                                                }
                                                            }
                                                        }
                                                        QueryResult::Edge(old_start_edge) => {
                                                            return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                                context: Some("match variable".to_string())
                                                            }.into());
                                                        }
                                                    }
                                                }
                                                QueryResult::Node(old_var_node) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }
                                            contained = true;
                                        }
                                        (Some(old_var), None) => {
                                            match old_var {
                                                QueryResult::Edge(old_var_edge) => {
                                                    for new in potential_matches.iter() {
                                                        if new == old_var_edge {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                start.clone(),
                                                                QueryResult::Node(
                                                                    (*new.start.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?)
                                                                        .clone(),
                                                                ),
                                                            );
                                                            results.push(dict);
                                                        }
                                                    }
                                                }
                                                QueryResult::Node(old_var_node) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                                context: Some("match variable".to_string())
                                                            }.into());
                                                }
                                            }
                                            contained = true;
                                        }
                                        (None, Some(old_start)) => {
                                            match old_start {
                                                QueryResult::Node(old_start_node) => {
                                                    for new in potential_matches.iter() {
                                                        let new_start =
                                                            new.start.read().map_err(|e| {
                                                                ImplicaError::LockError {
                                                                    rw: "read".to_string(),
                                                                    message: e.to_string(),
                                                                    context: Some(
                                                                        "execute match edge"
                                                                            .to_string(),
                                                                    ),
                                                                }
                                                            })?;
                                                        if &*new_start == old_start_node {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                var.clone(),
                                                                QueryResult::Edge(new.clone()),
                                                            );
                                                        }
                                                    }
                                                }
                                                QueryResult::Edge(old_start_edge) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }
                                            contained = true;
                                        }
                                        (None, None) => (),
                                    }
                                }

                                if contained {
                                    self.matches = results;
                                } else {
                                    // Cartesian product
                                    let mut results = Vec::new();
                                    for m in potential_matches {
                                        for old_match in self.matches.iter() {
                                            let mut dict = old_match.clone();
                                            dict.extend([
                                                (
                                                    start.clone(),
                                                    QueryResult::Node(
                                                        (*m.end.read().map_err(|e| {
                                                            ImplicaError::LockError {
                                                                rw: "read".to_string(),
                                                                message: e.to_string(),
                                                                context: Some(
                                                                    "execute match edge"
                                                                        .to_string(),
                                                                ),
                                                            }
                                                        })?)
                                                        .clone(),
                                                    ),
                                                ),
                                                (var.clone(), QueryResult::Edge(m.clone())),
                                            ]);
                                            results.push(dict);
                                        }
                                    }
                                }
                            } else {
                                for m in self.matches.iter() {
                                    if let Some(old_start) = m.get(&start) {
                                        match old_start {
                                            QueryResult::Node(old_start_node) => {
                                                for new in potential_matches.iter() {
                                                    let new_start =
                                                        new.start.read().map_err(|e| {
                                                            ImplicaError::LockError {
                                                                rw: "read".to_string(),
                                                                message: e.to_string(),
                                                                context: Some(
                                                                    "execute match edge"
                                                                        .to_string(),
                                                                ),
                                                            }
                                                        })?;
                                                    if old_start_node == &*new_start {
                                                        results.push(m.clone());
                                                    }
                                                }
                                            }
                                            QueryResult::Edge(old_start_edge) => {
                                                return Err(ImplicaError::InvalidQuery {
                                                    message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", start),
                                                    context: Some("match variable".to_string())
                                                }.into());
                                            }
                                        }
                                        contained = true;
                                    }
                                }

                                if contained {
                                    self.matches = results;
                                } else {
                                    let mut results = Vec::new();
                                    for m in potential_matches {
                                        for old_match in self.matches.iter() {
                                            let mut dict = old_match.clone();
                                            dict.extend([(
                                                start.clone(),
                                                QueryResult::Node(
                                                    (*m.start.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?)
                                                    .clone(),
                                                ),
                                            )]);
                                            results.push(dict);
                                        }
                                    }
                                    self.matches = results;
                                }
                            }
                        }
                    }
                    (None, Some(end)) => {
                        if self.matches.is_empty() {
                            for m in potential_matches {
                                let mut dict = HashMap::from([(
                                    end.clone(),
                                    QueryResult::Node(
                                        (*m.end.read().map_err(|e| ImplicaError::LockError {
                                            rw: "read".to_string(),
                                            message: e.to_string(),
                                            context: Some("execute match edge".to_string()),
                                        })?)
                                        .clone(),
                                    ),
                                )]);
                                if let Some(ref var) = edge_pattern.variable {
                                    dict.insert(var.clone(), QueryResult::Edge(m));
                                }

                                self.matches.push(dict);
                            }
                        } else {
                            let mut results = Vec::new();
                            let mut contained = false;

                            if let Some(ref var) = edge_pattern.variable {
                                for m in self.matches.iter() {
                                    match (m.get(var), m.get(&end)) {
                                        (Some(old_var), Some(old_end)) => {
                                            match old_var {
                                                QueryResult::Edge(old_var_edge) => {
                                                    match old_end {
                                                        QueryResult::Node(old_end_node) => {
                                                            for new in potential_matches.iter() {
                                                                let new_end = new.end.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?;
                                                                if (new == old_var_edge)
                                                                    & (&*new_end == old_end_node)
                                                                {
                                                                    results.push(m.clone());
                                                                }
                                                            }
                                                        }
                                                        QueryResult::Edge(old_end_edge) => {
                                                            return Err(ImplicaError::InvalidQuery {
                                                                message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                                context: Some("match variable".to_string())
                                                            }.into());
                                                        }
                                                    }
                                                }
                                                QueryResult::Node(old_var_node) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                        context: Some("match edge".to_string())
                                                    }.into());
                                                }
                                            }

                                            contained = true;
                                        }
                                        (Some(old_var), None) => {
                                            match old_var {
                                                QueryResult::Edge(old_var_edge) => {
                                                    for new in potential_matches.iter() {
                                                        if new == old_var_edge {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                end.clone(),
                                                                QueryResult::Node(
                                                                    (*new.end.read().map_err(|e| {
                                                                                ImplicaError::LockError { rw: "read".to_string(), message: e.to_string(), context: Some("execute match edge".to_string()) }
                                                                            })?)
                                                                        .clone(),
                                                                ),
                                                            );
                                                            results.push(dict);
                                                        }
                                                    }
                                                }
                                                QueryResult::Node(old_var_node) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to a node has been assigned to a edge", var),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }

                                            contained = true;
                                        }
                                        (None, Some(old_end)) => {
                                            match old_end {
                                                QueryResult::Node(old_end_node) => {
                                                    for new in potential_matches.iter() {
                                                        let new_end =
                                                            new.end.read().map_err(|e| {
                                                                ImplicaError::LockError {
                                                                    rw: "read".to_string(),
                                                                    message: e.to_string(),
                                                                    context: Some(
                                                                        "execute match edge"
                                                                            .to_string(),
                                                                    ),
                                                                }
                                                            })?;
                                                        if &*new_end == old_end_node {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                var.clone(),
                                                                QueryResult::Edge(new.clone()),
                                                            );
                                                            results.push(dict);
                                                        }
                                                    }
                                                }
                                                QueryResult::Edge(old_end_edge) => {
                                                    return Err(ImplicaError::InvalidQuery {
                                                        message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                        context: Some("match variable".to_string())
                                                    }.into());
                                                }
                                            }

                                            contained = true;
                                        }
                                        (None, None) => (),
                                    }
                                }

                                if contained {
                                    self.matches = results;
                                } else {
                                    let mut results = Vec::new();
                                    for m in potential_matches {
                                        for old_match in self.matches.iter() {
                                            let mut dict = old_match.clone();
                                            dict.extend([
                                                (
                                                    end.clone(),
                                                    QueryResult::Node(
                                                        (*m.end.read().map_err(|e| {
                                                            ImplicaError::LockError {
                                                                rw: "read".to_string(),
                                                                message: e.to_string(),
                                                                context: Some(
                                                                    "execute match edge"
                                                                        .to_string(),
                                                                ),
                                                            }
                                                        })?)
                                                        .clone(),
                                                    ),
                                                ),
                                                (var.clone(), QueryResult::Edge(m.clone())),
                                            ]);
                                            results.push(dict);
                                        }
                                    }
                                    self.matches = results;
                                }
                            } else {
                                for m in self.matches.iter() {
                                    if let Some(old_end) = m.get(&end) {
                                        match old_end {
                                            QueryResult::Node(old_end_node) => {
                                                for new in potential_matches.iter() {
                                                    let new_end = new.end.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?;
                                                    if old_end_node == &*new_end {
                                                        results.push(m.clone());
                                                    }
                                                }
                                            }
                                            QueryResult::Edge(old_end_edge) => {
                                                return Err(ImplicaError::InvalidQuery {
                                                    message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", end),
                                                    context: Some("match variable".to_string())
                                                }.into());
                                            }
                                        }
                                        contained = true;
                                    }
                                }

                                if contained {
                                    self.matches = results;
                                } else {
                                    let mut results = Vec::new();
                                    for m in potential_matches {
                                        for old_match in self.matches.iter() {
                                            let mut dict = old_match.clone();
                                            dict.extend([(
                                                end.clone(),
                                                QueryResult::Node(
                                                    (*m.end.read().map_err(|e| {
                                                        ImplicaError::LockError {
                                                            rw: "read".to_string(),
                                                            message: e.to_string(),
                                                            context: Some(
                                                                "execute match edge".to_string(),
                                                            ),
                                                        }
                                                    })?)
                                                    .clone(),
                                                ),
                                            )]);
                                            results.push(dict);
                                        }
                                    }
                                    self.matches = results;
                                }
                            }
                        }
                    }
                    (None, None) => {
                        if let Some(ref var) = edge_pattern.variable {
                            if self.matches.is_empty() {
                                for m in potential_matches {
                                    let dict = HashMap::from([(var.clone(), QueryResult::Edge(m))]);
                                    self.matches.push(dict);
                                }
                            } else {
                                let mut results = Vec::new();
                                let mut contained = false;

                                for m in self.matches.iter() {
                                    if let Some(old) = m.get(var) {
                                        match old {
                                            QueryResult::Edge(old_edge) => {
                                                for new_edge in potential_matches.iter() {
                                                    if new_edge == old_edge {
                                                        results.push(m.clone());
                                                    }
                                                }
                                            }
                                            QueryResult::Node(old_node) => {
                                                return Err(ImplicaError::InvalidQuery {
                                                    message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                                    context: Some("match edge".to_string())
                                                }.into());
                                            }
                                        }

                                        contained = true;
                                    }
                                }

                                if contained {
                                    self.matches = results;
                                } else {
                                    let mut results = Vec::new();
                                    for m in potential_matches {
                                        for old_match in self.matches.iter() {
                                            let mut dict = old_match.clone();
                                            dict.extend([(
                                                var.clone(),
                                                QueryResult::Edge(m.clone()),
                                            )]);
                                            results.push(dict);
                                        }
                                    }
                                    self.matches = results;
                                }
                            }
                        }
                    }
                }
            }
            MatchOp::Path(mut path) => {
                let mut placeholder_variables = Vec::new();

                for np in path.nodes.iter_mut() {
                    if np.variable.is_none() {
                        let var_name = Uuid::new_v4().to_string();
                        np.variable = Some(var_name.clone());
                        placeholder_variables.push(var_name);
                    }
                }
                for ep in path.edges.iter_mut() {
                    if ep.variable.is_none() {
                        let var_name = Uuid::new_v4().to_string();
                        ep.variable = Some(var_name.clone());
                        placeholder_variables.push(var_name);
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

                for res in self.matches.iter_mut() {
                    for ph in placeholder_variables.iter() {
                        res.remove(ph);
                    }
                }
            }
        }
        Ok(())
    }

    fn execute_create(&mut self, create_op: CreateOp) -> Result<(), ImplicaError> {
        if self.matches.is_empty() {
            self.matches.push(HashMap::new());
        }

        match create_op {
            CreateOp::Node(node_pattern) => {
                for m in self.matches.iter_mut() {
                    if let Some(var) = &node_pattern.variable {
                        if m.contains_key(var) {
                            return Err(ImplicaError::VariableAlreadyExists {
                                name: var.clone(),
                                context: Some("create node".to_string()),
                            });
                        }
                    }

                    let r#type = if let Some(type_obj) = &node_pattern.r#type {
                        type_obj.clone()
                    } else if let Some(type_schema) = &node_pattern.type_schema {
                        Arc::new(type_schema.as_type(self.context.clone())?)
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                            message:
                                "To create a node you must provide either a 'type' or 'type_schema'"
                                    .to_string(),
                            context: Some("create node".to_string()),
                        });
                    };

                    let term = if let Some(term_obj) = &node_pattern.term {
                        Some(term_obj.clone())
                    } else if let Some(term_schema) = &node_pattern.term_schema {
                        Some(Arc::new(term_schema.as_term(self.context.clone())?))
                    } else {
                        None
                    };

                    let mut props = HashMap::new();

                    Python::attach(|py| {
                        for (k, v) in node_pattern.properties.iter() {
                            props.insert(k.clone(), v.clone_ref(py));
                        }
                    });

                    let node = Node::new(
                        r#type,
                        term.map(|t| Arc::new(RwLock::new((*t).clone()))),
                        Some(props),
                    );

                    self.graph.add_node(&node)?;

                    if let Some(var) = &node_pattern.variable {
                        m.insert(var.clone(), QueryResult::Node(node));
                    }
                }
            }
            CreateOp::Edge(edge_pattern, start, end) => {
                for m in self.matches.iter_mut() {
                    if let Some(ref var) = edge_pattern.variable {
                        if m.contains_key(var) {
                            return Err(ImplicaError::VariableAlreadyExists {
                                name: var.clone(),
                                context: Some("create edge".to_string()),
                            });
                        }
                    }

                    let start_node = if let Some(qr) = m.get(&start) {
                        match qr {
                            QueryResult::Node(n) => n.clone(),
                            QueryResult::Edge(_) => {
                                return Err(ImplicaError::InvalidQuery {
                                    message: format!(
                                        "start node identifier '{}' matches as an edge.",
                                        &start
                                    ),
                                    context: Some("create_edge".to_string()),
                                });
                            }
                        }
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                            message: format!(
                                "start node identifier '{}' did not appear in the match.",
                                &start
                            ),
                            context: Some("create edge".to_string()),
                        });
                    };

                    let end_node = if let Some(qr) = m.get(&end) {
                        match qr {
                            QueryResult::Node(n) => n.clone(),
                            QueryResult::Edge(_) => {
                                return Err(ImplicaError::InvalidQuery {
                                    message: format!(
                                        "end node identifier '{}' matches as an edge.",
                                        &start
                                    ),
                                    context: Some("create_edge".to_string()),
                                });
                            }
                        }
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                            message: format!(
                                "end node identifier '{}' did not appear in the match.",
                                &start
                            ),
                            context: Some("create edge".to_string()),
                        });
                    };

                    let term = if let Some(term_obj) = &edge_pattern.term {
                        (**term_obj).clone()
                    } else if let Some(term_schema) = &edge_pattern.term_schema {
                        term_schema.as_term(self.context.clone())?
                    } else {
                        return Err(ImplicaError::InvalidQuery {
                            message: "To create an edge you must provide either a 'term' or 'term_schema'".to_string(),
                            context: Some("create edge".to_string())
                        });
                    };

                    let mut props = HashMap::new();

                    Python::attach(|py| {
                        for (k, v) in edge_pattern.properties.iter() {
                            props.insert(k.clone(), v.clone_ref(py));
                        }
                    });

                    let edge = self.graph.add_edge(
                        Arc::new(term),
                        start_node,
                        end_node,
                        Some(Arc::new(RwLock::new(props))),
                    )?;

                    if let Some(ref var) = edge_pattern.variable {
                        m.insert(var.clone(), QueryResult::Edge(edge));
                    }
                }
            }
            CreateOp::Path(mut path) => {
                if path.edges.len() != path.nodes.len() + 1 {
                    return Err(ImplicaError::InvalidQuery {
                        message: format!(
                            "Expected number of edges {} for {} nodes, actual number of edges {}",
                            path.nodes.len() + 1,
                            path.nodes.len(),
                            path.edges.len()
                        ),
                        context: Some("create path".to_string()),
                    });
                }

                let nodes_len = path.nodes.len();

                let mut placeholder_variables = Vec::new();

                for np in path.nodes.iter_mut() {
                    if np.variable.is_none() {
                        let var_name = Uuid::new_v4().to_string();
                        np.variable = Some(var_name.clone());
                        placeholder_variables.push(var_name);
                    }

                    if let Some(ref type_schema) = np.type_schema {
                        np.r#type = Some(Arc::new(type_schema.as_type(self.context.clone())?));
                        np.type_schema = None;
                    }

                    if let Some(ref term_schema) = np.term_schema {
                        np.term = Some(Arc::new(term_schema.as_term(self.context.clone())?));
                        np.term_schema = None;
                    }

                    if np.r#type.is_none() {
                        if let Some(ref term) = np.term {
                            np.r#type = Some(term.r#type().clone());
                        }
                    }
                }
                for ep in path.edges.iter_mut() {
                    if ep.variable.is_none() {
                        let var_name = Uuid::new_v4().to_string();
                        ep.variable = Some(var_name.clone());
                        placeholder_variables.push(var_name);
                    }

                    if let Some(ref type_schema) = ep.type_schema {
                        ep.r#type = Some(Arc::new(type_schema.as_type(self.context.clone())?));
                        ep.type_schema = None;
                    }

                    if let Some(ref term_schema) = ep.term_schema {
                        ep.r#term = Some(Arc::new(term_schema.as_term(self.context.clone())?));
                        ep.term_schema = None;
                    }

                    if ep.r#type.is_none() {
                        if let Some(ref term) = ep.term {
                            ep.r#type = Some(term.r#type().clone());
                        }
                    }
                }

                for m in self.matches.iter_mut() {
                    for np in path.nodes.iter_mut() {
                        if let Some(ref var) = np.variable {
                            if let Some(qr) = m.get(var) {
                                match qr {
                                    QueryResult::Node(node) => {
                                        np.r#type = Some(node.r#type.clone());
                                        np.term = if let Some(t) = node.term.clone() {
                                            Some(Arc::new(
                                                (t.read().map_err(|e| {
                                                    ImplicaError::LockError {
                                                        rw: "read".to_string(),
                                                        message: e.to_string(),
                                                        context: Some("execute delete".to_string()),
                                                    }
                                                })?)
                                                .clone(),
                                            ))
                                        } else {
                                            None
                                        };
                                    }
                                    QueryResult::Edge(_) => {
                                        return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", var),
                                            context: Some("create path".to_string())
                                        });
                                    }
                                }
                            }
                        }
                    }

                    for ep in path.edges.iter_mut() {
                        if let Some(ref var) = ep.variable {
                            if let Some(qr) = m.get(var) {
                                match qr {
                                    QueryResult::Edge(edge) => {
                                        ep.r#type = Some(edge.term.r#type());
                                        ep.term = Some(edge.term.clone())
                                    }
                                    QueryResult::Node(_) => {
                                        return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to a node has been assigned to an edge", var),
                                            context: Some("create path".to_string())
                                        });
                                    }
                                }
                            }
                        }
                    }

                    let mut queue: Vec<(usize, bool)> = Vec::new();

                    queue.extend(zip(0..nodes_len, vec![true; nodes_len]));
                    queue.extend(zip(0..(nodes_len - 1), vec![false; nodes_len - 1]));

                    // Process the queue
                    while let Some((idx, is_node)) = queue.pop() {
                        if is_node {
                            // First, collect the values we need from other nodes/edges before mutably borrowing
                            let left_edge_type_update = if idx > 0 {
                                if let Some(left_edge) = path.edges.get(idx - 1) {
                                    if let Some(ref edge_type) = left_edge.r#type {
                                        if let Some(arr) = edge_type.as_arrow() {
                                            Some(arr.right.clone())
                                        } else {
                                            return Err(ImplicaError::InvalidQuery {
                                                message:
                                                    "The type of an edge must be an arrow type."
                                                        .to_string(),
                                                context: Some("create path node".to_string()),
                                            });
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len - 1,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            } else {
                                None
                            };

                            let left_edge_term_update = if idx > 0 {
                                if let Some(left_edge) = path.edges.get(idx - 1) {
                                    if let Some(ref edge_term) = left_edge.term {
                                        if let Some(left_node) = path.nodes.get(idx - 1) {
                                            if let Some(ref left_node_term) = left_node.term {
                                                Some(edge_term.apply(left_node_term)?)
                                            } else {
                                                None
                                            }
                                        } else {
                                            return Err(ImplicaError::IndexOutOfRange {
                                                idx,
                                                length: nodes_len,
                                                context: Some("create path node".to_string()),
                                            });
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len - 1,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            } else {
                                None
                            };

                            let right_edge_type_update = if idx < nodes_len - 1 {
                                if let Some(right_edge) = path.edges.get(idx) {
                                    if let Some(ref edge_type) = right_edge.r#type {
                                        if let Some(arr) = edge_type.as_arrow() {
                                            Some(arr.right.clone())
                                        } else {
                                            return Err(ImplicaError::InvalidQuery {
                                                message:
                                                    "The type of an edge must be an arrow type."
                                                        .to_string(),
                                                context: Some("create path node".to_string()),
                                            });
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len - 1,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            } else {
                                None
                            };

                            let right_edge_term_update = if idx < nodes_len - 1 {
                                if let Some(right_edge) = path.edges.get(idx) {
                                    if let Some(ref edge_term) = right_edge.term {
                                        if let Some(right_node) = path.nodes.get(idx + 1) {
                                            if let Some(ref right_node_term) = right_node.term {
                                                Some(edge_term.apply(right_node_term)?)
                                            } else {
                                                None
                                            }
                                        } else {
                                            return Err(ImplicaError::IndexOutOfRange {
                                                idx,
                                                length: nodes_len,
                                                context: Some("create path node".to_string()),
                                            });
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len - 1,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            } else {
                                None
                            };

                            // Now we can safely borrow the node mutably
                            if let Some(node) = path.nodes.get_mut(idx) {
                                let mut changed = false;

                                if idx > 0 {
                                    // Apply type update
                                    if node.r#type.is_none() {
                                        if let Some(type_result) = left_edge_type_update {
                                            node.r#type = Some(type_result);
                                            changed = true;
                                        }
                                    }

                                    // Apply term update
                                    if node.term.is_none() {
                                        if let Some(term_result) = left_edge_term_update {
                                            node.term = Some(Arc::new(term_result));
                                            changed = true;
                                        }
                                    }
                                }

                                if idx < nodes_len - 1 {
                                    if node.r#type.is_none() {
                                        if let Some(type_result) = right_edge_type_update {
                                            node.r#type = Some(type_result);
                                            changed = true;
                                        }
                                    }

                                    if node.term.is_none() {
                                        if let Some(term_result) = right_edge_term_update {
                                            node.term = Some(Arc::new(term_result));
                                            changed = true;
                                        }
                                    }
                                }

                                if changed {
                                    queue.extend([(idx - 1, false), (idx, false)]);
                                }
                            } else {
                                return Err(ImplicaError::IndexOutOfRange {
                                    idx,
                                    length: nodes_len,
                                    context: Some("create path node".to_string()),
                                });
                            }
                        } else {
                            let left_node = match path.nodes.get(idx) {
                                Some(n) => n,
                                None => {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            };

                            let right_node = match path.nodes.get(idx + 1) {
                                Some(n) => n,
                                None => {
                                    return Err(ImplicaError::IndexOutOfRange {
                                        idx,
                                        length: nodes_len,
                                        context: Some("create path node".to_string()),
                                    });
                                }
                            };

                            let type_update = match (&left_node.r#type, &right_node.r#type) {
                                (Some(left_type), Some(right_type)) => Some(Type::Arrow(
                                    Arrow::new(left_type.clone(), right_type.clone()),
                                )),
                                _ => None,
                            };

                            let term_update = match (&left_node.term, &right_node.term) {
                                (Some(left_term), Some(right_term)) => {
                                    if let Some(right_term) = right_term.as_application() {
                                        if left_term.as_ref() == right_term.argument.as_ref() {
                                            Some(right_term.function.clone())
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            };

                            if let Some(edge) = path.edges.get_mut(idx) {
                                let mut changed = false;
                                if edge.r#type.is_none() {
                                    edge.r#type = type_update.map(Arc::new);
                                    changed = true;
                                }
                                if edge.term.is_none() {
                                    edge.term = term_update;
                                    changed = true;
                                }

                                if changed {
                                    queue.extend([(idx, true), (idx + 1, true)]);
                                }
                            } else {
                                return Err(ImplicaError::IndexOutOfRange {
                                    idx,
                                    length: nodes_len,
                                    context: Some("create path edge".to_string()),
                                });
                            }
                        }
                    }

                    let mut nodes = Vec::new();

                    for np in path.nodes.iter() {
                        if let Some(ref var) = np.variable {
                            if let Some(qr) = m.get(var) {
                                match qr {
                                    QueryResult::Node(n) => {
                                        nodes.push(n.clone());
                                    }
                                    QueryResult::Edge(_) => {
                                        return Err(ImplicaError::InvalidQuery {
                                            message: format!("Variable '{}' previously assigned to an edge has been assigned to a node", var),
                                            context: Some("create path".to_string())
                                        });
                                    }
                                }

                                continue;
                            }
                        }

                        let r#type = match &np.r#type {
                            Some(t) => t.clone(),
                            None => {
                                return Err(ImplicaError::InvalidQuery {
                                    message: "could not resolve the type of a node from the provided pattern".to_string(),
                                    context: Some("create path".to_string())
                                });
                            }
                        };
                        let term = np.term.clone().map(|t| Arc::new(RwLock::new((*t).clone())));

                        let mut props = HashMap::new();

                        Python::attach(|py| {
                            for (k, v) in np.properties.iter() {
                                props.insert(k.clone(), v.clone_ref(py));
                            }
                        });

                        let mut node = Node::new(r#type, term, Some(props));

                        match self.graph.add_node(&node) {
                            Ok(()) => (),
                            Err(e) => match e {
                                ImplicaError::NodeAlreadyExists {
                                    message: _,
                                    existing,
                                    new: _,
                                } => node = existing.clone(),
                                _ => {
                                    return Err(e);
                                }
                            },
                        }

                        if let Some(ref var) = np.variable {
                            m.insert(var.clone(), QueryResult::Node(node.clone()));
                            nodes.push(node);
                        }
                    }

                    for (idx, ep) in path.edges.iter().enumerate() {
                        if let Some(ref var) = ep.variable {
                            if m.contains_key(var) {
                                continue;
                            }
                        }

                        let term = match &ep.term {
                            Some(t) => t.clone(),
                            None => {
                                return Err(ImplicaError::InvalidQuery {
                                    message: "could not resolve the term of an edge from the provided pattern".to_string(),
                                    context: Some("create path".to_string())
                                });
                            }
                        };

                        let mut props = HashMap::new();

                        Python::attach(|py| {
                            for (k, v) in ep.properties.iter() {
                                props.insert(k.clone(), v.clone_ref(py));
                            }
                        });

                        let start = match nodes.get(idx) {
                            Some(n) => n.clone(),
                            None => {
                                return Err(ImplicaError::IndexOutOfRange {
                                    idx,
                                    length: nodes_len,
                                    context: Some("create path".to_string()),
                                });
                            }
                        };

                        let end = match nodes.get(idx + 1) {
                            Some(n) => n.clone(),
                            None => {
                                return Err(ImplicaError::IndexOutOfRange {
                                    idx: idx + 1,
                                    length: nodes_len,
                                    context: Some("create path".to_string()),
                                });
                            }
                        };

                        let edge = self.graph.add_edge(
                            term,
                            start,
                            end,
                            Some(Arc::new(RwLock::new(props))),
                        )?;

                        if let Some(ref var) = ep.variable {
                            m.insert(var.clone(), QueryResult::Edge(edge));
                        }
                    }

                    for ph in placeholder_variables.iter() {
                        m.remove(ph);
                    }
                }
            }
        }
        Ok(())
    }

    fn execute_delete(&mut self, vars: Vec<String>) -> Result<(), ImplicaError> {
        for m in self.matches.iter_mut() {
            for var in vars.iter() {
                if let Some(qr) = m.remove(var) {
                    match qr {
                        QueryResult::Node(n) => {
                            self.graph.remove_node(n.uid())?;
                        }
                        QueryResult::Edge(e) => {
                            self.graph.remove_edge(e.uid())?;
                        }
                    }
                } else {
                    return Err(ImplicaError::VariableNotFound {
                        name: var.clone(),
                        context: Some("delete".to_string()),
                    });
                }
            }
        }

        Ok(())
    }

    fn execute_set(
        &mut self,
        var: String,
        props: HashMap<String, Py<PyAny>>,
        overwrite: bool,
    ) -> Result<(), ImplicaError> {
        for m in self.matches.iter() {
            if let Some(qr) = m.get(&var) {
                match qr {
                    QueryResult::Node(n) => {
                        let nodes =
                            self.graph
                                .nodes
                                .read()
                                .map_err(|e| ImplicaError::LockError {
                                    rw: "read".to_string(),
                                    message: e.to_string(),
                                    context: Some("execute set".to_string()),
                                })?;
                        if let Some(node_lock) = nodes.get(n.uid()) {
                            let node = node_lock.read().map_err(|e| ImplicaError::LockError {
                                rw: "read".to_string(),
                                message: e.to_string(),
                                context: Some("execute set".to_string()),
                            })?;
                            let mut node_props =
                                node.properties
                                    .write()
                                    .map_err(|e| ImplicaError::LockError {
                                        rw: "write".to_string(),
                                        message: e.to_string(),
                                        context: Some("execute set".to_string()),
                                    })?;

                            if overwrite {
                                node_props.clear();
                            }

                            Python::attach(|py| {
                                for (k, v) in props.iter() {
                                    node_props.insert(k.clone(), v.clone_ref(py));
                                }
                            })
                        } else {
                            return Err(ImplicaError::NodeNotFound {
                                uid: n.uid().to_string(),
                                context: Some("execute set node".to_string()),
                            });
                        }
                    }
                    QueryResult::Edge(e) => {
                        let edges =
                            self.graph
                                .nodes
                                .read()
                                .map_err(|e| ImplicaError::LockError {
                                    rw: "read".to_string(),
                                    message: e.to_string(),
                                    context: Some("execute set".to_string()),
                                })?;
                        if let Some(edge_lock) = edges.get(e.uid()) {
                            let edge = edge_lock.read().map_err(|e| ImplicaError::LockError {
                                rw: "read".to_string(),
                                message: e.to_string(),
                                context: Some("execute set".to_string()),
                            })?;
                            let mut edge_props =
                                edge.properties
                                    .write()
                                    .map_err(|e| ImplicaError::LockError {
                                        rw: "read".to_string(),
                                        message: e.to_string(),
                                        context: Some("execute set".to_string()),
                                    })?;

                            if overwrite {
                                edge_props.clear();
                            }

                            Python::attach(|py| {
                                for (k, v) in props.iter() {
                                    edge_props.insert(k.clone(), v.clone_ref(py));
                                }
                            });
                        } else {
                            return Err(ImplicaError::EdgeNotFound {
                                uid: e.uid().to_string(),
                                context: Some("execute set edge".to_string()),
                            });
                        }
                    }
                }
            } else {
                return Err(ImplicaError::VariableNotFound {
                    name: var.clone(),
                    context: Some("delete".to_string()),
                });
            }
        }

        Ok(())
    }

    fn execute_where(&mut self, condition: String) -> Result<(), ImplicaError> {
        let mut results = Vec::new();

        let evaluator = Evaluator::new()?;

        for m in self.matches.iter() {
            let mut scope = Scope::new();
            for (var, qr) in m.iter() {
                let props = match qr {
                    QueryResult::Node(n) => {
                        n.properties.read().map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some("execute where".to_string()),
                        })?
                    }
                    QueryResult::Edge(e) => {
                        e.properties.read().map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some("execute where".to_string()),
                        })?
                    }
                };

                let map = props_as_map(&props)?;
                scope.push(var.clone(), map);
            }

            if evaluator.eval(&mut scope, &condition)? {
                results.push(m.clone());
            }
        }

        self.matches = results;

        Ok(())
    }

    fn execute_with(&mut self, vars: Vec<String>) -> Result<(), ImplicaError> {
        for m in self.matches.iter_mut() {
            let mut dict = HashMap::new();

            for v in vars.iter() {
                match m.get(v) {
                    Some(qr) => {
                        dict.insert(v.clone(), qr.clone());
                    }
                    None => {
                        return Err(ImplicaError::VariableNotFound {
                            name: v.clone(),
                            context: Some("with".to_string()),
                        });
                    }
                }
            }

            *m = dict;
        }

        Ok(())
    }

    fn execute_order_by(&mut self, vars: Vec<String>, ascending: bool) -> Result<(), ImplicaError> {
        let mut props: Vec<(String, String)> = Vec::new();
        for var in &vars {
            let parts: Vec<&str> = var.split(".").collect();

            if parts.len() != 2 {
                return Err(ImplicaError::InvalidQuery {
                    message: format!("Invalid variable provided: {}", var),
                    context: Some("order by".to_string()),
                });
            }

            props.push((parts[0].to_string(), parts[1].to_string()));
        }

        Python::attach(|py| {
            self.matches.sort_by(|a, b| {
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

    fn execute_limit(&mut self, count: usize) -> PyResult<()> {
        // Limit the number of results for each variable
        self.matches.truncate(count);
        Ok(())
    }

    fn execute_skip(&mut self, count: usize) -> PyResult<()> {
        // Skip the first N results for each variable
        if count < self.matches.len() {
            self.matches.drain(0..count);
        } else {
            self.matches.clear();
        }
        Ok(())
    }
}
