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
use pyo3::prelude::*;
use pyo3::types::PyDict;
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
    OrderBy(String, String, bool),
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
            QueryOperation::OrderBy(v, k, asc) => {
                QueryOperation::OrderBy(v.clone(), k.clone(), *asc)
            }
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

    #[pyo3(signature = (variable, key, ascending=true))]
    pub fn order_by(&mut self, variable: String, key: String, ascending: bool) -> PyResult<Self> {
        self.operations
            .push(QueryOperation::OrderBy(variable, key, ascending));
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
    fn extract_var(obj: Option<Py<PyAny>>) -> PyResult<String> {
        Python::attach(|py| {
            if let Some(o) = obj {
                if let Ok(s) = o.bind(py).extract::<String>() {
                    Ok(s)
                } else {
                    Err(ImplicaError::InvalidQuery {
                        message: "Expected string variable name".to_string(),
                        context: Some("variable extraction".to_string()),
                    }
                    .into())
                }
            } else {
                Err(ImplicaError::invalid_query("Variable name required").into())
            }
        })
    }

    fn extract_var_or_none(obj: Option<Py<PyAny>>) -> PyResult<Option<String>> {
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
                    todo!("Implement this!");
                    //self.execute_where(py, condition)?;
                }
                QueryOperation::With(vars) => {
                    todo!("Implement this!");
                    //self.execute_with(vars)?;
                }
                QueryOperation::OrderBy(var, key, ascending) => {
                    todo!("Implement this!");
                    //self.execute_order_by(py, var, key, ascending)?;
                }
                QueryOperation::Limit(count) => {
                    todo!("Implement this!");
                    //self.execute_limit(count)?;
                }
                QueryOperation::Skip(count) => {
                    todo!("Implement this!");
                    //self.execute_skip(count)?;
                }
            }
        }
        Ok(())
    }

    fn execute_match(&mut self, match_op: MatchOp) -> PyResult<()> {
        match match_op {
            MatchOp::Node(node_pattern) => {
                let mut new_matches = Vec::new();

                for node_lock in self.graph.nodes.read().unwrap().values() {
                    let node = node_lock.read().unwrap();

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

                for edge_lock in self.graph.edges.read().unwrap().values() {
                    let edge = edge_lock.read().unwrap();
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
                                        QueryResult::Node((*m.start.read().unwrap()).clone()),
                                    ),
                                    (
                                        end.clone(),
                                        QueryResult::Node((*m.end.read().unwrap()).clone()),
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
                                                                            .unwrap();
                                                                        let new_end =
                                                                            new.end.read().unwrap();
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
                                                                    new.start.read().unwrap();
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
                                                QueryResult::Edge(old_var_edge) => match old_end {
                                                    QueryResult::Node(old_end_node) => {
                                                        for new in potential_matches.iter() {
                                                            let new_end = new.end.read().unwrap();
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
                                                },
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
                                                                    new.start.read().unwrap();
                                                                let new_end =
                                                                    new.end.read().unwrap();
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
                                                                    (*new.start.read().unwrap())
                                                                        .clone(),
                                                                ),
                                                            );
                                                            dict.insert(
                                                                end.clone(),
                                                                QueryResult::Node(
                                                                    (*new.end.read().unwrap())
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
                                                        let new_start = new.start.read().unwrap();
                                                        if &*new_start == old_start_node {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                var.clone(),
                                                                QueryResult::Edge(new.clone()),
                                                            );
                                                            dict.insert(
                                                                end.clone(),
                                                                QueryResult::Node(
                                                                    (*new.end.read().unwrap())
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
                                                        let new_end = new.end.read().unwrap();
                                                        if &*new_end == old_end_node {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                var.clone(),
                                                                QueryResult::Edge(new.clone()),
                                                            );
                                                            dict.insert(
                                                                start.clone(),
                                                                QueryResult::Node(
                                                                    (*new.start.read().unwrap())
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
                                                        (*m.start.read().unwrap()).clone(),
                                                    ),
                                                ),
                                                (
                                                    end.clone(),
                                                    QueryResult::Node(
                                                        (*m.end.read().unwrap()).clone(),
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
                                                                    new.start.read().unwrap();
                                                                let new_end =
                                                                    new.end.read().unwrap();
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
                                                        let new_start = new.start.read().unwrap();
                                                        if &*new_start == old_start_node {
                                                            let mut dict = m.clone();
                                                            dict.insert(
                                                                end.clone(),
                                                                QueryResult::Node(
                                                                    (*new.end.read().unwrap())
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
                                        (None, Some(old_end)) => match old_end {
                                            QueryResult::Node(old_end_node) => {
                                                for new in potential_matches.iter() {
                                                    let new_end = new.end.read().unwrap();
                                                    if &*new_end == old_end_node {
                                                        let mut dict = m.clone();
                                                        dict.insert(
                                                            start.clone(),
                                                            QueryResult::Node(
                                                                (*new.start.read().unwrap())
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
                                        },
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
                                                        (*m.start.read().unwrap()).clone(),
                                                    ),
                                                ),
                                                (
                                                    end.clone(),
                                                    QueryResult::Node(
                                                        (*m.end.read().unwrap()).clone(),
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
                                    QueryResult::Node((*m.start.read().unwrap()).clone()),
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
                                                                    new.start.read().unwrap();
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
                                                                    (*new.start.read().unwrap())
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
                                                        let new_start = new.start.read().unwrap();
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
                                                        (*m.end.read().unwrap()).clone(),
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
                                                    let new_start = new.start.read().unwrap();
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
                                                    (*m.start.read().unwrap()).clone(),
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
                                    QueryResult::Node((*m.end.read().unwrap()).clone()),
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
                                                QueryResult::Edge(old_var_edge) => match old_end {
                                                    QueryResult::Node(old_end_node) => {
                                                        for new in potential_matches.iter() {
                                                            let new_end = new.end.read().unwrap();
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
                                                },
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
                                                                    (*new.end.read().unwrap())
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
                                                        let new_end = new.end.read().unwrap();
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
                                                        (*m.end.read().unwrap()).clone(),
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
                                                    let new_end = new.end.read().unwrap();
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
                                                QueryResult::Node((*m.end.read().unwrap()).clone()),
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
                                        np.term = node
                                            .term
                                            .clone()
                                            .map(|t| Arc::new(t.read().unwrap().clone()));
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
                            self.graph.remove_node(&n.uid())?;
                        }
                        QueryResult::Edge(e) => {
                            self.graph.remove_edge(&e.uid())?;
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
                        let nodes = self.graph.nodes.read().unwrap();
                        if let Some(node_lock) = nodes.get(&n.uid()) {
                            let node = node_lock.read().unwrap();
                            let mut node_props = node.properties.write().unwrap();

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
                                uid: n.uid(),
                                context: Some("execute set node".to_string()),
                            });
                        }
                    }
                    QueryResult::Edge(e) => {
                        let edges = self.graph.nodes.read().unwrap();
                        if let Some(edge_lock) = edges.get(&e.uid()) {
                            let edge = edge_lock.read().unwrap();
                            let mut edge_props = edge.properties.write().unwrap();

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
                                uid: e.uid(),
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

    /*  TODO: Finish this

    fn execute_set(&mut self, py: Python, var: String, props: Py<PyDict>) -> PyResult<()> {
        // Set properties on matched nodes and edges
        if let Some(results) = self.matched_vars.get_mut(&var) {
            // Clone results to avoid borrow issues during re-indexing
            let results_clone = results.clone();

            for result in results_clone {
                match result {
                    QueryResult::Node(node) => {
                        // Update node properties by merging new props into existing
                        let mut node_props = node.properties.write().unwrap();
                        let new_props = props.bind(py).extract::<HashMap<String, Py<PyAny>>>()?;
                        for (key, value) in new_props.iter() {
                            node_props.insert(key.clone(), value.clone_ref(py));
                        }
                    }
                    QueryResult::Edge(edge) => {
                        // Update edge properties by merging new props into existing
                        let mut edge_props = edge.properties.write().unwrap();
                        let new_props = props.bind(py).extract::<HashMap<String, Py<PyAny>>>()?;
                        for (key, value) in new_props.iter() {
                            edge_props.insert(key.clone(), value.clone_ref(py));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn execute_set_term(&mut self, py: Python, var: String, term: Term) -> PyResult<()> {
        if let Some(results) = self.matched_vars.get_mut(&var) {
            let results_clone = results.clone();

            for result in results_clone {
                match result {
                    QueryResult::Node(node) => {
                        // Check node term type matches new term type
                        if node.r#type != term.r#type {
                            return Err(ImplicaError::InvalidQuery {
                                message: format!(
                                    "Cannot set term: node term type '{}' does not match new term type '{}'",
                                    node.r#type, term.r#type
                                ),
                                context: Some("set term".to_string()),
                            }.into());
                        }

                        // Update node term
                        match node.term {
                            Some(term_lock) => {
                                let mut node_term = term_lock.write().unwrap();
                                *node_term = term.clone();
                            }
                            None => {
                                node.term = Some(Arc::new(RwLock::new(term.clone())));
                            }
                        }
                    }
                    QueryResult::Edge(edge) => {}
                }
            }
        }
        Ok(())
    }

    fn execute_where(&mut self, py: Python, condition: String) -> PyResult<()> {
        // Parse and evaluate WHERE conditions with logical expressions
        // Supports: AND, OR, NOT, and parentheses
        // Examples: "n.age > 25 AND n.name = 'Alice'"
        //           "n.age < 18 OR n.age > 65"
        //           "NOT n.active = true"
        //           "(n.age > 20 AND n.age < 30) OR n.status = 'VIP'"

        let condition = condition.trim().to_string();

        // Collect all variables referenced in the condition
        let var_names = Self::extract_variables_from_condition(&condition);

        // For each variable, filter its results
        for var_name in var_names {
            if let Some(results) = self.matched_vars.get_mut(&var_name) {
                let cond_clone = condition.clone();
                let var_clone = var_name.clone();
                results.retain(|result| {
                    if let QueryResult::Node(node) = result {
                        Self::evaluate_logical_expression(py, &cond_clone, &var_clone, node)
                    } else {
                        false
                    }
                });
            }
        }

        Ok(())
    }

    /// Extract all variable names referenced in a condition string
    fn extract_variables_from_condition(condition: &str) -> Vec<String> {
        let mut vars = std::collections::HashSet::new();
        let mut current = String::new();
        let mut chars = condition.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch.is_alphanumeric() || ch == '_' {
                current.push(ch);
            } else if ch == '.' && !current.is_empty() {
                // This is a variable reference
                vars.insert(current.clone());
                current.clear();
                // Skip the property name
                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_alphanumeric() || next_ch == '_' {
                        chars.next();
                    } else {
                        break;
                    }
                }
            } else {
                current.clear();
            }
        }

        vars.into_iter().collect()
    }

    /// Evaluate a logical expression for a given node
    fn evaluate_logical_expression(py: Python, expr: &str, var_name: &str, node: &Node) -> bool {
        Self::parse_or_expression(py, expr, var_name, node)
    }

    /// Parse OR expression (lowest precedence, right-associative)
    fn parse_or_expression(py: Python, expr: &str, var_name: &str, node: &Node) -> bool {
        let parts = Self::split_by_operator(expr, " OR ");
        if parts.len() > 1 {
            // Right-associative: evaluate from right to left
            let last_idx = parts.len() - 1;
            let left = parts[..last_idx].join(" OR ");
            let right = parts[last_idx];

            let left_result = Self::parse_or_expression(py, &left, var_name, node);
            let right_result = Self::parse_and_expression(py, right, var_name, node);

            return left_result || right_result;
        }

        Self::parse_and_expression(py, expr, var_name, node)
    }

    /// Parse AND expression (higher precedence than OR, right-associative)
    fn parse_and_expression(py: Python, expr: &str, var_name: &str, node: &Node) -> bool {
        let parts = Self::split_by_operator(expr, " AND ");
        if parts.len() > 1 {
            // Right-associative: evaluate from right to left
            let last_idx = parts.len() - 1;
            let left = parts[..last_idx].join(" AND ");
            let right = parts[last_idx];

            let left_result = Self::parse_and_expression(py, &left, var_name, node);
            let right_result = Self::parse_not_expression(py, right, var_name, node);

            return left_result && right_result;
        }

        Self::parse_not_expression(py, expr, var_name, node)
    }

    /// Parse NOT expression (highest precedence)
    fn parse_not_expression(py: Python, expr: &str, var_name: &str, node: &Node) -> bool {
        let expr = expr.trim();

        if let Some(inner) = expr.strip_prefix("NOT ") {
            return !Self::parse_primary_expression(py, inner, var_name, node);
        }

        Self::parse_primary_expression(py, expr, var_name, node)
    }

    /// Parse primary expression (comparison or parenthesized expression)
    fn parse_primary_expression(py: Python, expr: &str, var_name: &str, node: &Node) -> bool {
        let expr = expr.trim();

        // Handle parentheses
        if expr.starts_with('(') && expr.ends_with(')') {
            let inner = &expr[1..expr.len() - 1];
            return Self::evaluate_logical_expression(py, inner, var_name, node);
        }

        // Parse simple comparison: var.property op value
        Self::evaluate_simple_condition(py, expr, var_name, node)
    }

    /// Split expression by operator, respecting parentheses
    fn split_by_operator<'a>(expr: &'a str, op: &str) -> Vec<&'a str> {
        let mut parts = Vec::new();
        let mut current_start = 0;
        let mut paren_depth = 0;
        let mut i = 0;
        let bytes = expr.as_bytes();

        while i < expr.len() {
            if bytes[i] == b'(' {
                paren_depth += 1;
                i += 1;
            } else if bytes[i] == b')' {
                paren_depth -= 1;
                i += 1;
            } else if paren_depth == 0 && i + op.len() <= expr.len() && &expr[i..i + op.len()] == op
            {
                parts.push(&expr[current_start..i]);
                i += op.len();
                current_start = i;
            } else {
                i += 1;
            }
        }

        if current_start < expr.len() {
            parts.push(&expr[current_start..]);
        }

        if parts.is_empty() {
            vec![expr]
        } else {
            parts
        }
    }

    /// Evaluate a simple comparison condition
    ///
    /// Supports:
    /// - Basic comparisons: =, !=, <, >, <=, >=
    /// - IN operator: n.status IN ['active', 'pending']
    /// - String operators: STARTS WITH, CONTAINS, ENDS WITH
    /// - Null checks: IS NULL, IS NOT NULL
    fn evaluate_simple_condition(py: Python, condition: &str, var_name: &str, node: &Node) -> bool {
        let condition = condition.trim();

        // Check for IS NULL / IS NOT NULL
        if condition.contains(" IS NOT NULL") {
            let parts: Vec<&str> = condition.split(" IS NOT NULL").collect();
            if parts.len() == 2 && parts[1].trim().is_empty() {
                let left = parts[0].trim();
                let left_parts: Vec<&str> = left.split('.').collect();
                if left_parts.len() == 2 && left_parts[0] == var_name {
                    let prop_name = left_parts[1];
                    let props = node.properties.read().unwrap();
                    return props.get(prop_name).is_some();
                }
            }
            return false;
        }

        if condition.contains(" IS NULL") {
            let parts: Vec<&str> = condition.split(" IS NULL").collect();
            if parts.len() == 2 && parts[1].trim().is_empty() {
                let left = parts[0].trim();
                let left_parts: Vec<&str> = left.split('.').collect();
                if left_parts.len() == 2 && left_parts[0] == var_name {
                    let prop_name = left_parts[1];
                    let props = node.properties.read().unwrap();
                    return props.get(prop_name).is_none();
                }
            }
            return false;
        }

        // Check for IN operator
        if condition.contains(" IN ") {
            return Self::evaluate_in_condition(py, condition, var_name, node);
        }

        // Check for string operators
        if condition.contains(" STARTS WITH ") {
            return Self::evaluate_string_operator(py, condition, var_name, node, "STARTS WITH");
        }
        if condition.contains(" ENDS WITH ") {
            return Self::evaluate_string_operator(py, condition, var_name, node, "ENDS WITH");
        }
        if condition.contains(" CONTAINS ") {
            return Self::evaluate_string_operator(py, condition, var_name, node, "CONTAINS");
        }

        // Standard comparison operators
        let (left, op, right) = if let Some(pos) = condition.find(">=") {
            (&condition[..pos], ">=", &condition[pos + 2..])
        } else if let Some(pos) = condition.find("<=") {
            (&condition[..pos], "<=", &condition[pos + 2..])
        } else if let Some(pos) = condition.find("!=") {
            (&condition[..pos], "!=", &condition[pos + 2..])
        } else if let Some(pos) = condition.find('=') {
            (&condition[..pos], "=", &condition[pos + 1..])
        } else if let Some(pos) = condition.find('>') {
            (&condition[..pos], ">", &condition[pos + 1..])
        } else if let Some(pos) = condition.find('<') {
            (&condition[..pos], "<", &condition[pos + 1..])
        } else {
            return false;
        };

        let left = left.trim();
        let right = right.trim().trim_matches(|c| c == '\'' || c == '"');

        // Extract variable and property from left side (e.g., "n.age")
        let left_parts: Vec<&str> = left.split('.').collect();
        if left_parts.len() != 2 {
            return false;
        }

        let cond_var_name = left_parts[0];
        if cond_var_name != var_name {
            return false;
        }

        let prop_name = left_parts[1];

        // Get property value from node
        let props = node.properties.read().unwrap();
        let value = match props.get(prop_name) {
            Some(v) => v,
            _ => return false,
        };

        // Evaluate the comparison
        match op {
            "=" => {
                // Try string comparison first
                if let Ok(val_str) = value.extract::<String>(py) {
                    return val_str == right;
                }
                // Try numeric comparison
                if let (Ok(val_num), Ok(right_num)) =
                    (value.extract::<f64>(py), right.parse::<f64>())
                {
                    return (val_num - right_num).abs() < f64::EPSILON;
                }
                // Try boolean comparison
                if let (Ok(val_bool), Ok(right_bool)) =
                    (value.extract::<bool>(py), right.parse::<bool>())
                {
                    return val_bool == right_bool;
                }
            }
            "!=" => {
                if let Ok(val_str) = value.extract::<String>(py) {
                    return val_str != right;
                }
                if let (Ok(val_num), Ok(right_num)) =
                    (value.extract::<f64>(py), right.parse::<f64>())
                {
                    return (val_num - right_num).abs() >= f64::EPSILON;
                }
                if let (Ok(val_bool), Ok(right_bool)) =
                    (value.extract::<bool>(py), right.parse::<bool>())
                {
                    return val_bool != right_bool;
                }
            }
            ">" | ">=" | "<" | "<=" => {
                if let (Ok(val_num), Ok(right_num)) =
                    (value.extract::<f64>(py), right.parse::<f64>())
                {
                    return match op {
                        ">" => val_num > right_num,
                        ">=" => val_num >= right_num,
                        "<" => val_num < right_num,
                        "<=" => val_num <= right_num,
                        _ => false,
                    };
                }
            }
            _ => {}
        }

        false
    }

    /// Evaluate IN operator: n.property IN ['value1', 'value2', 'value3']
    fn evaluate_in_condition(py: Python, condition: &str, var_name: &str, node: &Node) -> bool {
        let parts: Vec<&str> = condition.split(" IN ").collect();
        if parts.len() != 2 {
            return false;
        }

        let left = parts[0].trim();
        let right = parts[1].trim();

        // Extract variable and property from left side
        let left_parts: Vec<&str> = left.split('.').collect();
        if left_parts.len() != 2 || left_parts[0] != var_name {
            return false;
        }

        let prop_name = left_parts[1];

        // Get property value from node
        let props = node.properties.read().unwrap();
        let value = match props.get(prop_name) {
            Some(v) => v,
            _ => return false,
        };

        // Parse the list: ['value1', 'value2'] or ["value1", "value2"]
        let list_str = right.trim_matches(|c| c == '[' || c == ']').trim();
        if list_str.is_empty() {
            return false;
        }

        // Split by comma and check each value
        for item in list_str.split(',') {
            let item = item.trim().trim_matches(|c| c == '\'' || c == '"');

            // Try string comparison
            if let Ok(val_str) = value.extract::<String>(py) {
                if val_str == item {
                    return true;
                }
            }
            // Try numeric comparison
            if let (Ok(val_num), Ok(item_num)) = (value.extract::<f64>(py), item.parse::<f64>()) {
                if (val_num - item_num).abs() < f64::EPSILON {
                    return true;
                }
            }
            // Try boolean comparison
            if let (Ok(val_bool), Ok(item_bool)) = (value.extract::<bool>(py), item.parse::<bool>())
            {
                if val_bool == item_bool {
                    return true;
                }
            }
        }

        false
    }

    /// Evaluate string operators: STARTS WITH, ENDS WITH, CONTAINS
    fn evaluate_string_operator(
        py: Python,
        condition: &str,
        var_name: &str,
        node: &Node,
        operator: &str,
    ) -> bool {
        let sep = format!(" {} ", operator);
        let parts: Vec<&str> = condition.split(&sep).collect();
        if parts.len() != 2 {
            return false;
        }

        let left = parts[0].trim();
        let right = parts[1].trim().trim_matches(|c| c == '\'' || c == '"');

        // Extract variable and property from left side
        let left_parts: Vec<&str> = left.split('.').collect();
        if left_parts.len() != 2 || left_parts[0] != var_name {
            return false;
        }

        let prop_name = left_parts[1];

        // Get property value from node
        let props = node.properties.read().unwrap();
        let value = match props.get(prop_name) {
            Some(v) => v,
            _ => return false,
        };

        // Only works with strings
        if let Ok(val_str) = value.extract::<String>(py) {
            return match operator {
                "STARTS WITH" => val_str.starts_with(right),
                "ENDS WITH" => val_str.ends_with(right),
                "CONTAINS" => val_str.contains(right),
                _ => false,
            };
        }

        false
    }

    fn execute_with(&mut self, vars: Vec<String>) -> PyResult<()> {
        // WITH passes through only the specified variables
        // Remove all other variables from matched_vars
        let keys: Vec<String> = self.matched_vars.keys().cloned().collect();
        for key in keys {
            if !vars.contains(&key) {
                self.matched_vars.remove(&key);
            }
        }
        Ok(())
    }

    fn execute_order_by(
        &mut self,
        py: Python,
        var: String,
        key: String,
        ascending: bool,
    ) -> PyResult<()> {
        // Get matched bindings
        let Some(results) = self.matched_vars.get_mut(&var) else {
            return Ok(());
        };

        // This avoids borrowing from RwLock during sort.
        let mut items: Vec<(Option<Py<PyAny>>, QueryResult)> = results
            .iter()
            .cloned()
            .map(|qr| {
                let k = match &qr {
                    QueryResult::Node(node) => {
                        let props = node.properties.read().unwrap();
                        props.get(&key).map(|v| v.clone_ref(py)) // OWNED COPY ✔
                    }
                    _ => None,
                };
                (k, qr)
            })
            .collect();

        items.sort_by(|(ka, _), (kb, _)| {
            match (ka, kb) {
                (Some(a), Some(b)) => {
                    // Try numeric comparison
                    if let (Ok(na), Ok(nb)) = (a.extract::<f64>(py), b.extract::<f64>(py)) {
                        let ord = na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal);
                        return if ascending { ord } else { ord.reverse() };
                    }

                    // Try string comparison
                    if let (Ok(sa), Ok(sb)) = (a.extract::<String>(py), b.extract::<String>(py)) {
                        let ord = sa.cmp(&sb);
                        return if ascending { ord } else { ord.reverse() };
                    }

                    std::cmp::Ordering::Equal
                }

                // Missing keys: always order after present ones (or before if descending)
                (Some(_), None) => {
                    if ascending {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
                (None, Some(_)) => {
                    if ascending {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                (None, None) => std::cmp::Ordering::Equal,
            }
        });

        *results = items.into_iter().map(|(_, qr)| qr).collect();

        Ok(())
    }

    fn execute_limit(&mut self, count: usize) -> PyResult<()> {
        // Limit the number of results for each variable
        for results in self.matched_vars.values_mut() {
            results.truncate(count);
        }
        Ok(())
    }

    fn execute_skip(&mut self, count: usize) -> PyResult<()> {
        // Skip the first N results for each variable
        for results in self.matched_vars.values_mut() {
            if count < results.len() {
                *results = results.split_off(count);
            } else {
                results.clear();
            }
        }
        Ok(())
    }
    */
}
