#![allow(unused_variables)]

use crate::context::Context;
use crate::errors::ImplicaError;
use crate::graph::{Edge, Graph, Node};
use crate::patterns::{EdgePattern, NodePattern, PathPattern, TermSchema, TypeSchema};
use crate::typing::{python_to_term, python_to_type};

use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::sync::Arc;

// Include implementation modules with explicit paths
#[path = "executors/create.rs"]
mod create;
#[path = "executors/delete.rs"]
mod delete;
#[path = "executors/limit.rs"]
mod limit;
#[path = "executors/match.rs"]
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
}
