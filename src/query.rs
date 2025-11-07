//! Cypher-like query system for graph querying and manipulation.
//!
//! This module provides the `Query` structure for building and executing
//! Cypher-like queries on graphs. It supports pattern matching, creation,
//! deletion, merging, and other graph operations.

#![allow(unused_variables)]

use crate::errors::ImplicaError;
use crate::graph::{Edge, Graph, Node};
use crate::patterns::{EdgePattern, NodePattern, PathPattern};
use crate::types::type_to_python;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

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
    pub matched_vars: HashMap<String, Vec<QueryResult>>,
    pub operations: Vec<QueryOperation>,
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
    Set(String, Py<PyDict>),
    Delete(Vec<String>, bool),
    Merge(MergeOp),
    With(Vec<String>),
    OrderBy(String, String, bool),
    Limit(usize),
    Skip(usize),
}

impl Clone for QueryOperation {
    fn clone(&self) -> Self {
        Python::with_gil(|py| match self {
            QueryOperation::Match(m) => QueryOperation::Match(m.clone()),
            QueryOperation::Where(w) => QueryOperation::Where(w.clone()),
            QueryOperation::Create(c) => QueryOperation::Create(c.clone()),
            QueryOperation::Set(var, dict) => QueryOperation::Set(var.clone(), dict.clone_ref(py)),
            QueryOperation::Delete(vars, detach) => QueryOperation::Delete(vars.clone(), *detach),
            QueryOperation::Merge(m) => QueryOperation::Merge(m.clone()),
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

/// Merge operation types (internal).
///
/// Represents different elements that can be merged (create if not exists).
#[derive(Clone, Debug)]
pub enum MergeOp {
    Node(NodePattern),
    Edge(EdgePattern, String, String),
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
            matched_vars: HashMap::new(),
            operations: Vec::new(),
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
    #[pyo3(signature = (pattern=None, *, node=None, edge=None, start=None, end=None, r#type=None, type_schema=None, term=None, term_type_schema=None, properties=None))]
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
        term_type_schema: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        if let Some(p) = pattern {
            // Parse Cypher-like pattern
            let path = PathPattern::parse(p)?;
            self.operations
                .push(QueryOperation::Match(MatchOp::Path(path)));
        } else if node.is_some() {
            // Match node
            let node_pattern = NodePattern::new(node, r#type, type_schema, properties)?;
            self.operations
                .push(QueryOperation::Match(MatchOp::Node(node_pattern)));
        } else if edge.is_some() {
            // Match edge
            let edge_pattern = EdgePattern::new(
                edge.clone(),
                term,
                term_type_schema,
                properties,
                "forward".to_string(),
            )?;
            let start_var = Self::extract_var_or_none(start)?;
            let end_var = Self::extract_var_or_none(end)?;
            self.operations.push(QueryOperation::Match(MatchOp::Edge(
                edge_pattern,
                start_var,
                end_var,
            )));
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
    pub fn return_(&mut self, py: Python, variables: Vec<String>) -> PyResult<Vec<PyObject>> {
        // Execute all operations to build matched_vars
        self.execute_operations(py)?;

        // Collect results
        let mut results = Vec::new();

        if self.matched_vars.is_empty() {
            return Ok(results);
        }

        // Find maximum length
        let max_len = self
            .matched_vars
            .values()
            .map(|v| v.len())
            .max()
            .unwrap_or(0);

        for i in 0..max_len {
            let dict = PyDict::new(py);
            for var in &variables {
                if let Some(values) = self.matched_vars.get(var) {
                    if i < values.len() {
                        match &values[i] {
                            QueryResult::Node(n) => {
                                dict.set_item(var, Py::new(py, n.clone())?)?;
                            }
                            QueryResult::Edge(e) => {
                                dict.set_item(var, Py::new(py, e.clone())?)?;
                            }
                        }
                    }
                }
            }
            if !dict.is_empty() {
                results.push(dict.into());
            }
        }

        Ok(results)
    }

    pub fn return_count(&mut self, py: Python) -> PyResult<usize> {
        self.execute_operations(py)?;

        if self.matched_vars.is_empty() {
            return Ok(0);
        }

        Ok(self
            .matched_vars
            .values()
            .map(|v| v.len())
            .max()
            .unwrap_or(0))
    }

    #[pyo3(signature = (*variables))]
    pub fn return_distinct(
        &mut self,
        py: Python,
        variables: Vec<String>,
    ) -> PyResult<Vec<PyObject>> {
        // For now, just return regular results (would need proper deduplication)
        self.return_(py, variables)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (pattern=None, *, node=None, edge=None, r#type=None, term=None, start=None, end=None, properties=None))]
    pub fn create(
        &mut self,
        pattern: Option<String>,
        node: Option<String>,
        edge: Option<String>,
        r#type: Option<Py<PyAny>>,
        term: Option<Py<PyAny>>,
        start: Option<Py<PyAny>>,
        end: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        if let Some(p) = pattern {
            let path = PathPattern::parse(p)?;
            self.operations
                .push(QueryOperation::Create(CreateOp::Path(path)));
        } else if node.is_some() {
            let node_pattern = NodePattern::new(node, r#type, None, properties)?;
            self.operations
                .push(QueryOperation::Create(CreateOp::Node(node_pattern)));
        } else if edge.is_some() {
            let edge_pattern =
                EdgePattern::new(edge.clone(), term, None, properties, "forward".to_string())?;
            let start_var = Self::extract_var(start)?;
            let end_var = Self::extract_var(end)?;
            self.operations.push(QueryOperation::Create(CreateOp::Edge(
                edge_pattern,
                start_var,
                end_var,
            )));
        }

        Ok(self.clone())
    }

    pub fn set(&mut self, variable: String, properties: Py<PyDict>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let props_cloned = properties.clone_ref(py);
            self.operations
                .push(QueryOperation::Set(variable, props_cloned));
            Ok(self.clone())
        })
    }

    #[pyo3(signature = (*variables, detach=false))]
    pub fn delete(&mut self, variables: Vec<String>, detach: bool) -> PyResult<Self> {
        self.operations
            .push(QueryOperation::Delete(variables, detach));
        Ok(self.clone())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (pattern=None, *, node=None, edge=None, r#type=None, type_schema=None, term=None, term_type_schema=None, start=None, end=None, properties=None))]
    #[allow(unused_variables)]
    pub fn merge(
        &mut self,
        pattern: Option<String>,
        node: Option<String>,
        edge: Option<String>,
        r#type: Option<Py<PyAny>>,
        type_schema: Option<Py<PyAny>>,
        term: Option<Py<PyAny>>,
        term_type_schema: Option<Py<PyAny>>,
        start: Option<Py<PyAny>>,
        end: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        if node.is_some() {
            let node_pattern = NodePattern::new(node, r#type, type_schema, properties)?;
            self.operations
                .push(QueryOperation::Merge(MergeOp::Node(node_pattern)));
        } else if edge.is_some() {
            let edge_pattern = EdgePattern::new(
                edge.clone(),
                term,
                term_type_schema,
                properties,
                "forward".to_string(),
            )?;
            let start_var = Self::extract_var(start)?;
            let end_var = Self::extract_var(end)?;
            self.operations.push(QueryOperation::Merge(MergeOp::Edge(
                edge_pattern,
                start_var,
                end_var,
            )));
        }

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
        Python::with_gil(|py| {
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
        Python::with_gil(|py| {
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
                    self.execute_match(py, match_op)?;
                }
                QueryOperation::Create(create_op) => {
                    self.execute_create(py, create_op)?;
                }
                QueryOperation::Merge(merge_op) => {
                    self.execute_merge(py, merge_op)?;
                }
                QueryOperation::Delete(vars, detach) => {
                    self.execute_delete(py, vars, detach)?;
                }
                QueryOperation::Set(var, props) => {
                    self.execute_set(py, var, props)?;
                }
                _ => {
                    // Other operations not fully implemented yet
                }
            }
        }
        Ok(())
    }

    fn execute_match(&mut self, py: Python, match_op: MatchOp) -> PyResult<()> {
        match match_op {
            MatchOp::Node(node_pattern) => {
                let mut matches = Vec::new();

                // Optimization: If we have a specific type, use type index
                if let Some(ref type_obj) = node_pattern.type_obj {
                    let type_uid = type_obj.uid();
                    let type_nodes = self.graph.get_nodes_by_type(&type_uid, py)?;

                    for node in type_nodes {
                        if node_pattern.matches(&node, py)? {
                            matches.push(QueryResult::Node(node));
                        }
                    }
                } else {
                    // Fallback: iterate all nodes (when no type filter)
                    let nodes_dict = self.graph.nodes.bind(py);
                    for (_uid, node_obj) in nodes_dict.iter() {
                        let node: Node = node_obj.extract()?;
                        if node_pattern.matches(&node, py)? {
                            matches.push(QueryResult::Node(node));
                        }
                    }
                }

                if let Some(var) = node_pattern.variable {
                    self.matched_vars.insert(var, matches);
                }
            }
            MatchOp::Path(path) => {
                self.execute_path_match(py, path)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn execute_path_match(&mut self, py: Python, path: PathPattern) -> PyResult<()> {
        if path.nodes.len() == 1 && path.edges.is_empty() {
            // Simple node match
            self.execute_match(py, MatchOp::Node(path.nodes[0].clone()))?;
        } else if path.nodes.len() == 2 && path.edges.len() == 1 {
            // Simple edge pattern: (n)-[e]->(m)
            let start_pattern = &path.nodes[0];
            let edge_pattern = &path.edges[0];
            let end_pattern = &path.nodes[1];

            let edges_dict = self.graph.edges.bind(py);
            let mut start_matches = Vec::new();
            let mut edge_matches = Vec::new();
            let mut end_matches = Vec::new();

            for (_uid, edge_obj) in edges_dict.iter() {
                let edge: Edge = edge_obj.extract()?;

                // Check if edge matches pattern
                let edge_ok = if let Some(ref schema) = edge_pattern.term_type_schema {
                    schema.matches_type(&edge.term.r#type)
                } else {
                    true
                };

                if !edge_ok {
                    continue;
                }

                // Check start node
                if start_pattern.matches(&edge.start, py)? && end_pattern.matches(&edge.end, py)? {
                    start_matches.push(QueryResult::Node((*edge.start).clone()));
                    edge_matches.push(QueryResult::Edge(edge.clone()));
                    end_matches.push(QueryResult::Node((*edge.end).clone()));
                }
            }

            if let Some(ref var) = start_pattern.variable {
                self.matched_vars.insert(var.clone(), start_matches);
            }
            if let Some(ref var) = edge_pattern.variable {
                self.matched_vars.insert(var.clone(), edge_matches);
            }
            if let Some(ref var) = end_pattern.variable {
                self.matched_vars.insert(var.clone(), end_matches);
            }
        }
        Ok(())
    }

    fn execute_create(&mut self, py: Python, create_op: CreateOp) -> PyResult<()> {
        match create_op {
            CreateOp::Node(node_pattern) => {
                if let Some(type_obj) = node_pattern.type_obj {
                    let type_py = type_to_python(py, &type_obj)?;
                    let props = PyDict::new(py);
                    for (k, v) in node_pattern.properties {
                        props.set_item(k, v)?;
                    }

                    let node = Node::new(type_py, Some(props.into()))?;
                    let uid = node.uid();

                    let nodes_dict = self.graph.nodes.bind(py);
                    nodes_dict.set_item(uid, Py::new(py, node.clone())?)?;

                    if let Some(var) = node_pattern.variable {
                        self.matched_vars.insert(var, vec![QueryResult::Node(node)]);
                    }
                }
            }
            CreateOp::Path(path) => {
                // Create nodes and edges in path
                // This is a simplified implementation
                for node_pattern in path.nodes {
                    if node_pattern.type_obj.is_some() {
                        self.execute_create(py, CreateOp::Node(node_pattern))?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn execute_merge(&mut self, py: Python, merge_op: MergeOp) -> PyResult<()> {
        match merge_op {
            MergeOp::Node(node_pattern) => {
                let mut found = false;

                // Optimization: If we have a specific type, use type index
                if let Some(ref type_obj) = node_pattern.type_obj {
                    let type_uid = type_obj.uid();
                    let type_nodes = self.graph.get_nodes_by_type(&type_uid, py)?;

                    for node in type_nodes {
                        if node_pattern.matches(&node, py)? {
                            // Node exists, add to matched_vars
                            if let Some(ref var) = node_pattern.variable {
                                let matches = self.matched_vars.entry(var.clone()).or_default();
                                matches.push(QueryResult::Node(node));
                            }
                            found = true;
                            break;
                        }
                    }
                } else {
                    // Fallback: iterate all nodes
                    let nodes_dict = self.graph.nodes.bind(py);
                    for (_uid, node_obj) in nodes_dict.iter() {
                        let node: Node = node_obj.extract()?;
                        if node_pattern.matches(&node, py)? {
                            // Node exists, add to matched_vars
                            if let Some(ref var) = node_pattern.variable {
                                let matches = self.matched_vars.entry(var.clone()).or_default();
                                matches.push(QueryResult::Node(node));
                            }
                            found = true;
                            break;
                        }
                    }
                }

                // If not found, create it
                if !found {
                    if let Some(type_obj) = node_pattern.type_obj {
                        let type_py = type_to_python(py, &type_obj)?;
                        let props = PyDict::new(py);
                        for (k, v) in node_pattern.properties {
                            props.set_item(k, v)?;
                        }

                        let node = Node::new(type_py, Some(props.into()))?;
                        let uid = node.uid();

                        let nodes_dict = self.graph.nodes.bind(py);
                        nodes_dict.set_item(uid, Py::new(py, node.clone())?)?;

                        if let Some(var) = node_pattern.variable {
                            self.matched_vars.insert(var, vec![QueryResult::Node(node)]);
                        }
                    }
                }
            }
            MergeOp::Edge(edge_pattern, start_var, end_var) => {
                // Edge merge: match or create edge
                // This is a simplified implementation
                // In practice, would need to check if edge already exists
                if let (Some(start_matches), Some(end_matches)) = (
                    self.matched_vars.get(&start_var),
                    self.matched_vars.get(&end_var),
                ) {
                    if let (Some(QueryResult::Node(start)), Some(QueryResult::Node(end))) =
                        (start_matches.first(), end_matches.first())
                    {
                        // Check if edge already exists
                        let edges_dict = self.graph.edges.bind(py);

                        for (_uid, edge_obj) in edges_dict.iter() {
                            let edge: Edge = edge_obj.extract()?;
                            if edge.start.uid() == start.uid() && edge.end.uid() == end.uid() {
                                // Edge exists
                                if let Some(ref var) = edge_pattern.variable {
                                    let matches = self.matched_vars.entry(var.clone()).or_default();
                                    matches.push(QueryResult::Edge(edge));
                                }
                                break;
                            }
                        }

                        // If edge not found, would create it here
                        // For now, we skip edge creation in merge
                    }
                }
            }
        }
        Ok(())
    }

    fn execute_delete(&mut self, py: Python, vars: Vec<String>, _detach: bool) -> PyResult<()> {
        // Delete nodes/edges that were matched
        let nodes_dict = self.graph.nodes.bind(py);
        let edges_dict = self.graph.edges.bind(py);

        for var in vars {
            if let Some(results) = self.matched_vars.get(&var) {
                for result in results {
                    match result {
                        QueryResult::Node(node) => {
                            let uid = node.uid();
                            // Remove node from graph
                            let _ = nodes_dict.del_item(uid);
                        }
                        QueryResult::Edge(edge) => {
                            let uid = edge.uid();
                            // Remove edge from graph
                            let _ = edges_dict.del_item(uid);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn execute_set(&mut self, py: Python, var: String, props: Py<PyDict>) -> PyResult<()> {
        // Set properties on matched nodes
        if let Some(results) = self.matched_vars.get_mut(&var) {
            for result in results {
                if let QueryResult::Node(node) = result {
                    // Update node properties by merging new props into existing
                    let node_props = node.properties.bind(py);
                    let new_props = props.bind(py);
                    for (key, value) in new_props.iter() {
                        node_props.set_item(key, value)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_creation() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let graph = Graph::new().unwrap();
            let query = Query::new(graph);
            assert_eq!(query.operations.len(), 0);
        });
    }
}
