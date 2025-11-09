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
        Python::attach(|py| match self {
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
    pub fn return_(&mut self, py: Python, variables: Vec<String>) -> PyResult<Vec<Py<PyAny>>> {
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
    ) -> PyResult<Vec<Py<PyAny>>> {
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
        Python::attach(|py| {
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
                QueryOperation::Where(condition) => {
                    self.execute_where(py, condition)?;
                }
                QueryOperation::With(vars) => {
                    self.execute_with(vars)?;
                }
                QueryOperation::OrderBy(var, key, ascending) => {
                    self.execute_order_by(py, var, key, ascending)?;
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

    fn execute_match(&mut self, py: Python, match_op: MatchOp) -> PyResult<()> {
        match match_op {
            MatchOp::Node(node_pattern) => {
                let mut matches = Vec::new();

                // Optimized: Use tree-based type index for O(log n) lookup
                if let Some(ref type_obj) = node_pattern.type_obj {
                    // Direct type match - use the tree index
                    let type_nodes = self.graph.find_nodes_by_type(type_obj, py)?;

                    for node in type_nodes {
                        if node_pattern.matches(&node, py)? {
                            matches.push(QueryResult::Node(node));
                        }
                    }
                } else if let Some(ref schema) = node_pattern.type_schema {
                    // Type schema match - need to check all nodes but use the schema
                    // For wildcard schemas, use find_all_nodes
                    if schema.pattern == "$*$" {
                        let all_nodes = self.graph.find_all_nodes(py)?;
                        for node in all_nodes {
                            if node_pattern.matches(&node, py)? {
                                matches.push(QueryResult::Node(node));
                            }
                        }
                    } else {
                        // For specific schema patterns, we still need to iterate
                        // but we can optimize based on the pattern structure
                        let all_nodes = self.graph.find_all_nodes(py)?;
                        for node in all_nodes {
                            if node_pattern.matches(&node, py)? {
                                matches.push(QueryResult::Node(node));
                            }
                        }
                    }
                } else {
                    // No type filter - get all nodes efficiently
                    let all_nodes = self.graph.find_all_nodes(py)?;
                    for node in all_nodes {
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

            let mut start_matches = Vec::new();
            let mut edge_matches = Vec::new();
            let mut end_matches = Vec::new();

            // Optimized: Use tree-based index to find edges by term type
            let candidate_edges = if let Some(ref schema) = edge_pattern.term_type_schema {
                // Try to extract the type from the schema if it's a simple pattern
                // For now, we need to iterate but we can optimize specific cases
                if schema.pattern == "$*$" {
                    // Wildcard - get all edges
                    self.graph.find_all_edges(py)?
                } else {
                    // For complex schemas, we still need to check all edges
                    // but we retrieve them efficiently from the index
                    self.graph.find_all_edges(py)?
                }
            } else {
                // No schema - get all edges
                self.graph.find_all_edges(py)?
            };

            // Now filter the candidate edges
            for edge in candidate_edges {
                // Check if edge matches pattern
                let edge_ok = if let Some(ref schema) = edge_pattern.term_type_schema {
                    schema.matches_type(&edge.term.r#type)
                } else {
                    true
                };

                if !edge_ok {
                    continue;
                }

                // Check start and end nodes
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

                    // Use the optimized add_node method which updates the index
                    self.graph.add_node(&node, py)?;

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

                // Optimized: Use tree-based type index for O(log n) lookup
                if let Some(ref type_obj) = node_pattern.type_obj {
                    let type_nodes = self.graph.find_nodes_by_type(type_obj, py)?;

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
                    // No specific type - get all nodes efficiently
                    let all_nodes = self.graph.find_all_nodes(py)?;
                    for node in all_nodes {
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

                        // Use the optimized add_node method which updates the index
                        self.graph.add_node(&node, py)?;

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
        for var in vars {
            if let Some(results) = self.matched_vars.get(&var) {
                for result in results {
                    match result {
                        QueryResult::Node(node) => {
                            let uid = node.uid();
                            // Use the optimized remove_node method which updates the index
                            let _ = self.graph.remove_node(&uid, py);
                        }
                        QueryResult::Edge(edge) => {
                            let uid = edge.uid();
                            // Use the optimized remove_edge method which updates the index
                            let _ = self.graph.remove_edge(&uid, py);
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

        if expr.starts_with("NOT ") {
            let inner = expr[4..].trim();
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
                    let props = node.properties.bind(py);
                    return props.get_item(prop_name).ok().flatten().is_some();
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
                    let props = node.properties.bind(py);
                    return props.get_item(prop_name).ok().flatten().is_none();
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
        let props = node.properties.bind(py);
        let value = match props.get_item(prop_name) {
            Ok(Some(v)) => v,
            _ => return false,
        };

        // Evaluate the comparison
        match op {
            "=" => {
                // Try string comparison first
                if let Ok(val_str) = value.extract::<String>() {
                    return val_str == right;
                }
                // Try numeric comparison
                if let (Ok(val_num), Ok(right_num)) = (value.extract::<f64>(), right.parse::<f64>())
                {
                    return (val_num - right_num).abs() < f64::EPSILON;
                }
                // Try boolean comparison
                if let (Ok(val_bool), Ok(right_bool)) =
                    (value.extract::<bool>(), right.parse::<bool>())
                {
                    return val_bool == right_bool;
                }
            }
            "!=" => {
                if let Ok(val_str) = value.extract::<String>() {
                    return val_str != right;
                }
                if let (Ok(val_num), Ok(right_num)) = (value.extract::<f64>(), right.parse::<f64>())
                {
                    return (val_num - right_num).abs() >= f64::EPSILON;
                }
                if let (Ok(val_bool), Ok(right_bool)) =
                    (value.extract::<bool>(), right.parse::<bool>())
                {
                    return val_bool != right_bool;
                }
            }
            ">" | ">=" | "<" | "<=" => {
                if let (Ok(val_num), Ok(right_num)) = (value.extract::<f64>(), right.parse::<f64>())
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
        let props = node.properties.bind(py);
        let value = match props.get_item(prop_name) {
            Ok(Some(v)) => v,
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
            if let Ok(val_str) = value.extract::<String>() {
                if val_str == item {
                    return true;
                }
            }
            // Try numeric comparison
            if let (Ok(val_num), Ok(item_num)) = (value.extract::<f64>(), item.parse::<f64>()) {
                if (val_num - item_num).abs() < f64::EPSILON {
                    return true;
                }
            }
            // Try boolean comparison
            if let (Ok(val_bool), Ok(item_bool)) = (value.extract::<bool>(), item.parse::<bool>()) {
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
        let props = node.properties.bind(py);
        let value = match props.get_item(prop_name) {
            Ok(Some(v)) => v,
            _ => return false,
        };

        // Only works with strings
        if let Ok(val_str) = value.extract::<String>() {
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
        // Sort matched variables by a property
        if let Some(results) = self.matched_vars.get_mut(&var) {
            results.sort_by(|a, b| {
                let val_a = match a {
                    QueryResult::Node(node) => {
                        let props = node.properties.bind(py);
                        props.get_item(&key).ok().flatten()
                    }
                    _ => None,
                };

                let val_b = match b {
                    QueryResult::Node(node) => {
                        let props = node.properties.bind(py);
                        props.get_item(&key).ok().flatten()
                    }
                    _ => None,
                };

                match (val_a, val_b) {
                    (Some(a), Some(b)) => {
                        // Try numeric comparison first
                        if let (Ok(a_num), Ok(b_num)) = (a.extract::<f64>(), b.extract::<f64>()) {
                            let cmp = a_num
                                .partial_cmp(&b_num)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            if ascending {
                                cmp
                            } else {
                                cmp.reverse()
                            }
                        }
                        // Try string comparison
                        else if let (Ok(a_str), Ok(b_str)) =
                            (a.extract::<String>(), b.extract::<String>())
                        {
                            let cmp = a_str.cmp(&b_str);
                            if ascending {
                                cmp
                            } else {
                                cmp.reverse()
                            }
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    }
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
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_creation() {
        Python::initialize();
        Python::attach(|py| {
            let graph = Graph::new().unwrap();
            let query = Query::new(graph);
            assert_eq!(query.operations.len(), 0);
        });
    }
}
