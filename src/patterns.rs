//! Pattern matching structures for Cypher-like queries.
//!
//! This module provides pattern structures for matching nodes, edges, and paths
//! in the graph. These patterns are used by the Query system to find matching
//! elements in the graph.
//!
//! All patterns are compiled and validated at creation time for optimal performance
//! and early error detection.

use crate::errors::ImplicaError;
use crate::graph::Node;
use crate::term::Term;
use crate::type_schema::TypeSchema;
use crate::types::{python_to_type, Type};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

/// Internal compiled representation for efficient matching.
///
/// This enum represents the compiled/optimized form of a pattern,
/// allowing for efficient matching without re-parsing or re-validation.
#[derive(Clone, Debug)]
enum CompiledNodeMatcher {
    /// Match any node (no type constraint)
    Any,
    /// Match nodes with a specific type
    ExactType(Type),
    /// Match nodes with a type schema pattern
    SchemaType(TypeSchema),
}

/// Represents a node pattern in a Cypher-like query.
///
/// Node patterns are used to match nodes in the graph based on variable names,
/// types, type schemas, and properties. Patterns are compiled and validated
/// at creation time for optimal performance.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Match any node, bind to variable 'n'
/// pattern = implica.NodePattern(variable="n")
///
/// # Match nodes of a specific type
/// person_type = implica.Variable("Person")
/// pattern = implica.NodePattern(variable="n", type=person_type)
///
/// # Match nodes using a type schema
/// pattern = implica.NodePattern(variable="n", type_schema="Person")
///
/// # Match with properties
/// pattern = implica.NodePattern(
///     variable="n",
///     type_schema="Person",
///     properties={"age": 25}
/// )
/// ```
///
/// # Fields
///
/// * `variable` - Optional variable name to bind matched nodes
/// * `compiled_matcher` - Compiled type matcher for efficient matching
/// * `properties` - Dictionary of required property values
#[pyclass]
#[derive(Debug)]
pub struct NodePattern {
    #[pyo3(get)]
    pub variable: Option<String>,
    /// Compiled matcher for efficient type checking
    compiled_matcher: CompiledNodeMatcher,
    pub properties: HashMap<String, Py<PyAny>>,
    // Keep these for backward compatibility and introspection
    pub type_obj: Option<Type>,
    pub type_schema: Option<TypeSchema>,
}

impl Clone for NodePattern {
    fn clone(&self) -> Self {
        Python::attach(|py| {
            let mut props = HashMap::new();
            for (k, v) in &self.properties {
                props.insert(k.clone(), v.clone_ref(py));
            }
            NodePattern {
                variable: self.variable.clone(),
                compiled_matcher: self.compiled_matcher.clone(),
                properties: props,
                type_obj: self.type_obj.clone(),
                type_schema: self.type_schema.clone(),
            }
        })
    }
}

#[pymethods]
impl NodePattern {
    /// Creates a new node pattern.
    ///
    /// The pattern is compiled and validated at creation time for optimal performance.
    /// Invalid type schemas or conflicting constraints will cause immediate errors.
    ///
    /// # Arguments
    ///
    /// * `variable` - Optional variable name to bind matched nodes
    /// * `type` - Optional specific type to match
    /// * `type_schema` - Optional type schema pattern (string or TypeSchema)
    /// * `properties` - Optional dictionary of required properties
    ///
    /// # Returns
    ///
    /// A new `NodePattern` instance, compiled and ready for matching
    ///
    /// # Errors
    ///
    /// * `ValueError` if both `type` and `type_schema` are provided (conflicting constraints)
    /// * `ValueError` if `type_schema` string is invalid
    /// * `ValueError` if variable name is invalid (empty or whitespace-only)
    ///
    /// # Examples
    ///
    /// ```python
    /// # Simple pattern
    /// pattern = implica.NodePattern(variable="n")
    ///
    /// # With type schema
    /// pattern = implica.NodePattern(
    ///     variable="person",
    ///     type_schema="Person"
    /// )
    ///
    /// # With specific type
    /// person_type = implica.Variable("Person")
    /// pattern = implica.NodePattern(variable="n", type=person_type)
    /// ```
    #[new]
    #[pyo3(signature = (variable=None, r#type=None, type_schema=None, properties=None))]
    pub fn new(
        variable: Option<String>,
        r#type: Option<Py<PyAny>>,
        type_schema: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        Python::attach(|py| {
            // Validate variable name if provided
            if let Some(ref var) = variable {
                if var.trim().is_empty() {
                    return Err(ImplicaError::invalid_identifier(
                        var.clone(),
                        "variable name cannot be empty or whitespace-only",
                    )
                    .into());
                }
            }

            // Parse type if provided
            let type_obj = if let Some(t) = r#type {
                Some(python_to_type(t.bind(py))?)
            } else {
                None
            };

            // Parse schema if provided
            let schema = if let Some(s) = type_schema {
                if let Ok(schema_str) = s.bind(py).extract::<String>() {
                    Some(TypeSchema::new(schema_str)?) // Fail fast on invalid schema
                } else {
                    Some(s.bind(py).extract::<TypeSchema>()?)
                }
            } else {
                None
            };

            // Validate: cannot have both type and type_schema
            if type_obj.is_some() && schema.is_some() {
                return Err(ImplicaError::schema_validation(
                    "NodePattern",
                    "Cannot specify both 'type' and 'type_schema' - they are mutually exclusive",
                )
                .into());
            }

            // Build compiled matcher for efficient matching
            let compiled_matcher = if let Some(ref t) = type_obj {
                CompiledNodeMatcher::ExactType(t.clone())
            } else if let Some(ref s) = schema {
                CompiledNodeMatcher::SchemaType(s.clone())
            } else {
                CompiledNodeMatcher::Any
            };

            // Parse properties
            let mut props = HashMap::new();
            if let Some(p) = properties {
                for (k, v) in p.bind(py).iter() {
                    let key: String = k.extract()?;
                    if key.trim().is_empty() {
                        return Err(ImplicaError::invalid_identifier(
                            key,
                            "property key cannot be empty or whitespace-only",
                        )
                        .into());
                    }
                    props.insert(key, v.into());
                }
            }

            Ok(NodePattern {
                variable,
                compiled_matcher,
                properties: props,
                type_obj,
                type_schema: schema,
            })
        })
    }

    fn __repr__(&self) -> String {
        let type_info = if self.type_obj.is_some() {
            ", type=<specified>"
        } else if self.type_schema.is_some() {
            ", type_schema=<specified>"
        } else {
            ""
        };
        format!("NodePattern(variable={:?}{})", self.variable, type_info)
    }
}

impl NodePattern {
    /// Checks if a node matches this pattern.
    ///
    /// This uses the pre-compiled matcher for optimal performance.
    /// This is an internal method used by the query system.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to check
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the node matches, `Ok(false)` otherwise
    pub fn matches(&self, node: &Node, py: Python) -> PyResult<bool> {
        // Check type using compiled matcher (most efficient path)
        match &self.compiled_matcher {
            CompiledNodeMatcher::Any => {
                // No type constraint, continue to property check
            }
            CompiledNodeMatcher::ExactType(type_obj) => {
                if &*node.r#type != type_obj {
                    return Ok(false);
                }
            }
            CompiledNodeMatcher::SchemaType(schema) => {
                if !schema.matches_type(&node.r#type) {
                    return Ok(false);
                }
            }
        }

        // Check properties if specified
        if !self.properties.is_empty() {
            let node_props = node.properties.bind(py);
            for (key, value) in &self.properties {
                if let Ok(Some(node_value)) = node_props.get_item(key) {
                    if !node_value.eq(value.bind(py))? {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

/// Compiled direction for efficient matching.
#[derive(Clone, Debug, PartialEq)]
enum CompiledDirection {
    Forward,
    Backward,
    Any,
}

impl CompiledDirection {
    fn from_string(s: &str) -> Result<Self, ImplicaError> {
        match s {
            "forward" => Ok(CompiledDirection::Forward),
            "backward" => Ok(CompiledDirection::Backward),
            "any" => Ok(CompiledDirection::Any),
            _ => Err(ImplicaError::schema_validation(
                s,
                "Direction must be 'forward', 'backward', or 'any'",
            )),
        }
    }

    fn to_string(&self) -> &'static str {
        match self {
            CompiledDirection::Forward => "forward",
            CompiledDirection::Backward => "backward",
            CompiledDirection::Any => "any",
        }
    }
}

/// Internal compiled representation for efficient edge matching.
#[derive(Clone, Debug)]
enum CompiledEdgeMatcher {
    /// Match any term (no type constraint)
    Any,
    /// Match edges with a specific term
    ExactTerm(Term),
    /// Match edges with a term matching the schema
    SchemaTerm(TypeSchema),
}

/// Represents an edge pattern in a Cypher-like query.
///
/// Edge patterns are used to match edges in the graph based on variable names,
/// terms, term type schemas, properties, and direction. Patterns are compiled
/// and validated at creation time for optimal performance.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Match any edge in forward direction
/// pattern = implica.EdgePattern(variable="e", direction="forward")
///
/// # Match edges with a specific term type
/// pattern = implica.EdgePattern(
///     variable="rel",
///     term_type_schema="Person -> Address",
///     direction="forward"
/// )
///
/// # Match in any direction
/// pattern = implica.EdgePattern(variable="e", direction="any")
/// ```
///
/// # Fields
///
/// * `variable` - Optional variable name to bind matched edges
/// * `compiled_matcher` - Compiled term matcher for efficient matching
/// * `compiled_direction` - Compiled direction for efficient checking
/// * `properties` - Dictionary of required property values
#[pyclass]
#[derive(Debug)]
pub struct EdgePattern {
    #[pyo3(get)]
    pub variable: Option<String>,
    /// Compiled matcher for efficient term checking
    compiled_matcher: CompiledEdgeMatcher,
    /// Compiled direction for efficient checking
    compiled_direction: CompiledDirection,
    pub properties: HashMap<String, Py<PyAny>>,
    // Keep these for backward compatibility and introspection
    pub term: Option<Term>,
    pub term_type_schema: Option<TypeSchema>,
}

impl Clone for EdgePattern {
    fn clone(&self) -> Self {
        Python::attach(|py| {
            let mut props = HashMap::new();
            for (k, v) in &self.properties {
                props.insert(k.clone(), v.clone_ref(py));
            }
            EdgePattern {
                variable: self.variable.clone(),
                compiled_matcher: self.compiled_matcher.clone(),
                compiled_direction: self.compiled_direction.clone(),
                properties: props,
                term: self.term.clone(),
                term_type_schema: self.term_type_schema.clone(),
            }
        })
    }
}

#[pymethods]
impl EdgePattern {
    /// Creates a new edge pattern.
    ///
    /// The pattern is compiled and validated at creation time for optimal performance.
    /// Invalid term schemas, directions, or conflicting constraints will cause immediate errors.
    ///
    /// # Arguments
    ///
    /// * `variable` - Optional variable name to bind matched edges
    /// * `term` - Optional specific term to match
    /// * `term_type_schema` - Optional type schema for the term (string or TypeSchema)
    /// * `properties` - Optional dictionary of required properties
    /// * `direction` - Direction of the edge: "forward", "backward", or "any" (default: "forward")
    ///
    /// # Returns
    ///
    /// A new `EdgePattern` instance, compiled and ready for matching
    ///
    /// # Errors
    ///
    /// * `ValueError` if both `term` and `term_type_schema` are provided (conflicting constraints)
    /// * `ValueError` if `term_type_schema` string is invalid
    /// * `ValueError` if `direction` is not one of "forward", "backward", or "any"
    /// * `ValueError` if variable name is invalid (empty or whitespace-only)
    ///
    /// # Examples
    ///
    /// ```python
    /// # Forward edge
    /// pattern = implica.EdgePattern(variable="e", direction="forward")
    ///
    /// # Backward edge with type schema
    /// pattern = implica.EdgePattern(
    ///     variable="back",
    ///     term_type_schema="A -> B",
    ///     direction="backward"
    /// )
    /// ```
    #[new]
    #[pyo3(signature = (variable=None, term=None, term_type_schema=None, properties=None, direction="forward".to_string()))]
    pub fn new(
        variable: Option<String>,
        term: Option<Py<PyAny>>,
        term_type_schema: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
        direction: String,
    ) -> PyResult<Self> {
        Python::attach(|py| {
            // Validate variable name if provided
            if let Some(ref var) = variable {
                if var.trim().is_empty() {
                    return Err(ImplicaError::invalid_identifier(
                        var.clone(),
                        "variable name cannot be empty or whitespace-only",
                    )
                    .into());
                }
            }

            // Validate and compile direction
            let compiled_direction = CompiledDirection::from_string(&direction)?;

            // Parse term if provided
            let term_obj = if let Some(t) = term {
                Some(t.bind(py).extract::<Term>()?)
            } else {
                None
            };

            // Parse schema if provided
            let schema = if let Some(s) = term_type_schema {
                if let Ok(schema_str) = s.bind(py).extract::<String>() {
                    Some(TypeSchema::new(schema_str)?) // Fail fast on invalid schema
                } else {
                    Some(s.bind(py).extract::<TypeSchema>()?)
                }
            } else {
                None
            };

            // Validate: cannot have both term and term_type_schema
            if term_obj.is_some() && schema.is_some() {
                return Err(ImplicaError::schema_validation(
                    "EdgePattern",
                    "Cannot specify both 'term' and 'term_type_schema' - they are mutually exclusive",
                )
                .into());
            }

            // Build compiled matcher for efficient matching
            let compiled_matcher = if let Some(ref t) = term_obj {
                CompiledEdgeMatcher::ExactTerm(t.clone())
            } else if let Some(ref s) = schema {
                CompiledEdgeMatcher::SchemaTerm(s.clone())
            } else {
                CompiledEdgeMatcher::Any
            };

            // Parse properties
            let mut props = HashMap::new();
            if let Some(p) = properties {
                for (k, v) in p.bind(py).iter() {
                    let key: String = k.extract()?;
                    if key.trim().is_empty() {
                        return Err(ImplicaError::invalid_identifier(
                            key,
                            "property key cannot be empty or whitespace-only",
                        )
                        .into());
                    }
                    props.insert(key, v.into());
                }
            }

            Ok(EdgePattern {
                variable,
                compiled_matcher,
                compiled_direction,
                properties: props,
                term: term_obj,
                term_type_schema: schema,
            })
        })
    }

    /// Gets the direction of this edge pattern.
    ///
    /// # Returns
    ///
    /// The direction as a string: "forward", "backward", or "any"
    #[getter]
    pub fn direction(&self) -> String {
        self.compiled_direction.to_string().to_string()
    }

    fn __repr__(&self) -> String {
        let term_info = if self.term.is_some() {
            ", term=<specified>"
        } else if self.term_type_schema.is_some() {
            ", term_type_schema=<specified>"
        } else {
            ""
        };
        format!(
            "EdgePattern(variable={:?}, direction={}{})",
            self.variable,
            self.compiled_direction.to_string(),
            term_info
        )
    }
}

impl EdgePattern {
    /// Checks if an edge matches this pattern.
    ///
    /// This uses the pre-compiled matcher for optimal performance.
    /// This is an internal method used by the query system.
    ///
    /// Note: Direction matching is context-dependent and should be checked
    /// by the caller based on the traversal direction.
    ///
    /// # Arguments
    ///
    /// * `edge` - The edge to check
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the edge matches, `Ok(false)` otherwise
    pub fn matches(&self, edge: &crate::graph::Edge, py: Python) -> PyResult<bool> {
        // Check term using compiled matcher (most efficient path)
        match &self.compiled_matcher {
            CompiledEdgeMatcher::Any => {
                // No term constraint, continue to property check
            }
            CompiledEdgeMatcher::ExactTerm(term_obj) => {
                if &*edge.term != term_obj {
                    return Ok(false);
                }
            }
            CompiledEdgeMatcher::SchemaTerm(schema) => {
                if !schema.matches_type(&edge.term.r#type) {
                    return Ok(false);
                }
            }
        }

        // Check properties if specified
        if !self.properties.is_empty() {
            let edge_props = edge.properties.bind(py);
            for (key, value) in &self.properties {
                if let Ok(Some(edge_value)) = edge_props.get_item(key) {
                    if !edge_value.eq(value.bind(py))? {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Checks if the direction matches for traversal.
    ///
    /// This is a helper method to check if the edge can be traversed
    /// in the given direction according to the pattern.
    ///
    /// # Arguments
    ///
    /// * `forward` - true if traversing forward, false if backward
    ///
    /// # Returns
    ///
    /// `true` if the direction matches the pattern
    pub fn matches_direction(&self, forward: bool) -> bool {
        match self.compiled_direction {
            CompiledDirection::Any => true,
            CompiledDirection::Forward => forward,
            CompiledDirection::Backward => !forward,
        }
    }
}

/// Represents a path pattern in a Cypher-like query.
///
/// Path patterns describe sequences of nodes and edges, allowing complex
/// graph traversals to be specified. They can be created programmatically
/// or parsed from Cypher-like pattern strings.
///
/// # Pattern Syntax
///
/// - Nodes: `(variable)`, `(variable:Type)`, `(:Type)`, `()`
/// - Edges: `-[variable]->` (forward), `<-[variable]-` (backward), `-[variable]-` (any)
/// - Typed edges: `-[var:schema]->`
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Parse from string
/// pattern = implica.PathPattern("(n:Person)-[e:knows]->(m:Person)")
///
/// # Parse complex path
/// pattern = implica.PathPattern("(a:A)-[r1]->(b:B)-[r2]->(c:C)")
///
/// # Anonymous nodes
/// pattern = implica.PathPattern("()-[e:relation]->()")
///
/// # Programmatic construction
/// pattern = implica.PathPattern()
/// pattern.add_node(implica.NodePattern(variable="n"))
/// pattern.add_edge(implica.EdgePattern(variable="e"))
/// pattern.add_node(implica.NodePattern(variable="m"))
/// ```
///
/// # Fields
///
/// * `nodes` - List of node patterns in the path
/// * `edges` - List of edge patterns connecting the nodes
#[pyclass]
#[derive(Clone, Debug)]
pub struct PathPattern {
    #[pyo3(get)]
    pub nodes: Vec<NodePattern>,
    #[pyo3(get)]
    pub edges: Vec<EdgePattern>,
}

#[pymethods]
impl PathPattern {
    /// Creates a new path pattern, optionally from a pattern string.
    ///
    /// # Arguments
    ///
    /// * `pattern` - Optional Cypher-like pattern string to parse
    ///
    /// # Returns
    ///
    /// A new `PathPattern` instance
    ///
    /// # Errors
    ///
    /// Returns an error if the pattern string is invalid
    ///
    /// # Examples
    ///
    /// ```python
    /// # Empty pattern
    /// pattern = implica.PathPattern()
    ///
    /// # Parse from string
    /// pattern = implica.PathPattern("(n:Person)-[e]->(m:Person)")
    /// ```
    #[new]
    #[pyo3(signature = (pattern=None))]
    pub fn new(pattern: Option<String>) -> PyResult<Self> {
        if let Some(p) = pattern {
            PathPattern::parse(p)
        } else {
            Ok(PathPattern {
                nodes: Vec::new(),
                edges: Vec::new(),
            })
        }
    }

    /// Adds a node pattern to the path.
    ///
    /// # Arguments
    ///
    /// * `pattern` - The node pattern to add
    ///
    /// # Returns
    ///
    /// A clone of self with the added node (for method chaining)
    pub fn add_node(&mut self, pattern: NodePattern) -> Self {
        self.nodes.push(pattern);
        self.clone()
    }

    /// Adds an edge pattern to the path.
    ///
    /// # Arguments
    ///
    /// * `pattern` - The edge pattern to add
    ///
    /// # Returns
    ///
    /// A clone of self with the added edge (for method chaining)
    pub fn add_edge(&mut self, pattern: EdgePattern) -> Self {
        self.edges.push(pattern);
        self.clone()
    }

    /// Parses a Cypher-like pattern string into a PathPattern.
    ///
    /// This is the main parser for pattern strings, supporting nodes, edges,
    /// and complete paths with types and properties.
    ///
    /// # Supported Syntax
    ///
    /// - Simple nodes: `(n)`, `(n:Type)`, `(:Type)`, `()`
    /// - Forward edges: `-[e]->`, `-[e:type]->`
    /// - Backward edges: `<-[e]-`, `<-[e:type]-`
    /// - Bidirectional: `-[e]-`
    /// - Paths: `(a)-[e1]->(b)-[e2]->(c)`
    ///
    /// # Arguments
    ///
    /// * `pattern` - The pattern string to parse
    ///
    /// # Returns
    ///
    /// A `PathPattern` representing the parsed pattern
    ///
    /// # Errors
    ///
    /// * `PyValueError` if the pattern is empty, malformed, or has syntax errors
    ///
    /// # Examples
    ///
    /// ```python
    /// # Simple path
    /// p = implica.PathPattern.parse("(n)-[e]->(m)")
    ///
    /// # Typed path
    /// p = implica.PathPattern.parse("(n:Person)-[e:knows]->(m:Person)")
    ///
    /// # Complex path
    /// p = implica.PathPattern.parse("(a:A)-[r1]->(b:B)<-[r2]-(c:C)")
    /// ```
    #[staticmethod]
    pub fn parse(pattern: String) -> PyResult<Self> {
        // Enhanced parser for Cypher-like path patterns
        // Supports: (n)-[e]->(m), (n:A)-[e:term]->(m:B), etc.

        let pattern = pattern.trim();
        if pattern.is_empty() {
            return Err(ImplicaError::invalid_pattern(pattern, "Pattern cannot be empty").into());
        }

        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // Split pattern into components
        let components = tokenize_pattern(pattern)?;

        // Parse components in sequence
        let mut i = 0;
        while i < components.len() {
            let comp = &components[i];

            match comp.kind {
                TokenKind::Node => {
                    nodes.push(parse_node_pattern(&comp.text)?);
                }
                TokenKind::Edge => {
                    edges.push(parse_edge_pattern(&comp.text)?);
                }
            }

            i += 1;
        }

        // Validate: should have at least one node
        if nodes.is_empty() {
            return Err(ImplicaError::invalid_pattern(
                pattern,
                "Pattern must contain at least one node",
            )
            .into());
        }

        // Validate: edges should be between nodes
        if edges.len() >= nodes.len() {
            return Err(ImplicaError::invalid_pattern(
                pattern,
                "Invalid pattern: too many edges for the number of nodes",
            )
            .into());
        }

        Ok(PathPattern { nodes, edges })
    }

    fn __repr__(&self) -> String {
        format!(
            "PathPattern({} nodes, {} edges)",
            self.nodes.len(),
            self.edges.len()
        )
    }
}

/// Token types for pattern parsing.
///
/// Represents the type of a parsed token: either a node or an edge.
#[derive(Debug, PartialEq)]
enum TokenKind {
    Node,
    Edge,
}

/// A token from pattern parsing.
///
/// Contains the token type and the actual text that was parsed.
#[derive(Debug)]
struct Token {
    kind: TokenKind,
    text: String,
}

/// Tokenizes a pattern string into nodes and edges.
///
/// This function breaks down a pattern string into individual node and edge
/// tokens, handling parentheses and brackets correctly.
///
/// # Arguments
///
/// * `pattern` - The pattern string to tokenize
///
/// # Returns
///
/// A vector of tokens representing the parsed components
///
/// # Errors
///
/// * `PyValueError` if parentheses or brackets are unmatched
/// * `PyValueError` if there are unexpected characters outside patterns
fn tokenize_pattern(pattern: &str) -> PyResult<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_parens = 0;
    let mut in_brackets = 0;
    let mut edge_buffer = String::new();

    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        match c {
            '(' => {
                if in_brackets == 0 && in_parens == 0 {
                    // Start of a new node
                    if !edge_buffer.is_empty() {
                        let trimmed_edge = edge_buffer.trim().to_string();
                        if !trimmed_edge.is_empty() {
                            tokens.push(Token {
                                kind: TokenKind::Edge,
                                text: trimmed_edge,
                            });
                        }
                        edge_buffer.clear();
                    }
                    current.clear();
                }
                in_parens += 1;
                current.push(c);
            }
            ')' => {
                current.push(c);
                in_parens -= 1;
                if in_parens == 0 && in_brackets == 0 {
                    // End of node
                    tokens.push(Token {
                        kind: TokenKind::Node,
                        text: current.clone(),
                    });
                    current.clear();
                }
            }
            '[' => {
                if in_parens == 0 {
                    in_brackets += 1;
                    edge_buffer.push(c);
                } else {
                    current.push(c);
                }
            }
            ']' => {
                if in_parens == 0 {
                    edge_buffer.push(c);
                    in_brackets -= 1;
                } else {
                    current.push(c);
                }
            }
            '-' | '>' | '<' => {
                if in_parens == 0 {
                    edge_buffer.push(c);
                } else {
                    current.push(c);
                }
            }
            ' ' | '\t' | '\n' | '\r' => {
                // Skip whitespace outside of patterns
                if in_parens > 0 {
                    current.push(c);
                } else if in_brackets > 0 {
                    edge_buffer.push(c);
                }
                // Otherwise skip whitespace
            }
            _ => {
                if in_parens > 0 {
                    current.push(c);
                } else if in_brackets > 0 {
                    edge_buffer.push(c);
                } else {
                    return Err(ImplicaError::invalid_pattern(
                        pattern,
                        format!(
                            "Unexpected character '{}' outside of node or edge pattern",
                            c
                        ),
                    )
                    .into());
                }
            }
        }

        i += 1;
    }

    // Check for unclosed patterns
    if in_parens != 0 {
        return Err(
            ImplicaError::invalid_pattern(pattern, "Unmatched parentheses in pattern").into(),
        );
    }
    if in_brackets != 0 {
        return Err(ImplicaError::invalid_pattern(pattern, "Unmatched brackets in pattern").into());
    }

    // Add remaining edge if any
    if !edge_buffer.is_empty() {
        return Err(
            ImplicaError::invalid_pattern(pattern, "Pattern cannot end with an edge").into(),
        );
    }

    Ok(tokens)
}

/// Parses a node pattern from a token string.
///
/// Extracts the variable name, type schema, and properties from a node pattern
/// like "(n:Type {prop: value})".
///
/// # Arguments
///
/// * `s` - The node pattern string (including parentheses)
///
/// # Returns
///
/// A `NodePattern` representing the parsed node
///
/// # Errors
///
/// * `ValueError` if the string is not properly enclosed in parentheses
fn parse_node_pattern(s: &str) -> PyResult<NodePattern> {
    let s = s.trim();
    if !s.starts_with('(') || !s.ends_with(')') {
        return Err(ImplicaError::invalid_pattern(
            s,
            "Node pattern must be enclosed in parentheses",
        )
        .into());
    }

    let inner = &s[1..s.len() - 1].trim();

    // Parse: (var:type {props}) or (var:type) or (var) or (:type)
    let mut variable = None;
    let mut type_schema = None;

    if inner.is_empty() {
        // Empty node pattern - matches any node
        return NodePattern::new(None, None, None, None);
    }

    // Check for properties (for future expansion)
    let content = if let Some(brace_idx) = inner.find('{') {
        // Has properties - for now we ignore them
        inner[..brace_idx].trim()
    } else {
        inner
    };

    // Split by : if present (for type specification)
    if let Some(colon_idx) = content.find(':') {
        let var_part = content[..colon_idx].trim();
        if !var_part.is_empty() {
            variable = Some(var_part.to_string());
        }

        let type_part = content[colon_idx + 1..].trim();
        if !type_part.is_empty() {
            // Parse and validate the type schema
            type_schema = Some(TypeSchema::new(type_part.to_string())?);
        }
    } else {
        // No colon, just variable name
        if !content.is_empty() {
            variable = Some(content.to_string());
        }
    }

    // Use the validated NodePattern constructor
    Python::attach(|py| {
        let schema_py = type_schema.map(|s| Py::new(py, s).unwrap().into_any());
        NodePattern::new(variable, None, schema_py, None)
    })
}

/// Parses an edge pattern from a token string.
///
/// Extracts the variable name, term type schema, direction, and properties
/// from an edge pattern like "-[e:type]->" or "<-[e]-".
///
/// # Arguments
///
/// * `s` - The edge pattern string (including arrows and brackets)
///
/// # Returns
///
/// An `EdgePattern` representing the parsed edge
///
/// # Errors
///
/// * `ValueError` if the pattern doesn't contain brackets
/// * `ValueError` if brackets are mismatched
/// * `ValueError` if both <- and -> appear (invalid direction)
fn parse_edge_pattern(s: &str) -> PyResult<EdgePattern> {
    let s = s.trim();

    // Determine direction based on arrows
    // Patterns: -[e]-> (forward), <-[e]- (backward), -[e]- (any)
    let direction = if s.starts_with('<') && s.contains("->") {
        return Err(
            ImplicaError::invalid_pattern(s, "Cannot have both <- and -> in same edge").into(),
        );
    } else if s.starts_with("<-") || (s.starts_with('<') && s.contains('-')) {
        "backward"
    } else if s.contains("->") || s.ends_with('>') {
        "forward"
    } else {
        "any"
    };

    // Extract the part inside brackets
    let bracket_start = s
        .find('[')
        .ok_or_else(|| ImplicaError::invalid_pattern(s, "Edge pattern must contain brackets"))?;
    let bracket_end = s.rfind(']').ok_or_else(|| {
        ImplicaError::invalid_pattern(s, "Edge pattern must contain closing bracket")
    })?;

    if bracket_end <= bracket_start {
        return Err(ImplicaError::invalid_pattern(s, "Brackets are mismatched").into());
    }

    let inner = &s[bracket_start + 1..bracket_end].trim();

    let mut variable = None;
    let mut term_type_schema = None;

    if !inner.is_empty() {
        // Check for properties
        let content = if let Some(brace_idx) = inner.find('{') {
            inner[..brace_idx].trim()
        } else {
            inner
        };

        // Parse: [var:term] or [var] or [:term]
        if let Some(colon_idx) = content.find(':') {
            let var_part = content[..colon_idx].trim();
            if !var_part.is_empty() {
                variable = Some(var_part.to_string());
            }

            let term_part = content[colon_idx + 1..].trim();
            if !term_part.is_empty() {
                // Parse and validate the type schema
                term_type_schema = Some(TypeSchema::new(term_part.to_string())?);
            }
        } else {
            // No colon, just variable
            if !content.is_empty() {
                variable = Some(content.to_string());
            }
        }
    }

    // Use the validated EdgePattern constructor
    Python::attach(|py| {
        let schema_py = term_type_schema.map(|s| Py::new(py, s).unwrap().into_any());
        EdgePattern::new(variable, None, schema_py, None, direction.to_string())
    })
}
