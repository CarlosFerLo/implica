//! Pattern matching structures for Cypher-like queries.
//!
//! This module provides pattern structures for matching nodes, edges, and paths
//! in the graph. These patterns are used by the Query system to find matching
//! elements in the graph.

use crate::errors::ImplicaError;
use crate::graph::Node;
use crate::term::Term;
use crate::type_schema::TypeSchema;
use crate::types::{python_to_type, Type};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

/// Represents a node pattern in a Cypher-like query.
///
/// Node patterns are used to match nodes in the graph based on variable names,
/// types, type schemas, and properties.
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
/// pattern = implica.NodePattern(variable="n", type_schema="$Person$")
///
/// # Match with properties
/// pattern = implica.NodePattern(
///     variable="n",
///     type_schema="$Person$",
///     properties={"age": 25}
/// )
/// ```
///
/// # Fields
///
/// * `variable` - Optional variable name to bind matched nodes
/// * `type_obj` - Optional specific type to match (internal)
/// * `type_schema` - Optional type schema pattern to match
/// * `properties` - Dictionary of required property values
#[pyclass]
#[derive(Debug)]
pub struct NodePattern {
    #[pyo3(get)]
    pub variable: Option<String>,
    pub type_obj: Option<Type>,
    pub type_schema: Option<TypeSchema>,
    pub properties: HashMap<String, PyObject>,
}

impl Clone for NodePattern {
    fn clone(&self) -> Self {
        Python::with_gil(|py| {
            let mut props = HashMap::new();
            for (k, v) in &self.properties {
                props.insert(k.clone(), v.clone_ref(py));
            }
            NodePattern {
                variable: self.variable.clone(),
                type_obj: self.type_obj.clone(),
                type_schema: self.type_schema.clone(),
                properties: props,
            }
        })
    }
}

#[pymethods]
impl NodePattern {
    /// Creates a new node pattern.
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
    /// A new `NodePattern` instance
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
    ///     type_schema="$Person$"
    /// )
    /// ```
    #[new]
    #[pyo3(signature = (variable=None, r#type=None, type_schema=None, properties=None))]
    pub fn new(
        variable: Option<String>,
        r#type: Option<Py<PyAny>>,
        type_schema: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            let type_obj = if let Some(t) = r#type {
                Some(python_to_type(t.bind(py))?)
            } else {
                None
            };

            let schema = if let Some(s) = type_schema {
                if let Ok(schema_str) = s.bind(py).extract::<String>() {
                    Some(TypeSchema::new(schema_str))
                } else {
                    s.bind(py).extract::<TypeSchema>().ok()
                }
            } else {
                None
            };

            let mut props = HashMap::new();
            if let Some(p) = properties {
                for (k, v) in p.bind(py).iter() {
                    let key: String = k.extract()?;
                    props.insert(key, v.into());
                }
            }

            Ok(NodePattern {
                variable,
                type_obj,
                type_schema: schema,
                properties: props,
            })
        })
    }

    fn __repr__(&self) -> String {
        format!("NodePattern(variable={:?})", self.variable)
    }
}

impl NodePattern {
    /// Checks if a node matches this pattern.
    ///
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
        // Check type if specified
        if let Some(ref type_obj) = self.type_obj {
            if &*node.r#type != type_obj {
                return Ok(false);
            }
        }

        // Check type schema if specified
        if let Some(ref schema) = self.type_schema {
            if !schema.matches_type(&node.r#type) {
                return Ok(false);
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

/// Represents an edge pattern in a Cypher-like query.
///
/// Edge patterns are used to match edges in the graph based on variable names,
/// terms, term type schemas, properties, and direction.
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
///     term_type_schema="$Person -> Address$",
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
/// * `term` - Optional specific term to match (internal)
/// * `term_type_schema` - Optional type schema for the term's type
/// * `properties` - Dictionary of required property values
/// * `direction` - Edge direction: "forward", "backward", or "any"
#[pyclass]
#[derive(Debug)]
pub struct EdgePattern {
    #[pyo3(get)]
    pub variable: Option<String>,
    pub term: Option<Term>,
    pub term_type_schema: Option<TypeSchema>,
    pub properties: HashMap<String, PyObject>,
    #[pyo3(get)]
    pub direction: String, // "forward", "backward", "any"
}

impl Clone for EdgePattern {
    fn clone(&self) -> Self {
        Python::with_gil(|py| {
            let mut props = HashMap::new();
            for (k, v) in &self.properties {
                props.insert(k.clone(), v.clone_ref(py));
            }
            EdgePattern {
                variable: self.variable.clone(),
                term: self.term.clone(),
                term_type_schema: self.term_type_schema.clone(),
                properties: props,
                direction: self.direction.clone(),
            }
        })
    }
}

#[pymethods]
impl EdgePattern {
    /// Creates a new edge pattern.
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
    /// A new `EdgePattern` instance
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
    ///     term_type_schema="$A -> B$",
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
        Python::with_gil(|py| {
            let term_obj = if let Some(t) = term {
                Some(t.bind(py).extract::<Term>()?)
            } else {
                None
            };

            let schema = if let Some(s) = term_type_schema {
                if let Ok(schema_str) = s.bind(py).extract::<String>() {
                    Some(TypeSchema::new(schema_str))
                } else {
                    s.bind(py).extract::<TypeSchema>().ok()
                }
            } else {
                None
            };

            let mut props = HashMap::new();
            if let Some(p) = properties {
                for (k, v) in p.bind(py).iter() {
                    let key: String = k.extract()?;
                    props.insert(key, v.into());
                }
            }

            Ok(EdgePattern {
                variable,
                term: term_obj,
                term_type_schema: schema,
                properties: props,
                direction,
            })
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "EdgePattern(variable={:?}, direction={})",
            self.variable, self.direction
        )
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
    let properties = HashMap::new(); // Properties parsing could be added later

    if inner.is_empty() {
        return Ok(NodePattern {
            variable: None,
            type_obj: None,
            type_schema: None,
            properties,
        });
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
            // Check if it's already a schema pattern (contains $)
            if type_part.contains('$') {
                type_schema = Some(TypeSchema::new(type_part.to_string()));
            } else {
                // Wrap in $..$ for schema
                type_schema = Some(TypeSchema::new(format!("${}$", type_part)));
            }
        }
    } else {
        // No colon, just variable name
        if !content.is_empty() {
            variable = Some(content.to_string());
        }
    }

    Ok(NodePattern {
        variable,
        type_obj: None,
        type_schema,
        properties,
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
    let properties = HashMap::new(); // Properties parsing for future expansion

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
                // Check if already a schema pattern
                if term_part.contains('$') {
                    term_type_schema = Some(TypeSchema::new(term_part.to_string()));
                } else {
                    // Wrap in $..$ for schema
                    term_type_schema = Some(TypeSchema::new(format!("${}$", term_part)));
                }
            }
        } else {
            // No colon, just variable
            if !content.is_empty() {
                variable = Some(content.to_string());
            }
        }
    }

    Ok(EdgePattern {
        variable,
        term: None,
        term_type_schema,
        properties,
        direction: direction.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_node() {
        let pattern = PathPattern::parse("(n)".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 1);
        assert_eq!(pattern.nodes[0].variable, Some("n".to_string()));
        assert!(pattern.nodes[0].type_schema.is_none());
    }

    #[test]
    fn test_parse_typed_node() {
        let pattern = PathPattern::parse("(n:A)".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 1);
        assert_eq!(pattern.nodes[0].variable, Some("n".to_string()));
        assert!(pattern.nodes[0].type_schema.is_some());
    }

    #[test]
    fn test_parse_anonymous_node() {
        let pattern = PathPattern::parse("()".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 1);
        assert!(pattern.nodes[0].variable.is_none());
    }

    #[test]
    fn test_parse_typed_anonymous_node() {
        let pattern = PathPattern::parse("(:Person)".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 1);
        assert!(pattern.nodes[0].variable.is_none());
        assert!(pattern.nodes[0].type_schema.is_some());
    }

    #[test]
    fn test_parse_simple_path() {
        let pattern = PathPattern::parse("(n)-[e]->(m)".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 2);
        assert_eq!(pattern.edges.len(), 1);
        assert_eq!(pattern.edges[0].direction, "forward");
    }

    #[test]
    fn test_parse_typed_path() {
        let pattern = PathPattern::parse("(n:A)-[e:term]->(m:B)".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 2);
        assert_eq!(pattern.edges.len(), 1);
        assert_eq!(pattern.nodes[0].variable, Some("n".to_string()));
        assert_eq!(pattern.nodes[1].variable, Some("m".to_string()));
        assert_eq!(pattern.edges[0].variable, Some("e".to_string()));
        assert!(pattern.edges[0].term_type_schema.is_some());
    }

    #[test]
    fn test_parse_backward_edge() {
        let pattern = PathPattern::parse("(n)<-[e]-(m)".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 2);
        assert_eq!(pattern.edges.len(), 1);
        assert_eq!(pattern.edges[0].direction, "backward");
    }

    #[test]
    fn test_parse_bidirectional_edge() {
        let pattern = PathPattern::parse("(n)-[e]-(m)".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 2);
        assert_eq!(pattern.edges.len(), 1);
        assert_eq!(pattern.edges[0].direction, "any");
    }

    #[test]
    fn test_parse_complex_path() {
        let pattern = PathPattern::parse("(a:A)-[e1:term]->(b:B)-[e2]->(c:C)".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 3);
        assert_eq!(pattern.edges.len(), 2);
        assert_eq!(pattern.nodes[0].variable, Some("a".to_string()));
        assert_eq!(pattern.nodes[1].variable, Some("b".to_string()));
        assert_eq!(pattern.nodes[2].variable, Some("c".to_string()));
    }

    #[test]
    fn test_parse_schema_pattern() {
        let pattern = PathPattern::parse("(n:$A -> B$)".to_string()).unwrap();
        assert_eq!(pattern.nodes.len(), 1);
        assert!(pattern.nodes[0].type_schema.is_some());
    }

    #[test]
    fn test_parse_empty_pattern_fails() {
        let result = PathPattern::parse("".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unmatched_parens_fails() {
        let result = PathPattern::parse("(n".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unmatched_brackets_fails() {
        let result = PathPattern::parse("(n)-[e->(m)".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_edge_without_nodes_fails() {
        let result = PathPattern::parse("-[e]->".to_string());
        assert!(result.is_err());
    }
}
