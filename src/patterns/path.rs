use pyo3::prelude::*;

use crate::errors::ImplicaError;
use crate::patterns::{
    edge::EdgePattern,
    node::NodePattern,
    parsing::{parse_edge_pattern, parse_node_pattern, tokenize_pattern, TokenKind},
};

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
