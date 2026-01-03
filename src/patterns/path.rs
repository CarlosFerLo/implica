use pyo3::prelude::*;

use crate::errors::ImplicaError;
use crate::patterns::{
    edge::EdgePattern,
    node::NodePattern,
    parsing::{parse_edge_pattern, parse_node_pattern, tokenize_pattern, TokenKind},
};

#[pyclass]
#[derive(Clone, Debug)]
pub struct PathPattern {
    #[pyo3(get)]
    pub nodes: Vec<NodePattern>,
    #[pyo3(get)]
    pub edges: Vec<EdgePattern>,
}

impl PathPattern {
    pub fn validate(&self) -> Result<(), ImplicaError> {
        if self.nodes.is_empty() {
            return Err(ImplicaError::InvalidPattern {
                pattern: format!("{:?}", self),
                reason: "a path pattern cannot be empty".to_string(),
            });
        }

        if self.nodes.len() != self.edges.len() + 1 {
            return Err(ImplicaError::InvalidPattern {
                pattern: format!("{:?}", self),
                reason: "the number of nodes should be the number of edges plus 1".to_string(),
            });
        }
        Ok(())
    }
}

#[pymethods]
impl PathPattern {
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

    pub fn add_node(&mut self, pattern: NodePattern) -> Self {
        self.nodes.push(pattern);
        self.clone()
    }

    pub fn add_edge(&mut self, pattern: EdgePattern) -> Self {
        self.edges.push(pattern);
        self.clone()
    }

    #[staticmethod]
    pub fn parse(pattern: String) -> PyResult<Self> {
        // Enhanced parser for Cypher-like path patterns
        // Supports: (n)-[e]->(m), (n:A)-[e:term]->(m:B), etc.

        let pattern = pattern.trim();
        if pattern.is_empty() {
            return Err(ImplicaError::InvalidPattern {
                pattern: pattern.to_string(),
                reason: "Pattern cannot be empty".to_string(),
            }
            .into());
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
            return Err(ImplicaError::InvalidPattern {
                pattern: pattern.to_string(),
                reason: "Pattern must contain at least one node".to_string(),
            }
            .into());
        }

        // Validate: edges should be between nodes
        if edges.len() >= nodes.len() {
            return Err(ImplicaError::InvalidPattern {
                pattern: pattern.to_string(),
                reason: "Invalid pattern: too many edges for the number of nodes".to_string(),
            }
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
