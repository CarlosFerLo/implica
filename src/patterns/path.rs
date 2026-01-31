use std::fmt::Display;

use crate::errors::ImplicaError;
use crate::patterns::{
    edge::EdgePattern,
    node::NodePattern,
    parsing::{parse_edge_pattern, parse_node_pattern, tokenize_pattern, TokenKind},
};

#[derive(Clone, Debug)]
pub struct PathPattern {
    pattern: String,

    pub nodes: Vec<NodePattern>,
    pub edges: Vec<EdgePattern>,
}

impl Display for PathPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.pattern)
    }
}

impl PathPattern {
    pub fn validate(&self) -> Result<(), ImplicaError> {
        if self.nodes.is_empty() {
            return Err(ImplicaError::InvalidPattern {
                pattern: self.to_string(),
                reason: "a path pattern cannot be empty".to_string(),
            });
        }

        if self.nodes.len() != self.edges.len() + 1 {
            return Err(ImplicaError::InvalidPattern {
                pattern: self.to_string(),
                reason: "the number of nodes should be the number of edges plus 1".to_string(),
            });
        }
        Ok(())
    }
}

impl PathPattern {
    pub fn new(pattern: String) -> Result<Self, ImplicaError> {
        PathPattern::parse(pattern)
    }
    pub fn parse(pattern: String) -> Result<Self, ImplicaError> {
        // Enhanced parser for Cypher-like path patterns
        // Supports: (n)-[e]->(m), (n:A)-[e:term]->(m:B), etc.

        let pattern = pattern.trim();
        if pattern.is_empty() {
            return Err(ImplicaError::InvalidPattern {
                pattern: pattern.to_string(),
                reason: "Pattern cannot be empty".to_string(),
            });
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
            });
        }

        // Validate: edges should be between nodes
        if edges.len() >= nodes.len() {
            return Err(ImplicaError::InvalidPattern {
                pattern: pattern.to_string(),
                reason: "Invalid pattern: too many edges for the number of nodes".to_string(),
            });
        }

        Ok(PathPattern {
            pattern: pattern.to_string(),
            nodes,
            edges,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "PathPattern({} nodes, {} edges)",
            self.nodes.len(),
            self.edges.len()
        )
    }
}
