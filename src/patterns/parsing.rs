use pyo3::prelude::*;

use crate::errors::ImplicaError;

use crate::patterns::type_schema::TypeSchema;
use crate::patterns::{edge::EdgePattern, node::NodePattern};

/// Token types for pattern parsing.
///
/// Represents the type of a parsed token: either a node or an edge.
#[derive(Debug, PartialEq)]
pub(in crate::patterns) enum TokenKind {
    Node,
    Edge,
}

/// A token from pattern parsing.
///
/// Contains the token type and the actual text that was parsed.
#[derive(Debug)]
pub(in crate::patterns) struct Token {
    pub(in crate::patterns) kind: TokenKind,
    pub(in crate::patterns) text: String,
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
pub(in crate::patterns) fn tokenize_pattern(pattern: &str) -> PyResult<Vec<Token>> {
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
pub(in crate::patterns) fn parse_node_pattern(s: &str) -> PyResult<NodePattern> {
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
        return NodePattern::new(None, None, None, None, None);
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
        NodePattern::new(variable, None, schema_py, None, None)
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
pub(in crate::patterns) fn parse_edge_pattern(s: &str) -> PyResult<EdgePattern> {
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
