use pyo3::prelude::*;

use std::collections::HashMap;

use crate::errors::ImplicaError;

use crate::patterns::term_schema::TermSchema;
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
                    return Err(ImplicaError::InvalidPattern {
                        pattern: pattern.to_string(),
                        reason: format!(
                            "Unexpected character '{}' outside of node or edge pattern",
                            c
                        ),
                    }
                    .into());
                }
            }
        }

        i += 1;
    }

    // Check for unclosed patterns
    if in_parens != 0 {
        return Err(ImplicaError::InvalidPattern {
            pattern: pattern.to_string(),
            reason: "Unmatched parentheses in pattern".to_string(),
        }
        .into());
    }
    if in_brackets != 0 {
        return Err(ImplicaError::InvalidPattern {
            pattern: pattern.to_string(),
            reason: "Unmatched brackets in pattern".to_string(),
        }
        .into());
    }

    // Add remaining edge if any
    if !edge_buffer.is_empty() {
        return Err(ImplicaError::InvalidPattern {
            pattern: pattern.to_string(),
            reason: "Pattern cannot end with an edge".to_string(),
        }
        .into());
    }

    Ok(tokens)
}

/// Parses properties from a string in format "{key1: value1, key2: value2}".
///
/// Supports the following value types:
/// - Strings: "value" or 'value'
/// - Numbers: 42, 3.14, -10
/// - Booleans: true, false
/// - Null: null
///
/// # Arguments
///
/// * `props_str` - The properties string (including braces)
///
/// # Returns
///
/// A HashMap of property key-value pairs
///
/// # Errors
///
/// * `InvalidPattern` if the properties string is malformed
pub(in crate::patterns) fn parse_properties(
    props_str: &str,
) -> PyResult<HashMap<String, Py<PyAny>>> {
    let props_str = props_str.trim();

    // Check for proper braces
    if !props_str.starts_with('{') || !props_str.ends_with('}') {
        return Err(ImplicaError::InvalidPattern {
            pattern: props_str.to_string(),
            reason: "Properties must be enclosed in braces {}".to_string(),
        }
        .into());
    }

    let inner = props_str[1..props_str.len() - 1].trim();

    // Empty properties
    if inner.is_empty() {
        return Ok(HashMap::new());
    }

    Python::attach(|py| {
        let mut properties = HashMap::new();

        // Split by comma, but be careful with nested structures
        let mut current_key = String::new();
        let mut current_value = String::new();
        let mut in_string = false;
        let mut string_char = ' ';
        let mut after_colon = false;
        let mut depth = 0;

        for c in inner.chars() {
            match c {
                '"' | '\'' => {
                    if !in_string {
                        in_string = true;
                        string_char = c;
                    } else if c == string_char {
                        in_string = false;
                    }
                    current_value.push(c);
                }
                ':' if !in_string && depth == 0 => {
                    if after_colon {
                        return Err(ImplicaError::InvalidPattern {
                            pattern: props_str.to_string(),
                            reason: "Unexpected colon in property value".to_string(),
                        }
                        .into());
                    }
                    after_colon = true;
                    current_key = current_key.trim().to_string();
                    if current_key.is_empty() {
                        return Err(ImplicaError::InvalidPattern {
                            pattern: props_str.to_string(),
                            reason: "Empty property key".to_string(),
                        }
                        .into());
                    }
                }
                ',' if !in_string && depth == 0 => {
                    if !after_colon {
                        return Err(ImplicaError::InvalidPattern {
                            pattern: props_str.to_string(),
                            reason: "Missing colon in property definition".to_string(),
                        }
                        .into());
                    }

                    // Parse the value and add to properties
                    let value = parse_property_value(py, current_value.trim())?;
                    properties.insert(current_key.clone(), value);

                    // Reset for next property
                    current_key.clear();
                    current_value.clear();
                    after_colon = false;
                }
                '{' | '[' if !in_string => {
                    depth += 1;
                    current_value.push(c);
                }
                '}' | ']' if !in_string => {
                    depth -= 1;
                    current_value.push(c);
                }
                _ => {
                    if after_colon {
                        current_value.push(c);
                    } else {
                        current_key.push(c);
                    }
                }
            }
        }

        // Handle the last property
        if !current_key.is_empty() {
            if !after_colon {
                return Err(ImplicaError::InvalidPattern {
                    pattern: props_str.to_string(),
                    reason: "Missing colon in property definition".to_string(),
                }
                .into());
            }
            let value = parse_property_value(py, current_value.trim())?;
            properties.insert(current_key.trim().to_string(), value);
        }

        Ok(properties)
    })
}

/// Parses a single property value.
///
/// Supports the following types:
/// - Strings: "value" or 'value' (with escape sequences)
/// - Integers: 42, -10, 0
/// - Floats: 3.14, -2.5, 1e10, 1.5e-3
/// - Booleans: true, false
/// - Null: null, None
///
/// # Arguments
///
/// * `py` - Python GIL token
/// * `value_str` - The value string to parse
///
/// # Returns
///
/// A Python object representing the value
///
/// # Errors
///
/// Returns an error if a quoted string is not properly closed
fn parse_property_value(py: Python, value_str: &str) -> PyResult<Py<PyAny>> {
    let value_str = value_str.trim();

    // Check for empty value
    if value_str.is_empty() {
        return Err(ImplicaError::InvalidPattern {
            pattern: value_str.to_string(),
            reason: "Empty property value".to_string(),
        }
        .into());
    }

    // Try to parse as quoted string (with escape handling)
    if value_str.starts_with('"') || value_str.starts_with('\'') {
        let quote_char = value_str.chars().next().unwrap();

        // Check if string is properly closed
        if value_str.len() < 2 || !value_str.ends_with(quote_char) {
            return Err(ImplicaError::InvalidPattern {
                pattern: value_str.to_string(),
                reason: format!("Unclosed string literal (expected closing {})", quote_char),
            }
            .into());
        }

        let string_content = &value_str[1..value_str.len() - 1];

        // Handle escape sequences
        let unescaped = unescape_string(string_content)?;
        let py_str = unescaped.into_pyobject(py)?;
        return Ok(py_str.into_any().unbind());
    }

    // Try to parse as boolean (case-sensitive)
    match value_str {
        "true" => {
            let py_bool = true.into_pyobject(py)?.to_owned();
            return Ok(py_bool.into_any().unbind());
        }
        "false" => {
            let py_bool = false.into_pyobject(py)?.to_owned();
            return Ok(py_bool.into_any().unbind());
        }
        _ => {}
    }

    // Try to parse as null/None
    if value_str == "null" || value_str == "None" {
        return Ok(py.None());
    }

    // Try to parse as integer first (to avoid losing precision)
    // This will handle negative numbers too
    if let Ok(int_val) = value_str.parse::<i64>() {
        let py_int = int_val.into_pyobject(py)?;
        return Ok(py_int.into_any().unbind());
    }

    // Try to parse as float (including scientific notation)
    // This handles: 3.14, -2.5, 1e10, 1.5e-3, etc.
    if let Ok(float_val) = value_str.parse::<f64>() {
        // Check for special float values
        if float_val.is_nan() || float_val.is_infinite() {
            return Err(ImplicaError::InvalidPattern {
                pattern: value_str.to_string(),
                reason: "Invalid numeric value (NaN or Infinity not supported)".to_string(),
            }
            .into());
        }
        let py_float = float_val.into_pyobject(py)?;
        return Ok(py_float.into_any().unbind());
    }

    // If nothing else works, it's an error (unquoted strings are not allowed)
    Err(ImplicaError::InvalidPattern {
        pattern: value_str.to_string(),
        reason: "Invalid property value. Strings must be quoted, e.g., \"value\" or 'value'"
            .to_string(),
    }
    .into())
}

/// Unescapes common escape sequences in a string.
///
/// Supports: \n, \t, \r, \\, \", \'
fn unescape_string(s: &str) -> PyResult<String> {
    let mut result = String::new();
    let mut chars = s.chars();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('r') => result.push('\r'),
                Some('\\') => result.push('\\'),
                Some('"') => result.push('"'),
                Some('\'') => result.push('\''),
                Some('0') => result.push('\0'),
                Some(other) => {
                    // Unknown escape sequence - keep the backslash
                    result.push('\\');
                    result.push(other);
                }
                None => {
                    return Err(ImplicaError::InvalidPattern {
                        pattern: s.to_string(),
                        reason: "String ends with incomplete escape sequence".to_string(),
                    }
                    .into())
                }
            }
        } else {
            result.push(c);
        }
    }

    Ok(result)
}

/// Parses a node pattern from a token string.
///
/// Extracts the variable name, type schema, term schema, and properties from a node pattern
/// like "(n:Type:Term {prop: value})".
///
/// # Supported Patterns
///
/// * `(var)` - Variable only
/// * `(var:TypeSchema)` - Variable with type
/// * `(var:TypeSchema:TermSchema)` - Variable with type and term
/// * `(:TypeSchema:TermSchema)` - Type and term without variable
/// * `(:TypeSchema)` - Type without variable
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
        return Err(ImplicaError::InvalidPattern {
            pattern: s.to_string(),
            reason: "Node pattern must be enclosed in parentheses".to_string(),
        }
        .into());
    }

    let inner = &s[1..s.len() - 1].trim();

    // Parse: (var:type:term {props}) or (var:type:term) or (var:type) or (var) or (:type:term) or (:type)
    let mut variable = None;
    let mut type_schema = None;
    let mut term_schema = None;
    let mut properties = None;

    if inner.is_empty() {
        // Empty node pattern - matches any node
        return NodePattern::new(None, None, None, None, None, None);
    }

    // Check for properties
    let content = if let Some(brace_idx) = inner.find('{') {
        // Has properties - extract and parse them
        let props_str = &inner[brace_idx..];
        properties = Some(parse_properties(props_str)?);
        inner[..brace_idx].trim()
    } else {
        inner
    };

    // Split by : to parse variable:type_schema:term_schema
    let parts: Vec<&str> = content.split(':').collect();

    match parts.len() {
        1 => {
            // Only one part: could be (var) or empty
            let part = parts[0].trim();
            if !part.is_empty() {
                variable = Some(part.to_string());
            }
        }
        2 => {
            // Two parts: (var:type) or (:type)
            let var_part = parts[0].trim();
            let type_part = parts[1].trim();

            if !var_part.is_empty() {
                variable = Some(var_part.to_string());
            }

            if !type_part.is_empty() {
                type_schema = Some(TypeSchema::new(type_part.to_string())?);
            }
        }
        3 => {
            // Three parts: (var:type:term) or (:type:term)
            let var_part = parts[0].trim();
            let type_part = parts[1].trim();
            let term_part = parts[2].trim();

            if !var_part.is_empty() {
                variable = Some(var_part.to_string());
            }

            if !type_part.is_empty() {
                type_schema = Some(TypeSchema::new(type_part.to_string())?);
            }

            if !term_part.is_empty() {
                term_schema = Some(TermSchema::new(term_part.to_string())?);
            }
        }
        _ => {
            // Too many colons
            return Err(ImplicaError::InvalidPattern{
                pattern: s.to_string(),
                reason: "Node pattern has too many ':' separators. Expected format: (var:TypeSchema:TermSchema)".to_string(),
            }
            .into());
        }
    }

    NodePattern::new(variable, None, type_schema, None, term_schema, properties)
}

/// Parses an edge pattern from a token string.
///
/// Extracts the variable name, type schema, term schema, direction, and properties
/// from an edge pattern like "-[e:Type:Term]->" or "<-[e:Type]-".
///
/// # Supported Patterns
///
/// * `[var]` - Variable only
/// * `[var:TypeSchema]` - Variable with type
/// * `[var:TypeSchema:TermSchema]` - Variable with type and term
/// * `[:TypeSchema:TermSchema]` - Type and term without variable
/// * `[:TypeSchema]` - Type without variable
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
        return Err(ImplicaError::InvalidPattern {
            pattern: s.to_string(),
            reason: "Cannot have both <- and -> in same edge".to_string(),
        }
        .into());
    } else if s.starts_with("<-") || (s.starts_with('<') && s.contains('-')) {
        "backward"
    } else if s.contains("->") || s.ends_with('>') {
        "forward"
    } else {
        "any"
    };

    // Extract the part inside brackets
    let bracket_start = s.find('[').ok_or_else(|| ImplicaError::InvalidPattern {
        pattern: s.to_string(),
        reason: "Edge pattern must contain brackets".to_string(),
    })?;
    let bracket_end = s.rfind(']').ok_or_else(|| ImplicaError::InvalidPattern {
        pattern: s.to_string(),
        reason: "Edge pattern must contain closing bracket".to_string(),
    })?;

    if bracket_end <= bracket_start {
        return Err(ImplicaError::InvalidPattern {
            pattern: s.to_string(),
            reason: "Brackets are mismatched".to_string(),
        }
        .into());
    }

    let inner = &s[bracket_start + 1..bracket_end].trim();

    let mut variable = None;
    let mut type_schema = None;
    let mut term_schema = None;
    let mut properties = None;

    if !inner.is_empty() {
        // Check for properties
        let content = if let Some(brace_idx) = inner.find('{') {
            // Has properties - extract and parse them
            let props_str = &inner[brace_idx..];
            properties = Some(parse_properties(props_str)?);
            inner[..brace_idx].trim()
        } else {
            inner
        };

        // Parse: [var:type:term] or [var:type] or [var] or [:type:term] or [:type]
        let parts: Vec<&str> = content.split(':').collect();

        match parts.len() {
            1 => {
                // Only one part: just variable or empty
                let part = parts[0].trim();
                if !part.is_empty() {
                    variable = Some(part.to_string());
                }
            }
            2 => {
                // Two parts: [var:type] or [:type]
                let var_part = parts[0].trim();
                let type_part = parts[1].trim();

                if !var_part.is_empty() {
                    variable = Some(var_part.to_string());
                }

                if !type_part.is_empty() {
                    type_schema = Some(TypeSchema::new(type_part.to_string())?);
                }
            }
            3 => {
                // Three parts: [var:type:term] or [:type:term]
                let var_part = parts[0].trim();
                let type_part = parts[1].trim();
                let term_part = parts[2].trim();

                if !var_part.is_empty() {
                    variable = Some(var_part.to_string());
                }

                if !type_part.is_empty() {
                    type_schema = Some(TypeSchema::new(type_part.to_string())?);
                }

                if !term_part.is_empty() {
                    term_schema = Some(TermSchema::new(term_part.to_string())?);
                }
            }
            _ => {
                // Too many colons
                return Err(ImplicaError::InvalidPattern{
                    pattern: s.to_string(),
                    reason: "Edge pattern has too many ':' separators. Expected format: [var:TypeSchema:TermSchema]".to_string(),
                }
                .into());
            }
        }
    }
    EdgePattern::new(
        variable,
        None,
        type_schema,
        None,
        term_schema,
        properties,
        direction.to_string(),
    )
}
