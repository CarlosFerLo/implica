use pyo3::prelude::*;

use std::collections::HashMap;

use crate::errors::ImplicaError;

use crate::patterns::term_schema::TermSchema;
use crate::patterns::type_schema::TypeSchema;
use crate::patterns::{edge::EdgePattern, node::NodePattern};

#[derive(Debug, PartialEq)]
pub(in crate::patterns) enum TokenKind {
    Node,
    Edge,
}

#[derive(Debug)]
pub(in crate::patterns) struct Token {
    pub(in crate::patterns) kind: TokenKind,
    pub(in crate::patterns) text: String,
}

pub(in crate::patterns) fn tokenize_pattern(pattern: &str) -> PyResult<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut node_buffer = String::new();
    let mut edge_buffer = String::new();
    let mut node_paren_depth = 0;
    let mut edge_bracket_depth = 0;
    let mut edge_paren_depth = 0; // Track parens INSIDE edge brackets

    #[derive(Debug, PartialEq)]
    enum State {
        Outside, // Not in a node or edge
        InNode,  // Inside a node pattern (...)
        InEdge,  // Inside an edge pattern -[...]-
    }

    let mut state = State::Outside;
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        match state {
            State::Outside => {
                match c {
                    '(' => {
                        // Start of a node
                        state = State::InNode;
                        node_paren_depth = 1;
                        node_buffer.push(c);
                    }
                    '-' | '<' | '>' => {
                        // Start of edge
                        state = State::InEdge;
                        edge_buffer.push(c);
                    }
                    '[' => {
                        // Bracket without arrow prefix
                        state = State::InEdge;
                        edge_bracket_depth = 1;
                        edge_buffer.push(c);
                    }
                    ' ' | '\t' | '\n' | '\r' => {
                        // Skip whitespace
                    }
                    _ => {
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
            State::InNode => {
                node_buffer.push(c);
                match c {
                    '(' => {
                        node_paren_depth += 1;
                    }
                    ')' => {
                        node_paren_depth -= 1;
                        if node_paren_depth == 0 {
                            // End of node
                            tokens.push(Token {
                                kind: TokenKind::Node,
                                text: node_buffer.clone(),
                            });
                            node_buffer.clear();
                            state = State::Outside;
                        }
                    }
                    _ => {}
                }
            }
            State::InEdge => {
                edge_buffer.push(c);
                match c {
                    '[' => {
                        edge_bracket_depth += 1;
                    }
                    ']' => {
                        edge_bracket_depth -= 1;
                    }
                    '(' if edge_bracket_depth > 0 => {
                        // Paren inside brackets
                        edge_paren_depth += 1;
                    }
                    ')' if edge_bracket_depth > 0 => {
                        // Paren inside brackets
                        edge_paren_depth -= 1;
                    }
                    '-' | '>' | '<' => {
                        // Continue edge pattern
                    }
                    ' ' | '\t' | '\n' | '\r' => {
                        // Whitespace in edge
                    }
                    _ if edge_bracket_depth > 0 => {
                        // Any char inside brackets is ok
                    }
                    _ => {
                        return Err(ImplicaError::InvalidPattern {
                            pattern: pattern.to_string(),
                            reason: format!("Unexpected character '{}' in edge pattern", c),
                        }
                        .into());
                    }
                }

                // Check if edge pattern is complete
                // An edge is complete when we're outside brackets and hit a node or whitespace before a node
                if edge_bracket_depth == 0 && edge_paren_depth == 0 {
                    // Look ahead to see if next non-whitespace char is '('
                    let mut j = i + 1;
                    while j < chars.len() && matches!(chars[j], ' ' | '\t' | '\n' | '\r') {
                        j += 1;
                    }
                    if j < chars.len() && chars[j] == '(' {
                        // Next is a node, edge is complete
                        tokens.push(Token {
                            kind: TokenKind::Edge,
                            text: edge_buffer.clone(),
                        });
                        edge_buffer.clear();
                        edge_bracket_depth = 0;
                        edge_paren_depth = 0;
                        state = State::Outside;
                    }
                }
            }
        }

        i += 1;
    }

    // Check for unclosed patterns
    if node_paren_depth != 0 {
        return Err(ImplicaError::InvalidPattern {
            pattern: pattern.to_string(),
            reason: "Unmatched parentheses in pattern".to_string(),
        }
        .into());
    }
    if edge_bracket_depth != 0 {
        return Err(ImplicaError::InvalidPattern {
            pattern: pattern.to_string(),
            reason: "Unmatched brackets in pattern".to_string(),
        }
        .into());
    }

    // Check final state
    if state == State::InEdge {
        return Err(ImplicaError::InvalidPattern {
            pattern: pattern.to_string(),
            reason: "Pattern cannot end with an edge".to_string(),
        }
        .into());
    }

    Ok(tokens)
}

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

fn find_properties_start(s: &str) -> Option<usize> {
    // Find the start of properties section (opening brace not inside parentheses)
    let mut paren_depth = 0;
    let mut bracket_depth = 0;

    for (i, c) in s.char_indices() {
        match c {
            '(' => paren_depth += 1,
            ')' => paren_depth -= 1,
            '[' => bracket_depth += 1,
            ']' => bracket_depth -= 1,
            '{' if paren_depth == 0 && bracket_depth == 0 => {
                return Some(i);
            }
            _ => {}
        }
    }

    None
}

fn smart_split_colons(s: &str) -> PyResult<Vec<String>> {
    // Split by colons, but ignore colons inside parentheses, brackets, and braces
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;
    let mut bracket_depth = 0;
    let mut brace_depth = 0;

    for c in s.chars() {
        match c {
            '(' => {
                paren_depth += 1;
                current.push(c);
            }
            ')' => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    return Err(ImplicaError::InvalidPattern {
                        pattern: s.to_string(),
                        reason: "Unbalanced parentheses in pattern".to_string(),
                    }
                    .into());
                }
                current.push(c);
            }
            '[' => {
                bracket_depth += 1;
                current.push(c);
            }
            ']' => {
                bracket_depth -= 1;
                if bracket_depth < 0 {
                    return Err(ImplicaError::InvalidPattern {
                        pattern: s.to_string(),
                        reason: "Unbalanced brackets in pattern".to_string(),
                    }
                    .into());
                }
                current.push(c);
            }
            '{' => {
                brace_depth += 1;
                current.push(c);
            }
            '}' => {
                brace_depth -= 1;
                if brace_depth < 0 {
                    return Err(ImplicaError::InvalidPattern {
                        pattern: s.to_string(),
                        reason: "Unbalanced braces in pattern".to_string(),
                    }
                    .into());
                }
                current.push(c);
            }
            ':' if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                // Top-level colon - this is a separator
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => {
                current.push(c);
            }
        }
    }

    // Add the last part (trimmed)
    parts.push(current.trim().to_string());

    if paren_depth != 0 {
        return Err(ImplicaError::InvalidPattern {
            pattern: s.to_string(),
            reason: "Unbalanced parentheses in pattern".to_string(),
        }
        .into());
    }

    if bracket_depth != 0 {
        return Err(ImplicaError::InvalidPattern {
            pattern: s.to_string(),
            reason: "Unbalanced brackets in pattern".to_string(),
        }
        .into());
    }

    if brace_depth != 0 {
        return Err(ImplicaError::InvalidPattern {
            pattern: s.to_string(),
            reason: "Unbalanced braces in pattern".to_string(),
        }
        .into());
    }

    Ok(parts)
}
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

    // Check for properties - need to find the LAST { that's not inside parentheses
    let content = if let Some(brace_idx) = find_properties_start(inner) {
        // Has properties - extract and parse them
        let props_str = &inner[brace_idx..];
        properties = Some(parse_properties(props_str)?);
        inner[..brace_idx].trim()
    } else {
        inner
    };

    // Smart split by : to parse variable:type_schema:term_schema
    // Need to handle nested parentheses and arrows in type schemas
    let parts = smart_split_colons(content)?;

    match parts.len() {
        1 => {
            // Only one part: could be (var) or (:type) - need to distinguish
            let part = parts[0].trim();
            if !part.is_empty() {
                // Check if it looks like a TypeSchema (contains ->, *, or starts with ()
                if part.contains("->") || part.contains('*') || part.starts_with('(') {
                    type_schema = Some(TypeSchema::new(part.to_string())?);
                } else {
                    variable = Some(part.to_string());
                }
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

pub(in crate::patterns) fn parse_edge_pattern(s: &str) -> PyResult<EdgePattern> {
    let s = s.trim();

    // Extract the part inside brackets first
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

    // Determine direction based on arrows OUTSIDE the brackets
    // Patterns: -[e]-> (forward), <-[e]- (backward), -[e]- (any)
    let before_bracket = &s[..bracket_start];
    let after_bracket = &s[bracket_end + 1..];

    let direction = if before_bracket.contains("<-") && after_bracket.contains("->") {
        return Err(ImplicaError::InvalidPattern {
            pattern: s.to_string(),
            reason: "Cannot have both <- and -> in same edge".to_string(),
        }
        .into());
    } else if before_bracket.contains("<-") || before_bracket.contains('<') {
        "backward"
    } else if after_bracket.contains("->") || after_bracket.contains('>') {
        "forward"
    } else {
        "any"
    };

    let inner = &s[bracket_start + 1..bracket_end].trim();

    let mut variable = None;
    let mut type_schema = None;
    let mut term_schema = None;
    let mut properties = None;

    if !inner.is_empty() {
        // Check for properties - need to find the LAST { that's not inside parentheses
        let content = if let Some(brace_idx) = find_properties_start(inner) {
            // Has properties - extract and parse them
            let props_str = &inner[brace_idx..];
            properties = Some(parse_properties(props_str)?);
            inner[..brace_idx].trim()
        } else {
            inner
        };

        // Parse: [var:type:term] or [var:type] or [var] or [:type:term] or [:type]
        // Use smart_split_colons to handle colons inside TypeSchemas
        let parts = smart_split_colons(content)?;

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
