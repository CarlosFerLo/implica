use crate::errors::ImplicaError;

use crate::utils::validate_variable_name;
use pyo3::prelude::*;

use std::fmt::Display;

#[derive(Clone, Debug, PartialEq)]
pub enum TypePattern {
    Wildcard,
    Variable(String),
    Arrow {
        left: Box<TypePattern>,
        right: Box<TypePattern>,
    },
    Capture {
        name: String,
        pattern: Box<TypePattern>,
    },
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct TypeSchema {
    #[pyo3(get)]
    pub pattern: String,

    pub compiled: TypePattern,
}

impl Display for TypeSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TypeSchema('{}')", self.pattern)
    }
}

#[pymethods]
impl TypeSchema {
    #[new]
    pub fn py_new(pattern: String) -> PyResult<Self> {
        TypeSchema::new(pattern).map_err(|e| e.into())
    }

    fn __eq__(&self, other: TypeSchema) -> bool {
        self.compiled == other.compiled
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }
}

impl TypeSchema {
    pub fn new(pattern: String) -> Result<Self, ImplicaError> {
        let compiled = Self::parse_pattern(&pattern)?;

        Ok(TypeSchema { pattern, compiled })
    }

    fn parse_pattern(input: &str) -> Result<TypePattern, ImplicaError> {
        let trimmed = input.trim();

        Self::validate_balanced_parentheses(trimmed)?;

        Self::parse_pattern_recursive(trimmed)
    }

    fn validate_balanced_parentheses(input: &str) -> Result<(), ImplicaError> {
        let mut depth = 0;

        for ch in input.chars() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth < 0 {
                        return Err(ImplicaError::SchemaValidation {
                            schema: input.to_string(),
                            reason: "Unbalanced parentheses: too many closing parentheses"
                                .to_string(),
                        });
                    }
                }
                _ => {}
            }
        }

        if depth > 0 {
            return Err(ImplicaError::SchemaValidation {
                schema: input.to_string(),
                reason: "Unbalanced parentheses: too many opening parentheses".to_string(),
            });
        }

        Ok(())
    }

    fn parse_pattern_recursive(input: &str) -> Result<TypePattern, ImplicaError> {
        let input = input.trim();

        // Empty pattern is invalid
        if input.is_empty() {
            return Err(ImplicaError::SchemaValidation {
                schema: input.to_string(),
                reason: "Empty pattern".to_string(),
            });
        }

        // Wildcard
        if input == "*" {
            return Ok(TypePattern::Wildcard);
        }

        // Check for Arrow pattern FIRST (at top level): left -> right
        // This must be done before checking for captures to handle patterns like "(in:*) -> (out:*)"
        if let Some(arrow_pos) = find_arrow(input) {
            let left_str = input[..arrow_pos].trim();
            let right_str = input[arrow_pos + 2..].trim();

            let left_pattern = Self::parse_pattern_recursive(left_str)?;
            let right_pattern = Self::parse_pattern_recursive(right_str)?;

            return Ok(TypePattern::Arrow {
                left: Box::new(left_pattern),
                right: Box::new(right_pattern),
            });
        }

        // Check for capture group: (name:pattern) or (:pattern)
        // Only checked if no top-level arrow was found
        if input.starts_with('(') && input.ends_with(')') {
            let inner = &input[1..input.len() - 1];

            // Look for colon at the right depth
            if let Some(colon_pos) = find_colon_at_depth_zero(inner) {
                let name_part = inner[..colon_pos].trim();
                let pattern_part = inner[colon_pos + 1..].trim();

                // Parse the inner pattern
                let inner_pattern = Self::parse_pattern_recursive(pattern_part)?;

                // If name is empty, it's a structural constraint without capture
                if name_part.is_empty() {
                    return Ok(inner_pattern);
                }

                // Otherwise it's a named capture

                validate_variable_name(name_part)?;

                return Ok(TypePattern::Capture {
                    name: name_part.to_string(),
                    pattern: Box::new(inner_pattern),
                });
            }

            // No colon found - might be a simple parenthesized expression
            // Remove the parentheses and parse again
            return Self::parse_pattern_recursive(inner);
        }

        // If no special syntax, treat as variable name
        // Variable names should not be empty
        if input.is_empty() {
            return Err(ImplicaError::SchemaValidation {
                schema: input.to_string(),
                reason: "Empty variable name".to_string(),
            });
        }

        validate_variable_name(input)?;
        Ok(TypePattern::Variable(input.to_string()))
    }
}

fn find_arrow(s: &str) -> Option<usize> {
    let mut depth = 0;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '(' => depth += 1,
            ')' => depth -= 1,
            '-' if i + 1 < chars.len() && chars[i + 1] == '>' && depth == 0 => {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn find_colon_at_depth_zero(s: &str) -> Option<usize> {
    let mut depth = 0;
    let chars: Vec<char> = s.chars().collect();

    for (i, &ch) in chars.iter().enumerate() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ':' if depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}
