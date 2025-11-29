//! Type pattern matching and schema validation.
//!
//! This module provides the `TypeSchema` structure for defining regex-like patterns
//! that match against types. Schemas support wildcards, variable capture, and
//! Arrow type matching.

use crate::errors::ImplicaError;
use crate::typing::{python_to_type, type_to_python, Type};
use pyo3::prelude::*;
use std::collections::HashMap;

/// Internal representation of a parsed type pattern.
///
/// This enum represents the compiled/parsed form of a pattern string,
/// allowing for efficient matching without re-parsing.
#[derive(Clone, Debug, PartialEq)]
enum Pattern {
    /// Matches any type (*)
    Wildcard,

    /// Matches a specific variable by name
    Variable(String),

    /// Matches an Arrow type with sub-patterns for left and right
    Arrow {
        left: Box<Pattern>,
        right: Box<Pattern>,
    },

    /// Captures a matched type with a given name
    Capture { name: String, pattern: Box<Pattern> },
}

/// Represents a regex-like pattern for matching types.
///
/// Type schemas allow flexible pattern matching on types with support for:
/// - Wildcards: `*` matches any type
/// - Specific variables: `Person` matches only the Variable named "Person"
/// - Arrow patterns: `* -> *` matches Arrow types
/// - Named captures: `(name:pattern)` matches and captures the type
/// - Structural constraints: `(:pattern)` matches without capturing
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Wildcard - matches any type
/// schema = implica.TypeSchema("*")
///
/// # Specific variable - matches only Person
/// schema = implica.TypeSchema("Person")
///
/// # Arrow pattern - matches A -> B
/// schema = implica.TypeSchema("A -> B")
///
/// # Wildcard Arrow - matches any function type
/// schema = implica.TypeSchema("* -> *")
///
/// # Named capture example
/// schema = implica.TypeSchema("(x:*) -> (y:*)")
/// captures = schema.capture(some_type)  # Returns dict with 'x' and 'y'
///
/// # Structural constraint (matches A -> B -> A for any B)
/// schema = implica.TypeSchema("A -> (B:*) -> A")
///
/// # Structural constraint without name (matches app types)
/// schema = implica.TypeSchema("(:*->*) -> B")
/// ```
///
/// # Fields
///
/// * `pattern` - The original pattern string
/// * `compiled` - The compiled pattern for efficient matching
#[pyclass]
#[derive(Clone, Debug)]
pub struct TypeSchema {
    #[pyo3(get)]
    pub pattern: String,

    /// Compiled pattern for efficient matching
    compiled: Pattern,
}

#[pymethods]
impl TypeSchema {
    /// Creates a new type schema from a pattern string.
    ///
    /// # Arguments
    ///
    /// * `pattern` - The pattern string (e.g., "*", "Person", "A -> B")
    ///
    /// # Returns
    ///
    /// A new `TypeSchema` instance
    ///
    /// # Examples
    ///
    /// ```python
    /// # Match any type
    /// any_schema = implica.TypeSchema("*")
    ///
    /// # Match specific variable
    /// person_schema = implica.TypeSchema("Person")
    ///
    /// # Match function type
    /// func_schema = implica.TypeSchema("* -> *")
    /// ```
    #[new]
    pub fn new(pattern: String) -> PyResult<Self> {
        let compiled = Self::parse_pattern(&pattern)?;

        Ok(TypeSchema { pattern, compiled })
    }

    /// Checks if a type matches this schema.
    ///
    /// # Arguments
    ///
    /// * `type` - The type to check (Variable or Arrow)
    ///
    /// # Returns
    ///
    /// `True` if the type matches the schema pattern, `False` otherwise
    ///
    /// # Examples
    ///
    /// ```python
    /// schema = implica.TypeSchema("Person")
    /// person_type = implica.Variable("Person")
    /// assert schema.matches(person_type) == True
    /// ```
    pub fn matches(&self, r#type: Py<PyAny>) -> PyResult<bool> {
        Python::attach(|py| {
            let type_obj = python_to_type(r#type.bind(py))?;
            Ok(self.matches_internal(&type_obj).is_some())
        })
    }

    /// Captures variables from a type that matches this schema.
    ///
    /// If the type matches and the schema contains capture groups like `$(name:pattern)$`,
    /// this returns a dictionary mapping capture names to the matched types.
    ///
    /// # Arguments
    ///
    /// * `type` - The type to match and capture from
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// A Python dictionary with capture names as keys and matched types as values.
    /// Returns an empty dictionary if the type doesn't match or there are no captures.
    ///
    /// # Examples
    ///
    /// ```python
    /// schema = implica.TypeSchema("(input:*) -> (output:*)")
    /// func_type = implica.Arrow(
    ///     implica.Variable("A"),
    ///     implica.Variable("B")
    /// )
    /// captures = schema.capture(func_type)
    /// # captures = {"input": Variable("A"), "output": Variable("B")}
    /// ```
    pub fn capture(&self, r#type: Py<PyAny>, py: Python) -> PyResult<Py<PyAny>> {
        let type_obj = python_to_type(r#type.bind(py))?;
        if let Some(captures) = self.matches_internal(&type_obj) {
            let dict = pyo3::types::PyDict::new(py);
            for (key, val) in captures {
                dict.set_item(key, type_to_python(py, &val)?)?;
            }
            Ok(dict.into())
        } else {
            Ok(pyo3::types::PyDict::new(py).into())
        }
    }

    fn __str__(&self) -> String {
        format!("TypeSchema(\"{}\")", self.pattern)
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl TypeSchema {
    /// Parses a pattern string into a compiled Pattern.
    ///
    /// This is called once during TypeSchema construction for efficient matching.
    fn parse_pattern(input: &str) -> Result<Pattern, ImplicaError> {
        let trimmed = input.trim();

        // Validate balanced parentheses first
        Self::validate_balanced_parentheses(trimmed)?;

        Self::parse_pattern_recursive(trimmed)
    }

    /// Validates that parentheses are balanced in the pattern string.
    fn validate_balanced_parentheses(input: &str) -> Result<(), ImplicaError> {
        let mut depth = 0;

        for ch in input.chars() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth < 0 {
                        return Err(ImplicaError::schema_validation(
                            input,
                            "Unbalanced parentheses: too many closing parentheses",
                        ));
                    }
                }
                _ => {}
            }
        }

        if depth > 0 {
            return Err(ImplicaError::schema_validation(
                input,
                "Unbalanced parentheses: too many opening parentheses",
            ));
        }

        Ok(())
    }

    fn parse_pattern_recursive(input: &str) -> Result<Pattern, ImplicaError> {
        let input = input.trim();

        // Empty pattern is invalid
        if input.is_empty() {
            return Err(ImplicaError::schema_validation(input, "Empty pattern"));
        }

        // Wildcard
        if input == "*" {
            return Ok(Pattern::Wildcard);
        }

        // Check for Arrow pattern FIRST (at top level): left -> right
        // This must be done before checking for captures to handle patterns like "(in:*) -> (out:*)"
        if let Some(arrow_pos) = find_arrow(input) {
            let left_str = input[..arrow_pos].trim();
            let right_str = input[arrow_pos + 2..].trim();

            let left_pattern = Self::parse_pattern_recursive(left_str)?;
            let right_pattern = Self::parse_pattern_recursive(right_str)?;

            return Ok(Pattern::Arrow {
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
                return Ok(Pattern::Capture {
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
            return Err(ImplicaError::schema_validation(
                input,
                "Empty variable name",
            ));
        }

        Ok(Pattern::Variable(input.to_string()))
    }

    /// Internal matching function that returns captures.
    ///
    /// This is the internal implementation used by both `matches()` and `capture()`.
    ///
    /// # Returns
    ///
    /// `Some(HashMap)` with captures if the type matches, `None` otherwise
    fn matches_internal(&self, r#type: &Type) -> Option<HashMap<String, Type>> {
        let mut captures = HashMap::new();
        if Self::match_pattern(&self.compiled, r#type, &mut captures) {
            Some(captures)
        } else {
            None
        }
    }

    /// Recursively matches a pattern against a type.
    fn match_pattern(
        pattern: &Pattern,
        r#type: &Type,
        captures: &mut HashMap<String, Type>,
    ) -> bool {
        match pattern {
            Pattern::Wildcard => {
                // Wildcard matches anything
                true
            }

            Pattern::Variable(name) => {
                // Match only if type is a Variable with the same name
                match r#type {
                    Type::Variable(v) => v.name == *name,
                    _ => false,
                }
            }

            Pattern::Arrow { left, right } => {
                // Match only if type is an Arrow with matching parts
                match r#type {
                    Type::Arrow(app) => {
                        Self::match_pattern(left, &app.left, captures)
                            && Self::match_pattern(right, &app.right, captures)
                    }
                    _ => false,
                }
            }

            Pattern::Capture { name, pattern } => {
                // Try to match the inner pattern
                if Self::match_pattern(pattern, r#type, captures) {
                    // Check if this capture name already exists
                    if let Some(existing) = captures.get(name) {
                        // If it does, verify that the captured types are equal
                        if existing == r#type {
                            // Same value, match succeeds
                            true
                        } else {
                            // Different value, match fails
                            false
                        }
                    } else {
                        // First time capturing this name, insert it
                        captures.insert(name.clone(), r#type.clone());
                        true
                    }
                } else {
                    false
                }
            }
        }
    }

    /// Public helper for Rust code to check if a type matches.
    ///
    /// This is a convenience method for Rust code (not exposed to Python).
    ///
    /// # Arguments
    ///
    /// * `type` - The type to check
    ///
    /// # Returns
    ///
    /// `true` if the type matches, `false` otherwise
    pub fn matches_type(&self, r#type: &Type) -> bool {
        self.matches_internal(r#type).is_some()
    }
}

/// Finds the position of "->" at the correct nesting level (depth 0).
///
/// This helper function locates the arrow operator in a type pattern string,
/// taking into account parenthesis nesting to find the top-level arrow.
///
/// # Arguments
///
/// * `s` - The string to search in
///
/// # Returns
///
/// `Some(usize)` with the position of the arrow if found, `None` otherwise
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

/// Finds the position of ":" at depth 0 (not inside parentheses).
///
/// This is used to parse capture groups like "(name:pattern)".
///
/// # Arguments
///
/// * `s` - The string to search in
///
/// # Returns
///
/// `Some(usize)` with the position of the colon if found, `None` otherwise
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
