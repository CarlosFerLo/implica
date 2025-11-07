//! Type pattern matching and schema validation.
//!
//! This module provides the `TypeSchema` structure for defining regex-like patterns
//! that match against types. Schemas support wildcards, variable capture, and
//! application type matching.

use crate::types::Type;
use pyo3::prelude::*;
use std::collections::HashMap;

/// Represents a regex-like pattern for matching types.
///
/// Type schemas allow flexible pattern matching on types with support for:
/// - Wildcards: `$*$` matches any type
/// - Specific variables: `$Person$` matches only the Variable named "Person"
/// - Application patterns: `$A -> B$` matches Application types
/// - Capture groups: `$(name:pattern)$` matches and captures the type
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Wildcard - matches any type
/// schema = implica.TypeSchema("$*$")
///
/// # Specific variable - matches only Person
/// schema = implica.TypeSchema("$Person$")
///
/// # Application pattern - matches A -> B
/// schema = implica.TypeSchema("$A -> B$")
///
/// # Wildcard application - matches any function type
/// schema = implica.TypeSchema("$* -> *$")
///
/// # Capture example
/// schema = implica.TypeSchema("$(x:*) -> $(y:*)$")
/// captures = schema.capture(some_type)  # Returns dict with 'x' and 'y'
/// ```
///
/// # Fields
///
/// * `pattern` - The pattern string defining the schema
#[pyclass]
#[derive(Clone, Debug)]
pub struct TypeSchema {
    #[pyo3(get)]
    pub pattern: String,
}

#[pymethods]
impl TypeSchema {
    /// Creates a new type schema from a pattern string.
    ///
    /// # Arguments
    ///
    /// * `pattern` - The pattern string (e.g., "$*$", "$Person$", "$A -> B$")
    ///
    /// # Returns
    ///
    /// A new `TypeSchema` instance
    ///
    /// # Examples
    ///
    /// ```python
    /// # Match any type
    /// any_schema = implica.TypeSchema("$*$")
    ///
    /// # Match specific variable
    /// person_schema = implica.TypeSchema("$Person$")
    ///
    /// # Match function type
    /// func_schema = implica.TypeSchema("$A -> B$")
    /// ```
    #[new]
    pub fn new(pattern: String) -> Self {
        TypeSchema { pattern }
    }

    /// Checks if a type matches this schema.
    ///
    /// # Arguments
    ///
    /// * `type` - The type to check (Variable or Application)
    ///
    /// # Returns
    ///
    /// `True` if the type matches the schema pattern, `False` otherwise
    ///
    /// # Examples
    ///
    /// ```python
    /// schema = implica.TypeSchema("$Person$")
    /// person_type = implica.Variable("Person")
    /// assert schema.matches(person_type) == True
    /// ```
    pub fn matches(&self, r#type: Py<PyAny>) -> PyResult<bool> {
        Python::with_gil(|py| {
            let type_obj = crate::types::python_to_type(r#type.bind(py))?;
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
    /// schema = implica.TypeSchema("$(input:*) -> $(output:*)$")
    /// func_type = implica.Application(
    ///     implica.Variable("A"),
    ///     implica.Variable("B")
    /// )
    /// captures = schema.capture(func_type)
    /// # captures = {"input": Variable("A"), "output": Variable("B")}
    /// ```
    pub fn capture(&self, r#type: Py<PyAny>, py: Python) -> PyResult<PyObject> {
        let type_obj = crate::types::python_to_type(r#type.bind(py))?;
        if let Some(captures) = self.matches_internal(&type_obj) {
            let dict = pyo3::types::PyDict::new(py);
            for (key, val) in captures {
                dict.set_item(key, crate::types::type_to_python(py, &val)?)?;
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
    /// Internal matching function that returns captures.
    ///
    /// This is the internal implementation used by both `matches()` and `capture()`.
    ///
    /// # Returns
    ///
    /// `Some(HashMap)` with captures if the type matches, `None` otherwise
    fn matches_internal(&self, r#type: &Type) -> Option<HashMap<String, Type>> {
        let mut captures = HashMap::new();
        if self.matches_recursive(&self.pattern, r#type, &mut captures) {
            Some(captures)
        } else {
            None
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

    #[allow(clippy::only_used_in_recursion)]
    fn matches_recursive(
        &self,
        pattern: &str,
        r#type: &Type,
        captures: &mut HashMap<String, Type>,
    ) -> bool {
        let pattern = pattern.trim();

        // Wildcard: matches anything
        if pattern == "$*$" {
            return true;
        }

        // Variable matching: $name$
        if pattern.starts_with('$')
            && pattern.ends_with('$')
            && !pattern.contains("->")
            && !pattern.contains(':')
            && !pattern.contains('(')
        {
            let var_name = &pattern[1..pattern.len() - 1];
            if var_name == "*" {
                return true;
            }
            if let Type::Variable(v) = r#type {
                return v.name == var_name;
            }
            return false;
        }

        // Capture: $(name:pattern)$
        if pattern.starts_with("$(") && pattern.ends_with(")$") {
            let inner = &pattern[2..pattern.len() - 2];
            if let Some(colon_idx) = inner.find(':') {
                let capture_name = inner[..colon_idx].trim();
                let sub_pattern = inner[colon_idx + 1..].trim();
                let sub_pattern_with_delim = format!("${}$", sub_pattern);
                if self.matches_recursive(&sub_pattern_with_delim, r#type, captures) {
                    captures.insert(capture_name.to_string(), r#type.clone());
                    return true;
                }
            }
            return false;
        }

        // Application matching: $(a -> b)$ or $a -> b$ or similar
        if pattern.contains("->") {
            // Remove outer $ delimiters if present
            let inner_pattern = if pattern.starts_with('$') && pattern.ends_with('$') {
                &pattern[1..pattern.len() - 1]
            } else {
                pattern
            };

            // Remove outer parentheses if present
            let inner_pattern = if inner_pattern.starts_with('(') && inner_pattern.ends_with(')') {
                &inner_pattern[1..inner_pattern.len() - 1]
            } else {
                inner_pattern
            };

            if let Some(arrow_idx) = find_arrow(inner_pattern) {
                let left_pattern = inner_pattern[..arrow_idx].trim();
                let right_pattern = inner_pattern[arrow_idx + 2..].trim();

                if let Type::Application(app) = r#type {
                    let left_pat_with_delim = format!("${}$", left_pattern);
                    let right_pat_with_delim = format!("${}$", right_pattern);
                    return self.matches_recursive(&left_pat_with_delim, &app.left, captures)
                        && self.matches_recursive(&right_pat_with_delim, &app.right, captures);
                }
            }
            return false;
        }

        // If no special syntax, treat as variable name
        if let Type::Variable(v) = r#type {
            return v.name == pattern;
        }

        false
    }
}

/// Finds the position of "->" at the correct nesting level.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Application, Type, Variable};

    #[test]
    fn test_wildcard_schema() {
        let schema = TypeSchema::new("$*$".to_string());
        let var_a = Type::Variable(Variable::new("A".to_string()));
        assert!(schema.matches_type(&var_a));
    }

    #[test]
    fn test_variable_schema() {
        let schema = TypeSchema::new("$A$".to_string());
        let var_a = Type::Variable(Variable::new("A".to_string()));
        let var_b = Type::Variable(Variable::new("B".to_string()));
        assert!(schema.matches_type(&var_a));
        assert!(!schema.matches_type(&var_b));
    }

    #[test]
    fn test_application_schema() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let var_a = Py::new(py, Variable::new("A".to_string())).unwrap();
            let var_b = Py::new(py, Variable::new("B".to_string())).unwrap();
            let app = Application::new(var_a.into(), var_b.into()).unwrap();
            let app_type = Type::Application(app);

            let schema = TypeSchema::new("$A -> B$".to_string());
            assert!(schema.matches_type(&app_type));

            let schema2 = TypeSchema::new("$A -> *$".to_string());
            assert!(schema2.matches_type(&app_type));

            let schema3 = TypeSchema::new("$* -> *$".to_string());
            assert!(schema3.matches_type(&app_type));
        });
    }
}
