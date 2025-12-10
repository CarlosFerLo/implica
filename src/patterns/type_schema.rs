use crate::context::{python_to_context, Context, ContextElement};
use crate::errors::ImplicaError;
use crate::typing::{python_to_type, term_to_python, type_to_python, Arrow, Type, Variable};
use crate::utils::validate_variable_name;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use std::fmt::Display;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
enum TypePattern {
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

    compiled: TypePattern,
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

    #[pyo3(name = "matches", signature = (r#type, context = None))]
    pub fn py_matches(
        &self,
        py: Python,
        r#type: Py<PyAny>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let mut context_obj = match context.as_ref() {
            Some(c) => python_to_context(c.bind(py))?,
            None => Context::new(),
        };
        let type_obj = python_to_type(r#type.bind(py))?;

        let result = self.matches(&type_obj, &mut context_obj)?;

        if let Some(c) = context {
            let dict = c.bind(py).cast::<PyDict>()?;

            dict.clear();

            for (k, v) in context_obj.content.iter() {
                let t_obj = match v {
                    ContextElement::Type(t) => type_to_python(py, t)?,
                    ContextElement::Term(t) => term_to_python(py, t)?,
                };
                dict.set_item(k.clone(), t_obj)?;
            }
        }

        Ok(result)
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

    pub fn matches(&self, r#type: &Type, context: &mut Context) -> Result<bool, ImplicaError> {
        Self::match_pattern(&self.compiled, r#type, context)
    }

    pub fn as_type(&self, context: &Context) -> Result<Type, ImplicaError> {
        Self::generate_type(&self.compiled, context)
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

    /// Recursively matches a pattern against a type.
    fn match_pattern(
        pattern: &TypePattern,
        r#type: &Type,
        context: &mut Context,
    ) -> Result<bool, ImplicaError> {
        match pattern {
            TypePattern::Wildcard => {
                // Wildcard matches anything
                Ok(true)
            }

            TypePattern::Variable(name) => {
                if let Ok(e) = context.get(name) {
                    match e {
                        ContextElement::Type(ref t) => {
                            return Ok(r#type == t);
                        }
                        ContextElement::Term(_) => {
                            return Err(ImplicaError::ContextConflict {
                                message: "expected context element to be a type but is a term"
                                    .to_string(),
                                context: Some("type match pattern".to_string()),
                            });
                        }
                    }
                }
                // Match only if type is a Variable with the same name
                match r#type {
                    Type::Variable(v) => Ok(v.name == *name),
                    _ => Ok(false),
                }
            }

            TypePattern::Arrow { left, right } => {
                // Match only if type is an Arrow with matching parts
                match r#type {
                    Type::Arrow(app) => {
                        let result = Self::match_pattern(left, &app.left, context)?
                            && Self::match_pattern(right, &app.right, context)?;

                        Ok(result)
                    }
                    _ => Ok(false),
                }
            }

            TypePattern::Capture { name, pattern } => {
                // Try to match the inner pattern
                if Self::match_pattern(pattern, r#type, context)? {
                    if let Ok(e) = context.get(name) {
                        match e {
                            ContextElement::Type(ref t) => Ok(r#type == t),
                            ContextElement::Term(_) => Err(ImplicaError::ContextConflict {
                                message: "expected context element to be a type but is a term"
                                    .to_string(),
                                context: Some("type match pattern".to_string()),
                            }),
                        }
                    } else {
                        // First time capturing this name, insert it
                        context.add_type(name.clone(), r#type.clone())?;
                        Ok(true)
                    }
                } else {
                    Ok(false)
                }
            }
        }
    }

    fn generate_type(pattern: &TypePattern, context: &Context) -> Result<Type, ImplicaError> {
        match pattern {
            TypePattern::Wildcard => Err(ImplicaError::InvalidPattern {
                pattern: "*".to_string(),
                reason: "cannot use a wild card when describing a type in a create operation"
                    .to_string(),
            }),
            TypePattern::Capture { .. } => Err(ImplicaError::InvalidPattern {
                pattern: "()".to_string(),
                reason: "cannot use a capture when describing a type in a create operation"
                    .to_string(),
            }),
            TypePattern::Arrow { left, right } => {
                let left_type = Self::generate_type(left, context)?;
                let right_type = Self::generate_type(right, context)?;

                Ok(Type::Arrow(Arrow::new(
                    Arc::new(left_type),
                    Arc::new(right_type),
                )))
            }
            TypePattern::Variable(name) => {
                if let Ok(ref element) = context.get(name) {
                    match element {
                        ContextElement::Type(r#type) => Ok(r#type.clone()),
                        ContextElement::Term(_) => Err(ImplicaError::ContextConflict {
                            message: "Tried to access a type variable but it was a term variable."
                                .to_string(),
                            context: Some("generate_type".to_string()),
                        }),
                    }
                } else {
                    Ok(Type::Variable(Variable::new(name.clone())?))
                }
            }
        }
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
