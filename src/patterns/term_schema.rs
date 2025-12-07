use pyo3::prelude::*;
use pyo3::types::PyDict;

use std::{fmt::Display, sync::Arc};

use crate::context::{python_to_context, Context, ContextElement};
use crate::errors::ImplicaError;
use crate::typing::{python_to_term, term_to_python, type_to_python, Application, Term};
use crate::utils::validate_variable_name;

#[derive(Clone, Debug, PartialEq)]
enum TermPattern {
    Wildcard,
    Variable(String),
    Application {
        function: Box<TermPattern>,
        argument: Box<TermPattern>,
    },
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct TermSchema {
    #[pyo3(get)]
    pub pattern: String,
    compiled: TermPattern,
}

impl Display for TermSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TermSchema('{}')", self.pattern)
    }
}

#[pymethods]
impl TermSchema {
    #[new]
    pub fn py_new(pattern: String) -> PyResult<Self> {
        TermSchema::new(pattern).map_err(|e| e.into())
    }

    #[pyo3(name = "matches", signature=(term, context = None))]
    pub fn py_matches(
        &self,
        py: Python,
        term: Py<PyAny>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let context_obj = match context.as_ref() {
            Some(c) => Arc::new(python_to_context(c.bind(py))?),
            None => Arc::new(Context::new()),
        };
        let term_obj = python_to_term(term.bind(py))?;

        let result = self.matches(&term_obj, context_obj.clone())?;

        if let Some(c) = context {
            let dict = c.bind(py).cast::<PyDict>()?;

            dict.clear();
            let content = context_obj
                .content
                .read()
                .map_err(|e| ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some("py matches type schema".to_string()),
                })?;
            for (k, v) in content.iter() {
                let t_obj = match v {
                    ContextElement::Type(t) => type_to_python(py, t)?,
                    ContextElement::Term(t) => term_to_python(py, t)?,
                };
                dict.set_item(k.clone(), t_obj)?;
            }
        }

        Ok(result)
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }
}

impl TermSchema {
    pub fn new(pattern: String) -> Result<Self, ImplicaError> {
        let compiled = Self::parse_pattern(&pattern)?;

        Ok(TermSchema { pattern, compiled })
    }

    pub fn matches(&self, term: &Term, context: Arc<Context>) -> Result<bool, ImplicaError> {
        Self::match_pattern(&self.compiled, term, context)
    }

    pub fn as_term(&self, context: Arc<Context>) -> Result<Term, ImplicaError> {
        Self::generate_term(&self.compiled, context)
    }

    fn parse_pattern(input: &str) -> Result<TermPattern, ImplicaError> {
        let trimmed = input.trim();

        // Check for wildcard
        if trimmed == "*" {
            return Ok(TermPattern::Wildcard);
        }

        // Check if it contains spaces (application)
        // For left associativity, we need to find the LAST space, not the first
        if let Some(space_pos) = trimmed.rfind(' ') {
            // Split at the last space for left associativity
            // "f s t" becomes "(f s)" and "t"
            let left_str = trimmed[..space_pos].trim();
            let right_str = trimmed[space_pos + 1..].trim();

            if left_str.is_empty() || right_str.is_empty() {
                return Err(ImplicaError::InvalidPattern {
                    pattern: input.to_string(),
                    reason: "Invalid application pattern: empty left or right side".to_string(),
                });
            }

            // Recursively parse left and right
            let function = Box::new(Self::parse_pattern(left_str)?);
            let argument = Box::new(Self::parse_pattern(right_str)?);

            return Ok(TermPattern::Application { function, argument });
        }

        // Otherwise, it's a variable
        if trimmed.is_empty() {
            return Err(ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: "Invalid pattern: empty string".to_string(),
            });
        }

        validate_variable_name(trimmed)?;
        Ok(TermPattern::Variable(trimmed.to_string()))
    }

    fn match_pattern(
        pattern: &TermPattern,
        term: &Term,
        context: Arc<Context>,
    ) -> Result<bool, ImplicaError> {
        match pattern {
            TermPattern::Wildcard => {
                // Wildcard matches anything
                Ok(true)
            }
            TermPattern::Variable(var_name) => {
                if let Ok(e) = context.get(var_name) {
                    match e {
                        ContextElement::Term(ref t) => Ok(term == t),
                        ContextElement::Type(_) => Err(ImplicaError::ContextConflict {
                            message: "expected context element to be a term but is a type"
                                .to_string(),
                            context: Some("term match pattern".to_string()),
                        }),
                    }
                } else {
                    // Capture the term
                    context.add_term(var_name.clone(), term.clone())?;
                    Ok(true)
                }
            }
            TermPattern::Application { function, argument } => {
                // Term must be an application
                if let Some(app) = term.as_application() {
                    // Match function and argument recursively
                    let function_matches =
                        Self::match_pattern(function, &app.function, context.clone())?;
                    if !function_matches {
                        return Ok(false);
                    }
                    let argument_matches =
                        Self::match_pattern(argument, &app.argument, context.clone())?;
                    Ok(argument_matches)
                } else {
                    Ok(false)
                }
            }
        }
    }

    fn generate_term(pattern: &TermPattern, context: Arc<Context>) -> Result<Term, ImplicaError> {
        match pattern {
            TermPattern::Wildcard => Err(ImplicaError::InvalidPattern {
                pattern: "*".to_string(),
                reason: "cannot use a wild card when describing a term in a create operation"
                    .to_string(),
            }),
            TermPattern::Application { function, argument } => {
                let function_term = Self::generate_term(function, context.clone())?;
                let argument_term = Self::generate_term(argument, context.clone())?;

                Ok(Term::Application(Application::new(
                    function_term,
                    argument_term,
                )?))
            }
            TermPattern::Variable(name) => {
                if let Ok(ref element) = context.get(name) {
                    match element {
                        ContextElement::Term(t) => Ok(t.clone()),
                        ContextElement::Type(_) => Err(ImplicaError::ContextConflict {
                            message: "Tried to access a term variable but it was a type variable."
                                .to_string(),
                            context: Some("generate_term".to_string()),
                        }),
                    }
                } else {
                    Err(ImplicaError::VariableNotFound {
                        name: name.clone(),
                        context: Some("generate_term".to_string()),
                    })
                }
            }
        }
    }
}
