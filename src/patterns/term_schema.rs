use pyo3::prelude::*;
use pyo3::types::PyDict;

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use crate::context::{python_to_context, Context, ContextElement};
use crate::errors::ImplicaError;
use crate::patterns::TypeSchema;
use crate::typing::{python_to_term, term_to_python, type_to_python, Application, Constant, Term};
use crate::utils::validate_variable_name;

#[derive(Clone, Debug, PartialEq)]
enum TermPattern {
    Wildcard,
    Variable(String),
    Application {
        function: Box<TermPattern>,
        argument: Box<TermPattern>,
    },
    Constant {
        name: String,
        args: Vec<String>,
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

    #[pyo3(name = "matches", signature=(term, context = None, constants=None))]
    pub fn py_matches(
        &self,
        py: Python,
        term: Py<PyAny>,
        context: Option<Py<PyAny>>,
        constants: Option<Vec<Constant>>,
    ) -> PyResult<bool> {
        let mut context_obj = match context.as_ref() {
            Some(c) => python_to_context(c.bind(py))?,
            None => Context::new(),
        };
        let term_obj = python_to_term(term.bind(py))?;
        let constants = match constants {
            Some(cts) => Arc::new(cts.iter().map(|c| (c.name.clone(), c.clone())).collect()),
            None => Arc::new(HashMap::new()),
        };

        let result = self.matches(&term_obj, &mut context_obj, constants)?;

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

    fn __eq__(&self, other: TermSchema) -> bool {
        self.compiled == other.compiled
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

    pub fn matches(
        &self,
        term: &Term,
        context: &mut Context,
        constants: Arc<HashMap<String, Constant>>,
    ) -> Result<bool, ImplicaError> {
        Self::match_pattern(&self.compiled, term, context, constants)
    }

    pub fn as_term(
        &self,
        context: &Context,
        constants: Arc<HashMap<String, Constant>>,
    ) -> Result<Term, ImplicaError> {
        Self::generate_term(&self.compiled, context, constants)
    }

    fn parse_pattern(input: &str) -> Result<TermPattern, ImplicaError> {
        let trimmed = input.trim();

        // Check for wildcard
        if trimmed == "*" {
            return Ok(TermPattern::Wildcard);
        }

        // Check if it contains spaces (application)
        // For left associativity, we need to find the LAST space at depth 0 (not inside parentheses)
        if let Some(space_pos) = Self::find_last_space_at_depth_zero(trimmed) {
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

        // Check for constant pattern: @ConstantName(Arg1, Arg2, ...)
        if trimmed.starts_with('@') {
            return Self::parse_constant_pattern(trimmed);
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

    fn find_last_space_at_depth_zero(input: &str) -> Option<usize> {
        let mut paren_depth = 0;
        let mut last_space_pos = None;

        for (i, ch) in input.char_indices() {
            match ch {
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                ' ' if paren_depth == 0 => last_space_pos = Some(i),
                _ => {}
            }
        }

        last_space_pos
    }

    fn parse_constant_pattern(input: &str) -> Result<TermPattern, ImplicaError> {
        // Input should be like: @K(A, B) or @S(A, A->B, C)
        if !input.starts_with('@') {
            return Err(ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: "Constant pattern must start with '@'".to_string(),
            });
        }

        // Find the opening parenthesis
        let paren_start = input
            .find('(')
            .ok_or_else(|| ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: "Constant pattern must have parentheses with type arguments".to_string(),
            })?;

        // Extract constant name (everything between @ and '(')
        let name = input[1..paren_start].trim().to_string();

        if name.is_empty() {
            return Err(ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: "Constant name cannot be empty".to_string(),
            });
        }

        // Find the matching closing parenthesis
        let paren_end = Self::find_matching_closing_paren(input, paren_start)?;

        // Verify that the constant pattern ends at the closing parenthesis (no trailing content)
        if paren_end != input.len() - 1 {
            return Err(ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: format!(
                    "Constant pattern has unexpected content after closing parenthesis at position {}",
                    paren_end
                ),
            });
        }

        // Extract the arguments string (everything between '(' and ')')
        let args_str = input[paren_start + 1..paren_end].trim();

        // Parse the arguments - split by comma, but be careful with nested structures
        let args = if args_str.is_empty() {
            Vec::new()
        } else {
            Self::split_type_arguments(args_str)?
        };

        Ok(TermPattern::Constant { name, args })
    }

    fn find_matching_closing_paren(input: &str, open_pos: usize) -> Result<usize, ImplicaError> {
        let mut depth = 0;

        for (i, ch) in input[open_pos..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(open_pos + i);
                    }
                }
                _ => {}
            }
        }

        Err(ImplicaError::InvalidPattern {
            pattern: input.to_string(),
            reason: "Constant pattern has unmatched opening parenthesis".to_string(),
        })
    }

    fn split_type_arguments(args_str: &str) -> Result<Vec<String>, ImplicaError> {
        let mut args = Vec::new();
        let mut current_arg = String::new();
        let mut paren_depth = 0;

        for ch in args_str.chars() {
            match ch {
                '(' => {
                    paren_depth += 1;
                    current_arg.push(ch);
                }
                ')' => {
                    paren_depth -= 1;
                    current_arg.push(ch);
                }
                ',' if paren_depth == 0 => {
                    // This comma is a separator at the top level
                    let trimmed = current_arg.trim().to_string();
                    if !trimmed.is_empty() {
                        args.push(trimmed);
                    }
                    current_arg.clear();
                }
                _ => {
                    current_arg.push(ch);
                }
            }
        }

        // Don't forget the last argument
        let trimmed = current_arg.trim().to_string();
        if !trimmed.is_empty() {
            args.push(trimmed);
        }

        if paren_depth != 0 {
            return Err(ImplicaError::InvalidPattern {
                pattern: args_str.to_string(),
                reason: "Mismatched parentheses in constant type arguments".to_string(),
            });
        }

        Ok(args)
    }

    fn match_pattern(
        pattern: &TermPattern,
        term: &Term,
        context: &mut Context,
        constants: Arc<HashMap<String, Constant>>,
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
                        Self::match_pattern(function, &app.function, context, constants.clone())?;
                    if !function_matches {
                        return Ok(false);
                    }
                    let argument_matches =
                        Self::match_pattern(argument, &app.argument, context, constants.clone())?;
                    Ok(argument_matches)
                } else {
                    Ok(false)
                }
            }
            TermPattern::Constant { name, args } => {
                if let Some(constant) = constants.get(name) {
                    let args: Vec<_> = args
                        .iter()
                        .map(|s| match TypeSchema::new(s.to_string()) {
                            Ok(schema) => {
                                schema.as_type(context)
                            }
                            Err(e) => Err(ImplicaError::InvalidQuery {
                                message: format!(
                                    "could not parse type argument passed to constant: '{}', Error: '{}'",
                                    s, e
                                ),
                                context: Some("match term schema".to_string()),
                            }),
                        })
                        .collect::<Result<_, _ >>()?;

                    let const_term = constant.apply(&args)?;
                    Ok(term == &const_term)
                } else {
                    Err(ImplicaError::ConstantNotFound {
                        name: name.clone(),
                        context: Some("match term pattern".to_string()),
                    })
                }
            }
        }
    }

    fn generate_term(
        pattern: &TermPattern,
        context: &Context,
        constants: Arc<HashMap<String, Constant>>,
    ) -> Result<Term, ImplicaError> {
        match pattern {
            TermPattern::Wildcard => Err(ImplicaError::InvalidPattern {
                pattern: "*".to_string(),
                reason: "cannot use a wild card when describing a term in a create operation"
                    .to_string(),
            }),
            TermPattern::Application { function, argument } => {
                let function_term = Self::generate_term(function, context, constants.clone())?;
                let argument_term = Self::generate_term(argument, context, constants.clone())?;

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
            TermPattern::Constant { name, args } => {
                if let Some(constant) = constants.get(name) {
                    let args: Vec<_> = args
                        .iter()
                        .map(|s| match TypeSchema::new(s.to_string()) {
                            Ok(schema) => {
                                schema.as_type(context)
                            }
                            Err(e) => Err(ImplicaError::InvalidQuery {
                                message: format!(
                                    "could not parse type argument passed to constant: '{}', Error: '{}'",
                                    s, e
                                ),
                                context: Some("match term schema".to_string()),
                            }),
                        })
                        .collect::<Result<_, _ >>()?;

                    let const_term = constant.apply(&args)?;

                    Ok(const_term)
                } else {
                    Err(ImplicaError::ConstantNotFound {
                        name: name.clone(),
                        context: Some("match term pattern".to_string()),
                    })
                }
            }
        }
    }
}
