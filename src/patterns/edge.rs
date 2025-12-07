use pyo3::prelude::*;

use std::collections::HashMap;
use std::sync::Arc;

use crate::context::Context;
use crate::errors::ImplicaError;
use crate::graph::Edge;
use crate::patterns::term_schema::TermSchema;
use crate::patterns::type_schema::TypeSchema;
use crate::typing::{Term, Type};
use crate::utils::validate_variable_name;

#[derive(Clone, Debug, PartialEq)]
enum CompiledDirection {
    Forward,
    Backward,
    Any,
}

impl CompiledDirection {
    fn from_string(s: &str) -> Result<Self, ImplicaError> {
        match s {
            "forward" => Ok(CompiledDirection::Forward),
            "backward" => Ok(CompiledDirection::Backward),
            "any" => Ok(CompiledDirection::Any),
            _ => Err(ImplicaError::SchemaValidation {
                schema: s.to_string(),
                reason: "Direction must be 'forward', 'backward', or 'any'".to_string(),
            }),
        }
    }

    fn to_string(&self) -> &'static str {
        match self {
            CompiledDirection::Forward => "forward",
            CompiledDirection::Backward => "backward",
            CompiledDirection::Any => "any",
        }
    }
}

#[derive(Clone, Debug)]
enum CompiledTypeEdgeMatcher {
    Any,
    ExactType(Arc<Type>),
    SchemaTerm(TypeSchema),
}

#[derive(Clone, Debug)]
enum CompiledTermEdgeMatcher {
    Any,
    ExactTerm(Arc<Term>),
    SchemaTerm(TermSchema),
}

#[pyclass]
#[derive(Debug)]
pub struct EdgePattern {
    #[pyo3(get)]
    pub variable: Option<String>,
    /// Compiled matcher for efficient term checking
    compiled_term_matcher: CompiledTermEdgeMatcher,
    compiled_type_matcher: CompiledTypeEdgeMatcher,
    compiled_direction: CompiledDirection,
    pub properties: HashMap<String, Py<PyAny>>,

    // Keep these for backward compatibility and introspection
    pub term: Option<Arc<Term>>,
    pub type_schema: Option<TypeSchema>,
    pub r#type: Option<Arc<Type>>,
    pub term_schema: Option<TermSchema>,
}

impl Clone for EdgePattern {
    fn clone(&self) -> Self {
        Python::attach(|py| {
            let mut props = HashMap::new();
            for (k, v) in &self.properties {
                props.insert(k.clone(), v.clone_ref(py));
            }
            EdgePattern {
                variable: self.variable.clone(),
                compiled_type_matcher: self.compiled_type_matcher.clone(),
                compiled_term_matcher: self.compiled_term_matcher.clone(),
                compiled_direction: self.compiled_direction.clone(),
                properties: props,
                term: self.term.clone(),
                r#type: self.r#type.clone(),
                type_schema: self.type_schema.clone(),
                term_schema: self.term_schema.clone(),
            }
        })
    }
}

#[pymethods]
impl EdgePattern {
    #[getter]
    pub fn direction(&self) -> String {
        self.compiled_direction.to_string().to_string()
    }

    fn __repr__(&self) -> String {
        let term_info = if self.term.is_some() {
            ", term=<specified>"
        } else if self.type_schema.is_some() {
            ", type_schema=<specified>"
        } else {
            ""
        };
        format!(
            "EdgePattern(variable={:?}, direction={}{})",
            self.variable,
            self.compiled_direction.to_string(),
            term_info
        )
    }
}

impl EdgePattern {
    pub fn new(
        variable: Option<String>,
        r#type: Option<Arc<Type>>,
        type_schema: Option<TypeSchema>,
        term: Option<Arc<Term>>,
        term_schema: Option<TermSchema>,
        properties: Option<HashMap<String, Py<PyAny>>>,
        direction: String,
    ) -> PyResult<Self> {
        if let Some(ref var) = variable {
            validate_variable_name(var)?;
        }

        let compiled_direction = CompiledDirection::from_string(&direction)?;

        if term.is_some() && term_schema.is_some() {
            return Err(ImplicaError::InvalidPattern {
                pattern: "EdgePattern".to_string(),
                reason:
                    "Cannot specify both 'term' and 'term_schema' - they are mutually exclusive"
                        .to_string(),
            }
            .into());
        }

        if r#type.is_some() && type_schema.is_some() {
            return Err(ImplicaError::InvalidPattern {
                pattern: "EdgePattern".to_string(),
                reason:
                    "Cannot specify both 'type' and 'type_schema' - they are mutually exclusive"
                        .to_string(),
            }
            .into());
        }

        let compiled_term_matcher = if let Some(t) = term.clone() {
            CompiledTermEdgeMatcher::ExactTerm(t.clone())
        } else if let Some(t) = term_schema.clone() {
            CompiledTermEdgeMatcher::SchemaTerm(t)
        } else {
            CompiledTermEdgeMatcher::Any
        };

        let compiled_type_matcher = if let Some(t) = r#type.clone() {
            CompiledTypeEdgeMatcher::ExactType(t.clone())
        } else if let Some(t) = type_schema.clone() {
            CompiledTypeEdgeMatcher::SchemaTerm(t.clone())
        } else {
            CompiledTypeEdgeMatcher::Any
        };

        Ok(EdgePattern {
            variable,
            compiled_type_matcher,
            compiled_term_matcher,
            compiled_direction,
            properties: properties.unwrap_or_default(),
            term: term.clone(),
            r#type: r#type.clone(),
            type_schema,
            term_schema,
        })
    }

    pub fn matches_direction(&self, forward: bool) -> bool {
        match self.compiled_direction {
            CompiledDirection::Any => true,
            CompiledDirection::Forward => forward,
            CompiledDirection::Backward => !forward,
        }
    }

    pub fn matches(&self, edge: &Edge, context: Arc<Context>) -> PyResult<bool> {
        // Check term using compiled matcher (most efficient path)
        match &self.compiled_type_matcher {
            CompiledTypeEdgeMatcher::Any => {
                // No term constraint, continue to property check
            }
            CompiledTypeEdgeMatcher::ExactType(type_obj) => {
                let edge_term = edge.term.clone();

                if &*edge_term.r#type() != type_obj.as_ref() {
                    return Ok(false);
                }
            }
            CompiledTypeEdgeMatcher::SchemaTerm(type_schema) => {
                let edge_term = edge.term.clone();

                if !type_schema.matches(&edge_term.r#type(), context.clone())? {
                    return Ok(false);
                }
            }
        }

        match &self.compiled_term_matcher {
            CompiledTermEdgeMatcher::Any => {}
            CompiledTermEdgeMatcher::ExactTerm(term_obj) => {
                let edge_term = edge.term.clone();

                if &*edge_term != term_obj.as_ref() {
                    return Ok(false);
                }
            }
            CompiledTermEdgeMatcher::SchemaTerm(term_schema) => {
                let edge_term = edge.term.clone();

                if !term_schema.matches(&edge_term, context.clone())? {
                    return Ok(false);
                }
            }
        }

        // Check properties if specified
        if !self.properties.is_empty() {
            for (key, value) in &self.properties {
                let e_props = edge
                    .properties
                    .read()
                    .map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("edge pattern matches".to_string()),
                    })?;
                if let Some(edge_value) = e_props.get(key) {
                    if Python::attach(|py| !edge_value.bind(py).eq(value.bind(py)).unwrap()) {
                        return Ok(false);
                    };
                } else {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}
