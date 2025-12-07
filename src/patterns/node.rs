use pyo3::prelude::*;

use std::collections::HashMap;
use std::sync::Arc;

use crate::context::Context;
use crate::errors::ImplicaError;
use crate::graph::Node;
use crate::patterns::term_schema::TermSchema;
use crate::patterns::type_schema::TypeSchema;
use crate::typing::{Term, Type};
use crate::utils::validate_variable_name;

#[derive(Clone, Debug)]
enum CompiledTypeNodeMatcher {
    Any,
    ExactType(Arc<Type>),
    SchemaType(TypeSchema),
}

#[derive(Clone, Debug)]
enum CompiledTermNodeMatcher {
    Any,
    ExactTerm(Arc<Term>),
    SchemaTerm(TermSchema),
}

#[pyclass]
#[derive(Debug)]
pub struct NodePattern {
    #[pyo3(get)]
    pub variable: Option<String>,

    pub properties: HashMap<String, Py<PyAny>>,
    pub r#type: Option<Arc<Type>>,
    pub type_schema: Option<TypeSchema>,

    compiled_type_matcher: CompiledTypeNodeMatcher,

    pub term: Option<Arc<Term>>,
    pub term_schema: Option<TermSchema>,

    compiled_term_matcher: CompiledTermNodeMatcher,
}

impl Clone for NodePattern {
    fn clone(&self) -> Self {
        Python::attach(|py| {
            let mut props = HashMap::new();
            for (k, v) in &self.properties {
                props.insert(k.clone(), v.clone_ref(py));
            }
            NodePattern {
                variable: self.variable.clone(),
                properties: props,
                r#type: self.r#type.clone(),
                type_schema: self.type_schema.clone(),
                compiled_type_matcher: self.compiled_type_matcher.clone(),
                term: self.term.clone(),
                term_schema: self.term_schema.clone(),
                compiled_term_matcher: self.compiled_term_matcher.clone(),
            }
        })
    }
}

#[pymethods]
impl NodePattern {
    fn __repr__(&self) -> String {
        let type_info = if self.r#type.is_some() {
            ", type=<specified>"
        } else if self.type_schema.is_some() {
            ", type_schema=<specified>"
        } else {
            ""
        };
        format!("NodePattern(variable={:?}{})", self.variable, type_info)
    }
}

impl NodePattern {
    pub fn new(
        variable: Option<String>,
        r#type: Option<Arc<Type>>,
        type_schema: Option<TypeSchema>,
        term: Option<Arc<Term>>,
        term_schema: Option<TermSchema>,
        properties: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Self> {
        if let Some(ref var) = variable {
            validate_variable_name(var)?;
        }

        if r#type.is_some() && type_schema.is_some() {
            return Err(ImplicaError::InvalidPattern {
                pattern: "NodePattern".to_string(),
                reason:
                    "Cannot specify both 'type' and 'type_schema' - they are mutually exclusive"
                        .to_string(),
            }
            .into());
        }

        if term.is_some() && term_schema.is_some() {
            return Err(ImplicaError::InvalidPattern {
                pattern: "NodePattern".to_string(),
                reason:
                    "Cannot specify bothe 'term' and 'type_schema' - they are mutually exclusive"
                        .to_string(),
            }
            .into());
        }

        let compiled_type_matcher = if let Some(ref t) = r#type {
            CompiledTypeNodeMatcher::ExactType(t.clone())
        } else if let Some(ref s) = type_schema {
            CompiledTypeNodeMatcher::SchemaType(s.clone())
        } else {
            CompiledTypeNodeMatcher::Any
        };

        let compiled_term_matcher = if let Some(ref t) = term {
            CompiledTermNodeMatcher::ExactTerm(t.clone())
        } else if let Some(ref s) = term_schema {
            CompiledTermNodeMatcher::SchemaTerm(s.clone())
        } else {
            CompiledTermNodeMatcher::Any
        };

        Ok(NodePattern {
            variable,
            properties: properties.unwrap_or_default(),
            r#type,
            type_schema,
            term,
            term_schema,
            compiled_type_matcher,
            compiled_term_matcher,
        })
    }

    pub fn matches(&self, node: &Node, context: Arc<Context>) -> Result<bool, ImplicaError> {
        match &self.compiled_type_matcher {
            CompiledTypeNodeMatcher::Any => {}
            CompiledTypeNodeMatcher::ExactType(type_obj) => {
                if node.r#type.as_ref() != type_obj.as_ref() {
                    return Ok(false);
                }
            }
            CompiledTypeNodeMatcher::SchemaType(schema) => {
                if !schema.matches(&node.r#type, context.clone())? {
                    return Ok(false);
                }
            }
        }

        match &self.compiled_term_matcher {
            CompiledTermNodeMatcher::Any => {}
            CompiledTermNodeMatcher::ExactTerm(term_obj) => {
                if let Some(ref term_lock) = node.term {
                    let term = term_lock.read().map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("node pattern matches".to_string()),
                    })?;
                    if &*term != term_obj.as_ref() {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
            CompiledTermNodeMatcher::SchemaTerm(term_schema) => {
                if let Some(ref term_lock) = node.term {
                    let term = term_lock.read().map_err(|e| ImplicaError::LockError {
                        rw: "read".to_string(),
                        message: e.to_string(),
                        context: Some("node pattern matches".to_string()),
                    })?;
                    if !term_schema.matches(&term, context.clone())? {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        }

        if !self.properties.is_empty() {
            return Python::attach(|py| -> Result<bool, ImplicaError> {
                for (key, value) in &self.properties {
                    let n_props = node
                        .properties
                        .read()
                        .map_err(|e| ImplicaError::LockError {
                            rw: "read".to_string(),
                            message: e.to_string(),
                            context: Some("node pattern matches".to_string()),
                        })?;
                    if let Some(node_value) = n_props.get(key) {
                        if !node_value.bind(py).eq(value.bind(py))? {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            });
        }
        Ok(true)
    }
}
