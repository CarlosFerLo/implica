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

/// Internal compiled representation for efficient matching.
///
/// This enum represents the compiled/optimized form of a pattern,
/// allowing for efficient matching without re-parsing or re-validation.
#[derive(Clone, Debug)]
enum CompiledTypeNodeMatcher {
    /// Match any node (no type constraint)
    Any,
    /// Match nodes with a specific type
    ExactType(Type),
    /// Match nodes with a type schema pattern
    SchemaType(TypeSchema),
}

#[derive(Clone, Debug)]
enum CompiledTermNodeMatcher {
    Any,
    ExactTerm(Term),
    SchemaTerm(TermSchema),
}

#[pyclass]
#[derive(Debug)]
pub struct NodePattern {
    #[pyo3(get)]
    pub variable: Option<String>,

    pub properties: HashMap<String, Py<PyAny>>,
    // Keep these for backward compatibility and introspection
    pub type_obj: Option<Type>,
    pub type_schema: Option<TypeSchema>,

    compiled_type_matcher: CompiledTypeNodeMatcher,
    /// Optional term to set when creating nodes
    pub term_obj: Option<Term>,
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
                type_obj: self.type_obj.clone(),
                type_schema: self.type_schema.clone(),
                compiled_type_matcher: self.compiled_type_matcher.clone(),
                term_obj: self.term_obj.clone(),
                term_schema: self.term_schema.clone(),
                compiled_term_matcher: self.compiled_term_matcher.clone(),
            }
        })
    }
}

#[pymethods]
impl NodePattern {
    fn __repr__(&self) -> String {
        let type_info = if self.type_obj.is_some() {
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
        r#type: Option<Type>,
        type_schema: Option<TypeSchema>,
        term: Option<Term>,
        term_schema: Option<TermSchema>,
        properties: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Self> {
        // Validate variable name if provided
        if let Some(ref var) = variable {
            validate_variable_name(var)?;
        }

        // Validate: cannot have both type and type_schema
        if r#type.is_some() && type_schema.is_some() {
            return Err(ImplicaError::schema_validation(
                "NodePattern",
                "Cannot specify both 'type' and 'type_schema' - they are mutually exclusive",
            )
            .into());
        }

        // Build compiled matcher for efficient matching
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

        // Parse properties

        Ok(NodePattern {
            variable,
            properties: properties.unwrap_or_default(),
            type_obj: r#type,
            type_schema,
            term_obj: term,
            term_schema,
            compiled_type_matcher,
            compiled_term_matcher,
        })
    }

    /// Checks if a node matches this pattern.
    ///
    /// This uses the pre-compiled matcher for optimal performance.
    /// This is an internal method used by the query system.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to check
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the node matches, `Ok(false)` otherwise
    pub fn matches(&self, node: &Node, context: Arc<Context>) -> Result<bool, ImplicaError> {
        // Check type using compiled matcher (most efficient path)
        match &self.compiled_type_matcher {
            CompiledTypeNodeMatcher::Any => {
                // No type constraint, continue to property check
            }
            CompiledTypeNodeMatcher::ExactType(type_obj) => {
                if &*node.r#type != type_obj {
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
                    let term = term_lock.read().unwrap();
                    if &*term != term_obj {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
            CompiledTermNodeMatcher::SchemaTerm(term_schema) => {
                if let Some(ref term_lock) = node.term {
                    let term = term_lock.read().unwrap();
                    if !term_schema.matches(&term, context.clone())? {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        }

        // Check properties if specified
        if !self.properties.is_empty() {
            return Python::attach(|py| -> Result<bool, ImplicaError> {
                for (key, value) in &self.properties {
                    if let Some(node_value) = node.properties.read().unwrap().get(key) {
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
