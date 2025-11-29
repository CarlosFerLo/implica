use pyo3::prelude::*;
use pyo3::types::PyDict;

use std::collections::HashMap;

use crate::errors::ImplicaError;
use crate::graph::Node;
use crate::patterns::type_schema::TypeSchema;
use crate::typing::{python_to_type, Type};

/// Internal compiled representation for efficient matching.
///
/// This enum represents the compiled/optimized form of a pattern,
/// allowing for efficient matching without re-parsing or re-validation.
#[derive(Clone, Debug)]
enum CompiledNodeMatcher {
    /// Match any node (no type constraint)
    Any,
    /// Match nodes with a specific type
    ExactType(Type),
    /// Match nodes with a type schema pattern
    SchemaType(TypeSchema),
}

/// Represents a node pattern in a Cypher-like query.
///
/// Node patterns are used to match nodes in the graph based on variable names,
/// types, type schemas, and properties. Patterns are compiled and validated
/// at creation time for optimal performance.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Match any node, bind to variable 'n'
/// pattern = implica.NodePattern(variable="n")
///
/// # Match nodes of a specific type
/// person_type = implica.Variable("Person")
/// pattern = implica.NodePattern(variable="n", type=person_type)
///
/// # Match nodes using a type schema
/// pattern = implica.NodePattern(variable="n", type_schema="Person")
///
/// # Match with properties
/// pattern = implica.NodePattern(
///     variable="n",
///     type_schema="Person",
///     properties={"age": 25}
/// )
/// ```
///
/// # Fields
///
/// * `variable` - Optional variable name to bind matched nodes
/// * `compiled_matcher` - Compiled type matcher for efficient matching
/// * `properties` - Dictionary of required property values
#[pyclass]
#[derive(Debug)]
pub struct NodePattern {
    #[pyo3(get)]
    pub variable: Option<String>,
    /// Compiled matcher for efficient type checking
    compiled_matcher: CompiledNodeMatcher,
    pub properties: HashMap<String, Py<PyAny>>,
    // Keep these for backward compatibility and introspection
    pub type_obj: Option<Type>,
    pub type_schema: Option<TypeSchema>,
    /// Optional term to set when creating nodes
    pub term: Option<Py<PyAny>>,
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
                compiled_matcher: self.compiled_matcher.clone(),
                properties: props,
                type_obj: self.type_obj.clone(),
                type_schema: self.type_schema.clone(),
                term: self.term.as_ref().map(|t| t.clone_ref(py)),
            }
        })
    }
}

#[pymethods]
impl NodePattern {
    /// Creates a new node pattern.
    ///
    /// The pattern is compiled and validated at creation time for optimal performance.
    /// Invalid type schemas or conflicting constraints will cause immediate errors.
    ///
    /// # Arguments
    ///
    /// * `variable` - Optional variable name to bind matched nodes
    /// * `type` - Optional specific type to match
    /// * `type_schema` - Optional type schema pattern (string or TypeSchema)
    /// * `properties` - Optional dictionary of required properties
    ///
    /// # Returns
    ///
    /// A new `NodePattern` instance, compiled and ready for matching
    ///
    /// # Errors
    ///
    /// * `ValueError` if both `type` and `type_schema` are provided (conflicting constraints)
    /// * `ValueError` if `type_schema` string is invalid
    /// * `ValueError` if variable name is invalid (empty or whitespace-only)
    ///
    /// # Examples
    ///
    /// ```python
    /// # Simple pattern
    /// pattern = implica.NodePattern(variable="n")
    ///
    /// # With type schema
    /// pattern = implica.NodePattern(
    ///     variable="person",
    ///     type_schema="Person"
    /// )
    ///
    /// # With specific type
    /// person_type = implica.Variable("Person")
    /// pattern = implica.NodePattern(variable="n", type=person_type)
    /// ```
    #[new]
    #[pyo3(signature = (variable=None, r#type=None, type_schema=None, properties=None, term=None))]
    pub fn new(
        variable: Option<String>,
        r#type: Option<Py<PyAny>>,
        type_schema: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
        term: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        Python::attach(|py| {
            // Validate variable name if provided
            if let Some(ref var) = variable {
                if var.trim().is_empty() {
                    return Err(ImplicaError::invalid_identifier(
                        var.clone(),
                        "variable name cannot be empty or whitespace-only",
                    )
                    .into());
                }
            }

            // Parse type if provided
            let type_obj = if let Some(t) = r#type {
                Some(python_to_type(t.bind(py))?)
            } else {
                None
            };

            // Parse schema if provided
            let schema = if let Some(s) = type_schema {
                if let Ok(schema_str) = s.bind(py).extract::<String>() {
                    Some(TypeSchema::new(schema_str)?) // Fail fast on invalid schema
                } else {
                    Some(s.bind(py).extract::<TypeSchema>()?)
                }
            } else {
                None
            };

            // Validate: cannot have both type and type_schema
            if type_obj.is_some() && schema.is_some() {
                return Err(ImplicaError::schema_validation(
                    "NodePattern",
                    "Cannot specify both 'type' and 'type_schema' - they are mutually exclusive",
                )
                .into());
            }

            // Build compiled matcher for efficient matching
            let compiled_matcher = if let Some(ref t) = type_obj {
                CompiledNodeMatcher::ExactType(t.clone())
            } else if let Some(ref s) = schema {
                CompiledNodeMatcher::SchemaType(s.clone())
            } else {
                CompiledNodeMatcher::Any
            };

            // Parse properties
            let mut props = HashMap::new();
            if let Some(p) = properties {
                for (k, v) in p.bind(py).iter() {
                    let key: String = k.extract()?;
                    if key.trim().is_empty() {
                        return Err(ImplicaError::invalid_identifier(
                            key,
                            "property key cannot be empty or whitespace-only",
                        )
                        .into());
                    }
                    props.insert(key, v.into());
                }
            }

            Ok(NodePattern {
                variable,
                compiled_matcher,
                properties: props,
                type_obj,
                type_schema: schema,
                term: term.map(|t| t.clone_ref(py)),
            })
        })
    }

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
    pub fn matches(&self, node: &Node, py: Python) -> PyResult<bool> {
        // Check type using compiled matcher (most efficient path)
        match &self.compiled_matcher {
            CompiledNodeMatcher::Any => {
                // No type constraint, continue to property check
            }
            CompiledNodeMatcher::ExactType(type_obj) => {
                if &*node.r#type != type_obj {
                    return Ok(false);
                }
            }
            CompiledNodeMatcher::SchemaType(schema) => {
                if !schema.matches_type(&node.r#type) {
                    return Ok(false);
                }
            }
        }

        // Check properties if specified
        if !self.properties.is_empty() {
            for (key, value) in &self.properties {
                if let Some(node_value) = node.properties.read().unwrap().get(key) {
                    if !node_value.bind(py).eq(value.bind(py))? {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}
