use pyo3::prelude::*;
use pyo3::types::PyDict;

use std::collections::HashMap;

use crate::errors::ImplicaError;
use crate::patterns::type_schema::TypeSchema;
use crate::typing::{python_to_term, Term};

/// Compiled direction for efficient matching.
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
            _ => Err(ImplicaError::schema_validation(
                s,
                "Direction must be 'forward', 'backward', or 'any'",
            )),
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

/// Internal compiled representation for efficient edge matching.
#[derive(Clone, Debug)]
enum CompiledEdgeMatcher {
    /// Match any term (no type constraint)
    Any,
    /// Match edges with a specific term
    ExactTerm(Term),
    /// Match edges with a term matching the schema
    SchemaTerm(TypeSchema),
}

/// Represents an edge pattern in a Cypher-like query.
///
/// Edge patterns are used to match edges in the graph based on variable names,
/// terms, term type schemas, properties, and direction. Patterns are compiled
/// and validated at creation time for optimal performance.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Match any edge in forward direction
/// pattern = implica.EdgePattern(variable="e", direction="forward")
///
/// # Match edges with a specific term type
/// pattern = implica.EdgePattern(
///     variable="rel",
///     term_type_schema="Person -> Address",
///     direction="forward"
/// )
///
/// # Match in any direction
/// pattern = implica.EdgePattern(variable="e", direction="any")
/// ```
///
/// # Fields
///
/// * `variable` - Optional variable name to bind matched edges
/// * `compiled_matcher` - Compiled term matcher for efficient matching
/// * `compiled_direction` - Compiled direction for efficient checking
/// * `properties` - Dictionary of required property values
#[pyclass]
#[derive(Debug)]
pub struct EdgePattern {
    #[pyo3(get)]
    pub variable: Option<String>,
    /// Compiled matcher for efficient term checking
    compiled_matcher: CompiledEdgeMatcher,
    /// Compiled direction for efficient checking
    compiled_direction: CompiledDirection,
    pub properties: HashMap<String, Py<PyAny>>,
    // Keep these for backward compatibility and introspection
    pub term: Option<Term>,
    pub term_type_schema: Option<TypeSchema>,
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
                compiled_matcher: self.compiled_matcher.clone(),
                compiled_direction: self.compiled_direction.clone(),
                properties: props,
                term: self.term.clone(),
                term_type_schema: self.term_type_schema.clone(),
            }
        })
    }
}

#[pymethods]
impl EdgePattern {
    /// Creates a new edge pattern.
    ///
    /// The pattern is compiled and validated at creation time for optimal performance.
    /// Invalid term schemas, directions, or conflicting constraints will cause immediate errors.
    ///
    /// # Arguments
    ///
    /// * `variable` - Optional variable name to bind matched edges
    /// * `term` - Optional specific term to match
    /// * `term_type_schema` - Optional type schema for the term (string or TypeSchema)
    /// * `properties` - Optional dictionary of required properties
    /// * `direction` - Direction of the edge: "forward", "backward", or "any" (default: "forward")
    ///
    /// # Returns
    ///
    /// A new `EdgePattern` instance, compiled and ready for matching
    ///
    /// # Errors
    ///
    /// * `ValueError` if both `term` and `term_type_schema` are provided (conflicting constraints)
    /// * `ValueError` if `term_type_schema` string is invalid
    /// * `ValueError` if `direction` is not one of "forward", "backward", or "any"
    /// * `ValueError` if variable name is invalid (empty or whitespace-only)
    ///
    /// # Examples
    ///
    /// ```python
    /// # Forward edge
    /// pattern = implica.EdgePattern(variable="e", direction="forward")
    ///
    /// # Backward edge with type schema
    /// pattern = implica.EdgePattern(
    ///     variable="back",
    ///     term_type_schema="A -> B",
    ///     direction="backward"
    /// )
    /// ```
    #[new]
    #[pyo3(signature = (variable=None, term=None, term_type_schema=None, properties=None, direction="forward".to_string()))]
    pub fn new(
        variable: Option<String>,
        term: Option<Py<PyAny>>,
        term_type_schema: Option<Py<PyAny>>,
        properties: Option<Py<PyDict>>,
        direction: String,
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

            // Validate and compile direction
            let compiled_direction = CompiledDirection::from_string(&direction)?;

            // Parse term if provided
            let term_obj = if let Some(t) = term {
                Some(python_to_term(t.bind(py))?)
            } else {
                None
            };

            // Parse schema if provided
            let schema = if let Some(s) = term_type_schema {
                if let Ok(schema_str) = s.bind(py).extract::<String>() {
                    Some(TypeSchema::new(schema_str)?) // Fail fast on invalid schema
                } else {
                    Some(s.bind(py).extract::<TypeSchema>()?)
                }
            } else {
                None
            };

            // Validate: cannot have both term and term_type_schema
            if term_obj.is_some() && schema.is_some() {
                return Err(ImplicaError::schema_validation(
                    "EdgePattern",
                    "Cannot specify both 'term' and 'term_type_schema' - they are mutually exclusive",
                )
                .into());
            }

            // Build compiled matcher for efficient matching
            let compiled_matcher = if let Some(ref t) = term_obj {
                CompiledEdgeMatcher::ExactTerm(t.clone())
            } else if let Some(ref s) = schema {
                CompiledEdgeMatcher::SchemaTerm(s.clone())
            } else {
                CompiledEdgeMatcher::Any
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

            Ok(EdgePattern {
                variable,
                compiled_matcher,
                compiled_direction,
                properties: props,
                term: term_obj,
                term_type_schema: schema,
            })
        })
    }

    /// Gets the direction of this edge pattern.
    ///
    /// # Returns
    ///
    /// The direction as a string: "forward", "backward", or "any"
    #[getter]
    pub fn direction(&self) -> String {
        self.compiled_direction.to_string().to_string()
    }

    fn __repr__(&self) -> String {
        let term_info = if self.term.is_some() {
            ", term=<specified>"
        } else if self.term_type_schema.is_some() {
            ", term_type_schema=<specified>"
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
    /// Checks if an edge matches this pattern.
    ///
    /// This uses the pre-compiled matcher for optimal performance.
    /// This is an internal method used by the query system.
    ///
    /// Note: Direction matching is context-dependent and should be checked
    /// by the caller based on the traversal direction.
    ///
    /// # Arguments
    ///
    /// * `edge` - The edge to check
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the edge matches, `Ok(false)` otherwise
    pub fn matches(&self, edge: &crate::graph::Edge, py: Python) -> PyResult<bool> {
        // Check term using compiled matcher (most efficient path)
        match &self.compiled_matcher {
            CompiledEdgeMatcher::Any => {
                // No term constraint, continue to property check
            }
            CompiledEdgeMatcher::ExactTerm(term_obj) => {
                let edge_term = edge.term.read().unwrap();

                if &*edge_term != term_obj {
                    return Ok(false);
                }
            }
            CompiledEdgeMatcher::SchemaTerm(schema) => {
                let edge_term = edge.term.read().unwrap();

                if !schema.matches_type(&edge_term.r#type()) {
                    return Ok(false);
                }
            }
        }

        // Check properties if specified
        if !self.properties.is_empty() {
            for (key, value) in &self.properties {
                if let Some(edge_value) = edge.properties.read().unwrap().get(key) {
                    if !edge_value.bind(py).eq(value.bind(py))? {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Checks if the direction matches for traversal.
    ///
    /// This is a helper method to check if the edge can be traversed
    /// in the given direction according to the pattern.
    ///
    /// # Arguments
    ///
    /// * `forward` - true if traversing forward, false if backward
    ///
    /// # Returns
    ///
    /// `true` if the direction matches the pattern
    pub fn matches_direction(&self, forward: bool) -> bool {
        match self.compiled_direction {
            CompiledDirection::Any => true,
            CompiledDirection::Forward => forward,
            CompiledDirection::Backward => !forward,
        }
    }
}
