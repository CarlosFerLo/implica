use pyo3::prelude::*;
use pyo3::types::PyDict;

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use crate::context::{python_to_context, Context, ContextElement};
use crate::errors::ImplicaError;
use crate::graph::{property_map_to_python, property_map_to_string, python_to_property_map, Node};
use crate::patterns::term_schema::TermSchema;
use crate::patterns::type_schema::TypeSchema;
use crate::typing::{python_to_term, python_to_type, term_to_python, type_to_python, Term, Type};
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

    pub r#type: Option<Arc<Type>>,
    #[pyo3(get)]
    pub type_schema: Option<TypeSchema>,

    compiled_type_matcher: CompiledTypeNodeMatcher,

    pub term: Option<Arc<Term>>,
    #[pyo3(get)]
    pub term_schema: Option<TermSchema>,

    compiled_term_matcher: CompiledTermNodeMatcher,

    pub properties: HashMap<String, Py<PyAny>>,
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

impl Display for NodePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut content = Vec::new();

        if let Some(ref var) = self.variable {
            content.push(format!("variable='{}'", var));
        }

        if let Some(ref typ) = self.r#type {
            content.push(format!("type='{}'", typ))
        }

        if let Some(ref type_schema) = self.type_schema {
            content.push(format!("type_schema={}", type_schema))
        }

        if let Some(ref term) = self.term {
            content.push(format!("term='{}'", term));
        }

        if let Some(ref term_schema) = self.term_schema {
            content.push(format!("term_schema={}", term_schema));
        }

        if !self.properties.is_empty() {
            content.push(format!(
                "properties={}",
                property_map_to_string(&self.properties)
            ));
        }

        write!(f, "NodePattern({})", content.join(", "))
    }
}

#[pymethods]
impl NodePattern {
    #[new]
    #[pyo3(signature=(variable=None, r#type=None, type_schema=None, term=None, term_schema=None, properties=None))]
    pub fn py_new(
        py: Python,
        variable: Option<String>,
        r#type: Option<Py<PyAny>>,
        type_schema: Option<TypeSchema>,
        term: Option<Py<PyAny>>,
        term_schema: Option<TermSchema>,
        properties: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let type_obj = match r#type {
            Some(t) => Some(Arc::new(python_to_type(t.bind(py))?)),
            None => None,
        };
        let term_obj = match term {
            Some(t) => Some(Arc::new(python_to_term(t.bind(py))?)),
            None => None,
        };
        let properties_obj = match properties {
            Some(props) => Some(python_to_property_map(props.bind(py))?),
            None => None,
        };

        NodePattern::new(
            variable,
            type_obj,
            type_schema,
            term_obj,
            term_schema,
            properties_obj,
        )
    }

    #[pyo3(name="matches", signature=(node, context=None))]
    pub fn py_matches(&self, py: Python, node: Node, context: Option<Py<PyAny>>) -> PyResult<bool> {
        let context_obj = match context.as_ref() {
            Some(c) => python_to_context(c.bind(py))?,
            None => Context::new(),
        };

        let result = self.matches(&node, &context_obj)?;

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

    #[getter]
    pub fn get_type(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        if let Some(ref typ) = self.r#type {
            Ok(Some(type_to_python(py, typ)?))
        } else {
            Ok(None)
        }
    }

    #[getter]
    pub fn get_term(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        if let Some(ref trm) = self.term {
            Ok(Some(term_to_python(py, trm)?))
        } else {
            Ok(None)
        }
    }

    #[getter]
    pub fn get_properties(&self, py: Python) -> PyResult<Py<PyAny>> {
        property_map_to_python(py, &self.properties)
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    fn __repr__(&self) -> String {
        self.to_string()
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

    pub fn matches(&self, node: &Node, context: &Context) -> Result<bool, ImplicaError> {
        match &self.compiled_type_matcher {
            CompiledTypeNodeMatcher::Any => {}
            CompiledTypeNodeMatcher::ExactType(type_obj) => {
                if node.r#type.as_ref() != type_obj.as_ref() {
                    return Ok(false);
                }
            }
            CompiledTypeNodeMatcher::SchemaType(schema) => {
                if !schema.matches(&node.r#type, context)? {
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
                    if !term_schema.matches(&term, context)? {
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
