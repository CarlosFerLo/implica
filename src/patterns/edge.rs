use pyo3::prelude::*;
use std::fmt::Display;

use crate::errors::ImplicaError;
use crate::patterns::term_schema::TermSchema;
use crate::patterns::type_schema::TypeSchema;
use crate::utils::validate_variable_name;

#[derive(Clone, Debug, PartialEq)]
pub enum CompiledDirection {
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

#[pyclass]
#[derive(Debug)]
pub struct EdgePattern {
    #[pyo3(get)]
    pub variable: Option<String>,

    pub(crate) compiled_direction: CompiledDirection,
    #[pyo3(get)]
    pub type_schema: Option<TypeSchema>,
    #[pyo3(get)]
    pub term_schema: Option<TermSchema>,
}

impl Clone for EdgePattern {
    fn clone(&self) -> Self {
        EdgePattern {
            variable: self.variable.clone(),
            compiled_direction: self.compiled_direction.clone(),
            type_schema: self.type_schema.clone(),
            term_schema: self.term_schema.clone(),
        }
    }
}

impl Display for EdgePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut content = Vec::new();

        if let Some(ref var) = self.variable {
            content.push(format!("variable='{}'", var));
        }

        if let Some(ref type_schema) = self.type_schema {
            content.push(format!("type_schema={}", type_schema));
        }

        if let Some(ref term_schema) = self.term_schema {
            content.push(format!("term_schema={}", term_schema));
        }

        content.push(format!(
            "direction='{}'",
            self.compiled_direction.to_string()
        ));

        write!(f, "EdgePattern({})", content.join(", "))
    }
}

#[pymethods]
impl EdgePattern {
    #[getter]
    pub fn direction(&self) -> String {
        self.compiled_direction.to_string().to_string()
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }
}

impl EdgePattern {
    pub fn new(
        variable: Option<String>,
        type_schema: Option<TypeSchema>,
        term_schema: Option<TermSchema>,
        direction: String,
    ) -> PyResult<Self> {
        if let Some(ref var) = variable {
            validate_variable_name(var)?;
        }

        let compiled_direction = CompiledDirection::from_string(&direction)?;

        Ok(EdgePattern {
            variable,
            compiled_direction,
            type_schema,
            term_schema,
        })
    }
}
