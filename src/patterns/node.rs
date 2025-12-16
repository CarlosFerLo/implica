use pyo3::prelude::*;
use std::fmt::Display;

use crate::patterns::term_schema::TermSchema;
use crate::patterns::type_schema::TypeSchema;
use crate::utils::validate_variable_name;

#[pyclass]
#[derive(Debug)]
pub struct NodePattern {
    #[pyo3(get)]
    pub variable: Option<String>,
    #[pyo3(get)]
    pub type_schema: Option<TypeSchema>,

    #[pyo3(get)]
    pub term_schema: Option<TermSchema>,
}

impl Clone for NodePattern {
    fn clone(&self) -> Self {
        NodePattern {
            variable: self.variable.clone(),

            type_schema: self.type_schema.clone(),

            term_schema: self.term_schema.clone(),
        }
    }
}

impl Display for NodePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut content = Vec::new();

        if let Some(ref var) = self.variable {
            content.push(format!("variable='{}'", var));
        }

        if let Some(ref type_schema) = self.type_schema {
            content.push(format!("type_schema={}", type_schema))
        }

        if let Some(ref term_schema) = self.term_schema {
            content.push(format!("term_schema={}", term_schema));
        }

        write!(f, "NodePattern({})", content.join(", "))
    }
}

#[pymethods]
impl NodePattern {
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
        type_schema: Option<TypeSchema>,
        term_schema: Option<TermSchema>,
    ) -> PyResult<Self> {
        if let Some(ref var) = variable {
            validate_variable_name(var)?;
        }

        Ok(NodePattern {
            variable,
            type_schema,
            term_schema,
        })
    }
}
