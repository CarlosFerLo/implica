use pyo3::prelude::*;

use crate::errors::IntoPyResult;
use crate::patterns::{TypePattern, TypeSchema};

#[pyclass]
#[derive(Debug, Clone)]
pub struct Constant {
    #[pyo3(get)]
    pub name: String,
    pub type_schema: TypeSchema,

    pub free_variables: Vec<String>,
}

#[pymethods]
impl Constant {
    #[new]
    pub fn new(name: String, type_schema: String) -> PyResult<Constant> {
        let type_schema = TypeSchema::new(type_schema).into_py_result()?;
        let free_variables = type_schema.get_free_variables();

        Ok(Constant {
            name,
            type_schema,
            free_variables,
        })
    }
}

impl TypeSchema {
    pub fn get_free_variables(&self) -> Vec<String> {
        Self::get_pattern_free_variables_recursive(&self.compiled)
    }

    fn get_pattern_free_variables_recursive(pattern: &TypePattern) -> Vec<String> {
        let mut variables = Vec::new();

        match pattern {
            TypePattern::Wildcard => (),
            TypePattern::Variable(_) => (),
            TypePattern::Capture { name, pattern } => {
                variables = Self::get_pattern_free_variables_recursive(pattern);
                variables.push(name.clone());
            }
            TypePattern::Arrow { left, right } => {
                variables = Self::get_pattern_free_variables_recursive(left);
                variables.append(&mut Self::get_pattern_free_variables_recursive(right));
            }
        }

        variables
    }
}
