use std::collections::HashMap;

use pyo3::{prelude::*, types::PyDict};

use crate::{
    errors::ImplicaError,
    typing::{python_to_term, python_to_type, term_to_python, type_to_python, Term, Type},
    utils::validate_variable_name,
};

#[derive(Clone, Debug)]
pub enum ContextElement {
    Term(Term),
    Type(Type),
}

#[derive(Clone, Debug, Default)]
pub struct Context {
    pub(crate) content: HashMap<String, ContextElement>,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_term(&mut self, name: String, term: Term) -> Result<(), ImplicaError> {
        validate_variable_name(&name)?;

        if self.content.contains_key(&name) {
            return Err(ImplicaError::ContextConflict {
                message: "tried to use a key that already has an element.".to_string(),
                context: Some("add term".to_string()),
            });
        }

        self.content.insert(name, ContextElement::Term(term));

        Ok(())
    }

    pub fn add_type(&mut self, name: String, r#type: Type) -> Result<(), ImplicaError> {
        validate_variable_name(&name)?;

        if self.content.contains_key(&name) {
            return Err(ImplicaError::ContextConflict {
                message: "tried to use a key that already has an element.".to_string(),
                context: Some("add type".to_string()),
            });
        }

        self.content.insert(name, ContextElement::Type(r#type));

        Ok(())
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.content.contains_key(name)
    }

    pub fn get(&self, name: &str) -> Result<ContextElement, ImplicaError> {
        match self.content.get(name) {
            Some(e) => Ok(e.clone()),
            None => Err(ImplicaError::ContextConflict {
                message: "no context element with that name".to_string(),
                context: Some("get".to_string()),
            }),
        }
    }
}

pub fn python_to_context(obj: &Bound<'_, PyAny>) -> Result<Context, ImplicaError> {
    let mut context = Context::new();

    let dict = obj
        .cast::<PyDict>()
        .map_err(|_| ImplicaError::PythonError {
            message: "Expected a dictionary".to_string(),
            context: Some("python_to_context".to_string()),
        })?;

    for item in dict.iter() {
        let key = item
            .0
            .extract::<String>()
            .map_err(|_| ImplicaError::PythonError {
                message: "Dictionary keys must be strings".to_string(),
                context: Some("python_to_context".to_string()),
            })?;

        // Try to parse as Term first, then as Type
        if let Ok(term) = python_to_term(&item.1) {
            context.add_term(key, term)?;
        } else if let Ok(typ) = python_to_type(&item.1) {
            context.add_type(key, typ)?;
        } else {
            return Err(ImplicaError::PythonError {
                message: format!("Value for key '{}' must be a Term or Type", key),
                context: Some("python_to_context".to_string()),
            });
        }
    }

    Ok(context)
}

pub fn context_to_python(py: Python, context: Context) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for (k, e) in context.content.iter() {
        let t_obj = match e {
            ContextElement::Type(t) => type_to_python(py, t)?,
            ContextElement::Term(t) => term_to_python(py, t)?,
        };

        dict.set_item(k, t_obj)?;
    }

    Ok(dict.unbind())
}
