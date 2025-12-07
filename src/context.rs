use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

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

#[derive(Clone, Debug)]
pub struct Context {
    pub(crate) content: Arc<RwLock<HashMap<String, ContextElement>>>,
}

impl Default for Context {
    fn default() -> Self {
        Context {
            content: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_term(&self, name: String, term: Term) -> Result<(), ImplicaError> {
        validate_variable_name(&name)?;

        let mut context = self.content.write().map_err(|e| ImplicaError::LockError {
            rw: "write".to_string(),
            message: e.to_string(),
            context: Some("context add term".to_string()),
        })?;

        if context.contains_key(&name) {
            return Err(ImplicaError::ContextConflict {
                message: "tried to use a key that already has an element.".to_string(),
                context: Some("add term".to_string()),
            });
        }

        context.insert(name, ContextElement::Term(term));

        Ok(())
    }

    pub fn add_type(&self, name: String, r#type: Type) -> Result<(), ImplicaError> {
        validate_variable_name(&name)?;

        let mut context = self.content.write().map_err(|e| ImplicaError::LockError {
            rw: "write".to_string(),
            message: e.to_string(),
            context: Some("context add type".to_string()),
        })?;

        if context.contains_key(&name) {
            return Err(ImplicaError::ContextConflict {
                message: "tried to use a key that already has an element.".to_string(),
                context: Some("add type".to_string()),
            });
        }

        context.insert(name, ContextElement::Type(r#type));

        Ok(())
    }

    pub fn contains_key(&self, name: &str) -> Result<bool, ImplicaError> {
        let context = self.content.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("context add type".to_string()),
        })?;
        Ok(context.contains_key(name))
    }

    pub fn get(&self, name: &str) -> Result<ContextElement, ImplicaError> {
        let context = self.content.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("context add type".to_string()),
        })?;
        match context.get(name) {
            Some(e) => Ok(e.clone()),
            None => Err(ImplicaError::ContextConflict {
                message: "no context element with that name".to_string(),
                context: Some("get".to_string()),
            }),
        }
    }
}

pub fn python_to_context(obj: &Bound<'_, PyAny>) -> Result<Context, ImplicaError> {
    let context = Context::new();

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
    let content = context
        .content
        .read()
        .map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("context to python".to_string()),
        })?;
    for (k, e) in content.iter() {
        let t_obj = match e {
            ContextElement::Type(t) => type_to_python(py, t)?,
            ContextElement::Term(t) => term_to_python(py, t)?,
        };

        dict.set_item(k, t_obj)?;
    }

    Ok(dict.unbind())
}
