use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    errors::ImplicaError,
    typing::{Term, Type},
    utils::validate_variable_name,
};

#[derive(Clone, Debug)]
pub enum ContextElement {
    Term(Term),
    Type(Type),
}

#[derive(Clone, Debug)]
pub struct Context {
    content: Arc<RwLock<HashMap<String, ContextElement>>>,
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
