use std::fmt;
use std::sync::Arc;

use crate::errors::ImplicaError;
use crate::utils::validate_variable_name;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Variable(Variable),
    Arrow(Arrow),
}

impl Type {
    pub fn _as_variable(&self) -> Option<&Variable> {
        match self {
            Type::Variable(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_arrow(&self) -> Option<&Arrow> {
        match self {
            Type::Arrow(a) => Some(a),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Variable(v) => write!(f, "{}", v),
            Type::Arrow(a) => write!(f, "{}", a),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Variable {
    pub name: String,
}

impl Variable {
    pub fn new(name: String) -> Result<Self, ImplicaError> {
        validate_variable_name(&name)?;
        Ok(Variable { name })
    }
}

impl fmt::Display for Variable {
    /// Formats the variable for display (shows the name).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl PartialEq for Variable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Variable {}

#[derive(Clone, Debug)]
pub struct Arrow {
    pub left: Arc<Type>,
    pub right: Arc<Type>,
}

impl Arrow {
    pub fn new(left: Arc<Type>, right: Arc<Type>) -> Self {
        Arrow { left, right }
    }
}

impl fmt::Display for Arrow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} -> {})", self.left, self.right)
    }
}

impl PartialEq for Arrow {
    fn eq(&self, other: &Self) -> bool {
        (self.right == other.right) && (self.left == other.left)
    }
}

impl Eq for Arrow {}
