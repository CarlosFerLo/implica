use std::{fmt::Display, sync::Arc};

use crate::{errors::ImplicaError, typing::Type, utils::validate_variable_name};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Term {
    Basic(BasicTerm),
    Application(Application),
}

impl Term {
    pub fn r#type(&self) -> Arc<Type> {
        match self {
            Term::Basic(basic) => basic.r#type.clone(),
            Term::Application(app) => app.r#type.clone(),
        }
    }

    pub fn _as_basic(&self) -> Option<&BasicTerm> {
        match self {
            Term::Basic(basic) => Some(basic),
            Term::Application(_) => None,
        }
    }

    pub fn as_application(&self) -> Option<&Application> {
        match self {
            Term::Application(app) => Some(app),
            Term::Basic(_) => None,
        }
    }

    pub fn apply(&self, other: &Term) -> Result<Term, ImplicaError> {
        Ok(Term::Application(Application::new(
            self.clone(),
            other.clone(),
        )?))
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Basic(b) => write!(f, "{}", b),
            Term::Application(a) => write!(f, "{}", a),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BasicTerm {
    pub name: String,
    pub r#type: Arc<Type>,
}

impl BasicTerm {
    pub fn new(name: String, r#type: Arc<Type>) -> Result<Self, ImplicaError> {
        validate_variable_name(&name)?;
        Ok(BasicTerm { name, r#type })
    }
}

impl Display for BasicTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl PartialEq for BasicTerm {
    fn eq(&self, other: &Self) -> bool {
        (self.name == other.name) && (self.r#type == other.r#type)
    }
}

impl Eq for BasicTerm {}

#[derive(Clone, Debug)]
pub struct Application {
    pub function: Arc<Term>,
    pub argument: Arc<Term>,
    r#type: Arc<Type>,
}

impl Display for Application {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} {})", self.function, self.argument)
    }
}

impl PartialEq for Application {
    fn eq(&self, other: &Self) -> bool {
        (self.function == other.function) && (self.argument == other.argument)
    }
}

impl Eq for Application {}

impl Application {
    pub fn new(function: Term, argument: Term) -> Result<Self, ImplicaError> {
        match function.r#type().as_ref() {
            Type::Variable(_) => Err(ImplicaError::TypeMismatch {
                expected: "Application Type".to_string(),
                got: "Variable Type".to_string(),
                context: Some("application creation".to_string()),
            }),
            Type::Arrow(arr) => {
                if arr.left != argument.r#type() {
                    Err(ImplicaError::TypeMismatch {
                        expected: arr.left.to_string(),
                        got: argument.r#type().to_string(),
                        context: Some("application creation".to_string()),
                    })
                } else {
                    Ok(Application {
                        function: Arc::new(function),
                        argument: Arc::new(argument),
                        r#type: arr.right.clone(),
                    })
                }
            }
        }
    }
}
