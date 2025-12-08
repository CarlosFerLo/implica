use pyo3::pyclass::PyClassGuardError;
use pyo3::{exceptions, PyErr};
use std::fmt::{Display, Formatter, Result};
use std::sync::{Arc, RwLock};

use crate::graph::{Edge, Node};

#[derive(Debug, Clone)]
pub enum ImplicaError {
    TypeMismatch {
        expected: String,
        got: String,
        context: Option<String>,
    },

    NodeNotFound {
        uid: String,
        context: Option<String>,
    },

    EdgeNotFound {
        uid: String,
        context: Option<String>,
    },

    InvalidPattern {
        pattern: String,
        reason: String,
    },

    InvalidQuery {
        message: String,
        context: Option<String>,
    },

    InvalidIdentifier {
        name: String,
        reason: String,
    },

    PropertyError {
        key: String,
        message: String,
    },

    VariableNotFound {
        name: String,
        context: Option<String>,
    },

    SchemaValidation {
        schema: String,
        reason: String,
    },

    PythonError {
        message: String,
        context: Option<String>,
    },

    NodeAlreadyExists {
        message: String,
        existing: Arc<RwLock<Node>>,
        new: Arc<RwLock<Node>>,
    },
    EdgeAlreadyExists {
        message: String,
        existing: Arc<RwLock<Edge>>,
        new: Arc<RwLock<Edge>>,
    },
    VariableAlreadyExists {
        name: String,
        context: Option<String>,
    },
    ContextConflict {
        message: String,
        context: Option<String>,
    },
    IndexOutOfRange {
        idx: usize,
        length: usize,
        context: Option<String>,
    },
    MissingIdentifier {
        message: String,
        context: Option<String>,
    },
    EvaluationError {
        message: String,
    },
    InvalidType {
        reason: String,
    },
    LockError {
        rw: String,
        message: String,
        context: Option<String>,
    },
}

impl Display for ImplicaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            ImplicaError::TypeMismatch {
                expected,
                got,
                context,
            } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, got)?;
                if let Some(ctx) = context {
                    write!(f, " (in {})", ctx)?;
                }
                Ok(())
            }
            ImplicaError::NodeNotFound { uid, context } => {
                write!(f, "Node not found: {}", uid)?;
                if let Some(ctx) = context {
                    write!(f, " ({})", ctx)?;
                }
                Ok(())
            }
            ImplicaError::EdgeNotFound { uid, context } => {
                write!(f, "Edge not found: {}", uid)?;
                if let Some(ctx) = context {
                    write!(f, " ({})", ctx)?;
                }
                Ok(())
            }
            ImplicaError::InvalidPattern { pattern, reason } => {
                write!(f, "Invalid pattern '{}': {}", pattern, reason)
            }
            ImplicaError::InvalidQuery { message, context } => {
                write!(f, "Invalid query: {}", message)?;
                if let Some(ctx) = context {
                    write!(f, " ({})", ctx)?;
                }
                Ok(())
            }
            ImplicaError::InvalidIdentifier { name, reason } => {
                write!(f, "Invalid identifier '{}': {}", name, reason)
            }
            ImplicaError::PropertyError { key, message } => {
                write!(f, "Property error for '{}': {}", key, message)
            }
            ImplicaError::VariableNotFound { name, context } => {
                write!(f, "Variable '{}' not found", name)?;
                if let Some(ctx) = context {
                    write!(f, " ({})", ctx)?;
                }
                Ok(())
            }
            ImplicaError::VariableAlreadyExists { name, context } => {
                write!(f, "Variable '{}' already exists", name)?;
                if let Some(ctx) = context {
                    write!(f, " ({})", ctx)?;
                }
                Ok(())
            }
            ImplicaError::SchemaValidation { schema, reason } => {
                write!(f, "Schema validation failed for '{}': {}", schema, reason)
            }

            ImplicaError::PythonError { message, context } => {
                write!(f, "Python error: '{}'", message)?;
                if let Some(ctx) = context {
                    write!(f, "({})", ctx)?;
                }
                Ok(())
            }
            ImplicaError::NodeAlreadyExists {
                message,
                existing,
                new,
            } => {
                let existing = existing.read().unwrap();
                let new = new.read().unwrap();
                write!(
                    f,
                    "Node already exists in the graph: '{}'\nExisting: '{}'\nNew: '{}'",
                    message, existing, new
                )
            }
            ImplicaError::EdgeAlreadyExists {
                message,
                existing,
                new,
            } => {
                let existing = existing.read().unwrap();
                let new = new.read().unwrap();
                write!(
                    f,
                    "Node already exists in the graph: '{}'\nExisting: '{}'\nNew: '{}'",
                    message, existing, new
                )
            }
            ImplicaError::ContextConflict { message, context } => {
                write!(f, "Context Conflict: '{}'", message)?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }
            ImplicaError::IndexOutOfRange {
                idx,
                length,
                context,
            } => {
                write!(
                    f,
                    "Index out of range. Tried to access index {} in where length was {}",
                    idx, length
                )?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }
            ImplicaError::MissingIdentifier { message, context } => {
                write!(f, "Missing Identifier: '{}'", message)?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }
            ImplicaError::EvaluationError { message } => {
                write!(f, "Evaluation Error: '{}'", message)
            }
            ImplicaError::InvalidType { reason } => {
                write!(f, "Invalid Type: '{}'", reason)
            }
            ImplicaError::LockError {
                rw,
                message,
                context,
            } => {
                write!(f, "Failed to acquire {} lock: '{}'", rw, message)?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ImplicaError {}

/// Convert ImplicaError to PyErr with appropriate Python exception types.
///
/// This implementation ensures that each error type maps to the most appropriate
/// Python built-in exception:
///
/// - `TypeMismatch` → `TypeError`
/// - `NodeNotFound`, `EdgeNotFound` → `KeyError`
/// - `InvalidPattern`, `InvalidQuery`, `InvalidIdentifier`, `SchemaValidation` → `ValueError`
/// - `PropertyError` → `AttributeError`
/// - `VariableNotFound` → `NameError`
impl From<ImplicaError> for PyErr {
    fn from(err: ImplicaError) -> PyErr {
        match err {
            ImplicaError::TypeMismatch { .. } => exceptions::PyTypeError::new_err(err.to_string()),
            ImplicaError::NodeNotFound { .. } | ImplicaError::EdgeNotFound { .. } => {
                exceptions::PyKeyError::new_err(err.to_string())
            }
            ImplicaError::InvalidPattern { .. }
            | ImplicaError::InvalidQuery { .. }
            | ImplicaError::InvalidIdentifier { .. }
            | ImplicaError::SchemaValidation { .. } => {
                exceptions::PyValueError::new_err(err.to_string())
            }
            ImplicaError::PropertyError { .. } => {
                exceptions::PyAttributeError::new_err(err.to_string())
            }
            ImplicaError::VariableNotFound { .. } => {
                exceptions::PyNameError::new_err(err.to_string())
            }
            ImplicaError::VariableAlreadyExists { .. } => {
                exceptions::PyNameError::new_err(err.to_string())
            }
            ImplicaError::PythonError { .. } => {
                exceptions::PyRuntimeError::new_err(err.to_string())
            }
            ImplicaError::NodeAlreadyExists { .. } => {
                exceptions::PyValueError::new_err(err.to_string())
            }
            ImplicaError::EdgeAlreadyExists { .. } => {
                exceptions::PyValueError::new_err(err.to_string())
            }
            ImplicaError::ContextConflict { .. } => {
                exceptions::PyValueError::new_err(err.to_string())
            }
            ImplicaError::IndexOutOfRange { .. } => {
                exceptions::PyIndexError::new_err(err.to_string())
            }
            ImplicaError::MissingIdentifier { .. } => {
                exceptions::PyIndexError::new_err(err.to_string())
            }
            ImplicaError::EvaluationError { .. } => {
                exceptions::PyRuntimeError::new_err(err.to_string())
            }
            ImplicaError::InvalidType { .. } => exceptions::PyTypeError::new_err(err.to_string()),
            ImplicaError::LockError { .. } => exceptions::PyRuntimeError::new_err(err.to_string()),
        }
    }
}

impl From<PyErr> for ImplicaError {
    fn from(value: PyErr) -> Self {
        ImplicaError::PythonError {
            message: value.to_string(),
            context: None,
        }
    }
}

impl From<PyClassGuardError<'_, '_>> for ImplicaError {
    fn from(value: PyClassGuardError<'_, '_>) -> Self {
        ImplicaError::PythonError {
            message: value.to_string(),
            context: None,
        }
    }
}
