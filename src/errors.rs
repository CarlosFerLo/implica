use pyo3::pyclass::PyClassGuardError;
use pyo3::{exceptions, PyErr};
use std::convert::Infallible;
use std::fmt::{Display, Formatter, Result};

use crate::graph::Uid;

#[derive(Debug, Clone)]
pub enum ImplicaError {
    TypeMismatch {
        expected: String,
        got: String,
        context: Option<String>,
    },

    InvalidPattern {
        pattern: String,
        reason: String,
    },

    SchemaValidation {
        schema: String,
        reason: String,
    },

    InvalidIdentifier {
        name: String,
        reason: String,
    },

    PythonError {
        message: String,
        context: Option<String>,
    },

    EvaluationError {
        message: String,
    },
    InvalidType {
        reason: String,
    },
    InvalidTerm {
        reason: String,
    },
    LockError {
        rw: String,
        message: String,
        context: Option<String>,
    },
    RuntimeError {
        message: String,
        context: Option<String>,
    },

    TypeNotFound {
        uid: Uid,
        context: Option<String>,
    },

    TermNotFound {
        uid: Uid,
        context: Option<String>,
    },

    NodeNotFound {
        uid: Uid,
        context: Option<String>,
    },
    EdgeNotFound {
        uid: (Uid, Uid),
        context: Option<String>,
    },

    VariableAlreadyExists {
        name: String,
        context: Option<String>,
    },
    VariableNotFound {
        name: String,
        context: Option<String>,
    },

    ContextConflict {
        name: String,
        original: String,
        new: String,
        context: Option<String>,
    },

    IndexCorruption {
        message: String,
        context: Option<String>,
    },
    IndexOutOfRange {
        index: usize,
        max_len: usize,
        context: Option<String>,
    },
    Infallible {},
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
            ImplicaError::InvalidPattern { pattern, reason } => {
                write!(f, "Invalid pattern '{}': {}", pattern, reason)
            }
            ImplicaError::SchemaValidation { schema, reason } => {
                write!(f, "Schema validation failed for '{}': {}", schema, reason)
            }
            ImplicaError::InvalidIdentifier { name, reason } => {
                write!(f, "Invalid identifier '{}': {}", name, reason)
            }
            ImplicaError::PythonError { message, context } => {
                write!(f, "Python error: '{}'", message)?;
                if let Some(ctx) = context {
                    write!(f, "({})", ctx)?;
                }
                Ok(())
            }
            ImplicaError::EvaluationError { message } => {
                write!(f, "Evaluation Error: '{}'", message)
            }
            ImplicaError::InvalidType { reason } => {
                write!(f, "Invalid Type: '{}'", reason)
            }
            ImplicaError::InvalidTerm { reason } => {
                write!(f, "Invalid Term: '{}'", reason)
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
            ImplicaError::RuntimeError { message, context } => {
                write!(f, "Something went wrong: '{message}'")?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }
            ImplicaError::TypeNotFound { uid, context } => {
                write!(f, "Type with Uid '{}' not found", hex::encode(uid))?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }

            ImplicaError::TermNotFound { uid, context } => {
                write!(f, "Term with Uid '{}' not found", hex::encode(uid))?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }

            ImplicaError::NodeNotFound { uid, context } => {
                write!(f, "Node with Uid '{}' not found", hex::encode(uid))?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }

            ImplicaError::EdgeNotFound { uid, context } => {
                write!(f, "Edge with Uid '({}, {})' not found", hex::encode(uid.0), hex::encode(uid.1))?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }

            ImplicaError::VariableAlreadyExists { name, context } => {
                write!(f, "Variable already exists: '{}'", name)?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }

            ImplicaError::VariableNotFound { name, context } => {
                write!(f, "Variable not found: '{}'", name)?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }
            ImplicaError::ContextConflict {
                name,
                original,
                new,
                context,
            } => {
                write!(f, "Context Conflict: tried to assign variable '{}' with current value of type '{}' to a new value of type '{}'", name, original, new)?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }
            ImplicaError::IndexCorruption { message, context } => {
                write!(f, "Index Corruption: '{}'", message)?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }
            ImplicaError::IndexOutOfRange {
                index,
                max_len,
                context,
            } => {
                write!(
                    f,
                    "Index Out of Range: tried to access index {} from an iterable of length {}",
                    index, max_len
                )?;
                if let Some(context) = context {
                    write!(f, " ({})", context)?;
                }
                Ok(())
            }
            ImplicaError::Infallible {  } => write!(f, "FATAL: An infallible error was returned, please contact the developers of the Implica Library as this should not occur.")
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
            ImplicaError::TypeMismatch { .. } | ImplicaError::InvalidType { .. } => {
                exceptions::PyTypeError::new_err(err.to_string())
            }

            ImplicaError::InvalidPattern { .. }
            | ImplicaError::InvalidIdentifier { .. }
            | ImplicaError::InvalidTerm { .. }
            | ImplicaError::SchemaValidation { .. }
            | ImplicaError::ContextConflict { .. } => {
                exceptions::PyValueError::new_err(err.to_string())
            }
            ImplicaError::VariableAlreadyExists { .. }
            | ImplicaError::VariableNotFound { .. }
            | ImplicaError::NodeNotFound { .. }
            | ImplicaError::EdgeNotFound { .. }
            | ImplicaError::TypeNotFound { .. }
            | ImplicaError::TermNotFound { .. } => exceptions::PyKeyError::new_err(err.to_string()),
            ImplicaError::PythonError { .. }
            | ImplicaError::RuntimeError { .. }
            | ImplicaError::EvaluationError { .. }
            | ImplicaError::LockError { .. } => {
                exceptions::PyRuntimeError::new_err(err.to_string())
            }
            ImplicaError::IndexCorruption { .. } => {
                exceptions::PyIndexError::new_err(err.to_string())
            }
            ImplicaError::IndexOutOfRange { .. } => {
                exceptions::PyKeyError::new_err(err.to_string())
            }
            ImplicaError::Infallible {} => exceptions::PySystemError::new_err(err.to_string()),
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

impl From<Infallible> for ImplicaError {
    fn from(_value: Infallible) -> Self {
        ImplicaError::Infallible {}
    }
}
