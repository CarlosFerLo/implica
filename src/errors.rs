use pyo3::pyclass::PyClassGuardError;
use pyo3::{exceptions, PyErr, PyResult};
use std::convert::Infallible;

use error_stack::Report;
use thiserror::Error;

use crate::graph::Uid;

#[derive(Debug, Clone, Error)]
pub enum ImplicaError {
    #[error("Type Mismatch: expected {expected}, got {got}{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    TypeMismatch {
        expected: String,
        got: String,
        context: Option<String>,
    },

    #[error("Invalid Pattern; '{pattern}': {reason}")]
    InvalidPattern { pattern: String, reason: String },

    #[error("Schema Validation Failed for '{schema}': {reason}")]
    SchemaValidation { schema: String, reason: String },

    #[error("Invalid Identifier '{name}': {reason}")]
    InvalidIdentifier { name: String, reason: String },

    #[error("Python Error: '{message}'{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    PythonError {
        message: String,
        context: Option<String>,
    },

    //#[error("Evaluation Error: '{message}'")]
    //EvaluationError { message: String },
    #[error("Invalid Type: '{reason}'")]
    InvalidType { reason: String },

    #[error("Invalid Term: '{reason}'")]
    InvalidTerm { reason: String },

    #[error("Failed to acquire {rw} lock: '{message}'{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    LockError {
        rw: String,
        message: String,
        context: Option<String>,
    },

    #[error("Something went wrong: '{message}'{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    RuntimeError {
        message: String,
        context: Option<String>,
    },

    #[error("Type with Uid: '{}' not found{}", hex::encode(.uid), context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    TypeNotFound { uid: Uid, context: Option<String> },

    #[error("Term with Uid: '{}' not found{}", hex::encode(.uid), context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    TermNotFound { uid: Uid, context: Option<String> },

    #[error("Node with Uid: '{}' not found{}", hex::encode(.uid), context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    NodeNotFound { uid: Uid, context: Option<String> },
    #[error("Edge with Uid: '({}, {})' not found{}", hex::encode(.uid.0), hex::encode(.uid.1), context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    EdgeNotFound {
        uid: (Uid, Uid),
        context: Option<String>,
    },
    #[error("Variable already exists: '{name}'{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    VariableAlreadyExists {
        name: String,
        context: Option<String>,
    },
    #[error("Variable not found: '{name}'{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    VariableNotFound {
        name: String,
        context: Option<String>,
    },
    #[error("Context Conflict: tried to assign variable '{name}' currently holding a '{original}' to a '{new}'{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    ContextConflict {
        name: String,
        original: String,
        new: String,
        context: Option<String>,
    },

    #[error("Index Corruption: '{message}'{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    IndexCorruption {
        message: String,
        context: Option<String>,
    },
    #[error("Index Out of Range: tried to access index {index} from an iterable of length {max_len}{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    IndexOutOfRange {
        index: usize,
        max_len: usize,
        context: Option<String>,
    },

    #[error("FATAL: An infallible error was returned, please contact the developers of the Implica Library as this should not occur.")]
    Infallible {},

    #[error("Invalid Query:\n{query}\n\nReason: {reason}{}", context.as_ref().map(|c| format!("\n({})", c)).unwrap_or_default())]
    InvalidQuery {
        query: String,
        reason: String,
        context: Option<String>,
    },

    #[error("Invalid Number of Arguments: expected {expected}, got {got}{}", context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    InvalidNumberOfArguments {
        expected: usize,
        got: usize,
        context: Option<String>,
    },

    #[error("Constant Not Found: '{name}'{}",context.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    ConstantNotFound {
        name: String,
        context: Option<String>,
    },
}

pub type ImplicaResult<T> = Result<T, Report<ImplicaError>>;

pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for ImplicaResult<T> {
    fn into_py_result(self) -> PyResult<T> {
        self.map_err(|report| {
            let current_error = report.current_context();
            let full_message = format_report(&report);

            match current_error {
                ImplicaError::TypeMismatch { .. } | ImplicaError::InvalidType { .. } => {
                    exceptions::PyTypeError::new_err(full_message)
                }

                ImplicaError::InvalidQuery { .. }
                | ImplicaError::InvalidPattern { .. }
                | ImplicaError::InvalidIdentifier { .. }
                | ImplicaError::InvalidTerm { .. }
                | ImplicaError::SchemaValidation { .. }
                | ImplicaError::ContextConflict { .. }
                | ImplicaError::InvalidNumberOfArguments { .. } => {
                    exceptions::PyValueError::new_err(full_message)
                }
                ImplicaError::VariableAlreadyExists { .. }
                | ImplicaError::VariableNotFound { .. }
                | ImplicaError::NodeNotFound { .. }
                | ImplicaError::EdgeNotFound { .. }
                | ImplicaError::TypeNotFound { .. }
                | ImplicaError::TermNotFound { .. }
                | ImplicaError::ConstantNotFound { .. } => {
                    exceptions::PyKeyError::new_err(full_message)
                }
                ImplicaError::PythonError { .. }
                | ImplicaError::RuntimeError { .. }
                //| ImplicaError::EvaluationError { .. }
                | ImplicaError::LockError { .. } => {
                    exceptions::PyRuntimeError::new_err(full_message)
                }
                ImplicaError::IndexCorruption { .. } => {
                    exceptions::PyIndexError::new_err(full_message)
                }
                ImplicaError::IndexOutOfRange { .. } => {
                    exceptions::PyKeyError::new_err(full_message)
                }
                ImplicaError::Infallible {} => exceptions::PySystemError::new_err(full_message),
            }
        })
    }
}

fn format_report(report: &Report<ImplicaError>) -> String {
    let mut message = report.current_context().to_string();
    for frame in report.frames() {
        if let Some(printable) = frame.downcast_ref::<String>() {
            message.push_str("\n → ");
            message.push_str(printable);
        }
    }
    message
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
