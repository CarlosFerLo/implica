//! Error types for the implica library.
//!
//! This module provides a comprehensive error handling system with specific error types
//! for different failure scenarios. Each error type maps to an appropriate Python exception,
//! providing clear and actionable error messages to users.
//!
//! # Error Types
//!
//! - [`ImplicaError::TypeMismatch`] - Type system errors (e.g., applying incompatible types)
//! - [`ImplicaError::NodeNotFound`] - Graph lookup failures
//! - [`ImplicaError::EdgeNotFound`] - Edge lookup failures
//! - [`ImplicaError::InvalidPattern`] - Pattern parsing/syntax errors
//! - [`ImplicaError::InvalidQuery`] - Query construction errors
//! - [`ImplicaError::InvalidIdentifier`] - Identifier validation errors
//! - [`ImplicaError::PropertyError`] - Property access/modification errors
//!
//! # Examples
//!
//! ```rust
//! use implica::errors::ImplicaError;
//! use pyo3::PyErr;
//!
//! // Type mismatch error
//! let err: PyErr = ImplicaError::TypeMismatch {
//!     expected: "Int".to_string(),
//!     got: "String".to_string(),
//!     context: Some("function Arrow".to_string()),
//! }.into();
//!
//! // Invalid pattern error
//! let err: PyErr = ImplicaError::InvalidPattern {
//!     pattern: "(unclosed".to_string(),
//!     reason: "Unmatched opening parenthesis".to_string(),
//! }.into();
//! ```

use pyo3::{exceptions, PyErr};
use std::fmt::{Display, Formatter, Result};

use crate::graph::{Edge, Node};

/// Main error type for the implica library.
///
/// This enum represents all possible errors that can occur during library operations.
/// Each variant contains specific information about the error context and is automatically
/// converted to an appropriate Python exception type.
#[derive(Debug, Clone)]
pub enum ImplicaError {
    /// Type mismatch error - occurs when types don't match in operations like Arrow.
    ///
    /// Maps to Python's `TypeError`.
    ///
    /// # Examples
    ///
    /// ```python
    /// # Python example
    /// import implica
    ///
    /// # Trying to apply incompatible types raises TypeError
    /// int_type = implica.Variable("Int")
    /// string_type = implica.Variable("String")
    /// term1 = implica.Term(implica.Arrow(int_type, string_type), {})
    /// term2 = implica.Term(string_type, {})
    /// # term1.apply(term2)  # TypeError: Type mismatch: expected String, got Int
    /// ```
    TypeMismatch {
        /// The expected type
        expected: String,
        /// The actual type received
        got: String,
        /// Optional context where the error occurred
        context: Option<String>,
    },

    /// Node not found error - occurs when a node lookup fails.
    ///
    /// Maps to Python's `KeyError`.
    ///
    /// # Examples
    ///
    /// ```python
    /// # Python example
    /// import implica
    ///
    /// graph = implica.Graph()
    /// # graph.get_node("nonexistent_uid")  # KeyError: Node not found: nonexistent_uid
    /// ```
    NodeNotFound {
        /// The UID that was searched for
        uid: String,
        /// Optional context about where the lookup was attempted
        context: Option<String>,
    },

    /// Edge not found error - occurs when an edge lookup fails.
    ///
    /// Maps to Python's `KeyError`.
    EdgeNotFound {
        /// The UID of the edge that was searched for
        uid: String,
        /// Optional context about where the lookup was attempted
        context: Option<String>,
    },

    /// Invalid pattern error - occurs when parsing or validating patterns.
    ///
    /// Maps to Python's `ValueError`.
    ///
    /// # Examples
    ///
    /// ```python
    /// # Python example
    /// import implica
    ///
    /// # Unmatched parentheses
    /// # pattern = implica.PathPattern.parse("(n")  # ValueError: Invalid pattern
    /// ```
    InvalidPattern {
        /// The invalid pattern string
        pattern: String,
        /// Reason why the pattern is invalid
        reason: String,
    },

    /// Invalid query error - occurs during query construction or execution.
    ///
    /// Maps to Python's `ValueError`.
    InvalidQuery {
        /// Description of what went wrong
        message: String,
        /// Optional context about the query operation
        context: Option<String>,
    },

    /// Invalid identifier error - occurs when validating names/identifiers.
    ///
    /// Maps to Python's `ValueError`.
    ///
    /// # Examples
    ///
    /// ```python
    /// # Python example
    /// import implica
    ///
    /// # Empty name
    /// # var = implica.Variable("")  # ValueError: Invalid identifier: name cannot be empty
    /// ```
    InvalidIdentifier {
        /// The invalid identifier
        name: String,
        /// Reason why it's invalid
        reason: String,
    },

    /// Property error - occurs during property access or modification.
    ///
    /// Maps to Python's `AttributeError`.
    PropertyError {
        /// The property key involved
        key: String,
        /// Description of the error
        message: String,
    },

    /// Variable not found error - occurs when a query references an undefined variable.
    ///
    /// Maps to Python's `NameError`.
    VariableNotFound {
        /// The variable name that was not found
        name: String,
        /// Optional context about where it was referenced
        context: Option<String>,
    },

    /// Schema validation error - occurs when type schema validation fails.
    ///
    /// Maps to Python's `ValueError`.
    SchemaValidation {
        /// The schema that failed validation
        schema: String,
        /// Reason for validation failure
        reason: String,
    },

    /// Invalid configuration error - occurs when configuration parameters are invalid.
    ///
    /// Maps to Python's `ValueError`.
    ///
    /// # Examples
    ///
    /// ```python
    /// # Python example
    /// import implica
    ///
    /// # Invalid bloom filter false positive rate
    /// # config = implica.IndexConfig(bloom_filter_fpr=1.5)  # ValueError: Invalid configuration
    /// ```
    InvalidConfiguration {
        /// The configuration parameter that is invalid
        parameter: String,
        /// The invalid value
        value: String,
        /// Reason why it's invalid
        reason: String,
    },

    /// Index operation error - occurs during index operations.
    ///
    /// Maps to Python's `RuntimeError`.
    IndexOperation {
        /// Description of the operation that failed
        operation: String,
        /// Reason for failure
        reason: String,
    },

    PythonError {
        message: String,
        context: Option<String>,
    },

    NodeAlreadyExists {
        message: String,
        existing: Node,
        new: Node,
    },
    EdgeAlreadyExists {
        message: String,
        existing: Edge,
        new: Edge,
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
            ImplicaError::InvalidConfiguration {
                parameter,
                value,
                reason,
            } => {
                write!(
                    f,
                    "Invalid configuration for '{}' = '{}': {}",
                    parameter, value, reason
                )
            }
            ImplicaError::IndexOperation { operation, reason } => {
                write!(f, "Index operation '{}' failed: {}", operation, reason)
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
            | ImplicaError::SchemaValidation { .. }
            | ImplicaError::InvalidConfiguration { .. } => {
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
            ImplicaError::IndexOperation { .. } => {
                exceptions::PyRuntimeError::new_err(err.to_string())
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
        }
    }
}

// Helper functions for creating common errors

impl ImplicaError {
    /// Creates a type mismatch error with context.
    pub fn type_mismatch(expected: impl Into<String>, got: impl Into<String>) -> Self {
        ImplicaError::TypeMismatch {
            expected: expected.into(),
            got: got.into(),
            context: None,
        }
    }

    /// Creates a type mismatch error with context.
    pub fn type_mismatch_with_context(
        expected: impl Into<String>,
        got: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        ImplicaError::TypeMismatch {
            expected: expected.into(),
            got: got.into(),
            context: Some(context.into()),
        }
    }

    /// Creates a node not found error.
    pub fn node_not_found(uid: impl Into<String>) -> Self {
        ImplicaError::NodeNotFound {
            uid: uid.into(),
            context: None,
        }
    }

    /// Creates a node not found error with context.
    pub fn node_not_found_with_context(uid: impl Into<String>, context: impl Into<String>) -> Self {
        ImplicaError::NodeNotFound {
            uid: uid.into(),
            context: Some(context.into()),
        }
    }

    /// Creates an edge not found error.
    pub fn edge_not_found(uid: impl Into<String>) -> Self {
        ImplicaError::EdgeNotFound {
            uid: uid.into(),
            context: None,
        }
    }

    /// Creates an invalid pattern error.
    pub fn invalid_pattern(pattern: impl Into<String>, reason: impl Into<String>) -> Self {
        ImplicaError::InvalidPattern {
            pattern: pattern.into(),
            reason: reason.into(),
        }
    }

    /// Creates an invalid query error.
    pub fn invalid_query(message: impl Into<String>) -> Self {
        ImplicaError::InvalidQuery {
            message: message.into(),
            context: None,
        }
    }

    /// Creates an invalid identifier error.
    pub fn invalid_identifier(name: impl Into<String>, reason: impl Into<String>) -> Self {
        ImplicaError::InvalidIdentifier {
            name: name.into(),
            reason: reason.into(),
        }
    }

    /// Creates a property error.
    pub fn property_error(key: impl Into<String>, message: impl Into<String>) -> Self {
        ImplicaError::PropertyError {
            key: key.into(),
            message: message.into(),
        }
    }

    /// Creates a variable not found error.
    pub fn variable_not_found(name: impl Into<String>) -> Self {
        ImplicaError::VariableNotFound {
            name: name.into(),
            context: None,
        }
    }

    /// Creates a schema validation error.
    pub fn schema_validation(schema: impl Into<String>, reason: impl Into<String>) -> Self {
        ImplicaError::SchemaValidation {
            schema: schema.into(),
            reason: reason.into(),
        }
    }

    /// Creates an invalid configuration error.
    pub fn invalid_configuration(
        parameter: impl Into<String>,
        value: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        ImplicaError::InvalidConfiguration {
            parameter: parameter.into(),
            value: value.into(),
            reason: reason.into(),
        }
    }

    /// Creates an index operation error.
    pub fn index_operation(operation: impl Into<String>, reason: impl Into<String>) -> Self {
        ImplicaError::IndexOperation {
            operation: operation.into(),
            reason: reason.into(),
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
