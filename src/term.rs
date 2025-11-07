//! Type theoretical terms with their types.
//!
//! This module provides the `Term` structure representing typed terms in the type theory.
//! Terms have a name and an associated type, and support application operations.

use crate::errors::ImplicaError;
use crate::types::{python_to_type, type_to_python, Type};
use pyo3::prelude::*;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Represents a typed term in the type theory.
///
/// A term consists of a name and a type. Terms can be applied to each other
/// following the type theoretical rules: if term `f` has type `A -> B` and
/// term `x` has type `A`, then `f(x)` produces a new term with type `B`.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Create type variables
/// A = implica.Variable("A")
/// B = implica.Variable("B")
///
/// # Create function type A -> B
/// func_type = implica.Application(A, B)
///
/// # Create terms
/// f = implica.Term("f", func_type)
/// x = implica.Term("x", A)
///
/// # Apply term f to x
/// result = f(x)  # has type B
/// print(result)  # Term("(f x)", B)
/// ```
///
/// # Fields
///
/// * `name` - The name of the term
/// * `type` - The type of the term (accessible via get_type())
#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Term {
    #[pyo3(get, set)]
    pub name: String,
    pub r#type: Arc<Type>,
    // Cached uid
    uid_cache: Option<String>,
}

#[pymethods]
impl Term {
    /// Creates a new term with the given name and type.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the term
    /// * `type` - The type of the term (Variable or Application)
    ///
    /// # Returns
    ///
    /// A new `Term` instance
    ///
    /// # Examples
    ///
    /// ```python
    /// A = implica.Variable("A")
    /// x = implica.Term("x", A)
    /// ```
    #[new]
    pub fn new(name: String, r#type: Py<PyAny>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let type_obj = python_to_type(r#type.bind(py))?;
            Ok(Term {
                name,
                r#type: Arc::new(type_obj),
                uid_cache: None,
            })
        })
    }

    /// Gets the type of this term.
    ///
    /// # Returns
    ///
    /// The type as a Python object (Variable or Application)
    #[getter]
    pub fn get_type(&self, py: Python) -> PyResult<PyObject> {
        type_to_python(py, &self.r#type)
    }

    /// Returns a unique identifier for this term.
    ///
    /// The UID is constructed using SHA256 hash based on the term's name and type UID.
    ///
    /// # Returns
    ///
    /// A SHA256 hash representing this term uniquely
    pub fn uid(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"term:");
        hasher.update(self.name.as_bytes());
        hasher.update(b":");
        hasher.update(self.r#type.uid().as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Returns a string representation of the term.
    ///
    /// Format: "name:type"
    fn __str__(&self) -> String {
        format!("{}:{}", self.name, self.r#type)
    }

    /// Returns a detailed representation of the term for debugging.
    ///
    /// Format: Term("name", type)
    fn __repr__(&self) -> String {
        format!("Term(\"{}\", {})", self.name, self.r#type)
    }

    /// Applies this term to another term (function application).
    ///
    /// This implements the type theoretical application operation. If `self` has
    /// type `A -> B` and `other` has type `A`, the result has type `B`.
    ///
    /// # Arguments
    ///
    /// * `other` - The term to apply this term to
    /// * `py` - Python context
    ///
    /// # Returns
    ///
    /// A new term representing the application, with name "(self.name other.name)"
    ///
    /// # Errors
    ///
    /// * `TypeError` if self's type is not an application type
    /// * `TypeError` if other's type doesn't match the expected input type
    ///
    /// # Examples
    ///
    /// ```python
    /// # f has type A -> B, x has type A
    /// result = f(x)  # result has type B
    /// ```
    fn __call__(&self, other: &Term, py: Python) -> PyResult<Term> {
        // Check if self has an application type
        if let Type::Application(app) = &*self.r#type {
            // Check if other has the correct type (should match app.left)
            if *other.r#type == *app.left {
                // Return a term with type app.right and name (self.name other.name)
                let new_name = format!("({} {})", self.name, other.name);
                let new_type_py = type_to_python(py, &app.right)?;
                Term::new(new_name, new_type_py)
            } else {
                Err(ImplicaError::type_mismatch_with_context(
                    app.left.to_string(),
                    other.r#type.to_string(),
                    "function application",
                )
                .into())
            }
        } else {
            Err(ImplicaError::TypeMismatch {
                expected: "application type (A -> B)".to_string(),
                got: self.r#type.to_string(),
                context: Some("term application".to_string()),
            }
            .into())
        }
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.r#type.hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.name == other.name && self.r#type == other.r#type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Application, Variable};

    #[test]
    fn test_term_creation() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let var_a = Py::new(py, Variable::new("A".to_string())).unwrap();
            let term = Term::new("x".to_string(), var_a.into()).unwrap();

            assert_eq!(term.name, "x");
            assert_eq!(term.__str__(), "x:A");
            assert!(term.uid().starts_with("term_x_"));
        });
    }

    #[test]
    fn test_term_application() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let var_a = Py::new(py, Variable::new("A".to_string())).unwrap();
            let var_b = Py::new(py, Variable::new("B".to_string())).unwrap();

            let app = Py::new(
                py,
                Application::new(var_a.clone_ref(py).into(), var_b.into()).unwrap(),
            )
            .unwrap();

            let f = Term::new("f".to_string(), app.into()).unwrap();
            let x = Term::new("x".to_string(), var_a.into()).unwrap();

            let result = f.__call__(&x, py).unwrap();
            assert_eq!(result.name, "(f x)");
        });
    }
}
