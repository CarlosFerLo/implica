//! Type theoretical terms with their types.
//!
//! This module provides the `Term` structure representing typed terms in the type theory.
//! Terms have a name and an associated type, and support Arrow operations.

use crate::errors::ImplicaError;
use crate::types::{python_to_type, type_to_python, Type};
use pyo3::prelude::*;
use sha2::{Digest, Sha256};
use std::sync::{Arc, RwLock};

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
/// func_type = implica.Arrow(A, B)
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
/// * `function_uid` - For application terms, the UID of the function being applied
/// * `argument_uid` - For application terms, the UID of the argument being applied
#[pyclass]
#[derive(Clone, Debug)]
pub struct Term {
    #[pyo3(get)]
    pub name: String,
    pub r#type: Arc<Type>,
    /// Cached UID for performance - computed once and reused
    uid_cache: Arc<RwLock<Option<String>>>,
    /// For application terms: UID of the function being applied
    #[pyo3(get)]
    pub function_uid: Option<String>,
    /// For application terms: UID of the argument being applied
    #[pyo3(get)]
    pub argument_uid: Option<String>,
}

#[pymethods]
impl Term {
    /// Creates a new term with the given name and type.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the term
    /// * `type` - The type of the term (Variable or Arrow)
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
        Python::attach(|py| {
            let type_obj = python_to_type(r#type.bind(py))?;
            Ok(Term {
                name,
                r#type: Arc::new(type_obj),
                uid_cache: Arc::new(RwLock::new(None)),
                function_uid: None,
                argument_uid: None,
            })
        })
    }

    /// Gets the type of this term.
    ///
    /// # Returns
    ///
    /// The type as a Python object (Variable or Arrow)
    #[getter]
    pub fn get_type(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.r#type)
    }

    /// Returns a unique identifier for this term.
    ///
    /// The UID is constructed using SHA256 hash based on the term's name and type UID.
    /// For Application terms, it also includes the UIDs of the function and argument terms.
    /// This result is cached to avoid recalculating for complex recursive types.
    ///
    /// # Returns
    ///
    /// A SHA256 hash representing this term uniquely
    pub fn uid(&self) -> String {
        // Check if we have a cached value
        if let Ok(cache) = self.uid_cache.read() {
            if let Some(cached) = cache.as_ref() {
                return cached.clone();
            }
        }

        // Calculate the UID
        let mut hasher = Sha256::new();

        // For Application terms, include function and argument UIDs
        if let (Some(func_uid), Some(arg_uid)) = (&self.function_uid, &self.argument_uid) {
            hasher.update(b"application:");
            hasher.update(func_uid.as_bytes());
            hasher.update(b":");
            hasher.update(arg_uid.as_bytes());
            hasher.update(b":");
            hasher.update(self.r#type.uid().as_bytes());
        } else {
            // Regular term
            hasher.update(b"term:");
            hasher.update(self.name.as_bytes());
            hasher.update(b":");
            hasher.update(self.r#type.uid().as_bytes());
        }

        let uid = format!("{:x}", hasher.finalize());

        // Cache it for future use
        if let Ok(mut cache) = self.uid_cache.write() {
            *cache = Some(uid.clone());
        }

        uid
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

    /// Applies this term to another term (function Arrow).
    ///
    /// This implements the type theoretical Arrow operation. If `self` has
    /// type `A -> B` and `other` has type `A`, the result has type `B`.
    ///
    /// The resulting term is an Application term that maintains references
    /// (via UIDs) to both the function and argument terms used to create it.
    ///
    /// # Arguments
    ///
    /// * `other` - The term to apply this term to
    ///
    /// # Returns
    ///
    /// A new Application term with name "(self.name other.name)", storing
    /// the UIDs of both the function (self) and argument (other) terms.
    ///
    /// # Errors
    ///
    /// * `TypeError` if self's type is not an Arrow type
    /// * `TypeError` if other's type doesn't match the expected input type
    ///
    /// # Examples
    ///
    /// ```python
    /// # f has type A -> B, x has type A
    /// result = f(x)  # result has type B
    /// # result.function_uid == f.uid()
    /// # result.argument_uid == x.uid()
    /// ```
    fn __call__(&self, other: &Term) -> PyResult<Term> {
        // Check if self has an Arrow type
        if let Type::Arrow(app) = &*self.r#type {
            // Check if other has the correct type (should match app.left)
            if *other.r#type == *app.left {
                // Create an application term with references to the original terms
                let new_name = format!("({} {})", self.name, other.name);
                let type_obj = (*app.right).clone();

                // Get UIDs from both terms
                let func_uid = self.uid();
                let arg_uid = other.uid();

                Ok(Term {
                    name: new_name,
                    r#type: Arc::new(type_obj),
                    uid_cache: Arc::new(RwLock::new(None)),
                    function_uid: Some(func_uid),
                    argument_uid: Some(arg_uid),
                })
            } else {
                Err(ImplicaError::type_mismatch_with_context(
                    app.left.to_string(),
                    other.r#type.to_string(),
                    "function Arrow",
                )
                .into())
            }
        } else {
            Err(ImplicaError::TypeMismatch {
                expected: "Arrow type (A -> B)".to_string(),
                got: self.r#type.to_string(),
                context: Some("term Arrow".to_string()),
            }
            .into())
        }
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        // Hash based on name, type, and application references (not the cache)
        self.name.hash(&mut hasher);
        self.r#type.hash(&mut hasher);
        self.function_uid.hash(&mut hasher);
        self.argument_uid.hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on name, type, and application references (not the cache)
        self.name == other.name
            && self.r#type == other.r#type
            && self.function_uid == other.function_uid
            && self.argument_uid == other.argument_uid
    }
}

impl PartialEq for Term {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.r#type == other.r#type
            && self.function_uid == other.function_uid
            && self.argument_uid == other.argument_uid
    }
}

impl Eq for Term {}

impl std::hash::Hash for Term {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.r#type.hash(state);
        self.function_uid.hash(state);
        self.argument_uid.hash(state);
    }
}
