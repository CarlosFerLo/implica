//! Type system for type theoretical modeling.
//!
//! This module provides the core type system with variables and Arrow types.
//! Types form the foundation for the type theoretical graph model.

use pyo3::prelude::*;
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::errors::ImplicaError;
use crate::utils::validate_variable_name;

/// Represents a type in the type theory.
///
/// A type can be either a variable (atomic type) or an Arrow (function type).
/// This enum is the core of the type system and is used throughout the library
/// to represent types of nodes and terms.
///
/// # Variants
///
/// * `Variable` - An atomic type variable (e.g., "A", "Person", "Number")
/// * `Arrow` - A function type (e.g., "A -> B", "(Person -> Number) -> String")
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    Variable(Variable),
    Arrow(Arrow),
}

impl Type {
    /// Returns a unique identifier for this type.
    ///
    /// The UID is constructed using SHA256 hash based on the type structure.
    ///
    /// # Returns
    ///
    /// A SHA256 hash representing this type uniquely
    pub fn uid(&self) -> String {
        match self {
            Type::Variable(v) => v.uid(),
            Type::Arrow(a) => a.uid(),
        }
    }

    /// Returns a reference to the inner Variable if this is a Variable type.
    ///
    /// # Returns
    ///
    /// `Some(&Variable)` if this is a Variable, `None` otherwise
    pub fn as_variable(&self) -> Option<&Variable> {
        match self {
            Type::Variable(v) => Some(v),
            _ => None,
        }
    }

    /// Returns a reference to the inner Arrow if this is an Arrow type.
    ///
    /// # Returns
    ///
    /// `Some(&Arrow)` if this is an Arrow, `None` otherwise
    pub fn as_arrow(&self) -> Option<&Arrow> {
        match self {
            Type::Arrow(a) => Some(a),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    /// Formats the type for display.
    ///
    /// Variables are shown as their name, Arrows as "(left -> right)".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Variable(v) => write!(f, "{}", v),
            Type::Arrow(a) => write!(f, "{}", a),
        }
    }
}

/// Represents an atomic type variable.
///
/// Variables are the basic building blocks of the type system. They represent
/// simple, atomic types like "A", "Person", "Number", etc.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Create type variables
/// person = implica.Variable("Person")
/// number = implica.Variable("Number")
///
/// print(person)  # "Person"
/// print(person.uid())  # "var_Person"
/// ```
///
/// # Fields
///
/// * `name` - The name of the type variable
#[pyclass]
#[derive(Clone, Debug)]
pub struct Variable {
    pub name: String,
    /// Cached UID for performance - computed once and reused
    uid_cache: Arc<RwLock<Option<String>>>,
}

#[pymethods]
impl Variable {
    /// Creates a new type variable with the given name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the type variable
    ///
    /// # Returns
    ///
    /// A new `Variable` instance
    ///
    /// # Examples
    ///
    /// ```python
    /// person_type = implica.Variable("Person")
    /// ```
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        // Validate that the name is not empty or whitespace-only
        if let Err(e) = validate_variable_name(&name) {
            return Err(e.into());
        }

        Ok(Variable {
            name,
            uid_cache: Arc::new(RwLock::new(None)),
        })
    }

    /// Gets the name of this variable.
    ///
    /// # Returns
    ///
    /// The name as a Python object
    #[getter]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Returns a unique identifier for this variable.
    ///
    /// This result is cached to maintain consistent performance.
    ///
    /// # Returns
    ///
    /// A SHA256 hash based on the variable name
    pub fn uid(&self) -> String {
        // Check if we have a cached value
        if let Ok(cache) = self.uid_cache.read() {
            if let Some(cached) = cache.as_ref() {
                return cached.clone();
            }
        }

        // Calculate the UID
        let mut hasher = Sha256::new();
        hasher.update(b"var:");
        hasher.update(self.name.as_bytes());
        let uid = format!("{:x}", hasher.finalize());

        // Cache it for future use
        if let Ok(mut cache) = self.uid_cache.write() {
            *cache = Some(uid.clone());
        }

        uid
    }

    /// Returns the name of the variable for string representation.
    fn __str__(&self) -> String {
        self.name.to_string()
    }

    /// Returns a detailed representation for debugging.
    ///
    /// Format: Variable("name")
    fn __repr__(&self) -> String {
        format!("Variable(\"{}\")", self.name)
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        // Hash based on name (not the cache)
        self.name.hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on uid
        self.uid() == other.uid()
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
        self.uid() == other.uid()
    }
}

impl Eq for Variable {}

impl std::hash::Hash for Variable {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

/// Represents a function type (Arrow type).
///
/// An Arrow represents a function type `left -> right`, where `left` is
/// the input type and `right` is the output type. Arrows can be nested
/// to create complex function types.
///
/// # Examples
///
/// ```python
/// import implica
///
/// # Create A -> B
/// A = implica.Variable("A")
/// B = implica.Variable("B")
/// func_type = implica.Arrow(A, B)
/// print(func_type)  # "(A -> B)"
///
/// # Create (A -> B) -> C (higher-order function type)
/// C = implica.Variable("C")
/// higher_order = implica.Arrow(func_type, C)
/// print(higher_order)  # "((A -> B) -> C)"
/// ```
///
/// # Fields (accessible via getters)
///
/// * `left` - The input type of the function
/// * `right` - The output type of the function
#[pyclass]
#[derive(Clone, Debug)]
pub struct Arrow {
    pub left: Arc<Type>,
    pub right: Arc<Type>,
    /// Cached UID for performance - computed once and reused
    uid_cache: Arc<RwLock<Option<String>>>,
}

impl Arrow {
    pub fn new(left: Arc<Type>, right: Arc<Type>) -> Self {
        Arrow {
            left,
            right,
            uid_cache: Arc::new(RwLock::new(None)),
        }
    }
}

#[pymethods]
impl Arrow {
    /// Gets the left (input) type of this Arrow.
    ///
    /// # Returns
    ///
    /// The input type as a Python object
    #[getter]
    pub fn left(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.left)
    }

    /// Gets the right (output) type of this Arrow.
    ///
    /// # Returns
    ///
    /// The output type as a Python object
    #[getter]
    pub fn right(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.right)
    }

    /// Returns a unique identifier for this Arrow.
    ///
    /// This result is cached to avoid recalculating for complex recursive types.
    ///
    /// # Returns
    ///
    /// A SHA256 hash based on the left and right types
    pub fn uid(&self) -> String {
        // Check if we have a cached value
        if let Ok(cache) = self.uid_cache.read() {
            if let Some(cached) = cache.as_ref() {
                return cached.clone();
            }
        }

        // Calculate the UID (may recursively compute UIDs of nested types)
        let mut hasher = Sha256::new();
        hasher.update(b"app:");
        hasher.update(self.left.uid().as_bytes());
        hasher.update(b":");
        hasher.update(self.right.uid().as_bytes());
        let uid = format!("{:x}", hasher.finalize());

        // Cache it for future use
        if let Ok(mut cache) = self.uid_cache.write() {
            *cache = Some(uid.clone());
        }

        uid
    }

    /// Returns a string representation of the Arrow.
    ///
    /// Format: "(left -> right)"
    fn __str__(&self) -> String {
        format!("({} -> {})", self.left, self.right)
    }

    /// Returns a detailed representation for debugging.
    ///
    /// Format: Arrow(left, right)
    fn __repr__(&self) -> String {
        format!("Arrow({}, {})", self.left, self.right)
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        // Hash based on left and right (not the cache)
        self.left.hash(&mut hasher);
        self.right.hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on uid
        self.uid() == other.uid()
    }
}

impl fmt::Display for Arrow {
    /// Formats the Arrow for display.
    ///
    /// Shows as "(left -> right)".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} -> {})", self.left, self.right)
    }
}

impl PartialEq for Arrow {
    fn eq(&self, other: &Self) -> bool {
        self.uid() == other.uid()
    }
}

impl Eq for Arrow {}

impl std::hash::Hash for Arrow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.right.hash(state);
    }
}

/// Converts a Python object to a Rust Type.
///
/// # Arguments
///
/// * `obj` - A Python object that should be either a Variable or Arrow
///
/// # Returns
///
/// `Ok(Type)` if conversion succeeds
///
/// # Errors
///
/// `PyTypeError` if the object is neither a Variable nor an Arrow
pub(crate) fn python_to_type(obj: &Bound<'_, PyAny>) -> Result<Type, ImplicaError> {
    if let Ok(var) = obj.extract::<Variable>() {
        Ok(Type::Variable(var))
    } else if let Ok(app) = obj.extract::<Arrow>() {
        Ok(Type::Arrow(app))
    } else {
        Err(ImplicaError::PythonError {
            message: format!("Error converting python object '{}' to type.", obj),
            context: Some("python_to_type".to_string()),
        })
    }
}

/// Converts a Rust Type to a Python object.
///
/// # Arguments
///
/// * `py` - Python context
/// * `typ` - The Type to convert
///
/// # Returns
///
/// A Python object representing the type (Variable or Arrow)
///
/// # Errors
///
/// Returns an error if the Python object creation fails
pub(crate) fn type_to_python(py: Python, typ: &Type) -> PyResult<Py<PyAny>> {
    match typ {
        Type::Variable(v) => Ok(Py::new(py, v.clone())?.into()),
        Type::Arrow(a) => Ok(Py::new(py, a.clone())?.into()),
    }
}
