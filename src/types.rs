//! Type system for type theoretical modeling.
//!
//! This module provides the core type system with variables and application types.
//! Types form the foundation for the type theoretical graph model.

use pyo3::prelude::*;
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;

/// Represents a type in the type theory.
///
/// A type can be either a variable (atomic type) or an application (function type).
/// This enum is the core of the type system and is used throughout the library
/// to represent types of nodes and terms.
///
/// # Variants
///
/// * `Variable` - An atomic type variable (e.g., "A", "Person", "Number")
/// * `Application` - A function type (e.g., "A -> B", "(Person -> Number) -> String")
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    Variable(Variable),
    Application(Application),
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
        let mut hasher = Sha256::new();
        match self {
            Type::Variable(v) => {
                hasher.update(b"var:");
                hasher.update(v.name.as_bytes());
            }
            Type::Application(a) => {
                hasher.update(b"app:");
                hasher.update(a.left.uid().as_bytes());
                hasher.update(b":");
                hasher.update(a.right.uid().as_bytes());
            }
        }
        format!("{:x}", hasher.finalize())
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

    /// Returns a reference to the inner Application if this is an Application type.
    ///
    /// # Returns
    ///
    /// `Some(&Application)` if this is an Application, `None` otherwise
    pub fn as_application(&self) -> Option<&Application> {
        match self {
            Type::Application(a) => Some(a),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    /// Formats the type for display.
    ///
    /// Variables are shown as their name, applications as "(left -> right)".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Variable(v) => write!(f, "{}", v),
            Type::Application(a) => write!(f, "{}", a),
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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Variable {
    #[pyo3(get, set)]
    pub name: String,
    // Cached uid
    uid_cache: Option<String>,
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
    pub fn new(name: String) -> Self {
        Variable {
            name,
            uid_cache: None,
        }
    }

    /// Returns a unique identifier for this variable.
    ///
    /// # Returns
    ///
    /// A SHA256 hash based on the variable name
    pub fn uid(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"var:");
        hasher.update(self.name.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Returns the name of the variable for string representation.
    fn __str__(&self) -> String {
        self.name.clone()
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
        self.name.hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl fmt::Display for Variable {
    /// Formats the variable for display (shows the name).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Represents a function type (application type).
///
/// An application represents a function type `left -> right`, where `left` is
/// the input type and `right` is the output type. Applications can be nested
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
/// func_type = implica.Application(A, B)
/// print(func_type)  # "(A -> B)"
///
/// # Create (A -> B) -> C (higher-order function type)
/// C = implica.Variable("C")
/// higher_order = implica.Application(func_type, C)
/// print(higher_order)  # "((A -> B) -> C)"
/// ```
///
/// # Fields (accessible via getters)
///
/// * `left` - The input type of the function
/// * `right` - The output type of the function
#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Application {
    pub left: Arc<Type>,
    pub right: Arc<Type>,
    // Cached uid
    uid_cache: Option<String>,
}

#[pymethods]
impl Application {
    /// Creates a new application type (function type).
    ///
    /// # Arguments
    ///
    /// * `left` - The input type (can be Variable or Application)
    /// * `right` - The output type (can be Variable or Application)
    ///
    /// # Returns
    ///
    /// A new `Application` representing the function type `left -> right`
    ///
    /// # Examples
    ///
    /// ```python
    /// A = implica.Variable("A")
    /// B = implica.Variable("B")
    /// func = implica.Application(A, B)  # A -> B
    /// ```
    #[new]
    pub fn new(left: Py<PyAny>, right: Py<PyAny>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let left_type = python_to_type(left.bind(py))?;
            let right_type = python_to_type(right.bind(py))?;
            Ok(Application {
                left: Arc::new(left_type),
                right: Arc::new(right_type),
                uid_cache: None,
            })
        })
    }

    /// Gets the left (input) type of this application.
    ///
    /// # Returns
    ///
    /// The input type as a Python object
    #[getter]
    pub fn left(&self, py: Python) -> PyResult<PyObject> {
        type_to_python(py, &self.left)
    }

    /// Gets the right (output) type of this application.
    ///
    /// # Returns
    ///
    /// The output type as a Python object
    #[getter]
    pub fn right(&self, py: Python) -> PyResult<PyObject> {
        type_to_python(py, &self.right)
    }

    /// Returns a unique identifier for this application.
    ///
    /// # Returns
    ///
    /// A SHA256 hash based on the left and right types
    pub fn uid(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"app:");
        hasher.update(self.left.uid().as_bytes());
        hasher.update(b":");
        hasher.update(self.right.uid().as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Returns a string representation of the application.
    ///
    /// Format: "(left -> right)"
    fn __str__(&self) -> String {
        format!("({} -> {})", self.left, self.right)
    }

    /// Returns a detailed representation for debugging.
    ///
    /// Format: Application(left, right)
    fn __repr__(&self) -> String {
        format!("Application({}, {})", self.left, self.right)
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.left.hash(&mut hasher);
        self.right.hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.left == other.left && self.right == other.right
    }
}

impl fmt::Display for Application {
    /// Formats the application for display.
    ///
    /// Shows as "(left -> right)".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} -> {})", self.left, self.right)
    }
}

/// Converts a Python object to a Rust Type.
///
/// # Arguments
///
/// * `obj` - A Python object that should be either a Variable or Application
///
/// # Returns
///
/// `Ok(Type)` if conversion succeeds
///
/// # Errors
///
/// `PyTypeError` if the object is neither a Variable nor an Application
pub fn python_to_type(obj: &Bound<'_, PyAny>) -> PyResult<Type> {
    if let Ok(var) = obj.extract::<Variable>() {
        Ok(Type::Variable(var))
    } else if let Ok(app) = obj.extract::<Application>() {
        Ok(Type::Application(app))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Expected Variable or Application",
        ))
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
/// A Python object representing the type (Variable or Application)
///
/// # Errors
///
/// Returns an error if the Python object creation fails
pub fn type_to_python(py: Python, typ: &Type) -> PyResult<PyObject> {
    match typ {
        Type::Variable(v) => Ok(Py::new(py, v.clone())?.into()),
        Type::Application(a) => Ok(Py::new(py, a.clone())?.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variable_creation() {
        let var = Variable::new("A".to_string());
        assert_eq!(var.name, "A");
        assert_eq!(var.uid(), "var_A");
        assert_eq!(var.__str__(), "A");
    }

    #[test]
    fn test_variable_equality() {
        let var1 = Variable::new("A".to_string());
        let var2 = Variable::new("A".to_string());
        let var3 = Variable::new("B".to_string());
        assert_eq!(var1, var2);
        assert_ne!(var1, var3);
    }

    #[test]
    fn test_type_display() {
        let var_a = Type::Variable(Variable::new("A".to_string()));
        let var_b = Type::Variable(Variable::new("B".to_string()));
        assert_eq!(format!("{}", var_a), "A");
        assert_eq!(format!("{}", var_b), "B");
    }
}
