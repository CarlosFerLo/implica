use pyo3::prelude::*;
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::errors::ImplicaError;
use crate::utils::validate_variable_name;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    Variable(Variable),
    Arrow(Arrow),
}

impl Type {
    pub fn uid(&self) -> &str {
        match self {
            Type::Variable(v) => v.uid(),
            Type::Arrow(a) => a.uid(),
        }
    }

    pub fn as_variable(&self) -> Option<&Variable> {
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

#[pyclass]
#[derive(Clone, Debug)]
pub struct Variable {
    pub name: String,
    /// Cached UID for performance - computed once and reused
    uid_cache: OnceLock<String>,
}

#[pymethods]
impl Variable {
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        // Validate that the name is not empty or whitespace-only
        if let Err(e) = validate_variable_name(&name) {
            return Err(e.into());
        }

        Ok(Variable {
            name,
            uid_cache: OnceLock::new(),
        })
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uid(&self) -> &str {
        self.uid_cache.get_or_init(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"var:");
            hasher.update(self.name.as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }

    fn __str__(&self) -> &str {
        &self.name
    }

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

#[pyclass]
#[derive(Clone, Debug)]
pub struct Arrow {
    pub left: Arc<Type>,
    pub right: Arc<Type>,
    /// Cached UID for performance - computed once and reused
    uid_cache: OnceLock<String>,
}

impl Arrow {
    pub fn new(left: Arc<Type>, right: Arc<Type>) -> Self {
        Arrow {
            left,
            right,
            uid_cache: OnceLock::new(),
        }
    }
}

#[pymethods]
impl Arrow {
    #[getter]
    pub fn left(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.left)
    }

    #[getter]
    pub fn right(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.right)
    }

    pub fn uid(&self) -> &str {
        self.uid_cache.get_or_init(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"app:");
            hasher.update(self.left.uid().as_bytes());
            hasher.update(b":");
            hasher.update(self.right.uid().as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }

    fn __str__(&self) -> String {
        format!("({} -> {})", self.left, self.right)
    }

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

pub(crate) fn python_to_type(obj: &Bound<'_, PyAny>) -> Result<Type, ImplicaError> {
    // Verificar que es del tipo correcto primero
    if obj.is_instance_of::<Variable>() {
        let var = obj.extract::<Variable>()?;
        // Validar integridad

        validate_variable_name(&var.name)?;

        Ok(Type::Variable(var))
    } else if obj.is_instance_of::<Arrow>() {
        Ok(Type::Arrow(obj.extract::<Arrow>()?))
    } else {
        Err(ImplicaError::PythonError {
            message: format!(
                "Expected Variable or Arrow, got {} of type {}",
                obj,
                obj.get_type()
                    .name()
                    .map(|n| { n.to_string() })
                    .unwrap_or("undefined".to_string())
            ),
            context: Some("python_to_type".to_string()),
        })
    }
}

pub(crate) fn type_to_python(py: Python, typ: &Type) -> PyResult<Py<PyAny>> {
    match typ {
        Type::Variable(v) => Ok(Py::new(py, v.clone())?.into()),
        Type::Arrow(a) => Ok(Py::new(py, a.clone())?.into()),
    }
}
