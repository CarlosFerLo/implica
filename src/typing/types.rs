use pyo3::prelude::*;
use std::fmt;
use std::sync::Arc;

use crate::errors::ImplicaError;
use crate::utils::validate_variable_name;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Variable(Variable),
    Arrow(Arrow),
}

impl Type {
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
    #[pyo3(get)]
    pub name: String,
}

#[pymethods]
impl Variable {
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        // Validate that the name is not empty or whitespace-only
        if let Err(e) = validate_variable_name(&name) {
            return Err(e.into());
        }

        Ok(Variable { name })
    }

    fn __str__(&self) -> &str {
        &self.name
    }

    fn __repr__(&self) -> String {
        format!("Variable(\"{}\")", self.name)
    }

    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on uid
        self == other
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
        self.name == other.name
    }
}

impl Eq for Variable {}

#[pyclass]
#[derive(Clone, Debug)]
pub struct Arrow {
    pub left: Arc<Type>,
    pub right: Arc<Type>,
}

impl Arrow {
    pub fn new(left: Arc<Type>, right: Arc<Type>) -> Self {
        Arrow { left, right }
    }
}

#[pymethods]
impl Arrow {
    #[new]
    pub fn py_new(py: Python, left: Py<PyAny>, right: Py<PyAny>) -> PyResult<Self> {
        let left_obj = python_to_type(left.bind(py))?;
        let right_obj = python_to_type(right.bind(py))?;

        Ok(Arrow {
            left: Arc::new(left_obj),
            right: Arc::new(right_obj),
        })
    }

    #[getter]
    pub fn left(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.left)
    }

    #[getter]
    pub fn right(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.right)
    }

    fn __str__(&self) -> String {
        format!("({} -> {})", self.left, self.right)
    }

    fn __repr__(&self) -> String {
        format!("Arrow({}, {})", self.left, self.right)
    }

    fn __eq__(&self, other: &Self) -> bool {
        // Equality based on uid
        self == other
    }
}

impl fmt::Display for Arrow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} -> {})", self.left, self.right)
    }
}

impl PartialEq for Arrow {
    fn eq(&self, other: &Self) -> bool {
        (self.right == other.right) && (self.left == other.left)
    }
}

impl Eq for Arrow {}

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
