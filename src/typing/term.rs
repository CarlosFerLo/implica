use pyo3::prelude::*;
use sha2::{Digest, Sha256};
use std::{
    fmt::Display,
    sync::{Arc, OnceLock},
};

use crate::{
    errors::ImplicaError,
    typing::{python_to_type, type_to_python, Type},
    utils::validate_variable_name,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Term {
    Basic(BasicTerm),
    Application(Application),
}

impl Term {
    pub fn r#type(&self) -> Arc<Type> {
        match self {
            Term::Basic(basic) => basic.r#type.clone(),
            Term::Application(app) => app.r#type.clone(),
        }
    }

    pub fn as_basic(&self) -> Option<&BasicTerm> {
        match self {
            Term::Basic(basic) => Some(basic),
            Term::Application(_) => None,
        }
    }

    pub fn as_application(&self) -> Option<&Application> {
        match self {
            Term::Application(app) => Some(app),
            Term::Basic(_) => None,
        }
    }

    pub fn uid(&self) -> &str {
        match self {
            Term::Basic(b) => b.uid(),
            Term::Application(a) => a.uid(),
        }
    }

    pub fn apply(&self, other: &Term) -> Result<Term, ImplicaError> {
        Ok(Term::Application(Application::new(
            self.clone(),
            other.clone(),
        )?))
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Basic(b) => write!(f, "{}", b),
            Term::Application(a) => write!(f, "{}", a),
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct BasicTerm {
    #[pyo3(get)]
    pub name: String,
    pub r#type: Arc<Type>,
    uid_cache: OnceLock<String>,
}

impl Display for BasicTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl PartialEq for BasicTerm {
    fn eq(&self, other: &Self) -> bool {
        self.uid() == other.uid()
    }
}

impl Eq for BasicTerm {}

impl std::hash::Hash for BasicTerm {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl BasicTerm {
    pub fn new(name: String, r#type: Arc<Type>) -> Self {
        BasicTerm {
            name,
            r#type,
            uid_cache: OnceLock::new(),
        }
    }
}

#[pymethods]
impl BasicTerm {
    #[new]
    pub fn py_new(py: Python, name: String, r#type: Py<PyAny>) -> PyResult<Self> {
        if let Err(e) = validate_variable_name(&name) {
            return Err(e.into());
        }

        let type_arc = Arc::new(python_to_type(r#type.bind(py))?);
        Ok(BasicTerm {
            name,
            r#type: type_arc,
            uid_cache: OnceLock::new(),
        })
    }

    #[pyo3(name = "type")]
    pub fn py_type(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.r#type)
    }

    pub fn uid(&self) -> &str {
        self.uid_cache.get_or_init(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"var:");
            hasher.update(self.name.as_bytes());
            hasher.update(b":");
            hasher.update(self.r#type.to_string().as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }

    fn __str__(&self) -> String {
        self.name.to_string()
    }

    fn __repr__(&self) -> String {
        format!("BasicTerm(\"{}\")", self.name)
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        // Hash based on name (not the cache)
        self.uid().hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }

    fn __call__(&self, py: Python, other: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let other_term = python_to_term(other.bind(py))?;
        let self_term = Term::Basic(self.clone());
        let result = self_term.apply(&other_term)?;
        term_to_python(py, &result)
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct Application {
    pub function: Arc<Term>,
    pub argument: Arc<Term>,
    r#type: Arc<Type>,
    uid_cache: OnceLock<String>,
}

impl Display for Application {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} {})", self.function, self.argument)
    }
}

impl PartialEq for Application {
    fn eq(&self, other: &Self) -> bool {
        self.uid() == other.uid()
    }
}

impl Eq for Application {}

impl std::hash::Hash for Application {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.function.hash(state);
        self.argument.hash(state);
    }
}

impl Application {
    pub fn new(function: Term, argument: Term) -> Result<Self, ImplicaError> {
        match function.r#type().as_ref() {
            Type::Variable(_) => Err(ImplicaError::TypeMismatch {
                expected: "Application Type".to_string(),
                got: "Variable Type".to_string(),
                context: Some("application creation".to_string()),
            }),
            Type::Arrow(arr) => {
                if arr.left != argument.r#type() {
                    Err(ImplicaError::TypeMismatch {
                        expected: arr.left.to_string(),
                        got: argument.r#type().to_string(),
                        context: Some("application creation".to_string()),
                    })
                } else {
                    Ok(Application {
                        function: Arc::new(function),
                        argument: Arc::new(argument),
                        r#type: arr.right.clone(),
                        uid_cache: OnceLock::new(),
                    })
                }
            }
        }
    }
}

#[pymethods]
impl Application {
    #[new]
    pub fn py_new(py: Python, function: Py<PyAny>, argument: Py<PyAny>) -> PyResult<Self> {
        let function_obj = python_to_term(function.bind(py))?;
        let argument_obj = python_to_term(argument.bind(py))?;

        Application::new(function_obj, argument_obj).map_err(|e| e.into())
    }

    #[getter]
    pub fn get_function(&self, py: Python) -> PyResult<Py<PyAny>> {
        term_to_python(py, &self.function)
    }

    #[getter]
    pub fn get_argument(&self, py: Python) -> PyResult<Py<PyAny>> {
        term_to_python(py, &self.argument)
    }

    #[pyo3(name = "type")]
    pub fn py_type(&self, py: Python) -> PyResult<Py<PyAny>> {
        type_to_python(py, &self.r#type)
    }

    pub fn uid(&self) -> &str {
        self.uid_cache.get_or_init(|| {
            let mut hasher = Sha256::new();

            hasher.update(b"app:");
            hasher.update(self.function.uid().as_bytes());
            hasher.update(b":");
            hasher.update(self.argument.uid().as_bytes());
            hasher.update(b":");
            hasher.update(self.r#type.uid().as_bytes());

            format!("{:x}", hasher.finalize())
        })
    }

    fn __str__(&self) -> String {
        format!("({} {})", self.function, self.argument)
    }

    fn __repr__(&self) -> String {
        format!("Application({}, {})", self.function, self.argument)
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.uid().hash(&mut hasher);
        hasher.finish()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }

    fn __call__(&self, py: Python, other: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let other_term = python_to_term(other.bind(py))?;
        let self_term = Term::Application(self.clone());
        let result = self_term.apply(&other_term)?;
        term_to_python(py, &result)
    }
}

pub(crate) fn python_to_term(obj: &Bound<'_, PyAny>) -> PyResult<Term> {
    if let Ok(basic) = obj.extract::<BasicTerm>() {
        Ok(Term::Basic(basic))
    } else if let Ok(app) = obj.extract::<Application>() {
        Ok(Term::Application(app))
    } else {
        Err(ImplicaError::PythonError {
            message: format!("Error converting python object '{}' to term.", obj),
            context: Some("python_to_term".to_string()),
        }
        .into())
    }
}

pub(crate) fn term_to_python(py: Python, term: &Term) -> PyResult<Py<PyAny>> {
    match term {
        Term::Basic(b) => Ok(Py::new(py, b.clone())?.into()),
        Term::Application(a) => Ok(Py::new(py, a.clone())?.into()),
    }
}
