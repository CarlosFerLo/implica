use pyo3::{prelude::*, types::PyTuple};

use crate::{
    errors::ImplicaError,
    typing::{python_to_term, type_to_python, Term, Type},
};

#[pyclass]
#[derive(Debug)]
pub struct Constant {
    #[pyo3(get)]
    pub name: String,
    func: Py<PyAny>,
}

#[pymethods]
impl Constant {
    #[new]
    pub fn new(py: Python, name: String, func: Py<PyAny>) -> PyResult<Self> {
        if !func.bind(py).is_callable() {
            return Err(ImplicaError::PythonError {
                message: "'func' argument must be a callable".to_string(),
                context: Some("new constant".to_string()),
            }
            .into());
        }

        Ok(Constant { name, func })
    }

    #[pyo3(signature=(*args))]
    pub fn __call__(&self, py: Python, args: Py<PyTuple>) -> PyResult<Py<PyAny>> {
        let results = self.func.call1(py, args.bind(py))?;
        Ok(results)
    }
}

impl Constant {
    pub fn apply(&self, args: &[Type]) -> Result<Term, ImplicaError> {
        Python::attach(|py| -> PyResult<Term> {
            let py_args: Vec<_> = args
                .iter()
                .map(|t| type_to_python(py, t))
                .collect::<PyResult<_>>()?;
            let tuple = PyTuple::new(py, py_args)?;
            let py_result = self.func.call1(py, tuple)?;

            let term = python_to_term(py_result.bind(py))?;
            Ok(term)
        })
        .map_err(|e| ImplicaError::PythonError {
            message: e.to_string(),
            context: Some(format!("constant '{}' apply", &self.name)),
        })
    }
}

impl Clone for Constant {
    fn clone(&self) -> Self {
        Python::attach(|py| Constant {
            name: self.name.clone(),
            func: self.func.clone_ref(py),
        })
    }
}

impl PartialEq for Constant {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Constant {}
