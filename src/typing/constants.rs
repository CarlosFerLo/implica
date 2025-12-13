use pyo3::{prelude::*, types::PyTuple};

use crate::{
    context::{Context, ContextElement},
    errors::ImplicaError,
    patterns::TypeSchema,
    typing::{python_to_term, type_to_python, Term, Type},
};

#[pyclass]
#[derive(Debug)]
pub struct Constant {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub type_schema: TypeSchema,
    func: Py<PyAny>,
}

#[pymethods]
impl Constant {
    #[new]
    pub fn new(
        py: Python,
        name: String,
        type_schema: TypeSchema,
        func: Py<PyAny>,
    ) -> PyResult<Self> {
        if !func.bind(py).is_callable() {
            return Err(ImplicaError::PythonError {
                message: "'func' argument must be a callable".to_string(),
                context: Some("new constant".to_string()),
            }
            .into());
        }

        Ok(Constant {
            name,
            type_schema,
            func,
        })
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

    pub fn matches(&self, r#type: &Type) -> Result<Option<Term>, ImplicaError> {
        let mut context = Context::new();
        if self.type_schema.matches(r#type, &mut context)? {
            let mut args = Vec::new();

            for key in self.type_schema.ordered_capture_keys()? {
                match context.get(&key)? {
                    ContextElement::Type(t) => args.push(t.clone()),
                    ContextElement::Term(_) => {
                        return Err(ImplicaError::ContextConflict {
                            message: "expected context element to be a type but is a term"
                                .to_string(),
                            context: Some("constant matches".to_string()),
                        });
                    }
                }
            }

            let term = self.apply(&args)?;

            Ok(Some(term))
        } else {
            Ok(None)
        }
    }
}

impl Clone for Constant {
    fn clone(&self) -> Self {
        Python::attach(|py| Constant {
            name: self.name.clone(),
            type_schema: self.type_schema.clone(),
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
