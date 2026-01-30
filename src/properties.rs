use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use rhai::{Dynamic, Map};
use std::sync::{Arc, RwLock};

use crate::errors::ImplicaError;

#[derive(Debug)]
pub(crate) struct PyOpaque(pub Py<PyAny>);

impl Clone for PyOpaque {
    fn clone(&self) -> Self {
        Python::attach(|py| PyOpaque(self.0.clone_ref(py)))
    }
}

#[derive(Debug, Clone)]
pub struct PropertyMap {
    data: Arc<RwLock<Map>>,
}

impl PropertyMap {
    pub fn new(data: &Bound<PyAny>) -> Result<Self, ImplicaError> {
        let dynamic_data = py_to_rhai(data)?;
        let map = dynamic_data
            .try_cast::<Map>()
            .ok_or_else(|| ImplicaError::PythonError {
                message: "Root of PropertyMap should be a Dict".to_string(),
                context: Some("property map new".to_string()),
            })?;

        Ok(PropertyMap {
            data: Arc::new(RwLock::new(map)),
        })
    }

    pub fn empty() -> Self {
        PropertyMap {
            data: Arc::new(RwLock::new(Map::new())),
        }
    }

    pub fn contains_key(&self, key: &str) -> Result<bool, ImplicaError> {
        let data_lock = self.data.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some("property map - contains key".to_string()),
        })?;

        Ok(data_lock.contains_key(key))
    }

    pub fn insert(&self, key: String, value: Dynamic) -> Result<(), ImplicaError> {
        if self.contains_key(&key)? {
            return Err(ImplicaError::VariableAlreadyExists {
                name: key,
                context: Some("property map - insert".to_string()),
            });
        }

        let mut data_lock = self.data.write().map_err(|e| ImplicaError::LockError {
            rw: "write".to_string(),
            message: e.to_string(),
            context: Some("property map - insert".to_string()),
        })?;

        data_lock.insert(key.into(), value);
        Ok(())
    }
}

fn py_to_rhai(obj: &Bound<PyAny>) -> Result<Dynamic, ImplicaError> {
    if obj.is_instance_of::<PyBool>() {
        let val: bool = obj.extract()?;
        return Ok(Dynamic::from(val));
    }
    if obj.is_instance_of::<PyInt>() {
        let val: i64 = obj.extract()?;
        return Ok(Dynamic::from(val));
    }
    if obj.is_exact_instance_of::<PyFloat>() {
        let val: f64 = obj.extract()?;
        return Ok(Dynamic::from(val));
    }
    if obj.is_instance_of::<PyString>() {
        let val: String = obj.extract()?;
        return Ok(Dynamic::from(val));
    }

    if let Ok(list) = obj.cast::<PyList>() {
        let mut vec = Vec::new();
        for item in list {
            vec.push(py_to_rhai(&item)?);
        }
        return Ok(Dynamic::from(vec));
    }

    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = Map::new();
        for (k, v) in dict {
            let key_str: String = k.extract()?;
            map.insert(key_str.into(), py_to_rhai(&v)?);
        }
        return Ok(Dynamic::from(map));
    }

    Ok(Dynamic::from(PyOpaque(obj.clone().unbind())))
}

fn _rhai_to_py<'py>(val: Dynamic, py: Python<'py>) -> Result<Bound<'py, PyAny>, ImplicaError> {
    if val.is::<PyOpaque>() {
        let opaque = val.cast::<PyOpaque>();
        return Ok(opaque.0.bind(py).clone());
    }

    if let Some(v) = val.clone().try_cast::<i64>() {
        return Ok(v.into_pyobject(py)?.into_any());
    }
    if let Some(v) = val.clone().try_cast::<f64>() {
        return Ok(v.into_pyobject(py)?.into_any());
    }
    if let Some(v) = val.clone().try_cast::<bool>() {
        return Ok(v.into_pyobject(py)?.to_owned().into_any());
    }
    if let Some(v) = val.clone().try_cast::<String>() {
        return Ok(v.into_pyobject(py)?.into_any());
    }

    if let Some(map) = val.clone().try_cast::<Map>() {
        let dict = PyDict::new(py);
        for (k, v) in map {
            dict.set_item(k.to_string(), _rhai_to_py(v, py)?)?;
        }
        return Ok(dict.into_any());
    }

    if let Some(vec) = val.clone().try_cast::<Vec<Dynamic>>() {
        let list = PyList::empty(py);
        for item in vec {
            list.append(_rhai_to_py(item, py)?)?;
        }
        return Ok(list.into_any());
    }

    Ok(py.None().bind(py).clone())
}
