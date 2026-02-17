use error_stack::{Report, ResultExt};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use pyo3::IntoPyObject;
use rayon::prelude::*;
use rhai::{Dynamic, Map};
use std::convert::Infallible;
use std::fmt::Display;
use std::sync::{Arc, RwLock};

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult, IntoPyResult};

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

impl Display for PropertyMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data_lock = self.data.read().map_err(|_| std::fmt::Error)?;

        write!(f, "{{")?;
        let mut first = true;
        for (key, value) in data_lock.iter() {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "{}: {:?}", key, value)?;
        }
        write!(f, "}}")
    }
}

impl<'py> IntoPyObject<'py> for PropertyMap {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let data_lock = self
            .data
            .read()
            .map_err(|e| {
                ImplicaError::LockError {
                    rw: "read".to_string(),
                    message: e.to_string(),
                    context: Some(ctx!("property map - into py object").to_string()),
                }
                .into()
            })
            .into_py_result()?;

        let dict = PyDict::new(py);
        for (key, value) in data_lock.iter() {
            dict.set_item(
                key.to_string(),
                rhai_to_py(value.clone(), py)
                    .attach(ctx!("property map - into py object"))
                    .into_py_result()?,
            )?;
        }
        Ok(dict.into_any())
    }
}

impl Default for PropertyMap {
    fn default() -> Self {
        PropertyMap {
            data: Arc::new(RwLock::new(Map::new())),
        }
    }
}

impl PropertyMap {
    pub fn new(data: &Bound<PyAny>) -> ImplicaResult<Self> {
        let dynamic_data = py_to_rhai(data).attach(ctx!("property map - new"))?;
        let map = dynamic_data
            .try_cast::<Map>()
            .ok_or_else(|| ImplicaError::PythonError {
                message: "Root of PropertyMap should be a Dict".to_string(),
                context: Some(ctx!("property map new").to_string()),
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

    //pub fn contains_key(&self, key: &str) -> ImplicaResult<bool> {
    //    let data_lock = self.data.read().map_err(|e| ImplicaError::LockError {
    //        rw: "read".to_string(),
    //        message: e.to_string(),
    //        context: Some(ctx!("property map - contains key").to_string()),
    //    })?;
    //
    //    Ok(data_lock.contains_key(key))
    //}

    pub fn insert(&self, key: String, value: Dynamic) -> ImplicaResult<()> {
        let mut data_lock = self.data.write().map_err(|e| ImplicaError::LockError {
            rw: "write".to_string(),
            message: e.to_string(),
            context: Some(ctx!("property map - insert").to_string()),
        })?;

        data_lock.insert(key.into(), value);
        Ok(())
    }

    pub fn get(&self, key: &str) -> ImplicaResult<Option<Dynamic>> {
        let data_lock = self.data.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some(ctx!("property map - get").to_string()),
        })?;

        Ok(data_lock.get(key).cloned())
    }

    pub fn try_par_compare<F>(&self, func: F) -> ImplicaResult<bool>
    where
        F: Fn(&str, &Dynamic) -> ImplicaResult<bool> + Send + Sync,
    {
        let data_lock = self.data.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some(ctx!("property map - try par compare").to_string()),
        })?;

        enum BreakReason {
            PredicateFailed,
            RuntimeError(Report<ImplicaError>),
        }

        let result = data_lock
            .par_iter()
            .try_for_each(|(key, value)| match func(key, value) {
                Ok(true) => Ok(()),
                Ok(false) => Err(BreakReason::PredicateFailed),
                Err(e) => Err(BreakReason::RuntimeError(
                    e.attach(ctx!("property map - try par compare - closure")),
                )),
            });

        match result {
            Ok(()) => Ok(true),
            Err(BreakReason::PredicateFailed) => Ok(false),
            Err(BreakReason::RuntimeError(e)) => Err(e),
        }
    }

    pub fn iter(&self) -> ImplicaResult<std::vec::IntoIter<(rhai::ImmutableString, Dynamic)>> {
        let map_lock = self.data.read().map_err(|e| ImplicaError::LockError {
            rw: "read".to_string(),
            message: e.to_string(),
            context: Some(ctx!("property map - iter").to_string()),
        })?;

        Ok(map_lock
            .iter()
            .map(|(k, v)| (k.clone().into(), v.clone()))
            .collect::<Vec<_>>()
            .into_iter())
    }
}

fn py_to_rhai(obj: &Bound<PyAny>) -> ImplicaResult<Dynamic> {
    if obj.is_instance_of::<PyBool>() {
        let val: bool = obj
            .extract()
            .map_err(|e: PyErr| Report::new(e.into()))
            .attach(ctx!("py to rhai - bool"))?;
        return Ok(Dynamic::from(val));
    }
    if obj.is_instance_of::<PyInt>() {
        let val: i64 = obj
            .extract()
            .map_err(|e: PyErr| Report::new(e.into()))
            .attach(ctx!("py to rhai - int"))?;
        return Ok(Dynamic::from(val));
    }
    if obj.is_exact_instance_of::<PyFloat>() {
        let val: f64 = obj
            .extract()
            .map_err(|e: PyErr| Report::new(e.into()))
            .attach(ctx!("py to rhai - float"))?;
        return Ok(Dynamic::from(val));
    }
    if obj.is_instance_of::<PyString>() {
        let val: String = obj
            .extract()
            .map_err(|e: PyErr| Report::new(e.into()))
            .attach(ctx!("py to rhai - string"))?;
        return Ok(Dynamic::from(val));
    }

    if let Ok(list) = obj.cast::<PyList>() {
        let mut vec = Vec::new();
        for item in list {
            vec.push(py_to_rhai(&item).attach(ctx!("py to rhai - list"))?);
        }
        return Ok(Dynamic::from(vec));
    }

    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = Map::new();
        for (k, v) in dict {
            let key_str: String = k.extract().map_err(|e: PyErr| Report::new(e.into()))?;
            map.insert(
                key_str.into(),
                py_to_rhai(&v).attach(ctx!("py to rhai - dict"))?,
            );
        }
        return Ok(Dynamic::from(map));
    }

    Ok(Dynamic::from(PyOpaque(obj.clone().unbind())))
}

fn rhai_to_py<'py>(val: Dynamic, py: Python<'py>) -> ImplicaResult<Bound<'py, PyAny>> {
    if val.is::<PyOpaque>() {
        let opaque = val.cast::<PyOpaque>();
        return Ok(opaque.0.bind(py).clone());
    }

    if let Some(v) = val.clone().try_cast::<i64>() {
        return Ok(v
            .into_pyobject(py)
            .map_err(|e: Infallible| Report::new(e.into()))
            .attach(ctx!("rhai to py - int"))?
            .into_any());
    }
    if let Some(v) = val.clone().try_cast::<f64>() {
        return Ok(v
            .into_pyobject(py)
            .map_err(|e: Infallible| Report::new(e.into()))
            .attach(ctx!("rhai to py - float"))?
            .into_any());
    }
    if let Some(v) = val.clone().try_cast::<bool>() {
        return Ok(v
            .into_pyobject(py)
            .map_err(|e: Infallible| Report::new(e.into()))
            .attach(ctx!("rhai to py - bool"))?
            .to_owned()
            .into_any());
    }
    if let Some(v) = val.clone().try_cast::<String>() {
        return Ok(v
            .into_pyobject(py)
            .map_err(|e: Infallible| Report::new(e.into()))
            .attach(ctx!("rhai to py - string"))?
            .into_any());
    }

    if let Some(map) = val.clone().try_cast::<Map>() {
        let dict = PyDict::new(py);
        for (k, v) in map {
            dict.set_item(
                k.to_string(),
                rhai_to_py(v, py).attach(ctx!("rhai to py - dict"))?,
            )
            .map_err(|e: PyErr| Report::new(e.into()))
            .attach(ctx!("rhai to py - dict"))?;
        }
        return Ok(dict.into_any());
    }

    if let Some(vec) = val.clone().try_cast::<Vec<Dynamic>>() {
        let list = PyList::empty(py);
        for item in vec {
            list.append(rhai_to_py(item, py).attach(ctx!("rhai to py - list"))?)
                .map_err(|e: PyErr| Report::new(e.into()))
                .attach(ctx!("rhai to py - list"))?;
        }
        return Ok(list.into_any());
    }

    Ok(py.None().bind(py).clone())
}
