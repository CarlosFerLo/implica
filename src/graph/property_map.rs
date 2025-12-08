use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule, PyTuple};
use pyo3::Python;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::errors::ImplicaError;

pub(crate) type PropertyMap = HashMap<String, Py<PyAny>>;

pub(crate) type SharedPropertyMap = Arc<RwLock<PropertyMap>>;

pub(crate) fn clone_property_map(map: &SharedPropertyMap) -> Result<PropertyMap, ImplicaError> {
    Python::attach(|py| {
        Ok(map
            .read()
            .map_err(|e| ImplicaError::LockError {
                rw: "read".to_string(),
                message: e.to_string(),
                context: Some("clone property map".to_string()),
            })?
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
            .collect())
    })
}

pub(crate) fn python_to_property_map(obj: &Bound<'_, PyAny>) -> PyResult<PropertyMap> {
    let mut props = HashMap::new();
    let dict = obj.cast::<PyDict>()?;
    let py = dict.py();

    // Import deepcopy from the copy module
    let copy_module = PyModule::import(py, "copy")?;
    let deepcopy = copy_module.getattr("deepcopy")?;

    for (k, v) in dict.iter() {
        let key = k.extract::<String>()?;
        // Perform deep copy of the value to ensure complete immutability
        let deep_copied_val = deepcopy.call1((v,))?;
        props.insert(key, deep_copied_val.unbind());
    }

    Ok(props)
}

pub(crate) fn property_map_to_python(py: Python, props: &PropertyMap) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);

    // Import deepcopy from the copy module
    let copy_module = PyModule::import(py, "copy")?;
    let deepcopy = copy_module.getattr("deepcopy")?;

    for (k, v) in props.iter() {
        // Perform deep copy when returning properties to ensure immutability
        let deep_copied_val = deepcopy.call1((v.bind(py),))?;
        // Make the value deeply immutable
        let immutable_val = make_deeply_immutable(py, deep_copied_val)?;
        dict.set_item(k, immutable_val)?;
    }

    let types_module = PyModule::import(py, "types")?;
    let mapping_proxy = types_module.getattr("MappingProxyType")?;
    let proxy = mapping_proxy.call1((dict,))?;

    Ok(proxy.into())
}

/// Recursively make Python objects immutable
fn make_deeply_immutable<'py>(
    py: Python<'py>,
    obj: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    // Check if it's a dict
    if obj.is_instance_of::<PyDict>() {
        let dict = obj.cast::<PyDict>()?;
        let new_dict = PyDict::new(py);
        for (k, v) in dict.iter() {
            let immutable_v = make_deeply_immutable(py, v)?;
            new_dict.set_item(k, immutable_v)?;
        }
        let types_module = PyModule::import(py, "types")?;
        let mapping_proxy = types_module.getattr("MappingProxyType")?;
        return mapping_proxy.call1((new_dict,));
    }

    // Check if it's a list
    if obj.is_instance_of::<PyList>() {
        let list = obj.cast::<PyList>()?;
        let immutable_items: PyResult<Vec<_>> = list
            .iter()
            .map(|item| make_deeply_immutable(py, item))
            .collect();
        let tuple = PyTuple::new(py, immutable_items?)?;
        return Ok(tuple.into_any());
    }

    // For other types (primitives, etc.), return as-is
    Ok(obj)
}

pub(crate) fn property_map_to_string(props: &PropertyMap) -> String {
    Python::attach(|py| {
        let mut content = Vec::new();

        for (k, v) in props.iter() {
            let v_bound = v.bind(py);
            let v_repr = v_bound
                .repr()
                .map(|r| r.to_string())
                .unwrap_or_else(|_| "None".to_string());
            content.push(format!("'{}': {}", k, v_repr));
        }

        format!("{{{}}}", content.join(", "))
    })
}
