use pyo3::Python;

use crate::{
    errors::ImplicaError,
    graph::{PropertyMap, SharedPropertyMap},
};

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
