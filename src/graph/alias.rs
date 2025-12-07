use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub(crate) type PropertyMap = HashMap<String, Py<PyAny>>;

pub(crate) type SharedPropertyMap = Arc<RwLock<PropertyMap>>;
