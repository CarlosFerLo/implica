use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// The properties map
pub(crate) type PropertyMap = HashMap<String, Py<PyAny>>;

// The shared + synchronized version
pub(crate) type SharedPropertyMap = Arc<RwLock<PropertyMap>>;
