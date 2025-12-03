use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// The properties map
pub(in crate::graph) type PropertyMap = HashMap<String, Py<PyAny>>;

// The shared + synchronized version
pub(in crate::graph) type SharedPropertyMap = Arc<RwLock<PropertyMap>>;
