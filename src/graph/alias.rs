use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// The inner value stored per property
type PyValue = Py<PyAny>;

// The properties map
pub(in crate::graph) type PropertyMap = HashMap<String, PyValue>;

// The shared + synchronized version
pub(in crate::graph) type SharedPropertyMap = Arc<RwLock<PropertyMap>>;
