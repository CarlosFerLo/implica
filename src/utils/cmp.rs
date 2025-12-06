use pyo3::prelude::*;
use std::cmp::Ordering;

pub(crate) fn compare_values(a: Option<Py<PyAny>>, b: Option<Py<PyAny>>, py: Python) -> Ordering {
    match (a, b) {
        (Some(val_a), Some(val_b)) => {
            let bound_a = val_a.bind(py);
            let bound_b = val_b.bind(py);

            match bound_a.compare(bound_b) {
                Ok(ord) => ord,
                Err(_) => {
                    let str_a = bound_a.to_string();
                    let str_b = bound_b.to_string();

                    str_a.cmp(&str_b)
                }
            }
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}
