use rhai::{Dynamic, Map};

use crate::properties::PyOpaque;

pub(crate) fn compare_values(value_1: &Dynamic, value_2: &Dynamic) -> bool {
    // Handle PyOpaque - compare Python object identity
    if value_1.is::<PyOpaque>() && value_2.is::<PyOpaque>() {
        let opaque_1 = value_1.clone().cast::<PyOpaque>();
        let opaque_2 = value_2.clone().cast::<PyOpaque>();
        return opaque_1.0.is(&opaque_2.0);
    }

    // Handle i64
    if let (Some(v1), Some(v2)) = (
        value_1.clone().try_cast::<i64>(),
        value_2.clone().try_cast::<i64>(),
    ) {
        return v1 == v2;
    }

    // Handle f64
    if let (Some(v1), Some(v2)) = (
        value_1.clone().try_cast::<f64>(),
        value_2.clone().try_cast::<f64>(),
    ) {
        return (v1 - v2).abs() < f64::EPSILON;
    }

    // Handle bool
    if let (Some(v1), Some(v2)) = (
        value_1.clone().try_cast::<bool>(),
        value_2.clone().try_cast::<bool>(),
    ) {
        return v1 == v2;
    }

    // Handle String
    if let (Some(v1), Some(v2)) = (
        value_1.clone().try_cast::<String>(),
        value_2.clone().try_cast::<String>(),
    ) {
        return v1 == v2;
    }

    // Handle Map
    if let (Some(map_1), Some(map_2)) = (
        value_1.clone().try_cast::<Map>(),
        value_2.clone().try_cast::<Map>(),
    ) {
        if map_1.len() != map_2.len() {
            return false;
        }
        for (key, val_1) in map_1.iter() {
            match map_2.get(key) {
                Some(val_2) => {
                    if !compare_values(val_1, val_2) {
                        return false;
                    }
                }
                None => return false,
            }
        }
        return true;
    }

    // Handle Vec<Dynamic>
    if let (Some(vec_1), Some(vec_2)) = (
        value_1.clone().try_cast::<Vec<Dynamic>>(),
        value_2.clone().try_cast::<Vec<Dynamic>>(),
    ) {
        if vec_1.len() != vec_2.len() {
            return false;
        }
        for (v1, v2) in vec_1.iter().zip(vec_2.iter()) {
            if !compare_values(v1, v2) {
                return false;
            }
        }
        return true;
    }

    // Types don't match or unknown type
    false
}
