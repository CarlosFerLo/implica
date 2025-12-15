use fancy_regex::Regex;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use rhai::{Dynamic, Engine, EvalAltResult, Map, Scope};
use std::collections::HashMap;

use crate::errors::ImplicaError;

#[derive(Debug)]
pub struct Evaluator {
    engine: Engine,
    replacements: Vec<(Regex, String)>,
}

impl Evaluator {
    pub fn new() -> Result<Self, ImplicaError> {
        let replacements = vec![
            (Regex::new(r"(?i)\bAND\b").unwrap(), "&&".to_string()),
            (Regex::new(r"(?i)\bOR\b").unwrap(), "||".to_string()),
            (Regex::new(r"(?i)\bNOT\b").unwrap(), "!".to_string()),
            (Regex::new(r"(?i)\bXOR\b").unwrap(), "^".to_string()),
            (Regex::new(r"(?<![<>=!])=(?!=)").unwrap(), "==".to_string()),
            (
                Regex::new(r"(\w+)\s+(?i)STARTS WITH\s+('[\w\s]+')").unwrap(),
                "starts_with($1, $2)".to_string(),
            ),
            (
                Regex::new(r"(\w+)\s+(?i)ENDS WITH\s+('[\w\s]+')").unwrap(),
                "ends_with($1, $2)".to_string(),
            ),
            (
                Regex::new(r"(\w+)\s+(?i)CONTAINS\s+('[\w\s]+')").unwrap(),
                "contains($1, $2)".to_string(),
            ),
        ];

        let mut engine = Engine::new();

        Self::register_custom_functions(&mut engine);

        Ok(Evaluator {
            engine,
            replacements,
        })
    }

    fn transpile(&self, query: &str) -> String {
        let mut processed = query.to_string();
        for (regex, replacement) in &self.replacements {
            processed = regex
                .replace_all(&processed, replacement.clone())
                .to_string();
        }
        processed
    }

    fn register_custom_functions(engine: &mut Engine) {
        engine.register_fn("starts_with", |s: Dynamic, prefix: Dynamic| {
            if s.is::<()>() || prefix.is::<()>() {
                return false;
            }

            let s_str = s.to_string();
            let prefix_str = prefix.to_string();

            s_str.starts_with(&prefix_str)
        });

        engine.register_fn("ends_with", |s: Dynamic, sufix: Dynamic| {
            if s.is::<()>() || sufix.is::<()>() {
                return false;
            }

            let s_str = s.to_string();
            let sufix_str = sufix.to_string();

            s_str.ends_with(&sufix_str)
        });

        engine.register_fn("contains", |s: Dynamic, pat: Dynamic| {
            if s.is::<()>() || pat.is::<()>() {
                return false;
            }

            let s_str = s.to_string();
            let pat_str = pat.to_string();

            s_str.contains(&pat_str)
        });
    }

    pub fn eval(&self, scope: &mut Scope, query: &str) -> Result<bool, ImplicaError> {
        let transpiled_query = self.transpile(query);

        match self
            .engine
            .eval_with_scope::<bool>(scope, &transpiled_query)
        {
            Ok(result) => Ok(result),
            Err(e) => match e.as_ref() {
                EvalAltResult::ErrorMismatchOutputType(output, _, _) => Ok(output != "()"),
                _ => Err(ImplicaError::EvaluationError {
                    message: e.to_string(),
                }),
            },
        }
    }
}

pub fn props_as_map(prop: &HashMap<String, Py<PyAny>>) -> Result<Map, ImplicaError> {
    let mut map = Map::new();

    Python::attach(|py| {
        for (k, obj_ref) in prop.iter() {
            let bound_obj = obj_ref.bind(py);
            let dynamic_val = to_dynamic(bound_obj);

            map.insert(k.clone().into(), dynamic_val);
        }
    });

    Ok(map)
}

fn to_dynamic(obj: &Bound<'_, PyAny>) -> Dynamic {
    if obj.is_none() {
        return Dynamic::UNIT;
    }

    if obj.is_instance_of::<PyBool>() {
        return match obj.extract::<bool>() {
            Ok(b) => Dynamic::from(b),
            Err(_) => Dynamic::FALSE,
        };
    }

    if obj.is_instance_of::<PyInt>() {
        return match obj.extract::<i64>() {
            Ok(i) => Dynamic::from(i),
            Err(_) => match obj.extract::<f64>() {
                Ok(f) => Dynamic::from(f),
                Err(_) => Dynamic::from(obj.to_string()),
            },
        };
    }

    if obj.is_instance_of::<PyFloat>() {
        return match obj.extract::<f64>() {
            Ok(f) => Dynamic::from(f),
            Err(_) => Dynamic::from(0.0),
        };
    }

    if obj.is_instance_of::<PyString>() {
        return match obj.extract::<String>() {
            Ok(s) => Dynamic::from(s),
            Err(_) => Dynamic::from(""),
        };
    }

    if let Ok(list) = obj.cast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list {
            arr.push(to_dynamic(&item));
        }
        return Dynamic::from_array(arr);
    }

    // 7. Check for Dict (Recursive)
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = Map::new();
        for (k, v) in dict {
            // Rhai keys must be strings. Force conversion of non-string keys.
            let key_str = k.extract::<String>().unwrap_or_else(|_| k.to_string());
            map.insert(key_str.into(), to_dynamic(&v));
        }
        return Dynamic::from_map(map);
    }

    // 8. Fallback: Any other Python object (Classes, Dates, etc.)
    // We convert them to their string representation to allow basic comparisons.
    Dynamic::from(obj.to_string())
}
