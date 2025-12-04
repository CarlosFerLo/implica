mod eval;
mod validation;

pub(crate) use eval::{props_as_map, Evaluator};
pub(crate) use validation::validate_variable_name;
