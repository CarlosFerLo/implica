//mod cmp;
//mod eval;
mod data_queue;
mod placeholder;
mod validation;

//pub(crate) use cmp::compare_values;
//pub(crate) use eval::{props_as_map, Evaluator};
pub(crate) use data_queue::{DataQueue, QueueItem};
pub(crate) use placeholder::{is_placeholder, PlaceholderGenerator};
pub(crate) use validation::validate_variable_name;
