mod cmp;
//mod eval;
mod data_queue;
mod hex_to_uid;
mod validation;

pub(crate) use cmp::compare_values;
//pub(crate) use eval::{props_as_map, Evaluator};
pub(crate) use data_queue::{DataQueue, QueueItem};
pub(crate) use hex_to_uid::hex_str_to_uid;
pub(crate) use validation::validate_variable_name;
