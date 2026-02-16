use crate::errors::{ImplicaError, ImplicaResult};
use crate::graph::Uid;

pub(crate) fn hex_str_to_uid(hex_str: &str) -> ImplicaResult<Uid> {
    let bytes = hex::decode(hex_str).map_err(|e| ImplicaError::HexConversionError {
        reason: format!("Invalid hex string: {} - '{}'", hex_str, e),
        context: Some("hex str to uid".to_string()),
    })?;

    if bytes.len() != 32 {
        return Err(ImplicaError::HexConversionError {
            reason: format!("Uid must be 32 bytes, got {}", bytes.len()),
            context: Some("hex str to uid".to_string()),
        }
        .into());
    }

    let mut uid: Uid = [0u8; 32];
    uid.copy_from_slice(&bytes);
    Ok(uid)
}
