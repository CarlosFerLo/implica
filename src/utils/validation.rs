use crate::errors::ImplicaError;

const MAX_NAME_LENGTH: usize = 255;
const RESERVED_NAMES: &[&str] = &["None", "True", "False"];

pub(crate) fn validate_variable_name(name: &str) -> Result<(), ImplicaError> {
    // Longitud
    if name.is_empty() || name.len() > MAX_NAME_LENGTH {
        return Err(ImplicaError::InvalidIdentifier {
            name: name.to_string(),
            reason: format!("Name must be between 1 and {} characters", MAX_NAME_LENGTH),
        });
    }

    // Whitespace
    if name.trim() != name || name.contains(char::is_whitespace) {
        return Err(ImplicaError::InvalidIdentifier {
            name: name.to_string(),
            reason: "Name cannot contain whitespace".to_string(),
        });
    }

    // Caracteres válidos
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(ImplicaError::InvalidIdentifier {
            name: name.to_string(),
            reason: "Name can only contain alphanumeric characters and underscores".to_string(),
        });
    }

    // Debe empezar con letra o underscore
    if !name.chars().next().unwrap().is_alphabetic() && !name.starts_with('_') {
        return Err(ImplicaError::InvalidIdentifier {
            name: name.to_string(),
            reason: "Name must start with a letter or underscore".to_string(),
        });
    }

    // Nombres reservados
    if RESERVED_NAMES.contains(&name) {
        return Err(ImplicaError::InvalidIdentifier {
            name: name.to_string(),
            reason: format!("'{}' is a reserved name", name),
        });
    }

    Ok(())
}
