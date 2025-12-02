use crate::errors::ImplicaError;

pub(crate) fn validate_variable_name(name: &str) -> Result<(), ImplicaError> {
    if name.trim().is_empty() {
        return Err(ImplicaError::InvalidIdentifier {
            name: name.to_string(),
            reason: "Name cannot be empty or just blank space".to_string(),
        });
    }

    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(ImplicaError::InvalidIdentifier {
            name: name.to_string(),
            reason: "Name can only have alphanumeric characters and underscores".to_string(),
        });
    }

    Ok(())
}
