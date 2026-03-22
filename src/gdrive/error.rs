use thiserror::Error;

#[derive(Error, Debug)]
pub enum DriveError {
    #[error("Permisos insuficientes: {0}")]
    InsufficientPermissions(String),
    
    #[error("Archivo no encontrado: {0}")]
    NotFound(String),
    
    #[error("Error de red: {0}")]
    Network(#[from] reqwest::Error),
    
    #[error("Error de la API de Google Drive: {0}")]
    ApiError(String),
    
    #[error("Error de autenticación: {0}")]
    Auth(String),
    
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl DriveError {
    /// Retorna true si el error es permanente (no vale la pena reintentar)
    #[cfg(test)]
    pub fn is_permanent(&self) -> bool {
        matches!(self, DriveError::InsufficientPermissions(_) | DriveError::NotFound(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case::permissions(DriveError::InsufficientPermissions("read denied".into()), true)]
    #[case::not_found(DriveError::NotFound("file_id_123".into()), true)]
    #[case::api_error(DriveError::ApiError("500 internal".into()), false)]
    #[case::auth(DriveError::Auth("token expired".into()), false)]
    #[case::other(DriveError::Other(anyhow::anyhow!("something")), false)]
    fn test_is_permanent(#[case] error: DriveError, #[case] expected: bool) {
        assert_eq!(error.is_permanent(), expected);
    }

    #[rstest]
    #[case::permissions(DriveError::InsufficientPermissions("read".into()), "Permisos insuficientes: read")]
    #[case::not_found(DriveError::NotFound("abc".into()), "Archivo no encontrado: abc")]
    #[case::api(DriveError::ApiError("429 rate limit".into()), "Error de la API de Google Drive: 429 rate limit")]
    #[case::auth(DriveError::Auth("expired".into()), "Error de autenticación: expired")]
    fn test_display_messages(#[case] error: DriveError, #[case] expected: &str) {
        assert_eq!(error.to_string(), expected);
    }
}
