use thiserror::Error;

#[derive(Error, Debug)]
pub enum DriveError {
    #[error("Permisos insuficientes: {0}")]
    InsufficientPermissions(String),
    
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
    #[allow(dead_code)] // Método auxiliar para uso futuro
    pub fn is_permanent(&self) -> bool {
        matches!(self, DriveError::InsufficientPermissions(_))
    }
}
