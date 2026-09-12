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
    /// Retorna true si el error es permanente (no vale la pena reintentar).
    /// Lo usa el uploader para liquidar archivos condenados en vez de
    /// reintentarlos cada ciclo eternamente.
    pub fn is_permanent(&self) -> bool {
        matches!(self, DriveError::InsufficientPermissions(_) | DriveError::NotFound(_))
    }

    /// Clasifica un status HTTP con su body en DriveError.
    /// 404 → NotFound (permanente). 403 solo es InsufficientPermissions si
    /// el body lo confirma: quota (`storageQuotaExceeded`) y rate-limit son
    /// accionables/transitorios y quedan como ApiError para reintento.
    pub fn from_http_status(status: u16, body: &str) -> Self {
        if status == 404 {
            return DriveError::NotFound(format!("Recurso no existe en Drive: {}", body));
        }
        if status == 403
            && (body.contains("insufficientFilePermissions")
                || body.contains("insufficientPermissions"))
        {
            return DriveError::InsufficientPermissions(format!(
                "Permiso denegado por Google Drive: {}",
                body
            ));
        }
        DriveError::ApiError(format!("{} - {}", status, body))
    }

    /// Extrae el DriveError permanente de una cadena anyhow (los métodos del
    /// cliente lo transportan bajo `.context()`). `None` = transitorio o
    /// desconocido: se reintenta con backoff, jamás se da de baja.
    pub fn permanent_of(err: &anyhow::Error) -> Option<&DriveError> {
        let found = err
            .chain()
            .find_map(|c| c.downcast_ref::<DriveError>())?;
        found.is_permanent().then_some(found)
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

    #[test]
    fn from_http_status_clasifica_404_y_403_con_body() {
        assert!(matches!(
            DriveError::from_http_status(404, "not found"),
            DriveError::NotFound(_)
        ));
        assert!(matches!(
            DriveError::from_http_status(403, r#"{"reason":"insufficientFilePermissions"}"#),
            DriveError::InsufficientPermissions(_)
        ));
        // 403 accionable/transitorio NO es permanente: quota y rate-limit
        // quedan como ApiError para reintento con backoff.
        assert!(matches!(
            DriveError::from_http_status(403, r#"{"reason":"storageQuotaExceeded"}"#),
            DriveError::ApiError(_)
        ));
        assert!(matches!(
            DriveError::from_http_status(429, "rate limit"),
            DriveError::ApiError(_)
        ));
        assert!(matches!(
            DriveError::from_http_status(500, "internal"),
            DriveError::ApiError(_)
        ));
    }

    #[test]
    fn permanent_of_atraviesa_context_y_rechaza_transitorio() {
        let perm = anyhow::anyhow!(DriveError::NotFound("x".into()))
            .context("Error actualizando archivo");
        assert!(DriveError::permanent_of(&perm).is_some_and(|d| d.is_permanent()));

        let trans = anyhow::anyhow!(DriveError::ApiError("500".into()))
            .context("Error subiendo archivo nuevo");
        assert!(DriveError::permanent_of(&trans).is_none());

        let plano = anyhow::anyhow!("timeout de red");
        assert!(DriveError::permanent_of(&plano).is_none());
    }
}
