//! Módulo de autenticación OAuth2 para Google Drive
//! 
//! Implementa el flujo de "Installed Application" que es seguro
//! y no requiere WebViews embebidos.

use anyhow::{Context, Result};
use std::sync::Arc;
use yup_oauth2::{ApplicationSecret, InstalledFlowAuthenticator, InstalledFlowReturnMethod};

use super::TokenStorage;

/// Gestor de autenticación OAuth2 para Google Drive
pub struct OAuth2Manager {
    app_secret: ApplicationSecret,
    #[allow(dead_code)] // Será usado para logout
    token_storage: Arc<TokenStorage>,
}

impl OAuth2Manager {
    pub fn new(app_secret: ApplicationSecret) -> Self {
        Self {
            app_secret,
            token_storage: Arc::new(TokenStorage::new()),
        }
    }

    /// Crea una nueva instancia cargando el secreto desde un archivo JSON
    pub async fn new_from_file(path: &str) -> Result<Self> {
        let secret = yup_oauth2::read_application_secret(path)
            .await
            .context(format!("No se pudo leer el archivo de credenciales: {}", path))?;
        
        Ok(Self::new(secret))
    }
    
    /// Construye y retorna el autenticador configurado
    pub async fn get_authenticator(&self) -> Result<yup_oauth2::authenticator::Authenticator<yup_oauth2::hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>> {
        // Resolver la ruta del home correctamente (~ no funciona en Rust)
        let home = std::env::var("HOME").context("No se pudo obtener variable HOME")?;
        let token_path = format!("{}/.config/fedoradrive/tokens.json", home);
        
        // Asegurar que el directorio padre existe
        let token_dir = std::path::Path::new(&token_path).parent();
        if let Some(dir) = token_dir {
            std::fs::create_dir_all(dir).ok();
        }
        
        InstalledFlowAuthenticator::builder(
            self.app_secret.clone(),
            InstalledFlowReturnMethod::HTTPRedirect,
        )
        .persist_tokens_to_disk(&token_path)
        .build()
        .await
        .context("Error al construir el autenticador OAuth2")
    }

    /// Ejecuta el flujo completo de autenticación OAuth2
    pub async fn authenticate(&self) -> Result<()> {
        tracing::info!("Iniciando proceso de autenticación OAuth2");
        
        let auth = self.get_authenticator().await?;
        
        let scopes = &["https://www.googleapis.com/auth/drive"];
        let token = auth
            .token(scopes)
            .await
            .context("Error al obtener token de acceso")?;
        
        tracing::info!("Autenticación exitosa, token obtenido");
        tracing::debug!("Token expira en: {:?}", token.expiration_time());
        
        Ok(())
    }
    
    /// Revoca la autenticación y elimina los tokens almacenados
    #[allow(dead_code)] // Feature para logout futuro
    pub async fn logout(&self) -> Result<()> {
        tracing::info!("Cerrando sesión y eliminando tokens");
        self.token_storage.delete_refresh_token().await?;
        
        // TODO: Revocar el token en los servidores de Google
        // usando la API de revocación: https://oauth2.googleapis.com/revoke
        
        Ok(())
    }
    
    /// Verifica si el usuario está autenticado
    #[allow(dead_code)] // Feature para verificación de sesión
    pub async fn is_authenticated(&self) -> bool {
        self.token_storage.has_stored_token().await
    }
}
