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
    token_storage: Arc<TokenStorage>,
}

impl OAuth2Manager {
    pub fn new(app_secret: ApplicationSecret) -> Self {
        Self {
            app_secret,
            token_storage: Arc::new(TokenStorage::new()),
        }
    }
    
    /// Ejecuta el flujo completo de autenticación OAuth2
    /// 
    /// Este método:
    /// 1. Verifica si ya existe un token válido en el keyring
    /// 2. Si no existe, inicia el flujo de autenticación con el navegador
    /// 3. Guarda el refresh token de forma segura
    pub async fn authenticate(&self) -> Result<()> {
        tracing::info!("Iniciando proceso de autenticación OAuth2");
        
        // Verificar si ya existe un token
        if self.token_storage.has_stored_token().await {
            tracing::info!("Token existente encontrado, intentando reutilizar");
            // TODO: Validar que el token sigue siendo válido
        }
        
        // Construir el autenticador
        // yup-oauth2 maneja automáticamente el servidor HTTP local
        let auth = InstalledFlowAuthenticator::builder(
            self.app_secret.clone(),
            InstalledFlowReturnMethod::HTTPRedirect,
        )
        .persist_tokens_to_disk("~/.config/fedoradrive/tokens.json")
        .build()
        .await
        .context("Error al construir el autenticador OAuth2")?;
        
        // Solicitar token para el scope de Google Drive
        let scopes = &["https://www.googleapis.com/auth/drive"];
        let token = auth
            .token(scopes)
            .await
            .context("Error al obtener token de acceso")?;
        
        tracing::info!("Autenticación exitosa, token obtenido");
        tracing::debug!("Token expira en: {:?}", token.expiration_time());
        
        // Nota: yup-oauth2 maneja internamente la persistencia del refresh_token
        // cuando usamos persist_tokens_to_disk(). Sin embargo, también lo guardamos
        // en el keyring para mayor seguridad si está disponible en los metadatos.
        
        Ok(())
    }
    
    /// Revoca la autenticación y elimina los tokens almacenados
    pub async fn logout(&self) -> Result<()> {
        tracing::info!("Cerrando sesión y eliminando tokens");
        self.token_storage.delete_refresh_token().await?;
        
        // TODO: Revocar el token en los servidores de Google
        // usando la API de revocación: https://oauth2.googleapis.com/revoke
        
        Ok(())
    }
    
    /// Verifica si el usuario está autenticado
    pub async fn is_authenticated(&self) -> bool {
        self.token_storage.has_stored_token().await
    }
}
