use anyhow::Result;
use keyring::Entry;

/// Gestiona el almacenamiento seguro de tokens en GNOME Keyring
pub struct TokenStorage {
    #[allow(dead_code)] // Usado en métodos de la estructura
    service: String,
}

impl TokenStorage {
    pub fn new() -> Self {
        Self {
            service: "org.gnome.FedoraDrive".to_string(),
        }
    }
    
    /// Guarda el refresh token de forma segura en el keyring del sistema
    #[allow(dead_code)] // Feature para gestión manual de tokens
    pub async fn save_refresh_token(&self, token: &str) -> Result<()> {
        let entry = Entry::new(&self.service, "refresh_token")?;
        entry.set_password(token)?;
        tracing::info!("Refresh token almacenado de forma segura en GNOME Keyring");
        Ok(())
    }
    
    /// Recupera el refresh token desde el keyring
    #[allow(dead_code)] // Feature para gestión manual de tokens
    pub async fn load_refresh_token(&self) -> Result<String> {
        let entry = Entry::new(&self.service, "refresh_token")?;
        let token = entry.get_password()?;
        tracing::debug!("Refresh token recuperado desde el keyring");
        Ok(token)
    }
    
    /// Elimina el refresh token del keyring (útil para logout)
    #[allow(dead_code)] // Usado por logout() en OAuth2Manager
    pub async fn delete_refresh_token(&self) -> Result<()> {
        let entry = Entry::new(&self.service, "refresh_token")?;
        entry.delete_credential()?;
        tracing::info!("Refresh token eliminado del keyring");
        Ok(())
    }
    
    /// Verifica si existe un token guardado
    #[allow(dead_code)] // Usado por is_authenticated() en OAuth2Manager
    pub async fn has_stored_token(&self) -> bool {
        let entry = Entry::new(&self.service, "refresh_token");
        entry.map(|e| e.get_password().is_ok()).unwrap_or(false)
    }
    
    /// Limpia todas las credenciales del keyring
    #[allow(dead_code)] // Método auxiliar, usado indirectamente por clear_all_auth_data()
    pub fn clear_all_credentials(&self) -> Result<()> {
        let entry = Entry::new(&self.service, "refresh_token")?;
        let _ = entry.delete_credential(); // Ignorar error si no existe
        tracing::info!("Credenciales eliminadas del keyring");
        Ok(())
    }
}

impl Default for TokenStorage {
    fn default() -> Self {
        Self::new()
    }
}
