use anyhow::Result;
use keyring::Entry;

/// Gestiona el almacenamiento seguro de tokens en GNOME Keyring
pub struct TokenStorage {
    service: String,
}

impl TokenStorage {
    pub fn new() -> Self {
        Self {
            service: "org.gnome.FedoraDrive".to_string(),
        }
    }
    
    /// Guarda el refresh token de forma segura en el keyring del sistema
    pub async fn save_refresh_token(&self, token: &str) -> Result<()> {
        let entry = Entry::new(&self.service, "refresh_token")?;
        entry.set_password(token)?;
        tracing::info!("Refresh token almacenado de forma segura en GNOME Keyring");
        Ok(())
    }
    
    /// Recupera el refresh token desde el keyring
    pub async fn load_refresh_token(&self) -> Result<String> {
        let entry = Entry::new(&self.service, "refresh_token")?;
        let token = entry.get_password()?;
        tracing::debug!("Refresh token recuperado desde el keyring");
        Ok(token)
    }
    
    /// Elimina el refresh token del keyring (útil para logout)
    pub async fn delete_refresh_token(&self) -> Result<()> {
        let entry = Entry::new(&self.service, "refresh_token")?;
        entry.delete_credential()?;
        tracing::info!("Refresh token eliminado del keyring");
        Ok(())
    }
    
    /// Verifica si existe un token guardado
    pub async fn has_stored_token(&self) -> bool {
        let entry = Entry::new(&self.service, "refresh_token");
        entry.map(|e| e.get_password().is_ok()).unwrap_or(false)
    }
}

impl Default for TokenStorage {
    fn default() -> Self {
        Self::new()
    }
}
