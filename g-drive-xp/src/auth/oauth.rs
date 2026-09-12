//! Módulo de autenticación OAuth2 para Google Drive
//! 
//! Implementa el flujo de "Installed Application" que es seguro
//! y no requiere WebViews embebidos.

use anyhow::{Context, Result};
use yup_oauth2::{ApplicationSecret, InstalledFlowAuthenticator, InstalledFlowReturnMethod};
use yup_oauth2::authenticator_delegate::InstalledFlowDelegate;
use std::future::Future;
use std::pin::Pin;

use super::keyring::{KeyringTokenStore, DRIVE_SCOPES};

/// Gestor de autenticación OAuth2 para Google Drive
pub struct OAuth2Manager {
    app_secret: ApplicationSecret,
}

/// Delegado para capturar la URL de autenticación y enviarla a la GUI
struct LoginUrlDelegate {
    ui_sender: Option<relm4::ComponentSender<crate::gui::app_model::AppModel>>,
}

impl InstalledFlowDelegate for LoginUrlDelegate {
    fn present_user_url<'a>(
        &'a self,
        url: &'a str,
        _need_code: bool,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        let url = url.to_string();
        let ui_sender = self.ui_sender.clone();
        
        Box::pin(async move {
            tracing::info!("captured URL: {}", url);
            if let Some(sender) = ui_sender {
                sender.input(crate::gui::app_model::AppMsg::SetLoginUrl(url));
            }
            Ok(String::new())
        })
    }
}

impl OAuth2Manager {
    pub fn new(app_secret: ApplicationSecret) -> Self {
        Self { app_secret }
    }

    /// Crea una nueva instancia cargando el secreto desde un archivo JSON
    pub async fn new_from_file(path: &str) -> Result<Self> {
        let secret = yup_oauth2::read_application_secret(path)
            .await
            .context(format!("No se pudo leer el archivo de credenciales: {}", path))?;
        
        Ok(Self::new(secret))
    }
    
    /// Construye y retorna el autenticador configurado
    pub async fn get_authenticator(&self, ui_sender: Option<relm4::ComponentSender<crate::gui::app_model::AppModel>>) -> Result<yup_oauth2::authenticator::Authenticator<yup_oauth2::hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>> {
        // Resolver la ruta del home correctamente (~ no funciona en Rust)
        let home = std::env::var("HOME").context("No se pudo obtener variable HOME")?;
        let token_path = format!("{}/.config/fedoradrive/tokens.json", home);
        
        // Asegurar que el directorio padre existe
        let token_dir = std::path::Path::new(&token_path).parent();
        if let Some(dir) = token_dir {
            std::fs::create_dir_all(dir).ok();
        }
        
        let mut builder = InstalledFlowAuthenticator::builder(
            self.app_secret.clone(),
            InstalledFlowReturnMethod::HTTPRedirect,
        );

        if ui_sender.is_some() {
            builder = builder.flow_delegate(Box::new(LoginUrlDelegate { ui_sender }));
        }

        // Migrar el tokens.json histórico (plano) al keyring cifrado.
        // Si la migración falla, el JSON se conserva y se sigue usando:
        // jamás bloquear el arranque por esto.
        match super::keyring::migrate_tokens_json_to_keyring(std::path::Path::new(&token_path)) {
            Ok(super::keyring::MigrateOutcome::Migrated { count }) => {
                tracing::info!("🔐 {} refresh token(s) migrados al keyring; JSON plano triturado", count)
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("No se pudo migrar tokens al keyring, se conserva JSON: {:?}", e),
        }

        // Keyring primero (cifrado); disco plano solo donde no hay secret-service.
        let builder = if super::keyring::keyring_available() {
            builder.with_storage(Box::new(KeyringTokenStore::new()))
        } else {
            tracing::warn!("Keyring no disponible: tokens en JSON plano (entorno sin secret-service)");
            builder.persist_tokens_to_disk(&token_path)
        };
        builder
            .build()
            .await
            .context("Error al construir el autenticador OAuth2")
    }

    /// Ejecuta el flujo completo de autenticación OAuth2
    pub async fn authenticate(&self, ui_sender: Option<relm4::ComponentSender<crate::gui::app_model::AppModel>>) -> Result<()> {
        tracing::info!("Iniciando proceso de autenticación OAuth2");
        
        let auth = self.get_authenticator(ui_sender).await?;
        
        let scopes = &["https://www.googleapis.com/auth/drive"];
        let token = auth
            .token(scopes)
            .await
            .context("Error al obtener token de acceso")?;
        
        tracing::info!("Autenticación exitosa, token obtenido");
        tracing::debug!("Token expira en: {:?}", token.expiration_time());
        
        Ok(())
    }

    /// Verifica si el usuario está autenticado (hay refresh en keyring o
    /// JSON pre-migración en disco).
    #[allow(dead_code)] // Feature para verificación de sesión
    pub async fn is_authenticated(&self) -> bool {
        if let Ok(Some(_)) = KeyringTokenStore::new().load_refresh(DRIVE_SCOPES) {
            return true;
        }
        std::env::var("HOME")
            .map(|home| std::path::Path::new(&format!("{}/.config/fedoradrive/tokens.json", home)).exists())
            .unwrap_or(false)
    }
}

/// Endpoint oficial de revocación OAuth2 de Google. Revocar el refresh
/// invalida también los access del mismo grant, por eso se revoca el
/// refresh siempre que se conoce.
/// Ref: https://developers.google.com/identity/protocols/oauth2/web-server#tokenrevoke
const REVOKE_URL: &str = "https://oauth2.googleapis.com/revoke";

/// `true` si el revoke quedó resuelto en Google: revocado ahora (2xx) o ya
/// inexistente (400 `invalid_token`). Función pura para testear sin red.
fn revoke_resolved(status: reqwest::StatusCode) -> bool {
    status.is_success() || status == reqwest::StatusCode::BAD_REQUEST
}

/// Revoca un token en los servidores de Google (timeout corto). Un 400 es
/// éxito idempotente: el token ya no existe allí.
async fn revoke_token(token: &str) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .context("no se pudo construir el cliente HTTP de revoke")?;
    let resp = http
        .post(REVOKE_URL)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(format!("token={}", urlencoding::encode(token)))
        .send()
        .await
        .context("red: no se pudo contactar el endpoint de revoke")?;
    if revoke_resolved(resp.status()) {
        return Ok(());
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    anyhow::bail!("revoke rechazado: {} - {}", status, body)
}

/// Access tokens del `tokens.json` histórico de yup (formato
/// `[{scopes, token{...}}]`). Fallback de revoke cuando no hay refresh
/// (entorno sin secret-service): revocar el access es lo único posible.
fn extract_access_tokens(json_bytes: &[u8]) -> Result<Vec<String>> {
    #[derive(serde::Deserialize)]
    struct DiskEntry {
        token: DiskToken,
    }
    #[derive(serde::Deserialize)]
    struct DiskToken {
        access_token: Option<String>,
    }
    let entries: Vec<DiskEntry> =
        serde_json::from_slice(json_bytes).context("tokens.json no es JSON válido")?;
    Ok(entries
        .into_iter()
        .filter_map(|e| e.token.access_token)
        .collect())
}

/// Tokens conocidos localmente (solo lectura): keyring primero y
/// `tokens.json` pre-migración como fallback. Devuelve `(refresh, access)`.
fn local_tokens_snapshot() -> (Vec<String>, Vec<String>) {
    let mut refresh: Vec<String> = Vec::new();
    let mut access: Vec<String> = Vec::new();
    match KeyringTokenStore::new().load_refresh(DRIVE_SCOPES) {
        Ok(Some(r)) => refresh.push(r),
        Ok(None) => {}
        Err(e) => tracing::warn!("revoke: keyring ilegible ({:?}), se intenta JSON", e),
    }
    if let Ok(home) = std::env::var("HOME") {
        let path =
            std::path::PathBuf::from(format!("{}/.config/fedoradrive/tokens.json", home));
        if path.exists() {
            match std::fs::read(&path) {
                Ok(bytes) => {
                    if let Ok(pairs) = super::keyring::extract_refresh_tokens(&bytes) {
                        refresh.extend(pairs.into_iter().map(|(_, r)| r));
                    }
                    if let Ok(acc) = extract_access_tokens(&bytes) {
                        access.extend(acc);
                    }
                }
                Err(e) => tracing::warn!("revoke: tokens.json ilegible: {:?}", e),
            }
        }
    }
    refresh.sort_unstable();
    refresh.dedup();
    access.sort_unstable();
    access.dedup();
    (refresh, access)
}

/// Cierre de sesión completo: revoca en Google y luego borra lo local.
/// La revocación es best-effort y nunca bloquea el logout (sin red también
/// se borra lo local: un secreto huérfano en disco es peor que un grant
/// pendiente de revocar a mano en la cuenta de Google).
pub async fn logout() -> Result<()> {
    tracing::info!("Cerrando sesión y revocando tokens");
    let (refresh, access) = local_tokens_snapshot();
    if !refresh.is_empty() {
        for token in &refresh {
            if let Err(e) = revoke_token(token).await {
                tracing::warn!(
                    "No se pudo revocar el refresh en Google: {:?} (se sigue con borrado local)",
                    e
                );
            }
        }
    } else {
        for token in &access {
            if let Err(e) = revoke_token(token).await {
                tracing::warn!(
                    "No se pudo revocar el access en Google: {:?} (se sigue con borrado local)",
                    e
                );
            }
        }
    }
    clear_all_auth_data()
}

/// Función independiente para limpiar todos los datos de autenticación
/// LOCALES (sin red): keyring (cuenta actual + legado) y `tokens.json`
/// triturado. Útil para llamar desde la GUI sin instancia de OAuth2Manager.
/// La revocación en Google la hace `logout()`; esto es solo el borrado local.
pub fn clear_all_auth_data() -> Result<()> {
    // 1. Keyring: cuenta actual por scopes + cuenta legado pre-fix.
    // Best-effort con warn (un keyring bloqueado no debe impedir el logout).
    match KeyringTokenStore::new().delete_all() {
        Ok(()) => tracing::info!("Credenciales eliminadas del keyring"),
        Err(e) => tracing::warn!("No se pudo limpiar el keyring: {:?}", e),
    }

    // 2. Triturar tokens.json (overwrite + unlink, no unlink simple).
    let home = std::env::var("HOME").context("No se pudo obtener HOME")?;
    let token_path =
        std::path::PathBuf::from(format!("{}/.config/fedoradrive/tokens.json", home));
    if token_path.exists() {
        super::keyring::shred_file(&token_path)?;
        tracing::info!("Archivo de tokens triturado");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revoke_resuelto_en_200_y_400() {
        assert!(revoke_resolved(reqwest::StatusCode::OK));
        assert!(revoke_resolved(reqwest::StatusCode::BAD_REQUEST));
    }

    #[test]
    fn revoke_no_resuelto_en_otro_error() {
        assert!(!revoke_resolved(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(!revoke_resolved(reqwest::StatusCode::UNAUTHORIZED));
        assert!(!revoke_resolved(reqwest::StatusCode::TOO_MANY_REQUESTS));
    }

    #[test]
    fn extract_access_saca_solo_access() {
        let json = br#"[{"scopes":["s"],"token":{"access_token":"ACC-1","refresh_token":"REF-1"}},{"scopes":["s"],"token":{"access_token":"ACC-2"}}]"#;
        let acc = extract_access_tokens(json).unwrap();
        assert_eq!(acc, vec!["ACC-1".to_string(), "ACC-2".to_string()]);
    }

    #[test]
    fn extract_access_vacio_sin_access_y_rechaza_json_malo() {
        let json = br#"[{"scopes":["s"],"token":{}}]"#;
        assert!(extract_access_tokens(json).unwrap().is_empty());
        assert!(extract_access_tokens(b"no-json").is_err());
    }
}
