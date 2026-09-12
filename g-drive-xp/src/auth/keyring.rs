//! Almacén de tokens OAuth2 en GNOME Keyring (cifrado con el login).
//!
//! El refresh token NUNCA vive en disco plano: solo existe en el keyring del
//! sistema y en memoria del proceso. El `tokens.json` histórico (yup-oauth2,
//! JSON plano) se migra una sola vez al arrancar y luego se tritura.

use anyhow::{Context, Result};
use keyring::Entry;

const SERVICE: &str = "org.gnome.FedoraDrive";

/// Scopes de Drive usados por la app (para localizar su entrada).
pub const DRIVE_SCOPES: &[&str] = &["https://www.googleapis.com/auth/drive"];

/// Cuenta determinista por conjunto de scopes (orden-insensible).
fn account_for_scopes(scopes: &[&str]) -> String {
    let mut sorted: Vec<&str> = scopes.to_vec();
    sorted.sort_unstable();
    format!("refresh_token {}", sorted.join(" "))
}

/// Almacén yup-oauth2 respaldado por GNOME Keyring.
///
/// Solo persiste el **refresh token** (longevo). El access token (1 h de vida)
/// se re-deriva por refresh en cada arranque: menos escrituras al keyring y
/// menos superficie secreta. `get` devuelve un `TokenInfo` solo-refresh y yup
/// refresca solo a partir de él.
pub struct KeyringTokenStore {
    service: String,
}

impl KeyringTokenStore {
    pub fn new() -> Self {
        Self {
            service: SERVICE.to_string(),
        }
    }

    /// Guarda un refresh token para esos scopes.
    pub fn store_refresh(&self, scopes: &[&str], refresh_token: &str) -> Result<()> {
        let entry = Entry::new(&self.service, &account_for_scopes(scopes))
            .context("keyring: no se pudo crear la entrada")?;
        entry
            .set_password(refresh_token)
            .context("keyring: no se pudo guardar el refresh token")?;
        tracing::info!("Refresh token almacenado en GNOME Keyring");
        Ok(())
    }

    /// Lee el refresh token (`None` si nunca se guardó).
    /// Un keyring bloqueado/ausente es `Err` (el caller decide fallback).
    pub fn load_refresh(&self, scopes: &[&str]) -> Result<Option<String>> {
        let entry = Entry::new(&self.service, &account_for_scopes(scopes))
            .context("keyring: no se pudo crear la entrada")?;
        match entry.get_password() {
            Ok(pw) => Ok(Some(pw)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(anyhow::anyhow!("keyring: no se pudo leer: {}", e)),
        }
    }

    /// Borra el refresh token (`Ok` aunque no exista).
    pub fn delete_refresh(&self, scopes: &[&str]) -> Result<()> {
        let entry = Entry::new(&self.service, &account_for_scopes(scopes))
            .context("keyring: no se pudo crear la entrada")?;
        match entry.delete_credential() {
            Ok(()) => {
                tracing::info!("Refresh token eliminado del keyring");
                Ok(())
            }
            Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("keyring: no se pudo borrar: {}", e)),
        }
    }

    /// Borra todas las entradas conocidas: la actual por scopes más la
    /// cuenta legado `"refresh_token"` que `clear_all_auth_data()` usaba
    /// pre-fix. Sin esto el logout dejaba un refresh huérfano y la sesión
    /// sobrevivía localmente (`is_authenticated()` seguía dando `true`).
    pub fn delete_all(&self) -> Result<()> {
        self.delete_refresh(DRIVE_SCOPES)?;
        let legacy = Entry::new(&self.service, "refresh_token")
            .context("keyring: no se pudo crear la entrada legado")?;
        match legacy.delete_credential() {
            Ok(()) => {
                tracing::info!("Refresh token legado eliminado del keyring");
                Ok(())
            }
            Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("keyring: no se pudo borrar legado: {}", e)),
        }
    }

    /// Reconstruye el TokenInfo que yup necesita a partir del refresh.
    /// Access ausente/expirado => yup refresca solo. Función pura (testeable).
    pub fn token_info_from_refresh(refresh_token: String) -> yup_oauth2::storage::TokenInfo {
        yup_oauth2::storage::TokenInfo {
            access_token: None,
            refresh_token: Some(refresh_token),
            expires_at: None,
            id_token: None,
        }
    }
}

impl Default for KeyringTokenStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl yup_oauth2::storage::TokenStorage for KeyringTokenStore {
    async fn set(&self, scopes: &[&str], token: yup_oauth2::storage::TokenInfo) -> anyhow::Result<()> {
        match token.refresh_token {
            Some(refresh) => self.store_refresh(scopes, &refresh).map_err(Into::into),
            None => {
                // Sin refresh (p.ej. re-auth sin consent): conservar el anterior.
                tracing::debug!("yup set() sin refresh_token: se conserva el guardado");
                Ok(())
            }
        }
    }

    async fn get(&self, scopes: &[&str]) -> Option<yup_oauth2::storage::TokenInfo> {
        match self.load_refresh(scopes) {
            Ok(Some(refresh)) => {
                tracing::debug!("Refresh token recuperado desde el keyring");
                Some(Self::token_info_from_refresh(refresh))
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("keyring ilegible, se fuerza re-login: {}", e);
                None
            }
        }
    }
}

/// ¿Hay backend de keyring operativo? (NoEntry = backend OK sin secreto aún;
/// cualquier otro fallo = sin keyring, p.ej. sesión sin secret-service.)
pub fn keyring_available() -> bool {
    let entry = match Entry::new(SERVICE, "__probe__") {
        Ok(e) => e,
        Err(_) => return false,
    };
    !matches!(
        entry.get_password(),
        Err(keyring::Error::NoStorageAccess(_)) | Err(keyring::Error::PlatformFailure(_))
    )
}

/// Par `(scopes, refresh_token)` extraído del `tokens.json` histórico de yup.
/// Función pura (testeable): el formato yup es `[{scopes, token{...}}]`.
pub fn extract_refresh_tokens(json_bytes: &[u8]) -> Result<Vec<(Vec<String>, String)>> {
    #[derive(serde::Deserialize)]
    struct DiskEntry {
        scopes: Vec<String>,
        token: DiskToken,
    }
    #[derive(serde::Deserialize)]
    struct DiskToken {
        refresh_token: Option<String>,
    }
    let entries: Vec<DiskEntry> =
        serde_json::from_slice(json_bytes).context("tokens.json no es JSON válido")?;
    Ok(entries
        .into_iter()
        .filter_map(|e| e.token.refresh_token.map(|r| (e.scopes, r)))
        .collect())
}

pub enum MigrateOutcome {
    /// No hay JSON: nada que hacer.
    NoFile,
    /// Hay JSON pero sin refresh (solo access de corta vida): se conserva.
    NoRefreshTokens,
    /// Refresh migrados al keyring y JSON triturado.
    Migrated { count: usize },
}

/// Migra el `tokens.json` plano al keyring y lo tritura.
/// REGLA DE SEGURIDAD: el JSON solo se borra si TODOS los refresh quedaron
/// guardados (fallo parcial = archivo intacto = sin lockout).
pub fn migrate_tokens_json_to_keyring(path: &std::path::Path) -> Result<MigrateOutcome> {
    migrate_tokens_json_with(path, &|scopes, refresh| {
        KeyringTokenStore::new().store_refresh(scopes, refresh)
    })
}

fn migrate_tokens_json_with(
    path: &std::path::Path,
    store: &dyn Fn(&[&str], &str) -> Result<()>,
) -> Result<MigrateOutcome> {
    if !path.exists() {
        return Ok(MigrateOutcome::NoFile);
    }
    let bytes = std::fs::read(path).context("no se pudo leer tokens.json")?;
    let pairs = extract_refresh_tokens(&bytes)?;
    if pairs.is_empty() {
        return Ok(MigrateOutcome::NoRefreshTokens);
    }
    for (scopes, refresh) in &pairs {
        let scope_refs: Vec<&str> = scopes.iter().map(|s| s.as_str()).collect();
        store(&scope_refs, refresh)?;
    }
    shred_file(path)?;
    Ok(MigrateOutcome::Migrated { count: pairs.len() })
}

/// Sobrescribe con ceros + sync + unlink. Best-effort documentado: si el
/// overwrite falla, igual se intenta el unlink (un secreto parcial en disco
/// es peor que un error reportado). Reusado por el logout para triturar
/// `tokens.json` en vez de un unlink simple.
pub(crate) fn shred_file(path: &std::path::Path) -> Result<()> {
    let len = std::fs::metadata(path).context("stat para triturar")?.len();
    if len > 0 {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .context("apertura para triturar")?;
        let zeros = vec![0u8; 64 * 1024];
        let mut remaining = len;
        while remaining > 0 {
            let n = std::cmp::min(remaining, zeros.len() as u64) as usize;
            if f.write_all(&zeros[..n]).is_err() {
                break;
            }
            remaining -= n as u64;
        }
        let _ = f.sync_all();
    }
    std::fs::remove_file(path).context("unlink tras triturar")?;
    tracing::info!("tokens.json plano triturado tras migrar al keyring");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_disk_json() -> Vec<u8> {
        br#"[{"scopes":["https://www.googleapis.com/auth/drive"],"token":{"access_token":"ACC","expires_at":"2026-01-01T00:00:00Z","id_token":"ID","refresh_token":"REF-1"}},{"scopes":[" scope-b "],"token":{"access_token":"ACC2"}}]"#.to_vec()
    }

    #[test]
    fn extract_saca_refresh_e_ignora_sin_refresh() {
        let pairs = extract_refresh_tokens(&sample_disk_json()).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, vec!["https://www.googleapis.com/auth/drive".to_string()]);
        assert_eq!(pairs[0].1, "REF-1");
    }

    #[test]
    fn extract_rechaza_json_malo_y_vacio() {
        assert!(extract_refresh_tokens(b"no-json").is_err());
        let pairs = extract_refresh_tokens(br#"[]"#).unwrap();
        assert!(pairs.is_empty());
    }

    #[test]
    fn account_determinista_y_separa_scopes() {
        let a = account_for_scopes(&["b", "a"]);
        let b = account_for_scopes(&["a", "b"]);
        assert_eq!(a, b, "el orden no debe importar");
        assert_ne!(a, account_for_scopes(&["a"]));
    }

    #[test]
    fn token_info_solo_refresh_sin_access() {
        let t = KeyringTokenStore::token_info_from_refresh("R".to_string());
        assert_eq!(t.refresh_token.as_deref(), Some("R"));
        assert!(t.access_token.is_none());
    }

    #[test]
    fn shred_elimina_archivo_con_secreto() {
        let dir = std::env::temp_dir().join("gdrivexp-shred-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("tokens.json");
        std::fs::write(&p, b"secreto-secreto").unwrap();
        shred_file(&p).unwrap();
        assert!(!p.exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn migracion_guarda_todo_y_tritura() {
        let dir = std::env::temp_dir().join("gdrivexp-migrate-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("tokens.json");
        std::fs::write(&p, sample_disk_json()).unwrap();
        let saved = std::cell::RefCell::new(Vec::new());
        let r = migrate_tokens_json_with(&p, &|scopes, refresh| {
            saved.borrow_mut().push((scopes.join(","), refresh.to_string()));
            Ok(())
        })
        .unwrap();
        assert!(matches!(r, MigrateOutcome::Migrated { count: 1 }));
        assert_eq!(saved.borrow().len(), 1);
        assert!(!p.exists(), "el JSON debe desaparecer tras migrar");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn migracion_fallida_conserva_json_sin_lockout() {
        let dir = std::env::temp_dir().join("gdrivexp-migrate-fail-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("tokens.json");
        std::fs::write(&p, sample_disk_json()).unwrap();
        let r = migrate_tokens_json_with(&p, &|_, _| {
            Err(anyhow::anyhow!("keyring bloqueado"))
        });
        assert!(r.is_err());
        assert!(p.exists(), "fallo parcial = JSON intacto, sin lockout");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn migracion_sin_archivo_es_noop() {
        let r = migrate_tokens_json_to_keyring(std::path::Path::new("/no/existe/tokens.json")).unwrap();
        assert!(matches!(r, MigrateOutcome::NoFile));
    }

    /// Extremo a extremo contra el backend que haya: con keyring verifica
    /// migrar→leer→borrar de verdad (scope de prueba, sin tocar el real);
    /// sin keyring verifica que el JSON se conserva (sin lockout).
    #[test]
    fn migracion_extremo_a_extremo_segun_backend() {
        let dir = std::env::temp_dir().join("gdrivexp-migrate-e2e-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("tokens.json");
        let fixture = br#"[{"scopes":["https://example.com/test-scope-e2e"],"token":{"access_token":"A","refresh_token":"REF-E2E"}}]"#;
        std::fs::write(&p, fixture).unwrap();
        let scopes = ["https://example.com/test-scope-e2e"];
        let store = KeyringTokenStore::new();
        if keyring_available() {
            let r = migrate_tokens_json_to_keyring(&p).unwrap();
            assert!(matches!(r, MigrateOutcome::Migrated { count: 1 }));
            assert!(!p.exists(), "migrado = JSON triturado");
            assert_eq!(store.load_refresh(&scopes).unwrap().as_deref(), Some("REF-E2E"));
            store.delete_refresh(&scopes).unwrap();
            assert_eq!(store.load_refresh(&scopes).unwrap(), None);
        } else {
            assert!(migrate_tokens_json_to_keyring(&p).is_err());
            assert!(p.exists(), "sin backend el JSON debe conservarse");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Contra keyring REAL (secret-service). Ignorado por defecto: requiere
    /// sesión gráfica con colección desbloqueada. Correr a mano con
    /// `cargo test keyring_real -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn keyring_real_roundtrip() {
        let store = KeyringTokenStore::new();
        let scopes = ["https://www.googleapis.com/auth/drive-test"];
        store.store_refresh(&scopes, "roundtrip-probe").unwrap();
        assert_eq!(store.load_refresh(&scopes).unwrap().as_deref(), Some("roundtrip-probe"));
        store.delete_refresh(&scopes).unwrap();
        assert_eq!(store.load_refresh(&scopes).unwrap(), None);
    }
}
