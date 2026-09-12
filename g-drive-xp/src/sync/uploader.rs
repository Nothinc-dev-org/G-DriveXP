//! Uploader en background para subir archivos dirty a Google Drive
//!
//! Escanea la base de datos buscando archivos marcados como dirty=1 y los sube
//! usando la API "Resumable Upload" de Google Drive.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use futures::stream::{self, StreamExt};

use crate::db::MetadataRepository;
use crate::gdrive::client::{DriveClient, CONTROL_TIMEOUT};

/// Intervalo máximo de backoff en segundos
const MAX_BACKOFF_SECS: u64 = 300;

/// Fallos permanentes clasificados tras los cuales un upload se da de baja
/// (dirty limpio + error visible en el historial). Con 30 s/ciclo ≈ 5 min,
/// la misma ventana que la cuarentena del syncer (5 fallos × 60 s).
const MAX_UPLOAD_FAILURES: i64 = 10;
/// Backoff por archivo ante error transitorio: base y tope (1 h). El dirty
/// se conserva siempre aquí: un outage largo jamás tira ediciones del usuario.
const RETRY_BASE_SECS: u64 = 30;
const RETRY_MAX_SECS: u64 = 3600;

/// Clave de cuarentena/backoff para un inode FUSE. No colisiona con file_ids
/// de Drive (estos nunca contienen ':').
fn fuse_upload_key(inode: u64) -> String {
    format!("up:{}", inode)
}

/// Delay de reintento por archivo: exponencial con tope. Función pura.
fn upload_retry_delay_secs(fails: u32) -> u64 {
    std::cmp::min(
        RETRY_BASE_SECS.saturating_mul(2u64.saturating_pow(fails.min(7))),
        RETRY_MAX_SECS,
    )
}

/// Aplazamiento por orden (padre aún no creado en Drive): no es fallo,
/// no cuenta para cuarentena ni backoff.
fn is_deferred_upload(err: &anyhow::Error) -> bool {
    err.to_string().contains("DEFERRED_PARENT_TEMP")
}

use crate::gui::history::{ActionHistory, ActionType, TransferOp};

/// Uploader en background que sube archivos dirty a Google Drive
pub struct Uploader {
    db: Arc<MetadataRepository>,
    client: Arc<DriveClient>,
    interval: Duration,
    cache_dir: std::path::PathBuf,
    mirror_path: std::path::PathBuf,
    history: ActionHistory,
    root_id: String,
    /// Backoff por archivo ante error transitorio: clave → (fallos
    /// consecutivos, próximo ciclo elegible). En memoria a propósito: un
    /// reinicio reintenta una vez y listo. El lock nunca cruza un `.await`.
    retries: Mutex<HashMap<String, (u32, Instant)>>,
}

impl Uploader {
    /// Crea un nuevo uploader
    pub fn new(
        db: Arc<MetadataRepository>,
        client: Arc<DriveClient>,
        interval_secs: u64,
        cache_dir: impl AsRef<Path>,
        mirror_path: impl AsRef<Path>,
        history: ActionHistory,
        root_id: String,
    ) -> Self {
        Self {
            db,
            client,
            interval: Duration::from_secs(interval_secs),
            cache_dir: cache_dir.as_ref().to_path_buf(),
            mirror_path: mirror_path.as_ref().to_path_buf(),
            history,
            root_id,
            retries: Mutex::new(HashMap::new()),
        }
    }

    /// Inicia el loop de upload en un task de Tokio separado
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("📤 Uploader iniciado (intervalo: {:?})", self.interval);
            
            let mut current_backoff = self.interval;

            loop {
                if crate::utils::shutdown::is_shutdown_requested() {
                    info!("🛑 Uploader: Shutdown detectado, deteniendo uploads.");
                    break;
                }

                match self.upload_cycle().await {
                    Ok(uploaded_count) => {
                        if uploaded_count > 0 {
                            info!("✅ Ciclo de upload completado: {} archivos subidos", uploaded_count);
                        }
                        // Reset backoff en caso de éxito
                        current_backoff = self.interval;
                    }
                    Err(e) => {
                        error!("❌ Error en ciclo de upload: {:?}", e);
                        
                        // Exponential backoff
                        current_backoff = std::cmp::min(
                            current_backoff * 2,
                            Duration::from_secs(MAX_BACKOFF_SECS)
                        );
                        warn!("Próximo intento de upload en {:?}", current_backoff);
                    }
                }
                
                sleep(current_backoff).await;
            }
        })
    }

    /// Ejecuta un ciclo de upload
    /// Retorna el número de archivos subidos
    async fn upload_cycle(&self) -> Result<usize> {
        // 1. Obtener archivos dirty de FUSE
        let dirty_files = self.get_dirty_files().await?;
        
        let mut uploaded_count = 0;
        
        // 2. Procesar archivos FUSE (los no elegibles por backoff se omiten
        // sin quemar cuota: siguen dirty para el próximo ciclo que les toque).
        let upload_results = stream::iter(dirty_files)
            .map(|(inode, gdrive_id, is_delete)| async move {
                let key = fuse_upload_key(inode);
                if !self.retry_eligible(&key) {
                    debug!("⏳ Inode {} en espera de backoff, se omite este ciclo", inode);
                    return (inode, gdrive_id, None);
                }
                let res = self.upload_file(inode, &gdrive_id, is_delete).await;
                (inode, gdrive_id, Some(res))
            })
            .buffer_unordered(4) // Concurrencia máxima de 4
            .collect::<Vec<_>>()
            .await;

        for (inode, gdrive_id, result) in upload_results {
            let Some(result) = result else { continue };
            let key = fuse_upload_key(inode);
            match result {
                Ok(()) => {
                    uploaded_count += 1;
                    self.note_success(&key).await;
                }
                Err(e) if is_deferred_upload(&e) => {
                    debug!("⏳ Inode {} aplazado: directorio padre aún no sincronizado", inode);
                }
                Err(e) => self.handle_fuse_error(inode, &gdrive_id, &key, &e).await,
            }
        }
        
        // 3. Procesar archivos de Local Sync
        match self.upload_local_sync_files().await {
            Ok(count) => uploaded_count += count,
            Err(e) => {
                warn!("Error en upload de local sync files: {:?}", e);
            }
        }
        
        if uploaded_count > 0 {
            debug!("📋 Total subidos en este ciclo: {}", uploaded_count);
        }
        
        Ok(uploaded_count)
    }

    /// Obtiene la lista de archivos dirty desde la base de datos
    async fn get_dirty_files(&self) -> Result<Vec<(u64, String, bool)>> {
        let rows = sqlx::query_as::<_, (i64, String, Option<i64>)>(
            "SELECT i.inode, i.gdrive_id, s.deleted_at 
             FROM inodes i 
             INNER JOIN sync_state s ON i.inode = s.inode 
             WHERE s.dirty = 1"
        )
        .fetch_all(self.db.pool())
        .await?;
        
        Ok(rows.into_iter()
            .map(|(inode, gdrive_id, deleted_at)| {
                (inode as u64, gdrive_id, deleted_at.is_some())
            })
            .collect())
    }

    /// ¿Toca reintentar esta clave en este ciclo? (backoff por archivo).
    fn retry_eligible(&self, key: &str) -> bool {
        let guard = self.retries.lock().unwrap_or_else(|p| p.into_inner());
        guard
            .get(key)
            .map(|(_, at)| Instant::now() >= *at)
            .unwrap_or(true)
    }

    /// Éxito: olvida el backoff y limpia la cuarentena previa.
    async fn note_success(&self, key: &str) {
        self.forget_retry(key);
        if let Err(e) = self.db.clear_change_failure(key).await {
            warn!("No se pudo limpiar cuarentena de upload {}: {:?}", key, e);
        }
    }

    /// Transitorio: programa el próximo reintento con backoff exponencial.
    /// El dirty se conserva: jamás se da de baja por esto.
    fn note_transient(&self, key: &str) {
        let mut guard = self.retries.lock().unwrap_or_else(|p| p.into_inner());
        let entry = guard.entry(key.to_string()).or_insert((0, Instant::now()));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = Instant::now() + Duration::from_secs(upload_retry_delay_secs(entry.0));
    }

    fn forget_retry(&self, key: &str) {
        let _ = self
            .retries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
    }

    /// Cuenta un permanente clasificado en `failed_changes` (reúsa la tabla
    /// de cuarentena del syncer con clave propia `up:*`/`uplocal:*`).
    /// Retorna (fallos acumulados, dado_de_baja_al_umbral).
    async fn count_permanent(&self, key: &str, err: &crate::gdrive::DriveError) -> (i64, bool) {
        let short: String = format!("{:?}", err).chars().take(500).collect();
        let n = self.db.record_change_failure(key, &short).await.unwrap_or(0);
        (n, n >= MAX_UPLOAD_FAILURES)
    }

    /// Clasifica un fallo de upload FUSE. NotFound sobre id real reconcilia
    /// de inmediato (gana la verdad remota, como el delete); otro permanente
    /// cuenta para cuarentena con baja visible al umbral; lo transitorio
    /// entra en backoff por archivo sin tocar dirty.
    async fn handle_fuse_error(&self, inode: u64, gdrive_id: &str, key: &str, e: &anyhow::Error) {
        use crate::gdrive::DriveError;
        match DriveError::permanent_of(e) {
            Some(DriveError::NotFound(_)) if !gdrive_id.starts_with("temp_") => {
                info!(
                    "ℹ️ Inode {} ya no existe en Drive: limpiando dirty (gana verdad remota)",
                    inode
                );
                self.history.log(
                    ActionType::Sync,
                    format!("Upload descartado, remoto ausente: inode {}", inode),
                );
                if let Err(dbe) = self.db.clear_dirty_and_bubble(inode).await {
                    warn!("No se pudo limpiar dirty de inode {}: {:?}", inode, dbe);
                } else {
                    self.note_success(key).await;
                }
            }
            Some(perm) => {
                let (n, done) = self.count_permanent(key, perm).await;
                if done {
                    error!(
                        "⛔ Upload en cuarentena tras {} fallos (inode={}): se limpia dirty con registro visible. Último: {:?}",
                        n, inode, perm
                    );
                    self.history.log(
                        ActionType::Error,
                        format!("Upload dado de baja: inode {} ({} fallos permanentes)", inode, n),
                    );
                    if let Err(dbe) = self.db.clear_dirty_and_bubble(inode).await {
                        warn!("No se pudo limpiar dirty de inode {}: {:?}", inode, dbe);
                    }
                    self.forget_retry(key);
                } else {
                    warn!(
                        "Error permanente subiendo inode {} (fallo {}/{}): {:?}",
                        inode, n, MAX_UPLOAD_FAILURES, perm
                    );
                }
            }
            None => {
                self.note_transient(key);
                warn!(
                    "Error transitorio subiendo inode {}: {:?} (backoff por archivo)",
                    inode, e
                );
            }
        }
    }

    /// Igual que el FUSE pero conservador: local_sync nunca reconcilia por
    /// NotFound inmediato (aquí no hay convergencia de tombstones que lo
    /// respalde); todo permanente va a cuarentena con baja visible al umbral.
    async fn handle_local_error(&self, file_id: i64, rel_path: &str, key: &str, e: &anyhow::Error) {
        use crate::gdrive::DriveError;
        match DriveError::permanent_of(e) {
            Some(perm) => {
                let (n, done) = self.count_permanent(key, perm).await;
                if done {
                    error!(
                        "⛔ Upload local-sync en cuarentena tras {} fallos ({}): se limpia dirty con registro visible. Último: {:?}",
                        n, rel_path, perm
                    );
                    self.history.log(
                        ActionType::Error,
                        format!("Upload local-sync dado de baja: {} ({} fallos permanentes)", rel_path, n),
                    );
                    if let Err(dbe) = self.db.clear_local_file_dirty(file_id).await {
                        warn!("No se pudo limpiar dirty local de {}: {:?}", rel_path, dbe);
                    }
                    self.forget_retry(key);
                } else {
                    warn!(
                        "Error permanente subiendo {} (fallo {}/{}): {:?}",
                        rel_path, n, MAX_UPLOAD_FAILURES, perm
                    );
                }
            }
            None => {
                self.note_transient(key);
                warn!(
                    "Error transitorio subiendo {}: {:?} (backoff por archivo)",
                    rel_path, e
                );
            }
        }
    }

    /// Sube un archivo individual a Google Drive
    async fn upload_file(&self, inode: u64, gdrive_id: &str, is_delete: bool) -> Result<()> {
        // Guard: nunca subir archivos de control interno (.hidden, manifiesto)
        if let Ok(name) = self.get_file_name(inode).await {
            if name == ".hidden" || name == ".gdrivexp_hidden_manifest" {
                info!("⏭️ Uploader: ignorando archivo de control interno '{}' (inode={}), limpiando dirty", name, inode);
                self.db.clear_dirty_and_bubble(inode).await?;
                return Ok(());
            }
        }

        // Caso 1: Archivo marcado para eliminación
        if is_delete {
            return self.delete_file(inode, gdrive_id).await;
        }

        // Caso 2: Archivo nuevo o modificado

        // Verificar si es un archivo temporal (recién creado)
        let is_temp = gdrive_id.starts_with("temp_");
        
        if is_temp {
            // Archivo nuevo: crear en GDrive
            self.create_file(inode, gdrive_id).await
        } else {
            // Archivo existente: actualizar en GDrive
            self.update_file(inode, gdrive_id).await
        }
    }

    /// Crea un nuevo archivo en Google Drive
    async fn create_file(&self, inode: u64, temp_gdrive_id: &str) -> Result<()> {
        info!("📤 Creando nuevo archivo en GDrive (inode={})", inode);
        
        // Obtener metadatos del archivo
        let attrs = self.db.get_attrs(inode).await?;
        let name = self.get_file_name(inode).await?;
        let parent_gdrive_id = self.get_parent_gdrive_id(inode).await?;
    
    if parent_gdrive_id.starts_with("temp_") {
        anyhow::bail!("DEFERRED_PARENT_TEMP");
    }
        
        // Validar si es una carpeta
        if attrs.is_dir {
            // Caso carpeta: crear solo con metadatos
            let real_gdrive_id = DriveClient::timed("create_folder", CONTROL_TIMEOUT, self.client.create_folder(
                &name,
                &parent_gdrive_id,
            )).await.context("Error creando carpeta")?;

            // Actualizar DB y retornar
            sqlx::query("UPDATE inodes SET gdrive_id = ? WHERE inode = ?")
                .bind(&real_gdrive_id)
                .bind(inode as i64)
                .execute(self.db.pool())
                .await?;
            
            // Optimistic Locking: Verificar si el estado cambió mientras creábamos la carpeta
            let current_name = self.get_file_name(inode).await?;
            let current_parent_id = self.get_parent_gdrive_id(inode).await?;

            if current_name != name || current_parent_id != parent_gdrive_id {
                warn!("⚠️ Modificación concurrente detectada durante creación de carpeta (inode={}). Manteniendo dirty=1.", inode);
                // No limpiamos el flag dirty, para que el próximo ciclo procese los cambios nuevos
            } else {
                self.db.clear_dirty_and_bubble(inode).await?;
            }
            
            info!("✅ Carpeta creada en GDrive: {} (inode={})", real_gdrive_id, inode);
            self.history.log(ActionType::Create, format!("Carpeta creada: {}", name));
            return Ok(());
        }

        // Ruta del archivo en caché
        let cache_path = self.cache_dir.join(temp_gdrive_id);

        if !cache_path.exists() {
            // El archivo fue copiado directamente al directorio mirror (no a través de FUSE),
            // por lo que su contenido nunca llegó a la caché. Buscar en el mirror como fallback.
            let mirror_source = self.db.resolve_inode_to_relative_path(inode).await
                .ok()
                .flatten()
                .map(|rel| self.mirror_path.join(rel));

            if restore_cache_from_mirror(mirror_source.as_deref(), &cache_path).await? {
                info!("📂 Copiado desde mirror a caché antes de subir (inode={})", inode);
            } else {
                // Contenido irrecuperable (ni caché ni mirror) de una creación que
                // Drive nunca vio: dar de baja el fantasma. JAMÁS fabricar un
                // size=0 sincronizado (pérdida disfrazada de éxito).
                warn!("Archivo de caché no existe y no se encontró en mirror: {:?}", cache_path);
                remove_lost_phantom(&self.db, inode, temp_gdrive_id).await?;
                self.history.log(ActionType::Delete, format!("Creación sin contenido dada de baja: {}", name));
                return Ok(());
            }
        }
        
        // Subir archivo usando la API (con tracking de progreso)
        let file_size = tokio::fs::metadata(&cache_path).await.map(|m| m.len()).unwrap_or(0);
        let transfer_id = self.history.start_transfer(&name, TransferOp::Upload, file_size);
        
        let history_clone = self.history.clone();
        let progress_cb = Box::new(move |offset: u64| {
            history_clone.update_transfer_progress(transfer_id, offset);
        });

        let upload_result = self.client.upload_file(
            &cache_path,
            &name,
            attrs.mime_type.as_deref(),
            &parent_gdrive_id,
            Some(progress_cb as Box<dyn Fn(u64) + Send + Sync>),
        ).await;

        self.history.complete_transfer(transfer_id);
        
        let real_gdrive_id = upload_result.context("Error subiendo archivo nuevo")?;
        
        // Actualizar el gdrive_id en la base de datos
        sqlx::query("UPDATE inodes SET gdrive_id = ? WHERE inode = ?")
            .bind(&real_gdrive_id)
            .bind(inode as i64)
            .execute(self.db.pool())
            .await?;
        
        // Marcar como limpio (no dirty)
        // Optimistic Locking: Verificar si el estado cambió mientras subíamos el archivo
        let current_name = self.get_file_name(inode).await?;
        let current_parent_id = self.get_parent_gdrive_id(inode).await?;

        if current_name != name || current_parent_id != parent_gdrive_id {
            warn!("⚠️ Modificación concurrente detectada durante creación de archivo (inode={}). Manteniendo dirty=1.", inode);
            // No limpiamos el flag dirty, para que el próximo ciclo procese los cambios nuevos
        } else {
            self.db.clear_dirty_and_bubble(inode).await?;
        }
        
        info!("✅ Archivo creado en GDrive: {} (inode={})", real_gdrive_id, inode);
        self.history.log(ActionType::Create, format!("Archivo creado: {}", name));
        
        Ok(())
    }

    /// Actualiza un archivo existente en Google Drive
    async fn update_file(&self, inode: u64, gdrive_id: &str) -> Result<()> {
        info!("📤 Actualizando archivo en GDrive: {} (inode={})", gdrive_id, inode);

        // Guard: Workspace (Docs/...) jamás aceptará contenido binario.
        // open()/write() ya lo rechazan (EROFS); esto cubre dirties previos a
        // ese guard: saltar sin quemar llamadas API, con aviso visible (el
        // contenido local diverge y requiere atención manual).
        let local_mime: Option<String> = sqlx::query_scalar("SELECT mime_type FROM attrs WHERE inode = ?")
            .bind(inode as i64)
            .fetch_optional(self.db.pool())
            .await?;
        if local_mime.as_deref().map(crate::fuse::shortcuts::is_workspace_file).unwrap_or(false) {
            warn!("⚠️ Subida omitida: inode {} es un documento de Workspace (mime={:?}); Drive no acepta contenido binario. Descarte el cambio local o expórtelo manualmente.", inode, local_mime);
            return Ok(());
        }
        
        // 1. Obtener Metadatos remotos completos (Name, Parent, MD5)
        let remote_meta = DriveClient::timed("get_file_metadata", CONTROL_TIMEOUT, self.client.get_file_metadata(gdrive_id)).await?;
        let current_remote_md5 = remote_meta.md5_checksum;
        let current_remote_name = remote_meta.name.unwrap_or_default();

        let known_md5 = self.db.get_remote_md5(inode).await?;
        
        // 2. Detectar conflicto: SOLO si tenemos un MD5 conocido previo Y difiere del remoto
        // Si known_md5 es None o vacío, significa que el archivo nunca fue registrado localmente
        // (ej: solo se movió/renombró), NO es un conflicto real.
        if is_real_conflict(known_md5.as_deref(), current_remote_md5.as_deref()) {
            let known = known_md5.as_deref().unwrap_or("?");
            let current = current_remote_md5.as_deref().unwrap_or("?");
            warn!("⚠️ CONFLICTO DETECTADO: archivo remoto cambió desde la última sync");
            warn!("   - MD5 conocido: {}", known);
            warn!("   - MD5 actual:   {}", current);
            return self.handle_conflict(inode, gdrive_id, current).await;
        }

        
        // 3. Detectar Cambio de Nombre (Rename) y MTime local vs remoto
        let local_name = self.get_file_name(inode).await?;
        let local_mtime: i64 = sqlx::query_scalar("SELECT mtime FROM attrs WHERE inode = ?")
            .bind(inode as i64)
            .fetch_optional(self.db.pool())
            .await?
            .unwrap_or(0);
        
        let mut metadata_updated = false;
        let mut new_name: Option<&str> = None;
        let mut new_mtime: Option<google_drive3::chrono::DateTime<google_drive3::chrono::Utc>> = None;
        let mut add_parent: Option<String> = None;
        let mut remove_parent: Option<String> = None;

        // --- VERIFICACIÓN DE PERMISOS ---
        let capabilities = remote_meta.capabilities.as_ref();
        let can_rename = capabilities.map(|c| c.can_rename.unwrap_or(false)).unwrap_or(true); // Asumir true si no hay info (seguridad por defecto: drive suele enviar capabilities)
        let can_move = capabilities.map(|c| c.can_move_item_within_drive.unwrap_or(false)).unwrap_or(true);
        let can_add_my_drive = capabilities.map(|c| c.can_add_my_drive_parent.unwrap_or(false)).unwrap_or(true);
        let can_edit = capabilities.map(|c| c.can_edit.unwrap_or(false)).unwrap_or(true);
        // --------------------------------

        // Persistir capacidades actualizadas en la DB (para que MirrorManager/FUSE las conozcan)
        if let Err(e) = sqlx::query("UPDATE attrs SET can_move = ? WHERE inode = ?")
            .bind(can_move)
            .bind(inode as i64)
            .execute(self.db.pool())
            .await {
            error!("Error actualizando can_move en DB: {:?}", e);
        }

        if local_name != current_remote_name {
            if !can_rename {
                warn!("⛔ PERMISO DENEGADO: No se puede renombrar '{}'. Revertiendo cambio local.", current_remote_name);
                // Rollback nombre
                sqlx::query("UPDATE dentry SET name = ? WHERE child_inode = ?")
                    .bind(&current_remote_name)
                    .bind(inode as i64)
                    .execute(self.db.pool())
                    .await?;
                // Limpiar dirty
                self.db.clear_dirty_and_bubble(inode).await?;
                return Ok(());
            }

            info!("🔄 Detectado cambio de nombre: '{}' -> '{}'", current_remote_name, local_name);
            new_name = Some(local_name.as_str());
            metadata_updated = true;
        }

        if let Some(remote_mtime) = remote_meta.modified_time {
             let remote_secs = remote_mtime.timestamp();
             // Tolerancia de 2 segundos para evitar loops por diferencias de precisión
             if (local_mtime - remote_secs).abs() > 2 {
                 info!("🔄 Detectado cambio de fecha: Remote={} vs Local={}", remote_secs, local_mtime);
                 use google_drive3::chrono::TimeZone;
                 let dt = google_drive3::chrono::Utc.timestamp_opt(local_mtime, 0).single()
                     .ok_or_else(|| anyhow::anyhow!("Invalid timestamp"))?;
                 new_mtime = Some(dt);
                 metadata_updated = true;
             }
        }

        // Detectar cambio de ubicación (Move)
        let remote_parents = remote_meta.parents.clone().unwrap_or_default();
        let local_parent_id = self.get_parent_gdrive_id(inode).await?;
        
        if local_parent_id.starts_with("temp_") {
            anyhow::bail!("DEFERRED_PARENT_TEMP");
        }
        
        // Verificar si el padre local está en la lista de padres remotos
        // Manejar el caso especial de "root" vs ID real del root
        let is_in_remote = if local_parent_id == "root" {
            // Obtener el ID real del root para comparar correctamente
            match DriveClient::timed("get_root_file_id", CONTROL_TIMEOUT, self.client.get_root_file_id()).await {
                Ok(root_id) => remote_parents.contains(&root_id) || remote_parents.contains(&"root".to_string()),
                Err(_) => remote_parents.contains(&"root".to_string()),
            }
        } else {
            remote_parents.contains(&local_parent_id)
        };



        if !is_in_remote {
            // Verificar permisos de Move ANTES de procesar
            let permission_ok = if remote_parents.is_empty() {
                // Caso especial: "Shared with me" (sin padres visibles)
                // Usualmente requiere can_add_my_drive_parent O can_move_item_within_drive
                can_add_my_drive || can_move
            } else {
                can_move
            };
            if !permission_ok {
                warn!("⛔ PERMISO DENEGADO: No se puede mover el archivo (ReadOnly). Revertiendo cambio local.");
                // --- ROLLBACK FÍSICO Y DB (Mirror) ---
                // 1. Obtener la ruta "incorrecta" actual (donde el usuario lo movió)
                let unauthorized_rel = self.db.resolve_inode_to_relative_path(inode).await?.unwrap_or_default();
                
                // 2. Rollback DB: Restaurar el padre remoto en la base de datos local
                let target_parent_inode = if let Some(parent_id) = remote_parents.first() {
                     sqlx::query_scalar::<_, i64>("SELECT inode FROM inodes WHERE gdrive_id = ?")
                        .bind(parent_id)
                        .fetch_optional(self.db.pool())
                        .await?
                        .unwrap_or(1)
                } else {
                     1 
                };

                sqlx::query("UPDATE dentry SET parent_inode = ?, name = ? WHERE child_inode = ?")
                    .bind(target_parent_inode)
                    .bind(&current_remote_name) // También restauramos el nombre por si hubo rename simultáneo
                    .bind(inode as i64)
                    .execute(self.db.pool())
                    .await?;

                // 3. Obtener la ruta "correcta" restaurada
                let correct_rel = self.db.resolve_inode_to_relative_path(inode).await?.unwrap_or_default();

                // 4. Limpiar dirty
                self.db.clear_dirty_and_bubble(inode).await?;

                if !unauthorized_rel.is_empty() && !correct_rel.is_empty() && unauthorized_rel != correct_rel {
                    warn!("🔄 Ejecutando Rollback Físico: {} -> {}", unauthorized_rel, correct_rel);
                    let old_p = self.mirror_path.join(unauthorized_rel);
                    let new_p = self.mirror_path.join(correct_rel);
                    
                    if let Err(e) = tokio::fs::rename(&old_p, &new_p).await {
                        error!("Fallo al revertir físicamente el movimiento: {:?}", e);
                    }
                }

                self.history.log(ActionType::Sync, format!("Movimiento bloqueado y revertido: {}", current_remote_name));
                return Ok(());
            }

            info!("🔄 Detectado cambio de ubicación (Move): padre local={}, padres remotos={:?}", 
                  local_parent_id, remote_parents);
            add_parent = Some(local_parent_id.clone());
            // Remover el primer padre remoto que no sea el nuevo
            if let Some(old) = remote_parents.first() {
                remove_parent = Some(old.clone());
            } else {
                // FALLBACK CRÍTICO REVISADO:
                // Si parents está vacío, es posible que el archivo esté en Root pero la API oculte el parent 
                // o use el alias "root" en lugar del ID.
                // Intentamos remover AMBOS para asegurar que liberamos el padre anterior.
                warn!("⚠️ Parents remoto vacío. Intentando liberar Root ID ({}) y alias 'root'.", self.root_id);
                remove_parent = Some(format!("{},root", self.root_id));
            }
            metadata_updated = true;
        }

        if metadata_updated {
             DriveClient::timed("update_file_metadata", CONTROL_TIMEOUT, self.client.update_file_metadata(
                 gdrive_id, 
                 new_name, 
                 add_parent.as_deref(), 
                 remove_parent.as_deref(), 
                 new_mtime
             )).await?;
        }


        // 4. Ruta del archivo en caché
        let cache_path = self.cache_dir.join(gdrive_id);
        
        if !cache_path.exists() {
            // Si solo cambiamos metadata (nombre) y el archivo no está en caché, es un RENOMBRADO válido.
            if metadata_updated {
                info!("✅ Renombrado completado sin cambios de contenido (sin caché).");
                // Marcar como limpio
                self.db.clear_dirty_and_bubble(inode).await?;
                if add_parent.is_some() {
                    self.history.log(ActionType::Sync, format!("Movido: {} → {}", current_remote_name, local_name));
                } else {
                    self.history.log(ActionType::Sync, format!("Renombrado: {} → {}", current_remote_name, local_name));
                }
                return Ok(());
            }

            // El contenido pendiente puede seguir en el mirror aunque la caché
            // se haya perdido: intentar restaurarlo antes de rendirse.
            let mirror_source = self.db.resolve_inode_to_relative_path(inode).await
                .ok()
                .flatten()
                .map(|rel| self.mirror_path.join(rel));
            if restore_cache_from_mirror(mirror_source.as_deref(), &cache_path).await? {
                info!("📂 Caché restaurada desde mirror; continuando actualización (inode={})", inode);
            } else {
                // Sin contenido recuperable en ningún lado, el delta local es
                // irrecuperable: converge a la verdad remota (que no se tocó)
                // en lugar de reintentar para siempre. Se registra como
                // conflicto ganado por el remoto, NO como sync exitosa.
                warn!("⚠️ Dirty sin contenido recuperable (ni caché ni mirror) para {}: se descarta el delta local y se conserva la versión remota.", gdrive_id);
                self.db.clear_dirty_and_bubble(inode).await?;

                self.history.log(ActionType::Conflict, format!("Delta local descartado (sin contenido): {}", gdrive_id));

                return Ok(());
            }
        }
        
        // 5. OPTIMIZACIÓN: Verificar si el contenido local es idéntico al remoto
        // Esto evita re-subir archivos que solo fueron "tocados" o migrados sin cambios reales
        match crate::utils::hash::compute_file_md5(&cache_path).await {
            Ok(local_md5) => {
                // Verificar contra el MD5 remoto actual (si existe)
                if let Some(remote_md5) = &current_remote_md5 {
                     if &local_md5 == remote_md5 {
                         info!("✨ OPTIMIZACIÓN: El contenido local de {} es idéntico al remoto. Saltando subida.", gdrive_id);
                         
                         // Actualizar estado para reflejar que está sincronizado
                         self.db.set_remote_md5(inode, remote_md5).await?;
                         
                         self.db.clear_dirty_and_bubble(inode).await?;
                            
                         self.history.log(ActionType::Sync, format!("Verificado sin cambios: {}", gdrive_id));
                         return Ok(());
                     }
                }
            }
            Err(e) => {
                warn!("No se pudo calcular MD5 local para optimización: {:?}", e);
                // Continuar con la subida normal
            }
        }
        
        // Verificar permisos de Edición de Contenido
        if !can_edit {
             warn!("⛔ PERMISO DENEGADO: No se puede editar contenido de {}. Revertiendo estado.", gdrive_id);
             // Como no tenemos hash del contenido original fácilmente restaurable (salvo que lo descargáramos),
             // lo mejor es marcar dirty=0 para que en la próxima lectura/sync baje la versión remota.
             // Opcionalmente borrar el caché local para forzar re-descarga.
             
             if cache_path.exists() {
                 tokio::fs::remove_file(&cache_path).await.ok();
             }

             self.db.clear_dirty_and_bubble(inode).await?;
             
             return Ok(());
        }

        // 6. Guardia anti-0-bytes: no sobrescribir archivo remoto con cache vacío
        let file_size = tokio::fs::metadata(&cache_path).await.map(|m| m.len()).unwrap_or(0);
        let remote_size = remote_meta.size.unwrap_or(0);

        if should_block_zero_byte_upload(file_size, remote_size) {
            warn!("🛡️ BLOQUEADO: upload de 0 bytes para archivo que en Drive pesa {} bytes (gdrive_id={}). Limpiando cache corrupto.", remote_size, gdrive_id);
            let _ = tokio::fs::remove_file(&cache_path).await;
            self.db.clear_dirty_and_bubble(inode).await?;
            return Ok(());
        }

        // 7. Actualizar contenido usando la API (con tracking de progreso)
        let transfer_id = self.history.start_transfer(&local_name, TransferOp::Upload, file_size);
        
        let history_clone = self.history.clone();
        let progress_cb = Box::new(move |offset: u64| {
            history_clone.update_transfer_progress(transfer_id, offset);
        });

        let update_result = self.client.update_file_content(
            gdrive_id, 
            &cache_path,
            Some(progress_cb as Box<dyn Fn(u64) + Send + Sync>),
        ).await;

        self.history.complete_transfer(transfer_id);
        
        update_result.context("Error actualizando archivo")?;
        
        // 6. Obtener el nuevo MD5 tras la actualización
        if let Some(new_md5) = DriveClient::timed("get_file_md5", CONTROL_TIMEOUT, self.client.get_file_md5(gdrive_id)).await? {
            self.db.set_remote_md5(inode, &new_md5).await?;
        }
        
        // 7. Marcar como limpio
        // 7. Optimistic Locking: Verificar si el estado cambió durante la actualización
        let current_name = self.get_file_name(inode).await?;
        let current_parent_id = self.get_parent_gdrive_id(inode).await?;

        if current_name != local_name || current_parent_id != local_parent_id {
            warn!("⚠️ Modificación concurrente detectada durante actualización (inode={}). Manteniendo dirty=1.", inode);
            // No limpiamos el flag dirty, para que el próximo ciclo procese los cambios nuevos
        } else {
            self.db.clear_dirty_and_bubble(inode).await?;
        }
        
        info!("✅ Archivo actualizado en GDrive: {} (inode={})", gdrive_id, inode);
        if add_parent.is_some() {
            self.history.log(ActionType::Sync, format!("Movido: {} → {}", current_remote_name, local_name));
        } else {
            self.history.log(ActionType::Upload, format!("Subido: {}", local_name));
        }
        
        Ok(())
    }

    /// Elimina un archivo en Google Drive (moverlo a la papelera)
    async fn delete_file(&self, inode: u64, gdrive_id: &str) -> Result<()> {
        info!("🗑️ Eliminando archivo en GDrive: {} (inode={})", gdrive_id, inode);
        
        // No eliminar archivos temporales que nunca se subieron
        if gdrive_id.starts_with("temp_") {
            debug!("Archivo temporal nunca subido, marcando como limpio directamente");
        } else {
            // Intentar mover a papelera en GDrive
            match self.client.trash_file(gdrive_id).await {
                Ok(()) => {
                    info!("✅ Archivo eliminado en GDrive: {}", gdrive_id);
                    self.history.log(ActionType::Delete, format!("Archivo eliminado: {}", gdrive_id));
                }
                Err(crate::gdrive::DriveError::InsufficientPermissions(msg)) => {
                    // Error permanente: no podemos eliminar archivos compartidos
                    warn!("⚠️ No se puede eliminar archivo compartido: {}", msg);
                    warn!("   Restaurando archivo localmente para mantener consistencia con Drive");
                    
                    // RESTAURAR: deshacer el soft delete (eliminar deleted_at)
                    sqlx::query("UPDATE sync_state SET deleted_at = NULL WHERE inode = ?")
                        .bind(inode as i64)
                        .execute(self.db.pool())
                        .await?;
                    
                    // Marcar como limpio (no reintentar)
                    self.db.clear_dirty_and_bubble(inode).await?;
                    
                    self.history.log(
                        ActionType::Sync, 
                        format!("Archivo compartido restaurado: {} (sin permisos de eliminación)", gdrive_id)
                    );
                    
                    return Ok(());
                }
                Err(crate::gdrive::DriveError::NotFound(_)) => {
                    // Archivo ya no existe en Drive: limpiar estado local y continuar
                    info!("ℹ️ Archivo ya eliminado en Drive: {}. Limpiando estado local.", gdrive_id);
                    self.history.log(ActionType::Delete, format!("Archivo ya eliminado en Drive: {}", gdrive_id));
                    // Continuar para limpiar dirty flag abajo
                }
                Err(e) => {
                    // Otros errores transitorios: propagar para reintentar
                    return Err(anyhow::anyhow!("Error moviendo archivo a papelera: {:?}", e));
                }
            }
        }
        
        // Marcar como limpio (eliminación exitosa)
        self.db.clear_dirty_and_bubble(inode).await?;
        
        Ok(())
    }

    /// Maneja un conflicto de sincronización creando una copia del archivo local
    async fn handle_conflict(&self, inode: u64, gdrive_id: &str, current_remote_md5: &str) -> Result<()> {
        warn!("📥 Resolviendo conflicto de sincronización para inode={}", inode);
        
        // 1. Obtener nombre original del archivo
        let original_name = self.get_file_name(inode).await?;
        
        // 2-3. Construir nombre de conflicto con milisegundos (sin colisiones)
        let conflict_name = build_conflict_name(&original_name, google_drive3::chrono::Utc::now());
        
        warn!("   Archivo original: {}", original_name);
        warn!("   Copia de conflicto: {}", conflict_name);
        
        // 4. Subir el archivo local como nuevo archivo con nombre de conflicto
        let parent_gdrive_id = self.get_parent_gdrive_id(inode).await?;
        let cache_path = self.cache_dir.join(gdrive_id);
        
        if !cache_path.exists() {
            warn!("Archivo de caché no existe para conflicto: {:?}", cache_path);
            return Ok(());
        }
        
        // Obtener metadatos para mime_type
        let attrs = self.db.get_attrs(inode).await?;
        
        // Crear el archivo de conflicto en GDrive
        let conflict_gdrive_id = self.client.upload_file(
            &cache_path,
            &conflict_name,
            attrs.mime_type.as_deref(),
            &parent_gdrive_id,
            None,
        ).await.context("Error subiendo copia de conflicto")?;
        
        // 5. Curar el conflicto: el original remoto no se tocó, así que su MD5
        //    actual pasa a ser la base conocida. Sin esto, cada dirty posterior
        //    generaría OTRA copia de conflicto en lugar de un update normal.
        mark_conflict_resolved(&self.db, inode, current_remote_md5).await?;
        
        warn!("✅ Conflicto resuelto: copia local guardada como {}", conflict_gdrive_id);
        warn!("   El archivo original permanece sin cambios en la nube");
        self.history.log(ActionType::Conflict, format!("Conflicto resuelto: {}", conflict_name));
        
        Ok(())
    }

    /// Obtiene el nombre de un archivo desde la base de datos
    async fn get_file_name(&self, inode: u64) -> Result<String> {
        let name = sqlx::query_scalar::<_, String>(
            "SELECT name FROM dentry WHERE child_inode = ? LIMIT 1"
        )
        .bind(inode as i64)
        .fetch_optional(self.db.pool())
        .await?
        .unwrap_or_else(|| format!("file_{}", inode));
        
        Ok(name)
    }

    /// Obtiene el gdrive_id del directorio padre
    async fn get_parent_gdrive_id(&self, inode: u64) -> Result<String> {
        let parent_inode = sqlx::query_scalar::<_, i64>(
            "SELECT parent_inode FROM dentry WHERE child_inode = ? LIMIT 1"
        )
        .bind(inode as i64)
        .fetch_optional(self.db.pool())
        .await?
        .unwrap_or(1); // Default a root
        
        if parent_inode == 1 {
            return Ok("root".to_string());
        }
        
        let parent_gdrive_id = sqlx::query_scalar::<_, String>(
            "SELECT gdrive_id FROM inodes WHERE inode = ?"
        )
        .bind(parent_inode)
        .fetch_one(self.db.pool())
        .await?;
        
        Ok(parent_gdrive_id)
    }

    // ============================================================
    // Local Sync Upload Methods
    // ============================================================

    /// Sube archivos dirty de local_sync_files
    async fn upload_local_sync_files(&self) -> Result<usize> {
        let dirty_files = self.db.get_dirty_local_sync_files().await?;
        
        if dirty_files.is_empty() {
            return Ok(0);
        }
        
        debug!("📋 Encontrados {} archivos local sync dirty", dirty_files.len());
        
        let local_results = stream::iter(dirty_files)
            .map(|file| async move {
                // Solo subir si está en modo local_online
                if file.availability != "local_online" {
                    debug!("Saltando archivo online_only: {}", file.relative_path);
                    return None;
                }

                // Backoff por archivo (misma política que FUSE).
                let key = format!("uplocal:{}", file.id);
                if !self.retry_eligible(&key) {
                    debug!("⏳ {} en espera de backoff, se omite este ciclo", file.relative_path);
                    return None;
                }
                                
                // Obtener path absoluto
                let base_dir = match self.db.get_local_sync_dir(file.sync_dir_id).await {
                    Ok(dir) => dir,
                    Err(e) => {
                        warn!("Error obteniendo sync_dir_id={}: {:?}", file.sync_dir_id, e);
                        return None;
                    }
                };
                
                let local_path = std::path::PathBuf::from(&base_dir.local_path).join(&file.relative_path);
                
                // Verificar que el archivo existe y NO es symlink
                if !local_path.exists() || local_path.is_symlink() {
                    debug!("Archivo no disponible localmente: {}", file.relative_path);
                    return None;
                }
                
                // Procesar según si tiene gdrive_id o no
                Some((file.id, file.relative_path.clone(), self.upload_local_file(&file, &local_path, &base_dir).await))
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;

        let mut uploaded = 0;
        for res in local_results.into_iter().flatten() {
            let (file_id, rel_path, result) = res;
            let key = format!("uplocal:{}", file_id);
            match result {
                Ok(()) => {
                    uploaded += 1;
                    self.note_success(&key).await;
                }
                Err(e) if is_deferred_upload(&e) => {
                    debug!("⏳ {} aplazado: directorio padre aún no sincronizado", rel_path);
                }
                Err(e) => self.handle_local_error(file_id, &rel_path, &key, &e).await,
            }
        }
        
        if uploaded > 0 {
            info!("✅ Subidos {} archivos de local sync", uploaded);
        }
        
        Ok(uploaded)
    }

    /// Sube un archivo individual de local sync
    async fn upload_local_file(
        &self,
        file: &crate::db::LocalSyncFile,
        local_path: &std::path::Path,
        base_dir: &crate::db::LocalSyncDir,
    ) -> Result<()> {
        if file.is_dir {
            // Los directorios se manejan por creación automática
            self.db.clear_local_file_dirty(file.id).await?;
            return Ok(());
        }
        
        // Determinar el padre en Drive
        let parent_gdrive_id = match &file.relative_path.rsplit_once('/') {
            Some((parent_rel_path, _)) => {
                // Buscar el gdrive_id del directorio padre
                match self.db.get_local_sync_file(file.sync_dir_id, parent_rel_path).await? {
                    Some(parent_file) if parent_file.gdrive_id.is_some() => {
                        parent_file.gdrive_id.unwrap()
                    }
                    _ => {
                        // Si no tiene padre conocido, usar la raíz del sync dir
                        base_dir.gdrive_folder_id.clone()
                            .unwrap_or_else(|| "root".to_string())
                    }
                }
            }
            None => {
                // Archivo en la raíz del sync dir
                base_dir.gdrive_folder_id.clone()
                    .unwrap_or_else(|| "root".to_string())
            }
        };
        
        let file_name = local_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        
        // Detectar MIME type
        let mime_type = mime_guess::from_path(local_path)
            .first()
            .map(|m| m.essence_str().to_string());
        
        match &file.gdrive_id {
            None => {
                // Archivo nuevo: subir a Drive
                info!("📤 Creando archivo local sync en Drive: {}", file.relative_path);

                let file_size = tokio::fs::metadata(local_path).await.map(|m| m.len()).unwrap_or(0);
                let transfer_id = self.history.start_transfer(file_name, TransferOp::Upload, file_size);
                let history_ref = self.history.clone();
                let progress_cb = Box::new(move |bytes: u64| {
                    history_ref.update_transfer_progress(transfer_id, bytes);
                });
                let upload_result = self.client.upload_file(
                    local_path,
                    file_name,
                    mime_type.as_deref(),
                    &parent_gdrive_id,
                    Some(progress_cb),
                ).await;
                self.history.complete_transfer(transfer_id);
                let gdrive_id = upload_result.context("Error subiendo archivo local sync")?;
                
                // Actualizar BD
                self.db.set_local_file_gdrive_id(file.id, &gdrive_id).await?;
                self.db.clear_local_file_dirty(file.id).await?;
                
                // Actualizar remote_md5
                if let Some(md5) = DriveClient::timed("get_file_md5", CONTROL_TIMEOUT, self.client.get_file_md5(&gdrive_id)).await? {
                    self.db.update_local_file_from_remote(file.id, Some(&md5), None).await?;
                }
                
                info!("✅ Archivo local sync creado: {}", gdrive_id);
                self.history.log(ActionType::Create, format!("Local sync: {}", file.relative_path));
                
                Ok(())
            }
            Some(gdrive_id) => {
                // Archivo existente: actualizar contenido
                info!("📤 Actualizando archivo local sync: {}", file.relative_path);

                // Un solo fetch de metadatos: sirve para detección de
                // conflictos y para la guardia anti-0-bytes.
                let remote_meta = DriveClient::timed("get_file_metadata", CONTROL_TIMEOUT, self.client.get_file_metadata(gdrive_id)).await?;
                let current_remote_md5 = remote_meta.md5_checksum.clone();

                // Conflicto real (ambos lados cambiaron desde la base
                // conocida): NO pisar el remoto. Se sube el contenido local
                // como archivo nuevo con nombre de conflicto y el original
                // remoto queda intacto (mismo patrón que update_file/FUSE).
                if is_real_conflict(file.remote_md5.as_deref(), current_remote_md5.as_deref()) {
                    let current = current_remote_md5.as_deref().unwrap_or("?");
                    warn!("⚠️ CONFLICTO local-sync: el remoto cambió desde la última sync ({})", file.relative_path);
                    warn!("   - MD5 conocido: {}", file.remote_md5.as_deref().unwrap_or("?"));
                    warn!("   - MD5 actual:   {}", current);
                    let conflict_name = build_conflict_name(file_name, google_drive3::chrono::Utc::now());
                    let transfer_id = self.history.start_transfer(&conflict_name, TransferOp::Upload, tokio::fs::metadata(local_path).await.map(|m| m.len()).unwrap_or(0));
                    let upload_result = self.client.upload_file(
                        local_path,
                        &conflict_name,
                        mime_type.as_deref(),
                        &parent_gdrive_id,
                        None,
                    ).await;
                    self.history.complete_transfer(transfer_id);
                    let conflict_gdrive_id = upload_result.context("Error subiendo copia de conflicto local-sync")?;
                    // Curar: el original remoto no se tocó, su MD5 actual
                    // pasa a ser la base conocida y el dirty queda limpio.
                    let remote_mtime = remote_meta.modified_time.as_ref().map(|t| t.timestamp());
                    self.db.update_local_file_from_remote(file.id, current_remote_md5.as_deref(), remote_mtime).await?;
                    warn!("✅ Conflicto local-sync resuelto: copia local guardada como {} ({})", conflict_gdrive_id, conflict_name);
                    self.history.log(ActionType::Conflict, format!("Conflicto local-sync resuelto: {}", conflict_name));
                    return Ok(());
                }

                // Guardia anti-0-bytes: no sobrescribir archivo remoto con archivo local vacío
                let file_size = tokio::fs::metadata(local_path).await.map(|m| m.len()).unwrap_or(0);
                if file_size == 0 {
                    let remote_size = remote_meta.size.unwrap_or(0);
                    if should_block_zero_byte_upload(file_size, remote_size) {
                        warn!("🛡️ BLOQUEADO: upload local_sync de 0 bytes para archivo que en Drive pesa {} bytes ({})", remote_size, file.relative_path);
                        self.db.clear_local_file_dirty(file.id).await?;
                        return Ok(());
                    }
                }

                let transfer_id = self.history.start_transfer(file_name, TransferOp::Upload, file_size);
                let history_ref = self.history.clone();
                let progress_cb = Box::new(move |bytes: u64| {
                    history_ref.update_transfer_progress(transfer_id, bytes);
                });
                let update_result = self.client.update_file_content(gdrive_id, local_path, Some(progress_cb)).await;
                self.history.complete_transfer(transfer_id);
                update_result.context("Error actualizando archivo local sync")?;
                
                // Actualizar BD
                self.db.clear_local_file_dirty(file.id).await?;
                
                // Actualizar remote_md5
                if let Some(md5) = DriveClient::timed("get_file_md5", CONTROL_TIMEOUT, self.client.get_file_md5(gdrive_id)).await? {
                    self.db.update_local_file_from_remote(file.id, Some(&md5), None).await?;
                }
                
                info!("✅ Archivo local sync actualizado: {}", gdrive_id);
                self.history.log(ActionType::Upload, format!("Local sync: {}", file.relative_path));
                
                Ok(())
            }
        }
    }
}

/// Decide si un upload debe bloquearse por protección anti-0-bytes.
/// Bloquea cuando el archivo local tiene 0 bytes pero el remoto tiene contenido real.
fn should_block_zero_byte_upload(local_size: u64, remote_size: i64) -> bool {
    local_size == 0 && remote_size > 0
}

/// Decide si un inode dirty presenta un conflicto real de sincronización.
///
/// Hay conflicto SOLO si existe un MD5 remoto conocido previo (no vacío) Y
/// difiere del MD5 remoto actual. Sin base conocida (renombro/movido sin
/// contenido registrado) o sin MD5 actual (tipos sin checksum, ej. Docs),
/// NO es conflicto.
fn is_real_conflict(known_md5: Option<&str>, current_remote_md5: Option<&str>) -> bool {
    match (known_md5, current_remote_md5) {
        (Some(known), Some(current)) if !known.is_empty() => known != current,
        _ => false,
    }
}

/// Construye el nombre de la copia de conflicto preservando la extensión.
///
/// Incluye milisegundos (`-mmm`): dos conflictos dentro del mismo segundo
/// generan nombres distintos (Drive permite duplicados y serían
/// indistinguibles). Usa fecha de calendario real.
fn build_conflict_name(original_name: &str, now: google_drive3::chrono::DateTime<google_drive3::chrono::Utc>) -> String {
    let timestamp = now.format("%Y-%m-%d-%H%M%S-%3f").to_string();

    if let Some(dot_pos) = original_name.rfind('.') {
        let (base, ext) = original_name.split_at(dot_pos);
        format!("{} (Conflicto local {}){}", base, timestamp, ext)
    } else {
        format!("{} (Conflicto local {})", original_name, timestamp)
    }
}

/// Registra la resolución de un conflicto en la DB: el original remoto no se
/// tocó (la copia se subió como archivo nuevo), así que el MD5 remoto actual
/// pasa a ser la base conocida y el inode queda limpio.
///
/// Sin esto, `known_md5` queda obsoleto para siempre y cada dirty posterior
/// genera OTRA copia de conflicto en lugar de un update normal.
async fn mark_conflict_resolved(
    db: &MetadataRepository,
    inode: u64,
    current_remote_md5: &str,
) -> Result<()> {
    db.set_remote_md5(inode, current_remote_md5).await?;
    db.clear_dirty_and_bubble(inode).await?;
    Ok(())
}

/// Intenta restaurar el contenido de caché desde el archivo visible del mirror.
///
/// Retorna `Ok(true)` si copió contenido, `Ok(false)` si no había nada usable
/// (ruta sin resolver, ausente, directorio o symlink roto). Nunca borra nada;
/// los errores de copia se degradan a `Ok(false)` con warning.
async fn restore_cache_from_mirror(mirror_source: Option<&Path>, cache_path: &Path) -> Result<bool> {
    let src = match mirror_source {
        Some(s) => s,
        None => return Ok(false),
    };
    // symlink_metadata SIN seguir enlaces: un symlink (OnlineOnly) no aporta
    // contenido local; copiarlo seguiría al montaje FUSE (posiblemente caído).
    // Solo archivos reales son fuente válida.
    let meta = match tokio::fs::symlink_metadata(src).await {
        Ok(m) => m,
        Err(_) => return Ok(false),
    };
    if !meta.is_file() {
        return Ok(false);
    }
    if let Some(parent) = cache_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::copy(src, cache_path).await {
        Ok(_) => {
            info!("📂 Contenido restaurado desde mirror a caché: {:?}", src);
            Ok(true)
        }
        Err(e) => {
            warn!("No se pudo copiar desde mirror a caché ({:?}): {:?}", src, e);
            Ok(false)
        }
    }
}

/// Da de baja un fantasma de creación: archivo nuevo (`temp_`) cuyo contenido
/// no existe ni en caché ni en mirror, y que Drive nunca vio.
///
/// En lugar de fabricar un `size = 0` sincronizado, elimina sus filas
/// (dentry/attrs/estado) con ajuste de contadores vía `hard_delete`.
/// Si la baja falla, propaga el error para mantener dirty=1 y reintentar.
async fn remove_lost_phantom(db: &MetadataRepository, inode: u64, temp_gdrive_id: &str) -> Result<()> {
    warn!(
        "🗑️ Dando de baja creación fantasma sin contenido (inode={}): no existe ni en caché ni en mirror",
        inode
    );
    if !db.hard_delete_by_gdrive_id(temp_gdrive_id).await? {
        warn!("El fantasma ya no existe en DB (carrera), nada que eliminar");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case::block_zero_local_real_remote(0, 1024, true)]
    #[case::allow_both_zero(0, 0, false)]
    #[case::allow_real_local(1024, 1024, false)]
    #[case::allow_real_local_zero_remote(1024, 0, false)]
    #[case::block_zero_local_large_remote(0, 50_000_000, true)]
    #[case::allow_remote_unknown(0, -1, false)]
    fn test_should_block_zero_byte_upload(
        #[case] local_size: u64,
        #[case] remote_size: i64,
        #[case] expected: bool,
    ) {
        assert_eq!(should_block_zero_byte_upload(local_size, remote_size), expected);
    }

    #[rstest]
    #[case::conflicto_real(Some("aaa"), Some("bbb"), true)]
    #[case::sin_cambios(Some("aaa"), Some("aaa"), false)]
    #[case::sin_base_conocida(None, Some("bbb"), false)]
    #[case::base_vacia(Some(""), Some("bbb"), false)]
    #[case::sin_md5_remoto(Some("aaa"), None, false)]
    #[case::sin_nada(None, None, false)]
    fn test_is_real_conflict(
        #[case] known: Option<&str>,
        #[case] current: Option<&str>,
        #[case] expected: bool,
    ) {
        assert_eq!(is_real_conflict(known, current), expected);
    }

    #[rstest]
    #[case::con_extension("informe.docx", "informe (Conflicto local ", ".docx")]
    #[case::sin_extension("notas", "notas (Conflicto local ", "")]
    #[case::doble_extension("backup.tar.gz", "backup.tar (Conflicto local ", ".gz")]
    fn test_build_conflict_name_preserva_extension(
        #[case] original: &str,
        #[case] expected_prefix: &str,
        #[case] expected_suffix: &str,
    ) {
        use google_drive3::chrono::{TimeZone, Utc};
        let now = Utc.timestamp_millis_opt(1_750_000_000_123).single().unwrap();
        let name = build_conflict_name(original, now);
        assert!(name.starts_with(expected_prefix), "nombre: {}", name);
        assert!(name.ends_with(expected_suffix), "nombre: {}", name);
        assert!(name.contains("-123"), "debe incluir milisegundos: {}", name);
    }

    #[test]
    fn test_conflict_names_unicos_en_mismo_segundo() {
        // Dos conflictos resueltos dentro del mismo segundo no deben
        // colisionar nombre (Drive permite duplicados: serían indistinguibles).
        use google_drive3::chrono::{TimeZone, Utc};
        let a = build_conflict_name("doc.txt", Utc.timestamp_millis_opt(1_750_000_000_100).single().unwrap());
        let b = build_conflict_name("doc.txt", Utc.timestamp_millis_opt(1_750_000_000_200).single().unwrap());
        assert_ne!(a, b, "colisión: dos conflictos en el mismo segundo generan el mismo nombre");
    }

    #[rstest]
    #[case::primer_fallo(1, 60)]
    #[case::segundo_fallo(2, 120)]
    #[case::crece(4, 480)]
    #[case::tope_una_hora(10, 3600)]
    #[case::tope_saturado(100, 3600)]
    fn test_upload_retry_delay_exponencial_con_tope(#[case] fails: u32, #[case] expected: u64) {
        assert_eq!(upload_retry_delay_secs(fails), expected);
    }

    #[test]
    fn test_is_deferred_upload_solo_marca_de_orden() {
        let e = anyhow::anyhow!("DEFERRED_PARENT_TEMP");
        assert!(is_deferred_upload(&e));
        let e = anyhow::anyhow!("Error API Drive get_file_metadata: 404 - not found");
        assert!(!is_deferred_upload(&e));
    }

    #[test]
    fn test_fuse_upload_key_no_colisiona_con_file_ids() {
        // Las claves propias usan ':' como separador, carácter que los
        // file_ids de Drive no contienen: imposible mezclar cuarentenas.
        assert_eq!(fuse_upload_key(42), "up:42");
        assert!(fuse_upload_key(7).contains(':'));
    }

    #[tokio::test]
    async fn test_conflicto_resuelto_no_regenera_copia() {
        // Contrato de curación: tras resolver un conflicto contra el MD5
        // remoto `current`, ese mismo estado ya NO debe detectarse como
        // conflicto (si no, cada dirty posterior genera otra copia).
        let tmp = tempfile::tempdir().unwrap();
        let db = crate::db::MetadataRepository::new(&tmp.path().join("c.sqlite"))
            .await
            .unwrap();
        let inode = db.get_or_create_inode("temp_heal").await.unwrap();
        db.set_remote_md5(inode, "md5_viejo").await.unwrap();
        db.set_dirty_and_bubble(inode).await.unwrap();
        assert!(db.is_dirty(inode).await.unwrap());

        mark_conflict_resolved(&db, inode, "md5_nuevo").await.unwrap();

        let known = db.get_remote_md5(inode).await.unwrap();
        assert!(!is_real_conflict(known.as_deref(), Some("md5_nuevo")));
        assert!(!db.is_dirty(inode).await.unwrap());
    }

    #[tokio::test]
    async fn test_local_sync_conflicto_no_pisa_y_se_cura() {
        // Contrato del fix del TODO uploader.rs:948 para filas local_sync:
        // dirty=1 + remoto cambiado desde la base conocida = conflicto real
        // (el uploader debe ir por la rama de copia, jamás update ciego);
        // y tras curar con update_local_file_from_remote, ese mismo estado
        // ya NO es conflicto y el dirty queda limpio (sin copias en bucle).
        // Además, mientras dirty=1 el syncer debe saltar la descarga.
        let tmp = tempfile::tempdir().unwrap();
        let db = crate::db::MetadataRepository::new(&tmp.path().join("ls.sqlite"))
            .await
            .unwrap();
        let dir = tmp.path().join("sync");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let dir_id = db.add_local_sync_dir(&dir).await.unwrap();
        let file_id = db.upsert_local_sync_file(dir_id, "doc.txt", false, "local_online", None, None, None).await.unwrap();
        db.update_local_file_remote_metadata(file_id, Some("md5_base")).await.unwrap();

        // Estado inicial: dirty=1 con base conocida → el syncer salta...
        let row = db.get_local_sync_file(dir_id, "doc.txt").await.unwrap().unwrap();
        assert!(row.dirty, "upsert debe dejar dirty=1 (si no, el syncer no saltaría)");
        // ...y el uploader ve conflicto real contra un remoto cambiado.
        assert!(is_real_conflict(row.remote_md5.as_deref(), Some("md5_remoto_nuevo")),
            "dirty + remoto cambiado debe detectarse como conflicto, no como update ciego");
        assert!(!is_real_conflict(row.remote_md5.as_deref(), Some("md5_base")),
            "sin cambio remoto no hay conflicto");

        // Curación (lo que hace la rama de conflicto tras subir la copia):
        // base conocida = md5 actual + dirty limpio.
        db.update_local_file_from_remote(file_id, Some("md5_remoto_nuevo"), None).await.unwrap();
        let row = db.get_local_sync_file(dir_id, "doc.txt").await.unwrap().unwrap();
        assert!(!row.dirty);
        assert!(!is_real_conflict(row.remote_md5.as_deref(), Some("md5_remoto_nuevo")),
            "tras curar, el mismo estado no debe regenerar otra copia");
    }

    #[tokio::test]
    async fn test_restore_cache_from_mirror_recupera_contenido() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("origen.txt");
        tokio::fs::write(&src, b"contenido-valioso").await.unwrap();
        let dst = tmp.path().join("cache").join("dst.bin");
        assert!(restore_cache_from_mirror(Some(src.as_path()), &dst).await.unwrap());
        assert_eq!(tokio::fs::read(&dst).await.unwrap(), b"contenido-valioso");
    }

    #[tokio::test]
    async fn test_restore_cache_from_mirror_sin_origen() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("c");
        // Ruta ausente
        assert!(!restore_cache_from_mirror(Some(tmp.path().join("nope").as_path()), &dst).await.unwrap());
        // Sin ruta
        assert!(!restore_cache_from_mirror(None, &dst).await.unwrap());
        // Directorio no es contenido
        assert!(!restore_cache_from_mirror(Some(tmp.path()), &dst).await.unwrap());
        // Symlink roto no es contenido recuperable
        let link = tmp.path().join("roto");
        std::os::unix::fs::symlink(tmp.path().join("ausente"), &link).unwrap();
        assert!(!restore_cache_from_mirror(Some(link.as_path()), &dst).await.unwrap());
        // Nada debe haberse creado en caché
        assert!(tokio::fs::metadata(&dst).await.is_err());
    }

    #[tokio::test]
    async fn test_remove_lost_phantom_elimina_fantasma() {
        // Un fantasma (creación temp_ sin contenido en ningún lado) debe
        // desaparecer de la DB, NO quedar como size=0 sincronizado.
        let tmp = tempfile::tempdir().unwrap();
        let db = crate::db::MetadataRepository::new(&tmp.path().join("p.sqlite"))
            .await
            .unwrap();
        // Invariante de la app: inode 1 es la raíz (el hard delete la rechaza
        // ruidosamente); el fantasma debe colgar de ella, no SER ella.
        db.get_or_create_inode("root").await.unwrap();
        let inode = db.get_or_create_inode("temp_fantasma").await.unwrap();
        db.upsert_file_metadata(inode, 1234, 0, 0o644, false, None, true, false, true).await.unwrap();
        db.upsert_dentry(1, inode, "fantasma.txt").await.unwrap();
        db.set_availability(inode, "local_online", false).await.unwrap();
        db.set_dirty_and_bubble(inode).await.unwrap();
        assert!(db.is_dirty(inode).await.unwrap());

        remove_lost_phantom(&db, inode, "temp_fantasma").await.unwrap();

        assert!(db.resolve_relative_path_to_inode("fantasma.txt").await.unwrap().is_none());
        assert!(db.get_inode_by_gdrive_id("temp_fantasma").await.unwrap().is_none());
        assert!(!db.is_dirty(inode).await.unwrap());
    }
}
