//! Uploader en background para subir archivos dirty a Google Drive
//!
//! Escanea la base de datos buscando archivos marcados como dirty=1 y los sube
//! usando la API "Resumable Upload" de Google Drive.

use anyhow::{Context, Result};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::db::MetadataRepository;
use crate::gdrive::client::DriveClient;

/// Intervalo máximo de backoff en segundos
const MAX_BACKOFF_SECS: u64 = 300;

use crate::gui::history::{ActionHistory, ActionType};

/// Uploader en background que sube archivos dirty a Google Drive
pub struct Uploader {
    db: Arc<MetadataRepository>,
    client: Arc<DriveClient>,
    interval: Duration,
    cache_dir: std::path::PathBuf,
    mirror_path: std::path::PathBuf,
    history: ActionHistory,
    root_id: String,
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
        }
    }

    /// Inicia el loop de upload en un task de Tokio separado
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("📤 Uploader iniciado (intervalo: {:?})", self.interval);
            
            let mut current_backoff = self.interval;
            
            loop {
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
        
        // 2. Procesar archivos FUSE
        for (inode, gdrive_id, is_delete) in dirty_files {
            match self.upload_file(inode, &gdrive_id, is_delete).await {
                Ok(()) => {
                    uploaded_count += 1;
                }
                Err(e) => {
                    warn!("Error subiendo inode {}: {:?}", inode, e);
                    // Continuamos con los demás
                }
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

    /// Sube un archivo individual a Google Drive
    async fn upload_file(&self, inode: u64, gdrive_id: &str, is_delete: bool) -> Result<()> {
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
        
        // Validar si es una carpeta
        if attrs.is_dir {
            // Caso carpeta: crear solo con metadatos
            let real_gdrive_id = self.client.create_folder(
                &name,
                &parent_gdrive_id,
            ).await.context("Error creando carpeta")?;

            // Actualizar DB y retornar
            sqlx::query("UPDATE inodes SET gdrive_id = ? WHERE inode = ?")
                .bind(&real_gdrive_id)
                .bind(inode as i64)
                .execute(self.db.pool())
                .await?;
            
            sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
                .bind(inode as i64)
                .execute(self.db.pool())
                .await?;
            
            info!("✅ Carpeta creada en GDrive: {} (inode={})", real_gdrive_id, inode);
            self.history.log(ActionType::Create, format!("Carpeta creada: {}", name));
            return Ok(());
        }

        // Ruta del archivo en caché
        let cache_path = self.cache_dir.join(temp_gdrive_id);
        
        if !cache_path.exists() {
            warn!("Archivo de caché no existe: {:?}, creando vacío", cache_path);
            tokio::fs::write(&cache_path, b"").await?;
        }
        
        // Subir archivo usando la API
        let real_gdrive_id = self.client.upload_file(
            &cache_path,
            &name,
            attrs.mime_type.as_deref(),
            &parent_gdrive_id,
        ).await.context("Error subiendo archivo nuevo")?;
        
        // Actualizar el gdrive_id en la base de datos
        sqlx::query("UPDATE inodes SET gdrive_id = ? WHERE inode = ?")
            .bind(&real_gdrive_id)
            .bind(inode as i64)
            .execute(self.db.pool())
            .await?;
        
        // Marcar como limpio (no dirty)
        sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
            .bind(inode as i64)
            .execute(self.db.pool())
            .await?;
        
        info!("✅ Archivo creado en GDrive: {} (inode={})", real_gdrive_id, inode);
        self.history.log(ActionType::Create, format!("Archivo creado: {}", name));
        
        Ok(())
    }

    /// Actualiza un archivo existente en Google Drive
    async fn update_file(&self, inode: u64, gdrive_id: &str) -> Result<()> {
        info!("📤 Actualizando archivo en GDrive: {} (inode={})", gdrive_id, inode);
        
        // 1. Obtener Metadatos remotos completos (Name, Parent, MD5)
        let remote_meta = self.client.get_file_metadata(gdrive_id).await?;
        let current_remote_md5 = remote_meta.md5_checksum;
        let current_remote_name = remote_meta.name.unwrap_or_default();

        let known_md5 = self.db.get_remote_md5(inode).await?;
        
        // 2. Detectar conflicto: SOLO si tenemos un MD5 conocido previo Y difiere del remoto
        // Si known_md5 es None o vacío, significa que el archivo nunca fue registrado localmente
        // (ej: solo se movió/renombró), NO es un conflicto real.
        if let Some(known) = &known_md5 {
            if !known.is_empty() {
                if let Some(current) = &current_remote_md5 {
                    if known != current {
                        warn!("⚠️ CONFLICTO DETECTADO: archivo remoto cambió desde la última sync");
                        warn!("   - MD5 conocido: {}", known);
                        warn!("   - MD5 actual:   {}", current);
                        return self.handle_conflict(inode, gdrive_id).await;
                    }
                }
            }
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
                sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
                    .bind(inode as i64)
                    .execute(self.db.pool())
                    .await?;
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
        
        // Verificar si el padre local está en la lista de padres remotos
        // Manejar el caso especial de "root" vs ID real del root
        let is_in_remote = if local_parent_id == "root" {
            // Obtener el ID real del root para comparar correctamente
            match self.client.get_root_file_id().await {
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
                sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
                    .bind(inode as i64)
                    .execute(self.db.pool())
                    .await?;

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
             self.client.update_file_metadata(
                 gdrive_id, 
                 new_name, 
                 add_parent.as_deref(), 
                 remove_parent.as_deref(), 
                 new_mtime
             ).await?;
        }


        // 4. Ruta del archivo en caché
        let cache_path = self.cache_dir.join(gdrive_id);
        
        if !cache_path.exists() {
            // Si solo cambiamos metadata (nombre) y el archivo no está en caché, es un RENOMBRADO válido.
            if metadata_updated {
                info!("✅ Renombrado completado sin cambios de contenido (sin caché).");
                // Marcar como limpio
                sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
                    .bind(inode as i64)
                    .execute(self.db.pool())
                    .await?;
                self.history.log(ActionType::Sync, format!("Renombrado: {}", local_name));
                return Ok(());
            }

            warn!("Archivo de caché no existe para actualización: {:?}", cache_path);
            
            // FIX: Si el archivo está marcado como dirty pero no tiene caché local,
            // está en un estado inconsistente. Limpiamos dirty para desbloquear el estado
            // y permitir que se muestre como CloudOnly/Synced.
            info!("⚠️ Corrigiendo estado inconsistente: dirty=1 pero sin caché local. Reseteando a dirty=0.");
            
            sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
                .bind(inode as i64)
                .execute(self.db.pool())
                .await?;
                
            self.history.log(ActionType::Sync, format!("Estado corregido (sin caché): {}", gdrive_id));

            return Ok(()); 
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
                         
                         sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
                            .bind(inode as i64)
                            .execute(self.db.pool())
                            .await?;
                            
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

             sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
                .bind(inode as i64)
                .execute(self.db.pool())
                .await?;
             
             return Ok(());
        }

        // 6. Actualizar contenido usando la API
        self.client.update_file_content(gdrive_id, &cache_path).await
            .context("Error actualizando archivo")?;
        
        // 6. Obtener el nuevo MD5 tras la actualización
        if let Some(new_md5) = self.client.get_file_md5(gdrive_id).await? {
            self.db.set_remote_md5(inode, &new_md5).await?;
        }
        
        // 7. Marcar como limpio
        sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
            .bind(inode as i64)
            .execute(self.db.pool())
            .await?;
        
        info!("✅ Archivo actualizado en GDrive: {} (inode={})", gdrive_id, inode);
        self.history.log(ActionType::Upload, format!("Archivo actualizado: {}", gdrive_id));
        
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
                    sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
                        .bind(inode as i64)
                        .execute(self.db.pool())
                        .await?;
                    
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
        sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
            .bind(inode as i64)
            .execute(self.db.pool())
            .await?;
        
        Ok(())
    }

    /// Maneja un conflicto de sincronización creando una copia del archivo local
    async fn handle_conflict(&self, inode: u64, gdrive_id: &str) -> Result<()> {
        warn!("📥 Resolviendo conflicto de sincronización para inode={}", inode);
        
        // 1. Obtener nombre original del archivo
        let original_name = self.get_file_name(inode).await?;
        
        // 2. Generar sufijo de timestamp (formato simple: YYYY-MM-DD-HHMMSS)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        
        // Convertir timestamp Unix a componentes de fecha aproximados
        // Esta es una aproximación simple para generar un nombre legible
        let days = now / 86400;
        let years_since_1970 = days / 365;
        let year = 1970 + years_since_1970;
        let remaining_days = days % 365;
        let month = (remaining_days / 30).min(11) + 1;
        let day = (remaining_days % 30).max(1);
        
        let seconds_today = now % 86400;
        let hour = seconds_today / 3600;
        let minute = (seconds_today % 3600) / 60;
        let second = seconds_today % 60;
        
        let timestamp = format!("{:04}-{:02}-{:02}-{:02}{:02}{:02}", 
            year, month, day, hour, minute, second);
        
        // 3. Construir nombre de conflicto
        let conflict_name = if let Some(dot_pos) = original_name.rfind('.') {
            let (base, ext) = original_name.split_at(dot_pos);
            format!("{} (Conflicto local {}){}", base, timestamp, ext)
        } else {
            format!("{} (Conflicto local {})", original_name, timestamp)
        };
        
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
        ).await.context("Error subiendo copia de conflicto")?;
        
        // 5. Marcar el archivo original como limpio (no lo modificamos)
        sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
            .bind(inode as i64)
            .execute(self.db.pool())
            .await?;
        
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
        
        let mut uploaded = 0;
        
        for file in dirty_files {
            // Solo subir si está en modo local_online
            if file.availability != "local_online" {
                debug!("Saltando archivo online_only: {}", file.relative_path);
                continue;
            }
            
            // Obtener path absoluto
            let base_dir = match self.db.get_local_sync_dir(file.sync_dir_id).await {
                Ok(dir) => dir,
                Err(e) => {
                    warn!("Error obteniendo sync_dir_id={}: {:?}", file.sync_dir_id, e);
                    continue;
                }
            };
            
            let local_path = std::path::PathBuf::from(&base_dir.local_path).join(&file.relative_path);
            
            // Verificar que el archivo existe y NO es symlink
            if !local_path.exists() || local_path.is_symlink() {
                debug!("Archivo no disponible localmente: {}", file.relative_path);
                continue;
            }
            
            // Procesar según si tiene gdrive_id o no
            match self.upload_local_file(&file, &local_path, &base_dir).await {
                Ok(()) => {
                    uploaded += 1;
                }
                Err(e) => {
                    warn!("Error subiendo {}: {:?}", file.relative_path, e);
                }
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
                
                let gdrive_id = self.client.upload_file(
                    local_path,
                    file_name,
                    mime_type.as_deref(),
                    &parent_gdrive_id,
                ).await.context("Error subiendo archivo local sync")?;
                
                // Actualizar BD
                self.db.set_local_file_gdrive_id(file.id, &gdrive_id).await?;
                self.db.clear_local_file_dirty(file.id).await?;
                
                // Actualizar remote_md5
                if let Some(md5) = self.client.get_file_md5(&gdrive_id).await? {
                    self.db.update_local_file_from_remote(file.id, Some(&md5), None).await?;
                }
                
                info!("✅ Archivo local sync creado: {}", gdrive_id);
                self.history.log(ActionType::Create, format!("Local sync: {}", file.relative_path));
                
                Ok(())
            }
            Some(gdrive_id) => {
                // Archivo existente: actualizar contenido
                info!("📤 Actualizando archivo local sync: {}", file.relative_path);
                
                // TODO: Implementar detección de conflictos similar a update_file
                
                self.client.update_file_content(gdrive_id, local_path).await
                    .context("Error actualizando archivo local sync")?;
                
                // Actualizar BD
                self.db.clear_local_file_dirty(file.id).await?;
                
                // Actualizar remote_md5
                if let Some(md5) = self.client.get_file_md5(gdrive_id).await? {
                    self.db.update_local_file_from_remote(file.id, Some(&md5), None).await?;
                }
                
                info!("✅ Archivo local sync actualizado: {}", gdrive_id);
                self.history.log(ActionType::Upload, format!("Local sync: {}", file.relative_path));
                
                Ok(())
            }
        }
    }
}
