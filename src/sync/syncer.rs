//! Sincronizador en background para detectar y aplicar cambios de Google Drive
//!
//! Utiliza la API changes.list para polling incremental de cambios.

use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio::sync::RwLock;
use futures::stream::{self, StreamExt};

use crate::db::MetadataRepository;
use crate::gdrive::client::DriveClient;

/// Clave en sync_meta para el page token de changes
const SYNC_META_PAGE_TOKEN: &str = "changes_page_token";

/// Intervalo máximo de backoff en segundos
const MAX_BACKOFF_SECS: u64 = 300;


/// Período de gracia para tombstones en días
const TOMBSTONE_GRACE_DAYS: i64 = 7;

use crate::gui::history::{ActionHistory, ActionType, TransferOp};
use std::sync::atomic::{AtomicBool, Ordering};

/// Sincronizador en background que detecta cambios de Google Drive
pub struct BackgroundSyncer {
    db: Arc<MetadataRepository>,
    client: Arc<DriveClient>,
    interval: Duration,
    history: ActionHistory,
    sync_paused: Arc<AtomicBool>,
    root_id_cache: Arc<RwLock<Option<String>>>,
}

impl BackgroundSyncer {
    /// Crea un nuevo sincronizador
    pub fn new(
        db: Arc<MetadataRepository>,
        client: Arc<DriveClient>,
        interval_secs: u64,
        history: ActionHistory,
        sync_paused: Arc<AtomicBool>,
    ) -> Self {
        Self {
            db,
            client,
            interval: Duration::from_secs(interval_secs),
            history,
            sync_paused,
            root_id_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Inicia el loop de sincronización en un task de Tokio separado
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!("🔄 Background Syncer iniciado (intervalo: {:?})", self.interval);
            
            let mut current_backoff = self.interval;
            
            loop {
                // Verificar si está pausado
                if self.sync_paused.load(Ordering::Relaxed) {
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }

                match self.sync_once().await {
                    Ok(changes_count) => {
                        if changes_count > 0 {
                            tracing::info!("✅ Sincronización completada: {} cambios procesados", changes_count);
                            self.history.log(
                                ActionType::Sync, 
                                format!("Sincronizados {} cambios remotos", changes_count)
                            );
                        }
                        // Reset backoff en caso de éxito
                        current_backoff = self.interval;
                    }
                    Err(e) => {
                        tracing::error!("❌ Error en sincronización: {:?}", e);
                        self.history.log(ActionType::Error, "Error en sincronización remota");
                        
                        // Exponential backoff
                        current_backoff = std::cmp::min(
                            current_backoff * 2,
                            Duration::from_secs(MAX_BACKOFF_SECS)
                        );
                        tracing::warn!("Próximo intento en {:?}", current_backoff);
                    }
                }
                
                sleep(current_backoff).await;
            }
        })
    }

    /// Ejecuta un ciclo de sincronización
    /// Retorna el número de cambios procesados
    async fn sync_once(&self) -> Result<usize> {
        // Asegurarnos de tener el ID del root
        let root_id = self.get_cached_root_id().await?;

        // 1. Obtener page_token guardado o solicitar uno nuevo
        let page_token = match self.db.get_sync_meta(SYNC_META_PAGE_TOKEN).await? {
            Some(token) => token,
            None => {
                // Primera vez: obtener startPageToken
                let token = self.client.get_start_page_token().await?;
                self.db.set_sync_meta(SYNC_META_PAGE_TOKEN, &token).await?;
                tracing::info!("Primer startPageToken obtenido y guardado: {}", token);
                token
            }
        };

        // 2. Consultar cambios
        let (changes, new_start_token) = self.client.list_changes(&page_token).await?;
        
        let changes_count = changes.len();

        // 3. Procesar cada cambio (con tracking de progreso)
        if changes_count > 0 {
            self.history.set_sync_progress(changes_count, 0);
        }
        
        let root_id_arc = Arc::new(root_id);

        let process_results = stream::iter(changes)
            .map(|change| {
                let root_id_ref = root_id_arc.clone();
                async move {
                    self.process_change(change, &root_id_ref).await
                }
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;

        for res in process_results {
            if let Err(e) = res {
                tracing::warn!("Error procesando cambio individual: {:?}", e);
            }
            self.history.increment_applied();
        }

        if changes_count > 0 {
            self.history.mark_all_synced();
        }

        // 4. Guardar nuevo token si es la última página
        if let Some(new_token) = new_start_token {
            self.db.set_sync_meta(SYNC_META_PAGE_TOKEN, &new_token).await?;
            tracing::debug!("Nuevo pageToken guardado: {}", new_token);
        }

        // 5. Purgar tombstones expirados (cada ciclo, es barato)
        let purged = self.db.purge_expired_tombstones(TOMBSTONE_GRACE_DAYS).await?;
        if purged > 0 {
            tracing::info!("Purgados {} tombstones expirados", purged);
        }

        Ok(changes_count)
    }

    /// Obtiene el root_id cacheado o lo descarga
    async fn get_cached_root_id(&self) -> Result<String> {
        {
            let guard = self.root_id_cache.read().await;
            if let Some(id) = &*guard {
                return Ok(id.clone());
            }
        }
        tracing::info!("Obteniendo ID canónico de Google Drive root...");
        let id = self.client.get_root_file_id().await?;
        let mut guard = self.root_id_cache.write().await;
        *guard = Some(id.clone());
        Ok(id)
    }

    /// Procesa un cambio individual de la API
    async fn process_change(&self, change: google_drive3::api::Change, root_id: &str) -> Result<()> {
        let file_id = change.file_id.as_deref()
            .context("Cambio sin file_id")?;

        // Caso 1: Archivo eliminado permanentemente (ya no existe en Drive)
        if change.removed == Some(true) {
            tracing::debug!("Cambio detectado: REMOVED (hard delete) file_id={}", file_id);
            // El archivo fue eliminado permanentemente de Drive (incluyendo papelera vacía)
            // → Hard delete: eliminar completamente de la DB local
            self.db.hard_delete_by_gdrive_id(file_id).await?;
            return Ok(());
        }

        // Caso 2: Archivo con datos
        if let Some(file) = change.file {
            // Verificar si está en la papelera
            if file.trashed == Some(true) {
                tracing::debug!("Cambio detectado: TRASHED file_id={}", file_id);
                self.db.soft_delete_by_gdrive_id(file_id).await?;
                return Ok(());
            }

            // Caso 3: Archivo restaurado (estaba en tombstone pero ya no está trashed)
            if self.db.has_tombstone(file_id).await? {
                tracing::debug!("Cambio detectado: RESTORED file_id={}", file_id);
                self.db.restore_by_gdrive_id(file_id).await?;
            }

            // Caso 4: Archivo nuevo o modificado
            let name = file.name.as_deref().unwrap_or("unknown");
            let is_dir = file.mime_type.as_deref() == Some("application/vnd.google-apps.folder");
            let size = file.size.unwrap_or(0);
            let mtime = file.modified_time
                .as_ref()
                .map(|t| t.timestamp())
                .unwrap_or(0);
            let mode = if is_dir { 0o755 } else { 0o644 };

            let can_move = file.capabilities.as_ref()
                .and_then(|c| c.can_move_item_within_drive)
                .unwrap_or(true);

            let shared = file.shared.unwrap_or(false);

            // Obtener o crear inode
            let inode = self.db.get_or_create_inode(file_id).await?;

            // Actualizar metadatos
            self.db.upsert_file_metadata(
                inode,
                size,
                mtime,
                mode,
                is_dir,
                file.mime_type.as_deref(),
                can_move,
                shared,
            ).await?;

            // Actualizar dentry (árbol de directorios)
            // IMPORTANTE: Si el archivo tiene cambios locales pendientes (dirty),
            // NO sobreescribir la dentry. El cambio remoto es probablemente un eco
            // de una operación previa nuestra, y el estado local (posiblemente un
            // segundo movimiento) tiene prioridad.
            let is_dirty = self.db.is_dirty(inode).await.unwrap_or(false);
            if !is_dirty {
                if let Some(parents) = &file.parents {
                    for parent_id in parents {
                        // Google Drive usa "root" o el ID canónico (root_id) para el "My Drive" del usuario
                        // Ambos deben mapearse al inode 1 (root del filesystem local)
                        let parent_inode = if parent_id == "root" || parent_id == root_id {
                            1u64
                        } else {
                            self.db.get_or_create_inode(parent_id).await?
                        };
                        self.db.upsert_dentry(parent_inode, inode, name).await?;
                    }
                } else {
                    // Sin padres → colgar del root
                    self.db.upsert_dentry(1, inode, name).await?;
                }
            } else {
                tracing::debug!(
                    "⏭️ Saltando actualización de dentry para inode={} (dirty): el cambio remoto es un eco",
                    inode
                );
            }

            // Actualizar remote_md5 si está disponible (para detección de conflictos)
            if let Some(md5) = file.md5_checksum.clone() {
                self.db.set_remote_md5(inode, &md5).await?;
            }

            tracing::trace!(
                "Cambio detectado: UPSERT file_id={}, name={}, is_dir={}",
                file_id, name, is_dir
            );

            // NUEVO: Verificar si este archivo pertenece a un Local Sync Directory
            if let Err(e) = self.process_local_sync_change(&file,file_id).await {
                tracing::warn!("Error procesando cambio local sync para {}: {:?}", file_id, e);
            }
        }

        Ok(())
    }

    /// Procesa cambios remotos para archivos que pertenecen a Local Sync
    async fn process_local_sync_change(
        &self,
        file: &google_drive3::api::File,
        file_id: &str,
    ) -> Result<()> {
        // Buscar si este archivo está en local_sync_files
        let local_file = match self.db.find_local_sync_file_by_gdrive_id(file_id).await? {
            Some(f) => f,
            None => return Ok(()), // No es un archivo de local sync
        };

        let base_dir = self.db.get_local_sync_dir(local_file.sync_dir_id).await?;
        let local_path = std::path::PathBuf::from(&base_dir.local_path).join(&local_file.relative_path);

        tracing::debug!("Procesando cambio local sync: {}", local_file.relative_path);

        match local_file.availability.as_str() {
            "local_online" => {
                // Descargar contenido actualizado al path local
                if !file.mime_type.as_deref().map(|m| m.contains("folder")).unwrap_or(false) {
                    let name_display = std::path::PathBuf::from(&local_file.relative_path)
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| local_file.relative_path.clone());
                    self.history.log(ActionType::Download, format!("Descargando: {}", name_display));
                    tracing::info!("📥 Descargando actualización para: {}", local_file.relative_path);
                    
                    // Descargar archivo usando chunks (con tracking de progreso)
                    let file_size = file.size.unwrap_or(0) as u64;
                    let mut content = Vec::with_capacity(file_size as usize);

                    let transfer_id = self.history.start_transfer(&name_display, TransferOp::Download, file_size);

                    const CHUNK_SIZE: u32 = 10 * 1024 * 1024; // 10 MB
                    let mut offset = 0u64;

                    while offset < file_size {
                        let size = std::cmp::min(CHUNK_SIZE, (file_size - offset) as u32);
                        let chunk = self.client.download_chunk(file_id, offset, size).await?;
                        content.extend_from_slice(&chunk);
                        offset += chunk.len() as u64;
                        self.history.update_transfer_progress(transfer_id, offset);
                    }

                    self.history.complete_transfer(transfer_id);
                    
                    // Asegurar que el directorio padre existe
                    if let Some(parent) = local_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    
                    // Escribir archivo
                    tokio::fs::write(&local_path, &content).await?;
                    
                    // Actualizar metadatos en DB
                    let md5 = file.md5_checksum.as_deref();
                    let mtime = file.modified_time.as_ref().map(|t| t.timestamp());
                    
                    self.db.update_local_file_from_remote(
                        local_file.id,
                        md5,
                        mtime,
                    ).await?;
                    
                    self.history.log(ActionType::Download, format!("Descargado: {}", name_display));
                    tracing::info!("✅ Archivo actualizado localmente: {}", local_file.relative_path);
                }
            }
            "online_only" => {
                // Solo actualizar metadatos (el symlink accederá a FUSE)
                let md5 = file.md5_checksum.as_deref();
                self.db.update_local_file_remote_metadata(local_file.id, md5).await?;
                tracing::debug!("Metadatos remotos actualizados para online_only: {}", local_file.relative_path);
            }
            _ => {}
        }

        Ok(())
    }
}
