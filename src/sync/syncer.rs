//! Sincronizador en background para detectar y aplicar cambios de Google Drive
//!
//! Utiliza la API changes.list para polling incremental de cambios.

use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::db::MetadataRepository;
use crate::gdrive::client::DriveClient;

/// Clave en sync_meta para el page token de changes
const SYNC_META_PAGE_TOKEN: &str = "changes_page_token";

/// Intervalo máximo de backoff en segundos
const MAX_BACKOFF_SECS: u64 = 300;


/// Período de gracia para tombstones en días
const TOMBSTONE_GRACE_DAYS: i64 = 7;

/// Sincronizador en background que detecta cambios de Google Drive
pub struct BackgroundSyncer {
    db: Arc<MetadataRepository>,
    client: Arc<DriveClient>,
    interval: Duration,
}

impl BackgroundSyncer {
    /// Crea un nuevo sincronizador
    pub fn new(
        db: Arc<MetadataRepository>,
        client: Arc<DriveClient>,
        interval_secs: u64,
    ) -> Self {
        Self {
            db,
            client,
            interval: Duration::from_secs(interval_secs),
        }
    }

    /// Inicia el loop de sincronización en un task de Tokio separado
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!("🔄 Background Syncer iniciado (intervalo: {:?})", self.interval);
            
            let mut current_backoff = self.interval;
            
            loop {
                match self.sync_once().await {
                    Ok(changes_count) => {
                        if changes_count > 0 {
                            tracing::info!("✅ Sincronización completada: {} cambios procesados", changes_count);
                        }
                        // Reset backoff en caso de éxito
                        current_backoff = self.interval;
                    }
                    Err(e) => {
                        tracing::error!("❌ Error en sincronización: {:?}", e);
                        
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
        
        // 3. Procesar cada cambio
        for change in changes {
            if let Err(e) = self.process_change(change).await {
                tracing::warn!("Error procesando cambio individual: {:?}", e);
                // Continuamos con los demás
            }
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

    /// Procesa un cambio individual de la API
    async fn process_change(&self, change: google_drive3::api::Change) -> Result<()> {
        let file_id = change.file_id.as_deref()
            .context("Cambio sin file_id")?;

        // Caso 1: Archivo eliminado/trashed
        if change.removed == Some(true) {
            tracing::debug!("Cambio detectado: REMOVED file_id={}", file_id);
            self.db.soft_delete_by_gdrive_id(file_id).await?;
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
            ).await?;

            // Actualizar dentry (árbol de directorios)
            if let Some(parents) = &file.parents {
                for parent_id in parents {
                    let parent_inode = if parent_id == "root" {
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

            // Actualizar remote_md5 si está disponible (para detección de conflictos)
            if let Some(md5) = file.md5_checksum {
                self.db.set_remote_md5(inode, &md5).await?;
            }

            tracing::debug!(
                "Cambio detectado: UPSERT file_id={}, name={}, is_dir={}",
                file_id, name, is_dir
            );
        }

        Ok(())
    }
}
