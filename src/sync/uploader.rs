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
    history: ActionHistory,
}

impl Uploader {
    /// Crea un nuevo uploader
    pub fn new(
        db: Arc<MetadataRepository>,
        client: Arc<DriveClient>,
        interval_secs: u64,
        cache_dir: impl AsRef<Path>,
        history: ActionHistory,
    ) -> Self {
        Self {
            db,
            client,
            interval: Duration::from_secs(interval_secs),
            cache_dir: cache_dir.as_ref().to_path_buf(),
            history,
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
        // 1. Obtener archivos dirty
        let dirty_files = self.get_dirty_files().await?;
        
        if dirty_files.is_empty() {
            return Ok(0);
        }
        
        debug!("📋 Encontrados {} archivos dirty para subir", dirty_files.len());
        
        let mut uploaded_count = 0;
        
        // 2. Procesar cada archivo
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
        
        // 1. Obtener MD5 remoto conocido de la DB
        let known_md5 = self.db.get_remote_md5(inode).await?;
        
        // 2. Consultar MD5 actual del servidor
        let current_remote_md5 = self.client.get_file_md5(gdrive_id).await?;
        
        // 3. Detectar conflicto: si ambos existen y son diferentes
        if let (Some(known), Some(current)) = (&known_md5, &current_remote_md5) {
            if known != current {
                warn!("⚠️ CONFLICTO DETECTADO: archivo remoto cambió desde la última sync");
                warn!("   - MD5 conocido: {}", known);
                warn!("   - MD5 actual:   {}", current);
                return self.handle_conflict(inode, gdrive_id).await;
            }
        }
        
        // 4. Ruta del archivo en caché
        let cache_path = self.cache_dir.join(gdrive_id);
        
        if !cache_path.exists() {
            warn!("Archivo de caché no existe para actualización: {:?}", cache_path);
            return Ok(()); // Skip
        }
        
        // 5. Actualizar contenido usando la API
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
}
