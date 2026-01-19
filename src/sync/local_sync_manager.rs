//! LocalSyncManager: Gestor de sincronización bidireccional de carpetas locales
//!
//! Estrategia: Archivos Reales + Symlinks Selectivos
//! - Los archivos pueden ser "Local & Online" (archivo real) o "Just Online" (symlink a FUSE)
//! - El usuario decide por archivo qué modo usar

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use std::os::unix::fs::symlink;

use crate::db::MetadataRepository;

/// Canal para comandos de sincronización desde la GUI
pub type LocalSyncCommandSender = mpsc::Sender<LocalSyncCommand>;
pub type LocalSyncCommandReceiver = mpsc::Receiver<LocalSyncCommand>;

/// Comandos que puede recibir el LocalSyncManager
#[derive(Debug)]
pub enum LocalSyncCommand {
    /// Inicializar una carpeta local recién añadida
    InitializeFolder { sync_dir_id: i64, local_path: PathBuf },
    
    /// Cambiar un archivo a "Just Online" (liberar espacio)
    SetOnlineOnly { sync_dir_id: i64, relative_path: String },
    
    /// Cambiar un archivo a "Local & Online" (descargar)
    SetLocalOnline { sync_dir_id: i64, relative_path: String },
    
    /// Escanear carpeta inicial y registrar todos los archivos
    ScanFolder { sync_dir_id: i64, local_path: PathBuf },
}

/// Gestor de sincronización bidireccional de carpetas locales
pub struct LocalSyncManager {
    db: Arc<MetadataRepository>,
    mount_point: PathBuf,
    command_rx: LocalSyncCommandReceiver,
}

impl LocalSyncManager {
    pub fn new(
        db: Arc<MetadataRepository>,
        mount_point: PathBuf,
    ) -> (Self, LocalSyncCommandSender) {
        let (tx, rx) = mpsc::channel(32);
        
        let manager = Self {
            db,
            mount_point,
            command_rx: rx,
        };
        
        (manager, tx)
    }

    /// Inicia el loop de procesamiento de comandos en un task separado
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("🔗 LocalSyncManager iniciado (Modo Híbrido)");

            // Al iniciar, escanear todas las carpetas habilitadas
            if let Err(e) = self.initialize_all_enabled().await {
                error!("Error inicializando carpetas locales: {:?}", e);
            }

            // Loop de mensajes
            while let Some(cmd) = self.command_rx.recv().await {
                match cmd {
                    LocalSyncCommand::InitializeFolder { sync_dir_id, local_path } => {
                        info!("Inicializando carpeta id={}, path={}", sync_dir_id, local_path.display());
                        if let Err(e) = self.scan_folder(sync_dir_id, &local_path).await {
                            error!("Error inicializando carpeta {}: {:?}", local_path.display(), e);
                        }
                    }
                    LocalSyncCommand::SetOnlineOnly { sync_dir_id, relative_path } => {
                        info!("Liberando espacio: sync_dir={}, path={}", sync_dir_id, relative_path);
                        if let Err(e) = self.set_online_only(sync_dir_id, &relative_path).await {
                            error!("Error liberando espacio {}: {:?}", relative_path, e);
                        }
                    }
                    LocalSyncCommand::SetLocalOnline { sync_dir_id, relative_path } => {
                        info!("Descargando localmente: sync_dir={}, path={}", sync_dir_id, relative_path);
                        if let Err(e) = self.set_local_online(sync_dir_id, &relative_path).await {
                            error!("Error descargando {}: {:?}", relative_path, e);
                        }
                    }
                    LocalSyncCommand::ScanFolder { sync_dir_id, local_path } => {
                        info!("Escaneando carpeta: {}", local_path.display());
                        if let Err(e) = self.scan_folder(sync_dir_id, &local_path).await {
                            error!("Error escaneando {}: {:?}", local_path.display(), e);
                        }
                    }
                }
            }
        })
    }

    async fn initialize_all_enabled(&self) -> Result<()> {
        let dirs = self.db.get_enabled_local_sync_dirs().await?;
        for dir in dirs {
            let path = PathBuf::from(&dir.local_path);
            if let Err(e) = self.scan_folder(dir.id, &path).await {
                warn!("Error procesando directorio {}: {:?}", dir.local_path, e);
            }
        }
        Ok(())
    }

    /// Escanea recursivamente una carpeta y registra todos los archivos en la DB
    async fn scan_folder(&self, sync_dir_id: i64, base_path: &Path) -> Result<()> {
        if !base_path.exists() {
            return Ok(());
        }

        self.scan_recursive(sync_dir_id, base_path, base_path).await?;
        
        info!("✅ Carpeta escaneada: {}", base_path.display());
        Ok(())
    }

    async fn scan_recursive(&self, sync_dir_id: i64, base_path: &Path, current_path: &Path) -> Result<()> {
        let mut entries = tokio::fs::read_dir(current_path).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_type = entry.file_type().await?;
            
            // Ignorar archivos ocultos y temporales
            if let Some(name) = path.file_name() {
                let name_str = name.to_string_lossy();
                if name_str.starts_with('.') || name_str.ends_with('~') {
                    continue;
                }
            }
            
            let relative_path = path.strip_prefix(base_path)?
                .to_string_lossy()
                .into_owned();
            
            if file_type.is_dir() {
                // Registrar directorio
                self.db.upsert_local_sync_file(
                    sync_dir_id,
                    &relative_path,
                    true,
                    "local_online",
                    None,
                    None,
                    None,
                ).await?;
                
                // Recursión
                Box::pin(self.scan_recursive(sync_dir_id, base_path, &path)).await?;
            } else if file_type.is_file() && !file_type.is_symlink() {
                // Registrar archivo real
                let metadata = tokio::fs::metadata(&path).await?;
                let size = metadata.len() as i64;
                let mtime = metadata.modified()?.duration_since(std::time::UNIX_EPOCH)?.as_secs() as i64;
                let md5 = compute_file_md5(&path).await?;
                
                self.db.upsert_local_sync_file(
                    sync_dir_id,
                    &relative_path,
                    false,
                    "local_online",
                    Some(mtime),
                    Some(size),
                    Some(&md5),
                ).await?;
                
                debug!("📄 Registrado: {}", relative_path);
            }
        }
        
        Ok(())
    }

    /// Convierte un archivo real en un symlink hacia FUSE (liberar espacio)
    async fn set_online_only(&self, sync_dir_id: i64, relative_path: &str) -> Result<()> {
        // 1. Obtener info del archivo
        let file_info = self.db.get_local_sync_file(sync_dir_id, relative_path).await?
            .context("Archivo no encontrado en DB")?;
        
        let base_dir = self.db.get_local_sync_dir(sync_dir_id).await?;
        let local_path = PathBuf::from(&base_dir.local_path).join(relative_path);
        
        // 2. Verificar que el archivo existe en FUSE
        let gdrive_folder_name = base_dir.gdrive_folder_id
            .as_ref()
            .context("Carpeta no tiene gdrive_folder_id configurado")?;
        
        let fuse_path = self.mount_point
            .join(gdrive_folder_name)
            .join(relative_path);
        
        if !fuse_path.exists() {
            return Err(anyhow!("El archivo no está disponible en Google Drive aún"));
        }
        
        // 3. Eliminar archivo local y crear symlink
        if local_path.is_file() && !local_path.is_symlink() {
            tokio::fs::remove_file(&local_path).await?;
            symlink(&fuse_path, &local_path)?;
            
            // 4. Actualizar DB
            self.db.set_file_availability(sync_dir_id, relative_path, "online_only").await?;
            
            info!("☁️ Archivo liberado: {} -> symlink a FUSE", relative_path);
        } else if local_path.is_symlink() {
            info!("⚠️ Archivo ya es symlink: {}", relative_path);
        }
        
        Ok(())
    }

    /// Convierte un symlink en un archivo real (descargar)
    async fn set_local_online(&self, sync_dir_id: i64, relative_path: &str) -> Result<()> {
        let base_dir = self.db.get_local_sync_dir(sync_dir_id).await?;
        let local_path = PathBuf::from(&base_dir.local_path).join(relative_path);
        
        // 1. Verificar que es un symlink
        if !local_path.is_symlink() {
            info!("⚠️ Archivo ya es local: {}", relative_path);
            return Ok(());
        }
        
        // 2. Leer contenido desde FUSE (el symlink apunta ahí)
        let content = tokio::fs::read(&local_path).await?;
        
        // 3. Eliminar symlink
        tokio::fs::remove_file(&local_path).await?;
        
        // 4. Escribir archivo real
        tokio::fs::write(&local_path, &content).await?;
        
        // 5. Actualizar DB con metadatos frescos
        let metadata = tokio::fs::metadata(&local_path).await?;
        let md5 = compute_file_md5(&local_path).await?;
        let size = metadata.len() as i64;
        let mtime = metadata.modified()?.duration_since(std::time::UNIX_EPOCH)?.as_secs() as i64;
        
        self.db.update_local_file_metadata(
            sync_dir_id,
            relative_path,
            "local_online",
            size,
            mtime,
            &md5,
        ).await?;
        
        info!("📥 Archivo descargado localmente: {}", relative_path);
        Ok(())
    }
}

async fn compute_file_md5(path: &Path) -> Result<String> {
    use md5::{Md5, Digest};
    let data = tokio::fs::read(path).await?;
    let hash = Md5::digest(&data);
    Ok(format!("{:x}", hash))
}
