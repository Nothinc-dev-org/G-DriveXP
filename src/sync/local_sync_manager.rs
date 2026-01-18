//! LocalSyncManager: Gestor de sincronización bidireccional de carpetas locales
//!
//! Estrategia: Migración a Symlink + FUSE
//! 1. El directorio local se mueve al punto de montaje FUSE (~/GoogleDrive/Nombre)
//! 2. Se crea un symlink desde la ubicación original apuntando al FUSE path
//! 3. Todas las operaciones locales pasan a través de FUSE automáticamente
//!

use anyhow::{Context, Result};
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
}

/// Gestor de sincronización bidireccional de carpetas locales
// ... (imports)

// ...

pub struct LocalSyncManager {
    db: Arc<MetadataRepository>,
    // drive_client removido
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
// ...

    /// Inicia el loop de procesamiento de comandos en un task separado
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("🔗 LocalSyncManager iniciado (Modo Symlink)");

            // Al iniciar, intentar migrar todas las carpetas habilitadas
            if let Err(e) = self.initialize_all_enabled().await {
                error!("Error inicializando carpetas locales: {:?}", e);
            }

            // Loop de mensajes
            while let Some(cmd) = self.command_rx.recv().await {
                match cmd {
                    LocalSyncCommand::InitializeFolder { sync_dir_id, local_path } => {
                        info!("Inicializando carpeta id={}, path={}", sync_dir_id, local_path.display());
                        if let Err(e) = self.initialize_folder(sync_dir_id, &local_path).await {
                            error!("Error inicializando carpeta {}: {:?}", local_path.display(), e);
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
            if let Err(e) = self.initialize_folder(dir.id, &path).await {
                warn!("Error procesando directorio {}: {:?}", dir.local_path, e);
            }
        }
        Ok(())
    }

    /// Convierte un directorio local en un symlink hacia FUSE
    async fn initialize_folder(&self, _sync_dir_id: i64, local_path: &Path) -> Result<()> {
        if !local_path.exists() {
            return Ok(());
        }

        // Obtener nombre de la carpeta
        let folder_name = local_path.file_name()
            .context("Nombre de carpeta inválido")?
            .to_os_string();
        
        // Destino en FUSE: ~/GoogleDrive/NombreCarpeta
        let fuse_path = self.mount_point.join(&folder_name);

        // Verificar si ya es un symlink correcto
        if local_path.is_symlink() {
            match local_path.read_link() {
                Ok(target) => {
                    if target == fuse_path {
                        debug!("Carpeta {} ya está enlazada correctamente", local_path.display());
                        return Ok(());
                    }
                }
                Err(_) => {}
            }
        }
        
        // Verificar si FUSE está montado (el directorio mount point debería estar accesible)
        if !self.mount_point.exists() {
             return Err(anyhow::anyhow!("El punto de montaje FUSE no está disponible"));
        }

        info!("Migrando {} -> FUSE path {}", local_path.display(), fuse_path.display());

        // 1. Asegurar que la carpeta destino existe en FUSE
        if !fuse_path.exists() {
            debug!("Creando directorio en FUSE: {}", fuse_path.display());
            tokio::fs::create_dir_all(&fuse_path).await
                .context("Error creando directorio en FUSE")?;
        }

        // 2. Proceso de migración
        let backup_path = local_path.with_extension("backup_migration");
        
        // HACK: rename() falla si el source está ocupado, pero en linux suele funcionar.
        // Si falla, es un blocker.
        if let Err(e) = tokio::fs::rename(local_path, &backup_path).await {
            error!("Fallo al renombrar directorio original: {:?}", e);
            return Err(e.into());
        }

        // 3. Crear symlink: local -> fuse
        if let Err(e) = symlink(&fuse_path, local_path) {
            error!("Error creando symlink, restaurando backup: {:?}", e);
            let _ = tokio::fs::rename(&backup_path, local_path).await;
            return Err(e.into());
        }
        
        info!("Symlink creado correctamente. Moviendo archivos...");

        // 4. Mover archivos desde backup hacia el symlink
        if let Err(e) = self.move_contents_recursive(&backup_path, local_path).await {
            error!("Error moviendo contenido a FUSE: {:?}", e);
            // El usuario tiene el backup_path preservado
            return Err(e);
        }

        // 5. Borrar backup si todo salió bien
        if let Err(e) = tokio::fs::remove_dir_all(&backup_path).await {
            warn!("No se pudo eliminar el backup {}: {:?}", backup_path.display(), e);
        }

        info!("✅ Migración completada para {}", local_path.display());
        Ok(())
    }

    /// Mueve el contenido recursivamente copiando y borrando
    async fn move_contents_recursive(&self, source: &Path, target: &Path) -> Result<()> {
        let mut entries = tokio::fs::read_dir(source).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            let entry_path = entry.path();
            let file_name = entry.file_name();
            let target_entry_path = target.join(file_name);

            if file_type.is_dir() {
                if !target_entry_path.exists() {
                    tokio::fs::create_dir(&target_entry_path).await?;
                }
                Box::pin(self.move_contents_recursive(&entry_path, &target_entry_path)).await?;
            } else {
                // Copiar a través del symlink (escribe en FUSE)
                match tokio::fs::copy(&entry_path, &target_entry_path).await {
                    Ok(_) => {
                        // Borrar original tras copia
                        if let Err(e) = tokio::fs::remove_file(&entry_path).await {
                            warn!("Error borrando archivo source {}: {:?}", entry_path.display(), e);
                        }
                    }
                    Err(e) => {
                        error!("Error copiando {} a FUSE: {:?}", entry_path.display(), e);
                        // Continuamos intentando otros archivos? O fallamos?
                        // Mejor fallar para alertar
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(())
    }
}
