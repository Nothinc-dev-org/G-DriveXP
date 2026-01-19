//! Servidor IPC Unix Socket para consultas de estado desde extensiones externas
//!
//! Escucha en /run/user/{UID}/gdrivexp.sock y responde queries de estado de sincronización.

use anyhow::{Context, Result};
use percent_encoding::percent_decode_str;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;

use crate::db::MetadataRepository;
use crate::sync::local_sync_manager::LocalSyncCommandSender;
use super::{IpcRequest, IpcResponse, SyncStatus, FileAvailability};

/// Servidor IPC para comunicación con extensiones externas
pub struct IpcServer {
    socket_path: PathBuf,
    db: Arc<MetadataRepository>,
    mount_point: PathBuf,
    cache_dir: PathBuf,
    local_sync_tx: Option<LocalSyncCommandSender>,
}

impl IpcServer {
    /// Crea un nuevo servidor IPC
    pub fn new(
        socket_path: PathBuf,
        db: Arc<MetadataRepository>,
        mount_point: PathBuf,
        cache_dir: PathBuf,
    ) -> Self {
        Self {
            socket_path,
            db,
            mount_point,
            cache_dir,
            local_sync_tx: None,
        }
    }

    /// Establece el canal de comandos para LocalSyncManager
    pub fn with_local_sync(mut self, tx: LocalSyncCommandSender) -> Self {
        self.local_sync_tx = Some(tx);
        self
    }

    /// Inicia el servidor IPC en un task de Tokio separado
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                tracing::error!("Error en servidor IPC: {:?}", e);
            }
        })
    }

    /// Loop principal del servidor
    async fn run(&self) -> Result<()> {
        // Eliminar socket existente si quedó de una ejecución anterior
        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path)
                .context("Error al eliminar socket existente")?;
        }

        let listener = UnixListener::bind(&self.socket_path)
            .context("Error al crear Unix Socket")?;

        tracing::info!("🔌 Servidor IPC escuchando en {:?}", self.socket_path);

        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let db = self.db.clone();
                    let mount_point = self.mount_point.clone();
                    let cache_dir = self.cache_dir.clone();
                    let local_sync_tx = self.local_sync_tx.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, db, mount_point, cache_dir, local_sync_tx).await {
                            tracing::debug!("Error manejando cliente IPC: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("Error aceptando conexión IPC: {:?}", e);
                }
            }
        }
    }
}

/// Maneja una conexión de cliente individual
async fn handle_client(
    mut stream: UnixStream,
    db: Arc<MetadataRepository>,
    mount_point: PathBuf,
    cache_dir: PathBuf,
    local_sync_tx: Option<LocalSyncCommandSender>,
) -> Result<()> {
    // Buffer para leer el request (max 4KB)
    let mut buf = vec![0u8; 4096];
    
    // Leer longitud del mensaje (4 bytes, big-endian)
    stream.read_exact(&mut buf[..4]).await?;
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    
    if len > 4096 {
        anyhow::bail!("Mensaje IPC demasiado grande: {} bytes", len);
    }
    
    // Leer el mensaje
    stream.read_exact(&mut buf[..len]).await?;
    
    // Deserializar request
    let request: IpcRequest = bincode::deserialize(&buf[..len])
        .context("Error deserializando request IPC")?;
    
    // Procesar request
    let response = match request {
        IpcRequest::Ping => IpcResponse::Pong,
        IpcRequest::GetFileStatus { path } => {
            let status = get_file_status(&db, &mount_point, &cache_dir, &path).await;
            IpcResponse::FileStatus(status)
        }
        IpcRequest::GetFileAvailability { path } => {
            let avail = get_file_availability(&db, &path).await;
            IpcResponse::Availability(avail)
        }
        IpcRequest::SetOnlineOnly { path } => {
            match set_availability(&db, &local_sync_tx, &path, "online_only").await {
                Ok(()) => IpcResponse::Success,
                Err(e) => IpcResponse::Error { message: e.to_string() },
            }
        }
        IpcRequest::SetLocalOnline { path } => {
            match set_availability(&db, &local_sync_tx, &path, "local_online").await {
                Ok(()) => IpcResponse::Success,
                Err(e) => IpcResponse::Error { message: e.to_string() },
            }
        }
    };
    
    // Serializar respuesta
    let response_bytes = bincode::serialize(&response)
        .context("Error serializando respuesta IPC")?;
    
    // Escribir longitud + respuesta
    let len_bytes = (response_bytes.len() as u32).to_be_bytes();
    stream.write_all(&len_bytes).await?;
    stream.write_all(&response_bytes).await?;
    
    Ok(())
}

/// Obtiene el estado de sincronización de un archivo dado su path
async fn get_file_status(
    db: &MetadataRepository,
    mount_point: &std::path::Path,
    cache_dir: &std::path::Path,
    file_path: &str,
) -> SyncStatus {
    // Convertir URI file:// a path si es necesario y decodificar URL encoding
    let raw_path = if file_path.starts_with("file://") {
        file_path.strip_prefix("file://").unwrap_or(file_path)
    } else {
        file_path
    };
    
    // Decodificar caracteres URL-encoded (espacios=%20, paréntesis=%28%29, etc.)
    let path = match percent_decode_str(raw_path).decode_utf8() {
        Ok(decoded) => decoded.into_owned(),
        Err(_) => return SyncStatus::Unknown,
    };
    
    // Verificar si el archivo está dentro del mount point
    let mount_str = mount_point.to_string_lossy();
    if !path.starts_with(mount_str.as_ref()) {
        return SyncStatus::Unknown;
    }
    
    // Extraer path relativo dentro del mount point
    let relative_path = match path.strip_prefix(mount_str.as_ref()) {
        Some(p) => p.trim_start_matches('/'),
        None => return SyncStatus::Unknown,
    };
    
    // Buscar el archivo en la base de datos por nombre
    // Recorremos el path para encontrar el inode
    let (inode, gdrive_id) = match resolve_path_to_inode_and_gdrive_id(db, relative_path).await {
        Ok(Some(result)) => result,
        Ok(None) => return SyncStatus::Unknown,
        Err(_) => return SyncStatus::Unknown,
    };
    
    // Consultar estado de sincronización, pasando info de cache
    match get_sync_state(db, cache_dir, inode, &gdrive_id).await {
        Ok(state) => state,
        Err(_) => SyncStatus::Unknown,
    }
}

/// Resuelve un path relativo a su inode y gdrive_id
async fn resolve_path_to_inode_and_gdrive_id(
    db: &MetadataRepository,
    relative_path: &str,
) -> Result<Option<(u64, String)>> {
    let parts: Vec<&str> = relative_path.split('/').filter(|s| !s.is_empty()).collect();
    
    let mut current_inode = 1u64; // Root inode
    
    for part in parts {
        match db.lookup(current_inode, part).await? {
            Some(child_inode) => current_inode = child_inode,
            None => return Ok(None),
        }
    }
    
    // Obtener gdrive_id del inode final
    let gdrive_id: Option<String> = sqlx::query_scalar(
        "SELECT gdrive_id FROM inodes WHERE inode = ?"
    )
    .bind(current_inode as i64)
    .fetch_optional(db.pool())
    .await?;
    
    match gdrive_id {
        Some(id) => Ok(Some((current_inode, id))),
        None => Ok(None),
    }
}

/// Consulta el estado de sincronización en sync_state
async fn get_sync_state(
    db: &MetadataRepository,
    _cache_dir: &std::path::Path,
    inode: u64,
    _gdrive_id: &str,
) -> Result<SyncStatus> {
    // Obtener atributos para verificar si es directorio
    let is_dir: Option<bool> = sqlx::query_scalar(
        "SELECT is_dir FROM attrs WHERE inode = ?"
    )
    .bind(inode as i64)
    .fetch_optional(db.pool())
    .await?;
    
    // Los directorios siempre se consideran "sincronizados" (solo contienen metadata)
    if is_dir == Some(true) {
        return Ok(SyncStatus::Synced);
    }
    
    // Obtener el tamaño esperado del archivo desde la base de datos
    let expected_size: Option<i64> = sqlx::query_scalar(
        "SELECT size FROM attrs WHERE inode = ?"
    )
    .bind(inode as i64)
    .fetch_optional(db.pool())
    .await?;
    
    // Verificar si el archivo está COMPLETAMENTE cacheado usando file_cache_chunks
    // Esta es la forma correcta ya que usamos caché por chunks, no archivos completos
    let has_complete_cache = if let Some(expected) = expected_size {
        if expected == 0 {
            // Archivos vacíos siempre están "completos"
            true
        } else {
            // Verificar si hay rangos faltantes en todo el archivo
            let missing_ranges = db.get_missing_ranges(inode, 0, (expected - 1) as u64).await?;
            missing_ranges.is_empty()
        }
    } else {
        // Si no hay tamaño esperado, asumimos no cacheado
        false
    };
    
    // Consultar si está dirty
    let result = sqlx::query_as::<_, (bool, Option<i64>)>(
        "SELECT dirty, deleted_at FROM sync_state WHERE inode = ?"
    )
    .bind(inode as i64)
    .fetch_optional(db.pool())
    .await?;
    
    match result {
        Some((dirty, deleted_at)) => {
            if deleted_at.is_some() {
                // Archivo marcado para eliminación
                Ok(SyncStatus::LocalOnly)
            } else if dirty {
                // Cambios locales pendientes de subir
                Ok(SyncStatus::LocalOnly)
            } else if has_complete_cache {
                // Sincronizado y completamente disponible localmente
                Ok(SyncStatus::Synced)
            } else {
                // En Drive pero no descargado completamente
                Ok(SyncStatus::CloudOnly)
            }
        }
        None => {
            // Sin registro en sync_state
            if has_complete_cache {
                Ok(SyncStatus::Synced)
            } else {
                // Archivo solo en la nube o parcialmente cacheado
                Ok(SyncStatus::CloudOnly)
            }
        }
    }
}

/// Obtiene la disponibilidad de un archivo en Local Sync
async fn get_file_availability(
    db: &MetadataRepository,
    file_path: &str,
) -> FileAvailability {
    // Decodificar URI file://
    let path = decode_file_uri(file_path);
    
    // Buscar en local_sync_files por path absoluto
    match db.find_local_sync_file_by_absolute_path(&path).await {
        Ok(Some(file)) => {
            match file.availability.as_str() {
                "local_online" => FileAvailability::LocalOnline,
                "online_only" => FileAvailability::OnlineOnly,
                _ => FileAvailability::NotTracked,
            }
        }
        _ => FileAvailability::NotTracked,
    }
}

/// Cambia la disponibilidad de un archivo
async fn set_availability(
    db: &MetadataRepository,
    local_sync_tx: &Option<LocalSyncCommandSender>,
    file_path: &str,
    availability: &str,
) -> Result<()> {
    use crate::sync::local_sync_manager::LocalSyncCommand;
    
    let path = decode_file_uri(file_path);
    
    // Resolver path a sync_dir_id y relative_path
    let (sync_dir_id, relative_path) = db.resolve_local_sync_path(&path).await?;
    
    // Obtener el canal de comandos
    let tx = local_sync_tx.as_ref()
        .ok_or_else(|| anyhow::anyhow!("LocalSyncManager no disponible"))?;
    
    // Enviar comando al LocalSyncManager
    let cmd = match availability {
        "online_only" => LocalSyncCommand::SetOnlineOnly {
            sync_dir_id,
            relative_path,
        },
        "local_online" => LocalSyncCommand::SetLocalOnline {
            sync_dir_id,
            relative_path,
        },
        _ => return Err(anyhow::anyhow!("Availability desconocida: {}", availability)),
    };
    
    tx.send(cmd).await
        .map_err(|e| anyhow::anyhow!("Error enviando comando: {}", e))?;
    
    Ok(())
}

/// Decodifica un URI file:// a path absoluto
fn decode_file_uri(uri: &str) -> String {
    let raw_path = uri.strip_prefix("file://").unwrap_or(uri);
    percent_decode_str(raw_path)
        .decode_utf8()
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| raw_path.to_string())
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        // Limpiar socket al cerrar
        let _ = std::fs::remove_file(&self.socket_path);
    }
}
