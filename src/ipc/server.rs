//! Servidor IPC Unix Socket para consultas de estado desde extensiones externas
//!
//! Escucha en /run/user/{UID}/gdrivexp.sock y responde queries de estado de sincronización.

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;

use crate::db::MetadataRepository;
use super::{IpcRequest, IpcResponse, SyncStatus};

/// Servidor IPC para comunicación con extensiones externas
pub struct IpcServer {
    socket_path: PathBuf,
    db: Arc<MetadataRepository>,
    mount_point: PathBuf,
}

impl IpcServer {
    /// Crea un nuevo servidor IPC
    pub fn new(
        socket_path: PathBuf,
        db: Arc<MetadataRepository>,
        mount_point: PathBuf,
    ) -> Self {
        Self {
            socket_path,
            db,
            mount_point,
        }
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
                    
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, db, mount_point).await {
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
            let status = get_file_status(&db, &mount_point, &path).await;
            IpcResponse::FileStatus(status)
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
    file_path: &str,
) -> SyncStatus {
    // Convertir URI file:// a path si es necesario
    let path = if file_path.starts_with("file://") {
        file_path.strip_prefix("file://").unwrap_or(file_path)
    } else {
        file_path
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
    let inode = match resolve_path_to_inode(db, relative_path).await {
        Ok(Some(inode)) => inode,
        Ok(None) => return SyncStatus::Unknown,
        Err(_) => return SyncStatus::Unknown,
    };
    
    // Consultar estado de sincronización
    match get_sync_state(db, inode).await {
        Ok(state) => state,
        Err(_) => SyncStatus::Unknown,
    }
}

/// Resuelve un path relativo a su inode
async fn resolve_path_to_inode(
    db: &MetadataRepository,
    relative_path: &str,
) -> Result<Option<u64>> {
    let parts: Vec<&str> = relative_path.split('/').filter(|s| !s.is_empty()).collect();
    
    let mut current_inode = 1u64; // Root inode
    
    for part in parts {
        match db.lookup(current_inode, part).await? {
            Some(child_inode) => current_inode = child_inode,
            None => return Ok(None),
        }
    }
    
    Ok(Some(current_inode))
}

/// Consulta el estado de sincronización en sync_state
async fn get_sync_state(db: &MetadataRepository, inode: u64) -> Result<SyncStatus> {
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
                Ok(SyncStatus::Pending)
            } else if dirty {
                Ok(SyncStatus::Pending)
            } else {
                Ok(SyncStatus::Synced)
            }
        }
        None => {
            // Sin registro en sync_state, asumimos sincronizado (solo lectura)
            Ok(SyncStatus::Synced)
        }
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        // Limpiar socket al cerrar
        let _ = std::fs::remove_file(&self.socket_path);
    }
}
