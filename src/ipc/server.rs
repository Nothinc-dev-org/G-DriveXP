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
use crate::mirror::MirrorCommand;
use super::{IpcRequest, IpcResponse, SyncStatus, FileAvailability};
use tokio::sync::mpsc;

/// Servidor IPC para comunicación con extensiones externas
/// Servidor IPC para comunicación con extensiones externas
pub struct IpcServer {
    socket_path: PathBuf,
    db: Arc<MetadataRepository>,
    mirror_path: PathBuf,
    cache_dir: PathBuf,
    mirror_tx: Option<mpsc::Sender<MirrorCommand>>,
}

impl IpcServer {
    /// Crea un nuevo servidor IPC
    pub fn new(
        socket_path: PathBuf,
        db: Arc<MetadataRepository>,
        mirror_path: PathBuf,
        cache_dir: PathBuf,
    ) -> Self {
        Self {
            socket_path,
            db,
            mirror_path,
            cache_dir,
            mirror_tx: None,
        }
    }

    /// Establece el canal de comandos para MirrorManager
    pub fn with_mirror_manager(mut self, tx: mpsc::Sender<MirrorCommand>) -> Self {
        self.mirror_tx = Some(tx);
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
                    let mirror_path = self.mirror_path.clone();
                    let cache_dir = self.cache_dir.clone();
                    let local_sync_tx = self.mirror_tx.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, db, mirror_path, cache_dir, local_sync_tx).await {
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
    mirror_path: PathBuf,
    cache_dir: PathBuf,
    mirror_tx: Option<mpsc::Sender<MirrorCommand>>,
) -> Result<()> {
    // Buffer para leer el request (max 4KB)
    let mut buf = vec![0u8; 4096];
    
    // Loop principal para conexión persistente
    loop {
        // Leer longitud del mensaje (4 bytes, big-endian)
        // Usamos read_exact pero manejamos EOF gracefulmente
        match stream.read_exact(&mut buf[..4]).await {
            Ok(_) => {}, // Continuamos
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // El cliente cerró la conexión
                return Ok(());
            },
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        
        if len > 4096 {
            anyhow::bail!("Mensaje IPC demasiado grande: {} bytes", len);
        }
        
        // Leer el mensaje
        if let Err(e) = stream.read_exact(&mut buf[..len]).await {
            // Si falla la lectura del contenido pero leímos el header, es un error real
            anyhow::bail!("Error leyendo cuerpo del mensaje IPC: {}", e);
        }
        
        // Deserializar request
        let request: IpcRequest = bincode::deserialize(&buf[..len])
            .context("Error deserializando request IPC")?;
        
        // Log de entrada (solo nivel trace para no saturar con el loop)
        tracing::trace!("📥 IPC Request: {:?}", request);
        
        // Procesar request
        let response = match request {
            IpcRequest::Ping => IpcResponse::Pong,
            IpcRequest::GetFileStatus { path } => {
                let data = get_extended_file_status(&db, &mirror_path, &cache_dir, &path).await;
                IpcResponse::ExtendedStatus(data)
            }
            IpcRequest::GetFileAvailability { path } => {
                let avail = get_file_availability(&db, &mirror_path, &path).await;
                IpcResponse::Availability(avail)
            }
            IpcRequest::SetOnlineOnly { path } => {
                // Validación para evitar borrar archivos no sincronizados
                let rel = if path.starts_with(mirror_path.to_string_lossy().as_ref()) {
                    path.strip_prefix(mirror_path.to_string_lossy().as_ref()).unwrap_or(&path).trim_start_matches('/')
                } else {
                    &path
                };
                
                let can_free_space = if let Ok(Some((_, gdrive_id))) = resolve_path_to_inode_and_gdrive_id(&db, rel).await {
                    !gdrive_id.starts_with("temp_")
                } else {
                    true // Si no encontramos inode, dejamos que el error se maneje más adelante
                };

                if !can_free_space {
                    IpcResponse::Error { message: "El archivo aún no se ha sincronizado con Google Drive. No se puede liberar espacio.".to_string() }
                } else {
                    match set_availability(&mirror_tx, &path, "online_only").await {
                        Ok(()) => IpcResponse::Success,
                        Err(e) => IpcResponse::Error { message: e.to_string() },
                    }
                }
            }
            IpcRequest::SetLocalOnline { path } => {
                match set_availability(&mirror_tx, &path, "local_online").await {
                    Ok(()) => IpcResponse::Success,
                    Err(e) => IpcResponse::Error { message: e.to_string() },
                }
            }
        };
        
        // Log de salida (trace)
        tracing::trace!("📤 IPC Response: {:?}", response);
        
        // Serializar respuesta
        let response_bytes = bincode::serialize(&response)
            .context("Error serializando respuesta IPC")?;
        
        // Escribir longitud + respuesta
        let len_bytes = (response_bytes.len() as u32).to_be_bytes();
        stream.write_all(&len_bytes).await?;
        stream.write_all(&response_bytes).await?;
    }
}

/// Obtiene el estado extendido de un archivo (sincronización, disponibilidad, compartido)
async fn get_extended_file_status(
    db: &MetadataRepository,
    mirror_path: &std::path::Path,
    cache_dir: &std::path::Path,
    file_path: &str,
) -> super::FileStatusData {
    // Decodificar URI
    let path_str = decode_file_uri(file_path);
    let _path = std::path::Path::new(&path_str);
    
    // Default safe response
    let mut data = super::FileStatusData {
        status: SyncStatus::Unknown,
        availability: FileAvailability::NotTracked,
        is_shared: false,
    };

    // 1. Determinar Disponibilidad
    data.availability = get_file_availability(db, mirror_path, file_path).await;
    
    // 2. Determinar SyncStatus
    if data.availability != FileAvailability::NotTracked {
        // Resolver inode para obtener status y shared
        let mirror_str = mirror_path.to_string_lossy();
        if path_str.starts_with(mirror_str.as_ref()) {
            if let Some(relative_path) = path_str.strip_prefix(mirror_str.as_ref()) {
                let rel = relative_path.trim_start_matches('/');
                if let Ok(Some((inode, gdrive_id))) = resolve_path_to_inode_and_gdrive_id(db, rel).await {
                    // Obtener status
                    data.status = get_sync_state(db, cache_dir, inode, &gdrive_id, &path_str)
                        .await
                        .unwrap_or(SyncStatus::Unknown);
                    
                    // Obtener flag de compartido
                    if let Ok(attrs) = db.get_attrs(inode).await {
                        data.is_shared = attrs.shared;
                    }
                }
            }
        }
    }

    data
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
    abs_path: &str,
) -> Result<SyncStatus> {
    // 1. Verificación Física (Source of Truth para UI)
    // Si es un Symlink -> CloudOnly
    // Si es File -> Synced
    // Si no existe -> CloudOnly (o Deleted)
    
    let path = std::path::Path::new(abs_path);
    let physical_state = match tokio::fs::symlink_metadata(path).await {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                Some(SyncStatus::CloudOnly)
            } else if meta.is_file() {
                Some(SyncStatus::Synced)
            } else {
                // Directorio u otro
                // Para directorios, consultamos DB (más abajo)
                None 
            }
        },
        Err(_) => {
            // No existe físicamente, probable CloudOnly puro (virtual)
            Some(SyncStatus::CloudOnly)
        }
    };

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
            } else {
                // Si no está sucio, retornamos el estado físico detectado
                // Si physical_state es None (e.g. directorio raro), fallback a lógica cache
                Ok(physical_state.unwrap_or(if has_complete_cache { SyncStatus::Synced } else { SyncStatus::CloudOnly }))
            }
        }
        None => {
            // Sin registro en sync_state, retornamos estado físico
             Ok(physical_state.unwrap_or(if has_complete_cache { SyncStatus::Synced } else { SyncStatus::CloudOnly }))
        }
    }
}

/// Obtiene la disponibilidad de un archivo en Local Sync o Mirror
async fn get_file_availability(
    db: &MetadataRepository,
    mirror_path: &std::path::Path,
    file_path: &str,
) -> FileAvailability {
    // Decodificar URI file://
    let path_str = decode_file_uri(file_path);
    let path = std::path::Path::new(&path_str);
    
    // 1. Verificar si es parte del Mirror Principal
    if path.starts_with(mirror_path) {
        // Es un archivo del mirror, consultamos su estado en sync_state
        if let Ok(relative) = path.strip_prefix(mirror_path) {
             let relative_str = relative.to_string_lossy();
             // Resolver inode
             if let Ok(Some((inode, _))) = resolve_path_to_inode_and_gdrive_id(db, &relative_str).await {
                 // Consultar disponibilidad en DB
                 if let Ok(avail_str) = db.get_availability(inode).await {
                     return match avail_str.as_str() {
                         "local_online" => FileAvailability::LocalOnline,
                         "online_only" => FileAvailability::OnlineOnly,
                         _ => FileAvailability::OnlineOnly, // Default safe
                     };
                 }
             }
        }
        // Si falla la resolución, asumimos OnlineOnly si está en mirror path (safe default)
        return FileAvailability::OnlineOnly;
    }

    // 2. Buscar en local_sync_files (Carpetas externas)
    match db.find_local_sync_file_by_absolute_path(&path_str).await {
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
    mirror_tx: &Option<mpsc::Sender<MirrorCommand>>,
    file_path: &str,
    availability: &str,
) -> Result<()> {
    let path = decode_file_uri(file_path);
    
    // Obtener el canal de comandos
    let tx = mirror_tx.as_ref()
        .ok_or_else(|| anyhow::anyhow!("MirrorManager no disponible"))?;
    
    // Enviar comando al MirrorManager
    let cmd = match availability {
        "online_only" => MirrorCommand::SetOnlineOnly {
            path: path.clone()
        },
        "local_online" => MirrorCommand::SetLocalOnline {
            path: path.clone()
        },
        _ => return Err(anyhow::anyhow!("Availability desconocida: {}", availability)),
    };
    
    tracing::info!("🔄 IPC enviando comando {:?} a MirrorManager", cmd);
    tx.send(cmd).await
        .map_err(|e| anyhow::anyhow!("Error enviando comando: {}", e))?;
    tracing::info!("✅ Comando enviado exitosamente al MirrorManager");
    
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
