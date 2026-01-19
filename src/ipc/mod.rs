//! Comunicación IPC para extensiones externas (Nautilus, etc.)
//!
//! Protocolo binario sobre Unix Domain Sockets para consultar estado de sincronización.

pub mod server;

use serde::{Deserialize, Serialize};

/// Request enviado por clientes externos (ej: extensión de Nautilus)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcRequest {
    /// Consultar estado de sincronización de un archivo
    GetFileStatus { path: String },
    /// Ping para verificar conexión
    Ping,
    /// Cambiar archivo a modo "Just Online" (liberar espacio local)
    SetOnlineOnly { path: String },
    /// Cambiar archivo a modo "Local & Online" (descargar y mantener local)
    SetLocalOnline { path: String },
    /// Obtener disponibilidad actual de un archivo
    GetFileAvailability { path: String },
}

/// Respuesta del servidor IPC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcResponse {
    /// Estado de sincronización del archivo solicitado
    FileStatus(SyncStatus),
    /// Estado extendido del archivo (incluye shared)
    ExtendedStatus(FileStatusData),
    /// Respuesta a Ping
    Pong,
    /// Disponibilidad del archivo
    Availability(FileAvailability),
    /// Operación exitosa
    Success,
    /// Error en la operación
    Error { message: String },
}

/// Datos completos de estado del archivo para el InfoProvider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatusData {
    pub status: SyncStatus,
    pub availability: FileAvailability,
    pub is_shared: bool,
}

/// Disponibilidad de un archivo en Local Sync
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileAvailability {
    /// Archivo real en disco, sincronizado con Drive
    LocalOnline,
    /// Symlink a FUSE, solo disponible online
    OnlineOnly,
    /// No es un archivo de Local Sync
    NotTracked,
}

/// Estado de sincronización de un archivo
/// - Synced: Local + Drive (verde)
/// - CloudOnly: Solo en Drive, no descargado (azul)
/// - LocalOnly: Solo local, pendiente de subir (naranja)
/// - Error: Error de sincronización (rojo)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    /// Sincronizado: existe en local y en Drive
    Synced,
    /// Solo en Drive: no descargado localmente
    CloudOnly,
    /// Solo local: pendiente de subir a Drive
    LocalOnly,
    /// Error de sincronización
    Error,
    /// No es un archivo de G-DriveXP
    Unknown,
}

/// Ruta del socket IPC (usando XDG_RUNTIME_DIR)
pub fn get_socket_path() -> std::path::PathBuf {
    let uid = unsafe { libc::getuid() };
    std::path::PathBuf::from(format!("/run/user/{}/gdrivexp.sock", uid))
}
