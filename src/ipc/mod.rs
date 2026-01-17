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
}

/// Respuesta del servidor IPC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcResponse {
    /// Estado de sincronización del archivo solicitado
    FileStatus(SyncStatus),
    /// Respuesta a Ping
    Pong,
    /// Error en la operación
    Error { message: String },
}

/// Estado de sincronización de un archivo
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    /// Sincronizado con Google Drive (dirty=0)
    Synced,
    /// Subida en progreso
    Syncing,
    /// Pendiente de subida (dirty=1)
    Pending,
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
