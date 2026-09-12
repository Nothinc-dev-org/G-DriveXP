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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    // --- Bincode roundtrip: contrato con nautilus-ext ---

    #[rstest]
    #[case::ping(IpcRequest::Ping)]
    #[case::get_status(IpcRequest::GetFileStatus { path: "/home/user/GoogleDrive/doc.txt".into() })]
    #[case::set_online(IpcRequest::SetOnlineOnly { path: "file:///home/user/GoogleDrive/foto.jpg".into() })]
    #[case::set_local(IpcRequest::SetLocalOnline { path: "/home/user/GoogleDrive/video.mp4".into() })]
    #[case::get_avail(IpcRequest::GetFileAvailability { path: "/home/user/GoogleDrive/notes.md".into() })]
    fn test_request_bincode_roundtrip(#[case] request: IpcRequest) {
        let bytes = bincode::serialize(&request).unwrap();
        let decoded: IpcRequest = bincode::deserialize(&bytes).unwrap();
        // Verificar variante (Debug string match, ya que no tiene PartialEq)
        assert_eq!(format!("{:?}", request), format!("{:?}", decoded));
    }

    #[rstest]
    #[case::pong(IpcResponse::Pong)]
    #[case::success(IpcResponse::Success)]
    #[case::error(IpcResponse::Error { message: "timeout".into() })]
    #[case::file_status(IpcResponse::FileStatus(SyncStatus::Synced))]
    #[case::availability(IpcResponse::Availability(FileAvailability::LocalOnline))]
    #[case::extended(IpcResponse::ExtendedStatus(FileStatusData {
        status: SyncStatus::CloudOnly,
        availability: FileAvailability::OnlineOnly,
        is_shared: true,
    }))]
    fn test_response_bincode_roundtrip(#[case] response: IpcResponse) {
        let bytes = bincode::serialize(&response).unwrap();
        let decoded: IpcResponse = bincode::deserialize(&bytes).unwrap();
        assert_eq!(format!("{:?}", response), format!("{:?}", decoded));
    }

    // --- Protocolo de framing (length-prefixed) ---

    #[rstest]
    fn test_length_prefix_framing() {
        let request = IpcRequest::GetFileStatus { path: "/test/path".into() };
        let payload = bincode::serialize(&request).unwrap();
        let len_bytes = (payload.len() as u32).to_be_bytes();

        // Simular frame completo
        let mut frame = Vec::new();
        frame.extend_from_slice(&len_bytes);
        frame.extend_from_slice(&payload);

        // Decodificar
        let decoded_len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(decoded_len, payload.len());

        let decoded: IpcRequest = bincode::deserialize(&frame[4..4 + decoded_len]).unwrap();
        assert_eq!(format!("{:?}", request), format!("{:?}", decoded));
    }

    // --- Enums: cobertura de variantes ---

    #[rstest]
    #[case::synced(SyncStatus::Synced)]
    #[case::cloud(SyncStatus::CloudOnly)]
    #[case::local(SyncStatus::LocalOnly)]
    #[case::error(SyncStatus::Error)]
    #[case::unknown(SyncStatus::Unknown)]
    fn test_sync_status_serialization(#[case] status: SyncStatus) {
        let bytes = bincode::serialize(&status).unwrap();
        let decoded: SyncStatus = bincode::deserialize(&bytes).unwrap();
        assert_eq!(status, decoded);
    }

    #[rstest]
    #[case::local_online(FileAvailability::LocalOnline)]
    #[case::online_only(FileAvailability::OnlineOnly)]
    #[case::not_tracked(FileAvailability::NotTracked)]
    fn test_file_availability_serialization(#[case] avail: FileAvailability) {
        let bytes = bincode::serialize(&avail).unwrap();
        let decoded: FileAvailability = bincode::deserialize(&bytes).unwrap();
        assert_eq!(avail, decoded);
    }

    #[rstest]
    fn test_socket_path_format() {
        let path = get_socket_path();
        let path_str = path.to_string_lossy();
        assert!(path_str.starts_with("/run/user/"), "Socket path should start with /run/user/, got: {}", path_str);
        assert!(path_str.ends_with("gdrivexp.sock"), "Socket path should end with gdrivexp.sock, got: {}", path_str);
    }

    // --- Unicode/special chars en paths ---

    #[rstest]
    #[case::unicode("file:///home/user/GoogleDrive/documentos/espa\u{00f1}ol.txt")]
    #[case::spaces("file:///home/user/GoogleDrive/My%20Documents/file.txt")]
    #[case::deep("/home/user/GoogleDrive/a/b/c/d/e/f/g/file.txt")]
    fn test_request_with_special_paths(#[case] path: &str) {
        let request = IpcRequest::GetFileStatus { path: path.into() };
        let bytes = bincode::serialize(&request).unwrap();
        let decoded: IpcRequest = bincode::deserialize(&bytes).unwrap();
        assert_eq!(format!("{:?}", request), format!("{:?}", decoded));
    }
}
