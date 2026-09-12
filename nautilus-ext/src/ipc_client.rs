//! Cliente IPC para comunicación con el daemon de G-DriveXP

use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use std::cell::RefCell;

/// Cliente IPC que se comunica con el daemon vía Unix Socket
pub struct IpcClient {
    socket_path: std::path::PathBuf,
    stream: RefCell<Option<UnixStream>>,
}

impl IpcClient {
    /// Crea un nuevo cliente IPC
    pub fn new() -> Self {
        let uid = unsafe { libc::getuid() };
        let socket_path = std::path::PathBuf::from(format!("/run/user/{}/gdrivexp.sock", uid));
        
        crate::log_debug(&format!("IpcClient initialized. UID: {}, Socket Path: {:?}", uid, socket_path));
        
        Self { 
            socket_path,
            stream: RefCell::new(None),
        }
    }
    
    /// Consulta el estado de sincronización y compartido de un archivo
    pub async fn get_extended_status(&self, path: &str) -> io::Result<crate::FileStatusData> {
        let request = IpcRequest::GetFileStatus {
            path: path.to_string(),
        };
        
        match self.send_request(request).await? {
            IpcResponse::ExtendedStatus(data) => Ok(data),
            _ => Ok(crate::FileStatusData {
                status: crate::SyncStatus::Unknown,
                availability: crate::FileAvailability::NotTracked,
                is_shared: false,
            }),
        }
    }


    
    /// Cambia archivo a online_only
    pub async fn set_online_only(&self, path: &str) -> io::Result<bool> {
        let request = IpcRequest::SetOnlineOnly {
            path: path.to_string(),
        };
        
        match self.send_request(request).await? {
            IpcResponse::Success => Ok(true),  // CAMBIADO de Ok a Success
            _ => Ok(false),
        }
    }
    
    /// Cambia archivo a local_online
    pub async fn set_local_online(&self, path: &str) -> io::Result<bool> {
        let request = IpcRequest::SetLocalOnline {
            path: path.to_string(),
        };
        
        match self.send_request(request).await? {
            IpcResponse::Success => Ok(true),  // CAMBIADO de Ok a Success
            _ => Ok(false),
        }
    }

    
    /// Helper genérico para enviar requests con reconexión automática
    async fn send_request(&self, request: IpcRequest) -> io::Result<IpcResponse> {
        // Serializar request
        let request_bytes = bincode::serialize(&request)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        
        // Intentar usar la conexión existente o reconectar
        let mut attempts = 0;
        loop {
            attempts += 1;
            
            // Garantizar que tenemos una conexión
            if self.stream.borrow().is_none() {
                crate::log_debug(&format!("Connecting attempt {}", attempts));
                match UnixStream::connect(&self.socket_path).await {
                    Ok(s) => {
                        crate::log_debug("Connected successfully");
                        *self.stream.borrow_mut() = Some(s);
                    },
                    Err(e) => {
                        crate::log_debug(&format!("Connection failed: {}", e));
                        // Fallo de transporte (socket ausente, daemon caído):
                        // se propaga como Err para NO colapsar con "no rastreado".
                        // Se conserva el kind original para clasificarlo arriba.
                        return Err(io::Error::new(e.kind(), format!("daemon no disponible: {}", e)));
                    }
                }
            }

            // Realizar I/O con borrow mutable del stream
            // Necesitamos extraer temporalmente el stream o usar un alcance limitado
            let mut stream_opt = self.stream.borrow_mut();
            if let Some(stream) = stream_opt.as_mut() {
                 match Self::perform_io(stream, &request_bytes).await {
                     Ok(response) => return Ok(response),
                     Err(_e) => {
                         crate::log_debug(&format!("IO Error: {}", _e));
                         // Si falló el I/O, el stream probablemente está roto
                         // Lo eliminamos y reintentamos si no hemos excedido intentos
                         // eprintln!("IPC Error (intento {}): {}", attempts, e);
                     }
                 }
            }
            
            // Si llegamos aquí, invalidamos el stream
            *stream_opt = None;
            
            if attempts >= 2 {
                // Reintentos agotados (daemon murió a mitad de la conversación):
                // Err, no Ok(Error), por la misma razón de arriba.
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Error de comunicación IPC tras reintentos",
                ));
            }
        }
    }

    async fn perform_io(stream: &mut UnixStream, request_bytes: &[u8]) -> io::Result<IpcResponse> {
        // Enviar longitud + request
        let len = (request_bytes.len() as u32).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(request_bytes).await?;
        
        // Leer longitud de respuesta
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let response_len = u32::from_be_bytes(len_buf) as usize;
        
        if response_len > 4096 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Respuesta IPC demasiado grande",
            ));
        }
        
        // Leer respuesta
        let mut response_buf = vec![0u8; response_len];
        stream.read_exact(&mut response_buf).await?;
        
        // Deserializar respuesta
        bincode::deserialize(&response_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

/// Request IPC (debe coincidir EXACTAMENTE con src/ipc/mod.rs del daemon)
#[derive(Debug, Serialize, Deserialize)]
enum IpcRequest {
    GetFileStatus { path: String },
    Ping,
    SetOnlineOnly { path: String },
    SetLocalOnline { path: String },
    GetFileAvailability { path: String },
}

/// Respuesta IPC (debe coincidir EXACTAMENTE con src/ipc/mod.rs del daemon)
#[derive(Debug, Serialize, Deserialize)]
enum IpcResponse {
    FileStatus(crate::SyncStatus),
    ExtendedStatus(crate::FileStatusData),
    Pong,
    Availability(crate::FileAvailability),
    Success,  // ¡CAMBIADO de Ok a Success!
    Error { message: String },
}

