use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
enum IpcRequest {
    GetFileStatus { path: String },
    Ping,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
enum IpcResponse {
    FileStatus(SyncStatus),
    Pong,
    Error { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum SyncStatus {
    Synced,      // Verde: en local y en drive
    CloudOnly,   // Azul: solo en drive
    LocalOnly,   // Naranja: solo local (pending upload)
    Error,       // Rojo: error de sincronización
    Unknown,     // Sin emblema
}

fn main() -> std::io::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(async {
        let args: Vec<String> = env::args().collect();
        if args.len() < 2 {
            eprintln!("Uso: {} <file_uri_or_path>", args[0]);
            eprintln!("Ejemplo: {} file:///home/alcss/GoogleDrive/archivo.txt", args[0]);
            std::process::exit(1);
        }

        let input_path = &args[1];
        println!("Consulta: {}", input_path);

        let uid = unsafe { libc::getuid() };
        let socket_path = PathBuf::from(format!("/run/user/{}/gdrivexp.sock", uid));
        
        println!("Conectando a socket: {:?}", socket_path);
        let mut stream = UnixStream::connect(&socket_path).await?;
        println!("Conectado.");

        // Construir request
        let request = IpcRequest::GetFileStatus {
            path: input_path.to_string(),
        };
        
        let request_bytes = bincode::serialize(&request)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        // Enviar longitud + request
        let len = (request_bytes.len() as u32).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(&request_bytes).await?;
        
        // Leer longitud
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let response_len = u32::from_be_bytes(len_buf) as usize;
        
        // Leer respuesta
        let mut response_buf = vec![0u8; response_len];
        stream.read_exact(&mut response_buf).await?;
        
        let response: IpcResponse = bincode::deserialize(&response_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        println!("Respuesta recibida: {:?}", response);

        Ok(())
    })
}
