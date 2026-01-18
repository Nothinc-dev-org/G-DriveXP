use anyhow::{Context, Result};
use std::path::Path;
use md5::{Md5, Digest};
use std::io::Read;

/// Calcula el hash MD5 de un archivo de manera asíncrona (usando spawn_blocking)
/// Compatible con el formato de hash de Google Drive
pub async fn compute_file_md5(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref().to_path_buf();
    
    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(&path)
            .with_context(|| format!("Error abriendo archivo para MD5: {:?}", path))?;
            
        let mut hasher = Md5::new();
        let mut buffer = [0; 8192]; // 8KB buffer

        loop {
            let count = file.read(&mut buffer)
                .context("Error leyendo chunk para MD5")?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
            
        let result = hasher.finalize();
        Ok(format!("{:x}", result))
    })
    .await
    .context("Error en tarea de cálculo de MD5")?
}
