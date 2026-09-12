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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use std::io::Write;

    #[rstest]
    #[case::empty(b"", "d41d8cd98f00b204e9800998ecf8427e")]
    #[case::hello(b"hello", "5d41402abc4b2a76b9719d911017c592")]
    #[case::hello_world(b"Hello, World!\n", "bea8252ff4e80f41719ea13cdf007273")]
    #[tokio::test]
    async fn test_compute_file_md5(#[case] content: &[u8], #[case] expected: &str) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.as_file().write_all(content).unwrap();

        let hash = compute_file_md5(tmp.path()).await.unwrap();
        assert_eq!(hash, expected);
    }

    #[rstest]
    #[tokio::test]
    async fn test_compute_file_md5_large_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        // Escribir > 8KB para forzar múltiples lecturas del buffer
        let data = vec![0xABu8; 16384];
        tmp.as_file().write_all(&data).unwrap();

        let hash = compute_file_md5(tmp.path()).await.unwrap();
        assert_eq!(hash.len(), 32, "MD5 hash should be 32 hex chars");
    }

    #[rstest]
    #[tokio::test]
    async fn test_compute_file_md5_nonexistent() {
        let result = compute_file_md5("/nonexistent/path/file.dat").await;
        assert!(result.is_err());
    }
}
