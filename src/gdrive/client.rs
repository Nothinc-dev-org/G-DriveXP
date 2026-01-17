use anyhow::{Context, Result};
use google_drive3::DriveHub;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use yup_oauth2::authenticator::Authenticator;

/// Cliente Wrapper para Google Drive API
pub struct DriveClient {
    hub: DriveHub<HttpsConnector<HttpConnector>>,
}

impl DriveClient {
    /// Inicializa el cliente de Google Drive
    pub fn new(auth: Authenticator<yup_oauth2::hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>) -> Self {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("no se pudieron cargar los certificados nativos")
            .https_or_http()
            .enable_http1()
            .build();

        let client = hyper::Client::builder().build(https);

        let hub = DriveHub::new(client, auth);

        Self { hub }
    }

    /// Descarga un chunk específico de un archivo usando Range Header
    pub async fn download_chunk(&self, file_id: &str, offset: u64, size: u32) -> Result<Vec<u8>> {
        let end = offset + size as u64 - 1;
        let range_header = format!("bytes={}-{}", offset, end);

        tracing::debug!("Descargando chunk: file_id={}, range={}", file_id, range_header);

        // 1. Obtener token válido
        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive.readonly"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido para la descarga")?;

        // 2. Construir URL de descarga
        let url = format!("https://www.googleapis.com/drive/v3/files/{}?alt=media", file_id);

        // 3. Realizar petición con reqwest
        let client = reqwest::Client::new();
        
        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("Range", range_header)
            .send()
            .await
            .context("Error de red al descargar chunk")?;

        // 4. Verificar estado
        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            tracing::error!("Error API Drive: {} - {}", status, error_text);
            anyhow::bail!("Error API Drive: {} - {}", status, error_text);
        }

        // 5. Devolver bytes
        let bytes = response.bytes().await.context("Error al leer cuerpo de respuesta")?;
        Ok(bytes.to_vec())
    }
}
