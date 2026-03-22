use anyhow::{Context, Result};
use google_drive3::DriveHub;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use std::io::{Read, Seek, SeekFrom};
use yup_oauth2::authenticator::Authenticator;

/// Tipo para callback de progreso de upload
pub type ProgressCallback = Box<dyn Fn(u64) + Send>;

/// Reader que envuelve otro Read y reporta progreso via callback
struct ProgressReader<R: Read + Seek> {
    inner: R,
    bytes_read: u64,
    callback: ProgressCallback,
}

impl<R: Read + Seek> ProgressReader<R> {
    fn new(inner: R, callback: ProgressCallback) -> Self {
        Self {
            inner,
            bytes_read: 0,
            callback,
        }
    }
}

impl<R: Read + Seek> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.bytes_read += n as u64;
            (self.callback)(self.bytes_read);
        }
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for ProgressReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let result = self.inner.seek(pos)?;
        self.bytes_read = result;
        Ok(result)
    }
}

/// Cliente Wrapper para Google Drive API
pub struct DriveClient {
    hub: DriveHub<HttpsConnector<HttpConnector>>,
    http: reqwest::Client,
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

        Self { hub, http: reqwest::Client::new() }
    }

    /// Obtiene el ID canónico de la carpeta 'root' (My Drive)
    pub async fn get_root_file_id(&self) -> Result<String> {
        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido")?;

        let client = &self.http;
        let url = "https://www.googleapis.com/drive/v3/files/root?fields=id";

        let response = client
            .get(url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .context("Error de red al obtener root id")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Error API Drive get_root_id: {} - {}", status, body);
            anyhow::bail!("Error API Drive get_root_id: {} - {}", status, body);
        }

        #[derive(serde::Deserialize)]
        struct FileId {
            id: String,
        }

        let file: FileId = response.json().await?;
        tracing::info!("Drive Root ID identificado como: {}", file.id);
        Ok(file.id)
    }

    /// Descarga un chunk específico de un archivo usando Range Header
    pub async fn download_chunk(&self, file_id: &str, offset: u64, size: u32) -> Result<Vec<u8>> {
        let end = offset + size as u64 - 1;
        let range_header = format!("bytes={}-{}", offset, end);

        tracing::debug!("Descargando chunk: file_id={}, range={}", file_id, range_header);

        // 1. Obtener token válido (usando el scope principal para evitar re-auth)
        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido para la descarga")?;

        // 2. Construir URL de descarga (Incluyendo acknowledgeAbuse=true para evitar 403 en falsos positivos de malware)
        let url = format!("https://www.googleapis.com/drive/v3/files/{}?alt=media&acknowledgeAbuse=true", file_id);

        // 3. Realizar petición con reqwest
        let client = &self.http;
        
        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("Range", range_header.clone())
            .send()
            .await
            .context("Error de red al descargar chunk")?;

        // 4. Verificar estado
        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            if status.as_u16() == 416 {
                // 416 es recuperable: el caller corregirá attrs.size y reintentará
                tracing::warn!("416 Range Not Satisfiable: file_id={} range={} (se corregirá automáticamente)", file_id, range_header);
            } else {
                tracing::error!("Error API Drive: {} - {}", status, error_text);
            }
            anyhow::bail!("Error API Drive: {} - {}", status, error_text);
        }

        // 5. Devolver bytes
        let bytes = response.bytes().await.context("Error al leer cuerpo de respuesta")?;
        Ok(bytes.to_vec())
    }

    /// Lista solo los hijos inmediatos del root de Drive.
    /// Usado para el primer nivel del bootstrap BFS (respuesta rápida ~1s).
    pub async fn list_root_children(&self, root_id: &str) -> Result<Vec<google_drive3::api::File>> {
        let mut all_files = Vec::new();
        let mut page_token: Option<String> = None;

        tracing::info!("Consultando hijos directos del root en Google Drive...");

        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido")?;

        let client = &self.http;
        let query = format!("'{}' in parents and trashed = false", root_id);

        loop {
            let mut url = format!(
                "https://www.googleapis.com/drive/v3/files?pageSize=1000&q={}&fields=nextPageToken,files(id,name,parents,mimeType,size,modifiedTime,md5Checksum,version,shared,ownedByMe,capabilities(canMoveItemWithinDrive))",
                urlencoding::encode(&query)
            );

            if let Some(ref token_str) = page_token {
                url.push_str(&format!("&pageToken={}", token_str));
            }

            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await
                .context("Error de red al listar hijos del root")?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                tracing::error!("Error API Drive (list_root_children): {} - {}", status, body);
                anyhow::bail!("Error API Drive: {} - {}", status, body);
            }

            let file_list: google_drive3::api::FileList = response.json()
                .await
                .context("Error al parsear respuesta JSON de Drive")?;

            if let Some(files) = file_list.files {
                tracing::debug!("Recibidos {} hijos del root en esta página", files.len());
                all_files.extend(files);
            }

            page_token = file_list.next_page_token;
            if page_token.is_none() {
                break;
            }
        }

        tracing::info!("📊 Bootstrap nivel 1: {} items en root", all_files.len());
        Ok(all_files)
    }

    /// Lista todos los archivos de Google Drive con los campos necesarios para el bootstrapping
    /// NOTA: Usamos reqwest directamente para evitar que google-drive3 añada scopes automáticos
    pub async fn list_all_files(&self) -> Result<Vec<google_drive3::api::File>> {
        let mut all_files = Vec::new();
        let mut page_token: Option<String> = None;

        tracing::info!("Consultando lista de archivos en Google Drive...");

        // Obtener token usando el scope principal
        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido")?;

        let client = &self.http;

        loop {
            let mut url = format!(
                "https://www.googleapis.com/drive/v3/files?pageSize=1000&q={}&fields=nextPageToken,files(id,name,parents,mimeType,size,modifiedTime,md5Checksum,version,shared,ownedByMe,capabilities(canMoveItemWithinDrive))",
                urlencoding::encode("trashed = false")
            );
            
            if let Some(ref token_str) = page_token {
                url.push_str(&format!("&pageToken={}", token_str));
            }

            let response = client
                .get(&url)
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await
                .context("Error de red al listar archivos")?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                tracing::error!("Error API Drive: {} - {}", status, body);
                anyhow::bail!("Error API Drive: {} - {}", status, body);
            }

            // Parsear respuesta como FileList
            let file_list: google_drive3::api::FileList = response.json()
                .await
                .context("Error al parsear respuesta JSON de Drive")?;

            if let Some(files) = file_list.files {
                tracing::debug!("Recibidos {} archivos en esta página", files.len());
                all_files.extend(files);
            }

            page_token = file_list.next_page_token;
            if page_token.is_none() {
                break;
            }
        }

        tracing::info!("📊 Sincronización: Se recuperaron {} archivos en total", all_files.len());
        Ok(all_files)
    }

    /// Obtiene una página de archivos de Drive. Retorna (archivos, next_page_token).
    /// Si next_page_token es None, no hay más páginas.
    pub async fn fetch_files_page(&self, page_token: Option<&str>) -> Result<(Vec<google_drive3::api::File>, Option<String>)> {
        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido")?;

        let mut url = format!(
            "https://www.googleapis.com/drive/v3/files?pageSize=1000&q={}&fields=nextPageToken,files(id,name,parents,mimeType,size,modifiedTime,md5Checksum,version,shared,ownedByMe,capabilities(canMoveItemWithinDrive))",
            urlencoding::encode("trashed = false")
        );

        if let Some(pt) = page_token {
            url.push_str(&format!("&pageToken={}", pt));
        }

        let response = self.http
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .context("Error de red al obtener página de archivos")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Error API Drive fetch_files_page: {} - {}", status, body);
        }

        let file_list: google_drive3::api::FileList = response.json()
            .await
            .context("Error al parsear respuesta JSON de Drive")?;

        let files = file_list.files.unwrap_or_default();
        Ok((files, file_list.next_page_token))
    }

    // ============================================================
    // Métodos para Changes API (sincronización incremental)
    // ============================================================

    /// Obtiene el token inicial para comenzar a escuchar cambios
    pub async fn get_start_page_token(&self) -> Result<String> {
        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido")?;

        let client = &self.http;
        let url = "https://www.googleapis.com/drive/v3/changes/startPageToken";

        let response = client
            .get(url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .context("Error de red al obtener startPageToken")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Error API Drive: {} - {}", status, body);
            anyhow::bail!("Error API Drive: {} - {}", status, body);
        }

        #[derive(serde::Deserialize)]
        struct StartPageTokenResponse {
            #[serde(rename = "startPageToken")]
            start_page_token: String,
        }

        let parsed: StartPageTokenResponse = response.json()
            .await
            .context("Error al parsear startPageToken")?;

        tracing::debug!("Obtenido startPageToken: {}", parsed.start_page_token);
        Ok(parsed.start_page_token)
    }

    /// Lista cambios desde un page_token dado
    /// Retorna: (cambios, nuevo_start_page_token si es la última página, has_more)
    pub async fn list_changes(&self, page_token: &str) -> Result<(Vec<google_drive3::api::Change>, Option<String>, bool)> {
        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido")?;

        let client = &self.http;
        
        // pageToken es requerido, fields especifica qué queremos recibir
        let url = format!(
            "https://www.googleapis.com/drive/v3/changes?pageSize=1000&pageToken={}&fields=nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,parents,mimeType,size,modifiedTime,md5Checksum,trashed,shared,ownedByMe,capabilities(canMoveItemWithinDrive)))",
            page_token
        );

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .context("Error de red al listar cambios")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Error API Drive changes: {} - {}", status, body);
            anyhow::bail!("Error API Drive changes: {} - {}", status, body);
        }

        let change_list: google_drive3::api::ChangeList = response.json()
            .await
            .context("Error al parsear respuesta de changes")?;

        let changes = change_list.changes.unwrap_or_default();
        let has_more = change_list.next_page_token.is_some();
        let next_token = change_list.next_page_token.clone().or(change_list.new_start_page_token.clone());

        tracing::debug!(
            "Changes: {} cambios, next_page={:?}, new_start={:?}",
            changes.len(),
            change_list.next_page_token,
            change_list.new_start_page_token
        );

        // Retornamos el siguiente token a usar (ya sea next_page_token para seguir iterando
        // o new_start_page_token si llegamos al final de los cambios actuales)
        Ok((changes, next_token, has_more))
    }

    /// Obtiene el MD5 checksum de un archivo remoto (para detectar conflictos)
    pub async fn get_file_md5(&self, file_id: &str) -> Result<Option<String>> {
        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido")?;

        let client = &self.http;
        let url = format!(
            "https://www.googleapis.com/drive/v3/files/{}?fields=md5Checksum",
            file_id
        );

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .context("Error de red al obtener md5Checksum")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Error API Drive get_file_md5: {} - {}", status, body);
            anyhow::bail!("Error API Drive get_file_md5: {} - {}", status, body);
        }

        let file: google_drive3::api::File = response.json()
            .await
            .context("Error al parsear respuesta de get_file_md5")?;

        Ok(file.md5_checksum)
    }

    // ============================================================
    // Métodos para Upload (escritura)
    // ============================================================

    /// Sube un nuevo archivo a Google Drive
    /// Retorna el gdrive_id del archivo creado
    pub async fn upload_file(
        &self,
        file_path: &std::path::Path,
        name: &str,
        mime_type: Option<&str>,
        parent_id: &str,
        progress_cb: Option<ProgressCallback>,
    ) -> Result<String> {
        tracing::info!("📤 Subiendo archivo: {}", name);

        // Leer contenido del archivo
        let content = tokio::fs::read(file_path).await
            .context("Error leyendo archivo local")?;

        // Construir metadata
        let mut file_metadata = google_drive3::api::File::default();
        file_metadata.name = Some(name.to_string());
        file_metadata.mime_type = Some(mime_type.unwrap_or("application/octet-stream").to_string());

        if parent_id != "root" {
            file_metadata.parents = Some(vec![parent_id.to_string()]);
        }

        let mime = mime_type.unwrap_or("application/octet-stream").parse().unwrap();
        let content_len = content.len();

        // Estrategia adaptativa:
        // - Archivos pequeños (< 5MB) o vacíos: Upload simple (evita panic en resumable con 0 bytes)
        // - Archivos grandes: Resumable upload
        let result = if content_len < 5 * 1024 * 1024 {
            tracing::debug!("Usando upload simple para archivo de {} bytes", content_len);
            match progress_cb {
                Some(cb) => {
                    let reader = ProgressReader::new(std::io::Cursor::new(content), cb);
                    self.hub.files().create(file_metadata)
                        .upload(reader, mime).await
                        .context("Error en upload simple")?
                }
                None => {
                    self.hub.files().create(file_metadata)
                        .upload(std::io::Cursor::new(content), mime).await
                        .context("Error en upload simple")?
                }
            }
        } else {
            tracing::debug!("Usando upload resumable para archivo de {} bytes", content_len);
            match progress_cb {
                Some(cb) => {
                    let reader = ProgressReader::new(std::io::Cursor::new(content), cb);
                    self.hub.files().create(file_metadata)
                        .upload_resumable(reader, mime).await
                        .context("Error en upload resumable")?
                }
                None => {
                    self.hub.files().create(file_metadata)
                        .upload_resumable(std::io::Cursor::new(content), mime).await
                        .context("Error en upload resumable")?
                }
            }
        };

        let file_id = result.1.id.ok_or_else(|| anyhow::anyhow!("No se recibió file_id en respuesta"))?;

        tracing::info!("✅ Archivo subido: {}", file_id);
        Ok(file_id)
    }

    /// Crea una nueva carpeta en Google Drive
    pub async fn create_folder(
        &self,
        name: &str,
        parent_id: &str,
    ) -> Result<String> {
        tracing::info!("📂 Creando carpeta: {}", name);

        let mut file_metadata = google_drive3::api::File::default();
        file_metadata.name = Some(name.to_string());
        file_metadata.mime_type = Some("application/vnd.google-apps.folder".to_string());
        
        if parent_id != "root" {
            file_metadata.parents = Some(vec![parent_id.to_string()]);
        }

        let result = self.hub
            .files()
            .create(file_metadata)
            .supports_all_drives(true)
            .ignore_default_visibility(true)
            .upload(
                std::io::Cursor::new(vec![]),
                "application/vnd.google-apps.folder".parse().unwrap(),
            )
            .await
            .context("Error creando carpeta en API")?;

        let file_id = result.1.id.ok_or_else(|| anyhow::anyhow!("No se recibió file_id para carpeta"))?;
        
        tracing::info!("✅ Carpeta creada: {}", file_id);
        Ok(file_id)
    }

    /// Actualiza el contenido de un archivo existente
    pub async fn update_file_content(
        &self,
        file_id: &str,
        file_path: &std::path::Path,
        progress_cb: Option<ProgressCallback>,
    ) -> Result<()> {
        tracing::info!("📝 Actualizando contenido de archivo: {}", file_id);

        // Leer contenido del archivo
        let content = tokio::fs::read(file_path).await
            .context("Error leyendo archivo local")?;

        // Metadata vacío (no cambiamos nombre ni padres, solo contenido)
        let file_metadata = google_drive3::api::File::default();
        let mime = "application/octet-stream".parse().unwrap();
        let content_len = content.len();

        // Estrategia adaptativa para updates
        if content_len < 5 * 1024 * 1024 {
            tracing::debug!("Usando update simple para archivo de {} bytes", content_len);
            match progress_cb {
                Some(cb) => {
                    let reader = ProgressReader::new(std::io::Cursor::new(content), cb);
                    self.hub.files().update(file_metadata, file_id)
                        .upload(reader, mime).await
                        .context("Error en update simple")?;
                }
                None => {
                    self.hub.files().update(file_metadata, file_id)
                        .upload(std::io::Cursor::new(content), mime).await
                        .context("Error en update simple")?;
                }
            }
        } else {
            tracing::debug!("Usando update resumable para archivo de {} bytes", content_len);
            match progress_cb {
                Some(cb) => {
                    let reader = ProgressReader::new(std::io::Cursor::new(content), cb);
                    self.hub.files().update(file_metadata, file_id)
                        .upload_resumable(reader, mime).await
                        .context("Error en update resumable")?;
                }
                None => {
                    self.hub.files().update(file_metadata, file_id)
                        .upload_resumable(std::io::Cursor::new(content), mime).await
                        .context("Error en update resumable")?;
                }
            }
        }

        tracing::info!("✅ Archivo actualizado: {}", file_id);
        Ok(())
    }

    /// Mueve un archivo a la papelera
    pub async fn trash_file(&self, file_id: &str) -> Result<(), super::DriveError> {
        tracing::info!("🗑️ Moviendo a papelera: {}", file_id);

        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| super::DriveError::Auth(format!("{}", e)))?
            .ok_or_else(|| super::DriveError::Auth("No token available".into()))?;

        let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);
        let client = &self.http;

        let response = client
            .patch(&url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&serde_json::json!({ "trashed": true }))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Error API Drive trash: {} - {}", status, body);
            
            // Detectar error 403 de permisos insuficientes
            if status == 403 && body.contains("insufficientFilePermissions") {
                return Err(super::DriveError::InsufficientPermissions(
                    format!("No se puede eliminar archivo compartido: {}", file_id)
                ));
            }
            
            // Detectar error 404 de archivo no encontrado
            if status == 404 {
                return Err(super::DriveError::NotFound(
                    format!("Archivo no existe en Drive: {}", file_id)
                ));
            }
            
            return Err(super::DriveError::ApiError(format!("{} - {}", status, body)));
        }

        tracing::info!("✅ Archivo movido a papelera: {}", file_id);
        Ok(())
    }
    /// Obtiene metadatos completos de un archivo (para detectar cambios de nombre/padre y contenido)
    pub async fn get_file_metadata(&self, file_id: &str) -> Result<google_drive3::api::File> {
        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido")?;

        let client = &self.http;
        // Solicitamos name, parents, md5Checksum, size y capabilities para verificar permisos
        let url = format!(
            "https://www.googleapis.com/drive/v3/files/{}?fields=id,name,parents,md5Checksum,mimeType,size,shared,ownedByMe,capabilities&supportsAllDrives=true",
            file_id
        );

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .context("Error de red al obtener metadata")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            tracing::error!("Error API Drive get_file_metadata: {} - {}", status, body);
            anyhow::bail!("Error API Drive get_file_metadata: {} - {}", status, body);
        }

        let body = response.text().await.context("Error leyendo body")?;
        tracing::debug!("🔍 RAW METADATA ({}): {}", file_id, body);

        let file: google_drive3::api::File = serde_json::from_str(&body)
            .context("Error al parsear respuesta de get_file_metadata")?;

        Ok(file)
    }

    /// Actualiza solo los metadatos de un archivo (nombre, padres, modifiedTime)
    pub async fn update_file_metadata(
        &self,
        file_id: &str,
        new_name: Option<&str>,
        add_parent: Option<&str>,
        remove_parent: Option<&str>,
        new_mtime: Option<google_drive3::chrono::DateTime<google_drive3::chrono::Utc>>,
    ) -> Result<()> {
        tracing::info!("📝 Actualizando metadatos de archivo: {} (name={:?}, mtime={:?})", 
                       file_id, new_name, new_mtime);

        let token = self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"])
            .await
            .map_err(|e| anyhow::anyhow!("Error de autenticación: {}", e))?
            .context("No se obtuvo ningún token válido")?;

        let mut url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);
        
        // Query params
        let mut params = Vec::new();
        // IMPORTANTE: supportsAllDrives=true asegura que veamos/editemos la jerarquía completa
        params.push("supportsAllDrives=true".to_string());

        if let Some(parent) = add_parent {
            params.push(format!("addParents={}", parent));
        }
        if let Some(parent) = remove_parent {
            params.push(format!("removeParents={}", parent));
        }
        
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        // Body with explicit fields to update
        let mut json_map = serde_json::Map::new();
        if let Some(name) = new_name {
            json_map.insert("name".to_string(), serde_json::Value::String(name.to_string()));
        }
        if let Some(mtime) = new_mtime {
            // Google Drive espera RFC3339
            use google_drive3::chrono::SecondsFormat;
            json_map.insert("modifiedTime".to_string(), serde_json::Value::String(mtime.to_rfc3339_opts(SecondsFormat::Secs, true)));
        }

        let client = &self.http;
        let response = client
            .patch(&url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&json_map)
            .send()
            .await
            .context("Error de red al actualizar metadatos")?;

        if !response.status().is_success() {
             let status = response.status();
             let body = response.text().await.unwrap_or_default();
             tracing::error!("Error API Drive update_file_metadata: {} - {}", status, body);
             anyhow::bail!("Error API Drive update_file_metadata: {} - {}", status, body);
        }

        tracing::info!("✅ Metadatos actualizados para: {}", file_id);
        Ok(())
    }
}

