use anyhow::{Context, Result};
use google_drive3::DriveHub;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use std::io::{Read, Seek, SeekFrom};
use std::time::Duration;
use yup_oauth2::authenticator::Authenticator;

/// Timeout para operaciones del plano de control (listados, metadatos,
/// carpetas, papelera, md5, tokens). Una red sana responde en segundos;
/// más de esto = red colgada, y colgarse para siempre es peor que fallar.
pub const CONTROL_TIMEOUT: Duration = Duration::from_secs(60);
/// Timeout por chunk de descarga (10 MB) + margen para refresco de token.
pub const CHUNK_TIMEOUT: Duration = Duration::from_secs(180);
/// Timeout para subidas simples (< 5 MB).
pub const SIMPLE_UPLOAD_TIMEOUT: Duration = Duration::from_secs(300);

/// Tipo para callback de progreso de upload
pub type ProgressCallback = Box<dyn Fn(u64) + Send>;

/// Clasifica un error del hub google-drive3 en DriveError.
/// `Failure` expone el status HTTP: 404 y 403-con-permiso-insuficiente se
/// preservan como variantes para que el uploader los distinga de lo
/// transitorio (quota/rate-limit quedan como ApiError: se reintentan).
async fn classify_hub_error(err: google_drive3::Error) -> super::DriveError {
    use google_drive3::Error as HubError;
    match err {
        HubError::Failure(resp) => {
            let status = resp.status().as_u16();
            let body = hyper::body::to_bytes(resp.into_body())
                .await
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default();
            super::DriveError::from_http_status(status, &body)
        }
        HubError::MissingToken(e) => {
            super::DriveError::Auth(format!("Token retrieval failed: {}", e))
        }
        other => super::DriveError::ApiError(other.to_string()),
    }
}

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
    /// Envuelve un futuro de red con timeout.
    ///
    /// Sin esto, una red de agujero negro (acepta TCP pero nunca responde)
    /// cuelga el futuro para siempre: el syncer se atasca a mitad de ciclo,
    /// el repair/BFS se vuelven zombies y ningún supervisor puede detectarlo
    /// (la tarea sigue "viva"). Con timeout, el hang se convierte en error
    /// recuperable (reintento, backoff, cuarentena) que los callers ya manejan.
    pub async fn timed<T, E>(
        what: &'static str,
        limit: Duration,
        fut: impl std::future::Future<Output = std::result::Result<T, E>>,
    ) -> Result<T>
    where
        E: Into<anyhow::Error>,
    {
        match tokio::time::timeout(limit, fut).await {
            // Los fallos HTTP del hub se clasifican aquí (404 / 403-con-permiso
            // → DriveError preservado) para que los callers distingan lo
            // permanente de lo transitorio sin cambiar firmas.
            Ok(inner) => match inner.map_err(Into::into) {
                Ok(v) => Ok(v),
                Err(e) => match e.downcast::<google_drive3::Error>() {
                    Ok(hub) => Err(anyhow::Error::from(classify_hub_error(hub).await)),
                    Err(e) => Err(e),
                },
            },
            Err(_) => {
                tracing::error!("⏱️ Timeout de red (más de {:?}) en {}", limit, what);
                Err(anyhow::anyhow!("timeout de red tras {:?} en {}", limit, what))
            }
        }
    }

    /// Timeout para subidas según tamaño: base + piso de 50 KB/s, tope 1 h.
    /// Un fallo por timeout deja el archivo dirty y se reintenta en el
    /// siguiente ciclo (nunca pérdida silenciosa).
    pub fn upload_timeout(content_len: u64) -> Duration {
        if content_len < 5 * 1024 * 1024 {
            SIMPLE_UPLOAD_TIMEOUT
        } else {
            std::cmp::min(
                Duration::from_secs(600 + content_len / 50_000),
                Duration::from_secs(3600),
            )
        }
    }

    /// Inicializa el cliente de Google Drive. Retorna error (en vez de panic)
    /// si el almacén de certificados nativos es ilegible: el arranque falla
    /// limpio con mensaje en vez de abortar el proceso.
    pub fn new(auth: Authenticator<yup_oauth2::hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>) -> Result<Self> {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .context("no se pudieron cargar los certificados nativos")?
            .https_or_http()
            .enable_http1()
            .build();

        let client = hyper::Client::builder().build(https);

        let hub = DriveHub::new(client, auth);

        // Timeout total por request (conexión + cuerpo): segunda red de
        // seguridad para todo lo que va por reqwest (listados, chunks).
        // El refresco de token (yup/hyper) se cubre con DriveClient::timed
        // en cada call site.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("no se pudo construir el cliente HTTP")?;

        Ok(Self { hub, http })
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
                "https://www.googleapis.com/drive/v3/files?pageSize=1000&q={}&fields=nextPageToken,files(id,name,parents,mimeType,size,modifiedTime,md5Checksum,version,shared,ownedByMe,capabilities(canMoveItemWithinDrive),shortcutDetails(targetId,targetMimeType))",
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
                "https://www.googleapis.com/drive/v3/files?pageSize=1000&q={}&fields=nextPageToken,files(id,name,parents,mimeType,size,modifiedTime,md5Checksum,version,shared,ownedByMe,capabilities(canMoveItemWithinDrive),shortcutDetails(targetId,targetMimeType))",
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
            "https://www.googleapis.com/drive/v3/files?pageSize=1000&q={}&fields=nextPageToken,files(id,name,parents,mimeType,size,modifiedTime,md5Checksum,version,shared,ownedByMe,capabilities(canMoveItemWithinDrive),shortcutDetails(targetId,targetMimeType))",
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
            "https://www.googleapis.com/drive/v3/changes?pageSize=1000&pageToken={}&fields=nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,parents,mimeType,size,modifiedTime,md5Checksum,trashed,shared,ownedByMe,capabilities(canMoveItemWithinDrive),shortcutDetails(targetId,targetMimeType)))",
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

        // Solo el tamaño por adelantado. El contenido se lee bajo demanda
        // según la estrategia (antes: fs::read completo → N GB de RAM
        // para un archivo de N GB).

        // Construir metadata
        let mut file_metadata = google_drive3::api::File::default();
        file_metadata.name = Some(name.to_string());
        file_metadata.mime_type = Some(mime_type.unwrap_or("application/octet-stream").to_string());

        if parent_id != "root" {
            file_metadata.parents = Some(vec![parent_id.to_string()]);
        }

        let mime = mime_type.unwrap_or("application/octet-stream").parse().unwrap();
        let content_len = tokio::fs::metadata(file_path).await
            .context("Error leyendo metadatos del archivo local")?
            .len();

        // Estrategia adaptativa:
        // - Archivos pequeños (< 5MB) o vacíos: Upload simple (evita panic en resumable con 0 bytes)
        // - Archivos grandes: Resumable upload en streaming desde disco
        let result = if content_len < 5 * 1024 * 1024 {
            // Acotado (< 5 MB): el Vec completo es barato y el upload simple
            // lo necesita de una pieza.
            let content = tokio::fs::read(file_path).await
                .context("Error leyendo archivo local")?;
            tracing::debug!("Usando upload simple para archivo de {} bytes", content_len);
            match progress_cb {
                Some(cb) => {
                    let reader = ProgressReader::new(std::io::Cursor::new(content), cb);
                    Self::timed("upload simple", Self::upload_timeout(content_len),
                        self.hub.files().create(file_metadata)
                            .upload(reader, mime)).await
                        .context("Error en upload simple")?
                }
                None => {
                    Self::timed("upload simple", Self::upload_timeout(content_len),
                        self.hub.files().create(file_metadata)
                            .upload(std::io::Cursor::new(content), mime)).await
                        .context("Error en upload simple")?
                }
            }
        } else {
            tracing::debug!("Usando upload resumable para archivo de {} bytes", content_len);
            match progress_cb {
                Some(cb) => {
                    // Streaming desde disco: la librería hace seek+read por
                    // chunks de 8 MB → RAM O(8MB) sin importar el tamaño.
                    // (ReadSeek exige I/O síncrono; el runtime es multi-thread
                    // y las subidas corren en tareas dedicadas.)
                    let file = std::fs::File::open(file_path)
                        .context("Error abriendo archivo local para subida")?;
                    let reader = ProgressReader::new(file, cb);
                    Self::timed("upload resumable", Self::upload_timeout(content_len),
                        self.hub.files().create(file_metadata)
                            .upload_resumable(reader, mime)).await
                        .context("Error en upload resumable")?
                }
                None => {
                    let file = std::fs::File::open(file_path)
                        .context("Error abriendo archivo local para subida")?;
                    Self::timed("upload resumable", Self::upload_timeout(content_len),
                        self.hub.files().create(file_metadata)
                            .upload_resumable(file, mime)).await
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

        // El hub devuelve su propio error: se clasifica (404/403 → DriveError
        // preservado) antes de envolverlo, igual que hace `timed`.
        let hub_res = self.hub
            .files()
            .create(file_metadata)
            .supports_all_drives(true)
            .ignore_default_visibility(true)
            .upload(
                std::io::Cursor::new(vec![]),
                "application/vnd.google-apps.folder".parse().unwrap(),
            )
            .await;
        let result = match hub_res {
            Ok(v) => v,
            Err(hub) => {
                return Err(anyhow::Error::from(classify_hub_error(hub).await)
                    .context("Error creando carpeta en API"))
            }
        };

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

        // Solo el tamaño por adelantado; el contenido se lee bajo demanda.

        // Metadata vacío (no cambiamos nombre ni padres, solo contenido)
        let file_metadata = google_drive3::api::File::default();
        let mime = "application/octet-stream".parse().unwrap();
        let content_len = tokio::fs::metadata(file_path).await
            .context("Error leyendo metadatos del archivo local")?
            .len();

        // Estrategia adaptativa para updates
        if content_len < 5 * 1024 * 1024 {
            // Acotado (< 5 MB): el Vec completo es barato.
            let content = tokio::fs::read(file_path).await
                .context("Error leyendo archivo local")?;
            tracing::debug!("Usando update simple para archivo de {} bytes", content_len);
            match progress_cb {
                Some(cb) => {
                    let reader = ProgressReader::new(std::io::Cursor::new(content), cb);
                    Self::timed("update simple", Self::upload_timeout(content_len),
                        self.hub.files().update(file_metadata, file_id)
                            .upload(reader, mime)).await
                        .context("Error en update simple")?;
                }
                None => {
                    Self::timed("update simple", Self::upload_timeout(content_len),
                        self.hub.files().update(file_metadata, file_id)
                            .upload(std::io::Cursor::new(content), mime)).await
                        .context("Error en update simple")?;
                }
            }
        } else {
            tracing::debug!("Usando update resumable para archivo de {} bytes", content_len);
            match progress_cb {
                Some(cb) => {
                    // Streaming desde disco (ver upload_file).
                    let file = std::fs::File::open(file_path)
                        .context("Error abriendo archivo local para actualización")?;
                    let reader = ProgressReader::new(file, cb);
                    Self::timed("update resumable", Self::upload_timeout(content_len),
                        self.hub.files().update(file_metadata, file_id)
                            .upload_resumable(reader, mime)).await
                        .context("Error en update resumable")?;
                }
                None => {
                    let file = std::fs::File::open(file_path)
                        .context("Error abriendo archivo local para actualización")?;
                    Self::timed("update resumable", Self::upload_timeout(content_len),
                        self.hub.files().update(file_metadata, file_id)
                            .upload_resumable(file, mime)).await
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

        // Timeout también al refresco de token: yup usa hyper sin timeout y un
        // hang aquí colgaría el borrado para siempre (el match del caller
        // distingue InsufficientPermissions, así que se preserva DriveError).
        let token = tokio::time::timeout(CONTROL_TIMEOUT, self.hub.auth.get_token(&["https://www.googleapis.com/auth/drive"]))
            .await
            .map_err(|_| super::DriveError::Auth("timeout de red obteniendo token OAuth".to_string()))?
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
            return Err(super::DriveError::from_http_status(status.as_u16(), &body).into());
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Un futuro que nunca resuelve (simula red de agujero negro) debe
    /// convertirse en error, no colgar para siempre.
    #[tokio::test]
    async fn timed_convierte_hang_en_error() {
        let r: Result<()> = DriveClient::timed(
            "test_hang",
            Duration::from_millis(50),
            std::future::pending::<Result<()>>(),
        )
        .await;
        let msg = format!("{:?}", r.unwrap_err());
        assert!(msg.contains("timeout"), "error sin marca de timeout: {}", msg);
    }

    /// El camino feliz pasa intacto (valor + propagación de error interno).
    #[tokio::test]
    async fn timed_deja_pasar_ok_y_error_interno() {
        let ok: Result<u32> = DriveClient::timed(
            "test_ok",
            Duration::from_secs(5),
            std::future::ready(Ok::<u32, anyhow::Error>(42)),
        )
        .await;
        assert_eq!(ok.unwrap(), 42);

        let inner: Result<u32> = DriveClient::timed(
            "test_err",
            Duration::from_secs(5),
            std::future::ready(Err(anyhow::anyhow!("fallo interno"))),
        )
        .await;
        let msg = format!("{:?}", inner.unwrap_err());
        assert!(msg.contains("fallo interno"), "se perdió el error interno: {}", msg);
        assert!(!msg.contains("timeout"), "falso timeout: {}", msg);
    }

    /// Escala de timeouts de subida: simple fijo, resumable por tamaño con tope.
    #[test]
    fn upload_timeout_escala_con_tamano() {
        assert_eq!(DriveClient::upload_timeout(0), SIMPLE_UPLOAD_TIMEOUT);
        assert_eq!(DriveClient::upload_timeout(4 * 1024 * 1024), SIMPLE_UPLOAD_TIMEOUT);
        // 10 MB: 600 + 200 = 800 s
        assert_eq!(
            DriveClient::upload_timeout(10 * 1024 * 1024),
            Duration::from_secs(600 + 10 * 1024 * 1024 / 50_000)
        );
        // Tope 1 h aunque el archivo sea gigante
        assert_eq!(
            DriveClient::upload_timeout(10 * 1024 * 1024 * 1024),
            Duration::from_secs(3600)
        );
    }

    /// El error de timeout debe caber en DriveError (lo usa trash/delete).
    #[test]
    fn timeout_cabe_en_drive_error() {
        let e: super::super::DriveError =
            super::super::DriveError::Other(anyhow::anyhow!("timeout de red tras 60s en x"));
        assert!(format!("{}", e).contains("timeout"));
    }

    fn hub_failure(status: u16, body: &str) -> google_drive3::Error {
        let resp = hyper::Response::builder()
            .status(status)
            .body(hyper::Body::from(body.to_string()))
            .unwrap();
        google_drive3::Error::Failure(resp)
    }

    /// El hub 404/403-con-permiso se preserva como DriveError (el uploader
    /// los distingue); quota/rate-limit quedan como ApiError (reintento).
    #[tokio::test]
    async fn classify_hub_error_preserva_permanentes() {
        let e = classify_hub_error(hub_failure(404, "not found")).await;
        assert!(matches!(e, super::super::DriveError::NotFound(_)));

        let e = classify_hub_error(hub_failure(
            403,
            r#"{"error":{"errors":[{"reason":"insufficientFilePermissions"}]}}"#,
        ))
        .await;
        assert!(matches!(
            e,
            super::super::DriveError::InsufficientPermissions(_)
        ));

        let e = classify_hub_error(hub_failure(
            403,
            r#"{"error":{"errors":[{"reason":"storageQuotaExceeded"}]}}"#,
        ))
        .await;
        assert!(matches!(e, super::super::DriveError::ApiError(_)));

        let e = classify_hub_error(hub_failure(500, "boom")).await;
        assert!(matches!(e, super::super::DriveError::ApiError(_)));
    }

    /// ProgressReader sobre un File real cuenta los bytes leídos (es lo que
    /// alimenta el resumable en streaming: progreso sin cargar el archivo).
    #[test]
    fn progress_reader_cuenta_bytes_desde_disco() {
        let dir = std::env::temp_dir().join("gdrivexp-progress-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("f.bin");
        std::fs::write(&p, vec![7u8; 100_000]).unwrap();

        let f = std::fs::File::open(&p).unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(0u64));
        let seen_cb = seen.clone();
        let cb: ProgressCallback = Box::new(move |n| *seen_cb.lock().unwrap() = n);
        let mut reader = ProgressReader::new(f, cb);
        let mut all = Vec::new();
        use std::io::Read;
        reader.read_to_end(&mut all).unwrap();
        assert_eq!(all.len(), 100_000);
        assert_eq!(*seen.lock().unwrap(), 100_000);

        // Seek rebobina el contador interno (el reintento del resumable relee
        // desde 0 sin duplicar progreso: el callback reporta bytes leídos
        // desde la posición actual).
        use std::io::Seek;
        reader.seek(SeekFrom::Start(0)).unwrap();
        let mut one = [0u8; 1];
        reader.read_exact(&mut one).unwrap();
        assert_eq!(*seen.lock().unwrap(), 1);

        std::fs::remove_dir_all(&dir).unwrap();
    }
}

