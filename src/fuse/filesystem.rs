
use fuse3::raw::prelude::*;
use fuse3::{Errno, Result};
use std::ffi::OsStr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};
use futures_util::stream::{self, BoxStream, StreamExt};

use crate::db::MetadataRepository;
use crate::gdrive::client::DriveClient;
use crate::fuse::shortcuts;


/// Implementación del sistema de archivos FUSE para Google Drive
pub struct GDriveFS {
    db: Arc<MetadataRepository>,
    drive_client: Arc<DriveClient>,
    cache_dir: std::path::PathBuf,
}

impl GDriveFS {
    pub fn new(db: Arc<MetadataRepository>, drive_client: Arc<DriveClient>, cache_dir: impl AsRef<std::path::Path>) -> Self {
        Self { 
            db, 
            drive_client,
            cache_dir: cache_dir.as_ref().to_path_buf(),
        }
    }
}


impl Filesystem for GDriveFS {
    type DirEntryStream<'a> = BoxStream<'a, Result<DirectoryEntry>>;
    type DirEntryPlusStream<'a> = BoxStream<'a, Result<DirectoryEntryPlus>>;

    // Inicialización del sistema de archivos
    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        debug!("Sistema de archivos inicializado");
        Ok(ReplyInit {
            max_write: NonZeroU32::new(1024 * 1024).unwrap(), // 1MB
        })
    }

    async fn destroy(&self, _req: Request) {
        debug!("Sistema de archivos desmontado");
    }

    // Listar directorio (readdir)
    async fn readdir(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream<'_>>> {
        tracing::trace!("👁️ readdir: parent={} offset={}", parent, offset);

        // 1. Verificación temprana: obtener conteo sin cargar datos
        let child_count = match self.db.count_children(parent).await {
            Ok(c) => c,
            Err(e) => {
                error!("❌ Error contando hijos de {}: {}", parent, e);
                return Err(Errno::from(libc::EIO));
            }
        };
        
        // Total = hijos + 2 (por . y ..)
        let total_entries = child_count + 2;
        
        // Short-circuit: si ya consumieron todo, retornar vacío sin consultar DB
        if offset as u64 >= total_entries {
            tracing::trace!("📊 readdir short-circuit: offset={} >= total={}", offset, total_entries);
            return Ok(ReplyDirectory {
                entries: Box::pin(stream::empty())
            });
        }

        // 2. Solo si hay entradas por retornar, consultar los datos
        let children = match self.db.list_children(parent).await {
            Ok(c) => c,
            Err(e) => {
                error!("❌ Error listando hijos de {}: {}", parent, e);
                return Err(Errno::from(libc::EIO));
            }
        };

        // 3. Construir lista completa SIEMPRE (. y .. + hijos)
        let mut entries: Vec<(u64, String, bool)> = Vec::with_capacity(children.len() + 2);
        entries.push((parent, ".".to_string(), true));
        entries.push((1.max(parent), "..".to_string(), true));
        entries.extend(children);

        // 4. Aplicar offset y generar stream
        let stream = stream::iter(entries)
            .skip(offset as usize)
            .enumerate()
            .map(move |(index, (inode, name, is_dir))| {
                Ok(DirectoryEntry {
                    inode,
                    kind: if is_dir { FileType::Directory } else { FileType::RegularFile },
                    name: name.into(),
                    offset: (offset + index as i64 + 1),
                })
            });

        Ok(ReplyDirectory {
            entries: Box::pin(stream)
        })
    }

    // Buscar un archivo en un directorio (ls)
    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> Result<ReplyEntry> {
        let name_str = name.to_str().ok_or(Errno::from(libc::EINVAL))?;
        // trace is enough for lookup
        tracing::trace!("lookup: parent={} name={}", parent, name_str);

        // Consultar la base de datos
        // NOTA: Implementación temporal simulando que todo existe en SQLite
        // En producción esto consultará realmente la DB
        let inode = self.db.lookup(parent, name_str)
            .await
            .map_err(|e| {
                error!("Error en lookup: {}", e);
                Errno::from(libc::EIO)
            })?
            .ok_or(Errno::from(libc::ENOENT))?;

        // Obtener atributos del archivo
        let attrs = self.db.get_attrs(inode)
            .await
            .map_err(|e| {
                error!("Error obteniendo atributos para inode {}: {}", inode, e);
                Errno::from(libc::EIO)
            })?;

        Ok(ReplyEntry {
            ttl: Duration::from_secs(1),
            attr: attrs.to_file_attr(),
            generation: 0,
        })
    }

    // Obtener atributos de un archivo (stat)
    async fn getattr(&self, _req: Request, inode: u64, _fh: Option<u64>, _flags: u32) -> Result<ReplyAttr> {
        tracing::trace!("getattr: inode={}", inode);

        let attrs = self.db.get_attrs(inode)
            .await
            .map_err(|e| {
                // Si el inodo es 1 (root) y no está en DB, devolver valores por defecto
                if inode == 1 {
                    debug!("Devolviendo atributos raíz por defecto");
                    return Errno::from(libc::ENOENT);
                }
                error!("Error en getattr para inode {}: {}", inode, e);
                Errno::from(libc::ENOENT)
            })?;

        // Si es archivo Workspace, ajustar el tamaño reportado al tamaño del .desktop
        let mut file_attr = attrs.to_file_attr();
        
        if let Some(ref mime) = attrs.mime_type {
            if shortcuts::is_workspace_file(mime) {
                let name = self.get_file_name(inode).await
                    .unwrap_or_else(|_| "Documento de Google".to_string());
                let gdrive_id = self.get_gdrive_id(inode).await
                    .unwrap_or_else(|_| "unknown".to_string());
                    
                let desktop_content = shortcuts::generate_desktop_entry(
                    &gdrive_id,
                    &name,
                    mime
                );
                file_attr.size = desktop_content.len() as u64;
            }
        }

        Ok(ReplyAttr {
            ttl: Duration::from_secs(1),
            attr: file_attr,
        })
    }
    
    // Métodos requeridos adicionales que faltaban (placeholders)
    async fn forget(&self, _req: Request, _inode: u64, _nlookup: u64) {}

    // Abrir directorio (requerido antes de readdir)
    async fn opendir(&self, _req: Request, inode: u64, _flags: u32) -> Result<ReplyOpen> {
        tracing::trace!("📂 opendir: inode={}", inode);
        
        // Verificar que el inode existe y es un directorio
        match self.db.get_attrs(inode).await {
            Ok(attrs) => {
                if !attrs.is_dir {
                    return Err(Errno::from(libc::ENOTDIR));
                }
                Ok(ReplyOpen { fh: 0, flags: 0 })
            }
            Err(_) => Err(Errno::from(libc::ENOENT)),
        }
    }

    // Cerrar directorio
    async fn releasedir(&self, _req: Request, inode: u64, _fh: u64, _flags: u32) -> Result<()> {
        tracing::trace!("📂 releasedir: inode={}", inode);
        Ok(())
    }


    // Abrir archivo (open)
    async fn open(&self, _req: Request, inode: u64, _flags: u32) -> Result<ReplyOpen> {
        debug!("open: inode={}", inode);
        
        // Validar que existe en DB y obtener metadatos
        let attrs = self.db.get_attrs(inode).await
            .map_err(|_| Errno::from(libc::ENOENT))?;
        
        // OPTIMIZACIÓN MULTIMEDIA: Pre-descargar archivos multimedia pequeños
        if let Some(ref mime_type) = attrs.mime_type {
            if Self::is_multimedia_file(mime_type) && !attrs.is_dir {
                let file_size = attrs.size as u64;
                
                // Para archivos < 10MB, pre-descargar completo en background
                const SMALL_FILE_THRESHOLD: u64 = 10 * 1024 * 1024; // 10MB
                
                if file_size > 0 && file_size < SMALL_FILE_THRESHOLD {
                    let gdrive_id = self.get_gdrive_id(inode).await
                        .unwrap_or_else(|_| String::new());
                    
                    if !gdrive_id.is_empty() {
                        let cache_path = self.get_cache_path(&gdrive_id);
                        
                        // Solo pre-descargar si no está completamente cacheado
                        if !cache_path.exists() || 
                           tokio::fs::metadata(&cache_path).await.ok()
                               .map(|m| m.len() != file_size).unwrap_or(true) {
                            
                            debug!("🎬 Prefetching multimedia completo: inode={} size={} mime={}", 
                                   inode, file_size, mime_type);
                            
                            // Spawn background task para pre-descarga
                            let db = self.db.clone();
                            let drive_client = self.drive_client.clone();
                            let gdrive_id_owned = gdrive_id.clone();
                            let cache_path_owned = cache_path.clone();
                            
                            tokio::spawn(async move {
                                if let Err(e) = Self::prefetch_entire_file(
                                    &db, 
                                    &drive_client, 
                                    inode, 
                                    &gdrive_id_owned, 
                                    &cache_path_owned, 
                                    file_size
                                ).await {
                                    error!("Error en prefetch multimedia: {}", e);
                                }
                            });
                        }
                    }
                } else if file_size >= SMALL_FILE_THRESHOLD {
                    // Para archivos grandes multimedia, prefetch COMPLETO en background
                    // Esto evita la descarga lenta chunk-por-chunk al abrir la imagen
                    let gdrive_id = self.get_gdrive_id(inode).await
                        .unwrap_or_else(|_| String::new());
                    
                    if !gdrive_id.is_empty() {
                        let cache_path = self.get_cache_path(&gdrive_id);
                        
                        // Solo pre-descargar si no está completamente cacheado
                        if !cache_path.exists() || 
                           tokio::fs::metadata(&cache_path).await.ok()
                               .map(|m| m.len() != file_size).unwrap_or(true) {
                            
                            debug!("🎬 Prefetching multimedia completo (grande): inode={} size={} mime={}", 
                                   inode, file_size, mime_type);
                            
                            let db = self.db.clone();
                            let drive_client = self.drive_client.clone();
                            let gdrive_id_owned = gdrive_id.clone();
                            let cache_path_owned = cache_path.clone();
                            
                            tokio::spawn(async move {
                                if let Err(e) = Self::prefetch_headers_and_tail(
                                    &db,
                                    &drive_client,
                                    inode,
                                    &gdrive_id_owned,
                                    &cache_path_owned,
                                    file_size
                                ).await {
                                    error!("Error en prefetch de cabeceras multimedia: {}", e);
                                }
                            });
                        } else {
                            tracing::info!("✅ Prefetch cabeceras+cola completado");
                        }
                    }
                }
            }
        }
        
        Ok(ReplyOpen { fh: 0, flags: 0 }) 
    }

    // Cerrar archivo (release)
    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        debug!("release");
        Ok(())
    }

    // Flush de datos pendientes (llamado en cada close())
    async fn flush(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        _lock_owner: u64,
    ) -> Result<()> {
        debug!("flush: inode={}", inode);
        // Los datos ya se persisten sincrónicamente en write(),
        // el upload a GDrive es asíncrono vía uploader
        Ok(())
    }

    // Sincronizar datos a disco
    async fn fsync(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        _datasync: bool,
    ) -> Result<()> {
        debug!("fsync: inode={}", inode);
        // Los datos ya se persisten sincrónicamente en write(),
        // el upload a GDrive es asíncrono vía uploader
        Ok(())
    }

    // Leer contenido (read) - CON CACHÉ LOCAL
    async fn read(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        debug!("read: inode={} offset={} size={}", inode, offset, size);

        // 1. Obtener el gdrive_id del archivo, mime_type y tamaño
        let (gdrive_id, mime_type, file_size) = match sqlx::query_as::<_, (String, Option<String>, i64)>(
            "SELECT i.gdrive_id, a.mime_type, a.size 
             FROM inodes i 
             LEFT JOIN attrs a ON i.inode = a.inode 
             WHERE i.inode = ?"
        )
            .bind(inode as i64)
            .fetch_one(self.db.pool())
            .await 
        {
            Ok(row) => row,
            Err(e) => {
                error!("Error buscando info para inode {}: {}", inode, e);
                return Err(Errno::from(libc::ENOENT));
            }
        };

        // 2. Si es archivo de Google Workspace, generar .desktop file on-the-fly
        if let Some(ref mime) = mime_type {
            if shortcuts::is_workspace_file(mime) {
                // Obtener el nombre del archivo
                let name = self.get_file_name(inode).await
                    .unwrap_or_else(|_| "Documento de Google".to_string());
                
                // Generar contenido del .desktop
                let desktop_content = shortcuts::generate_desktop_entry(
                    &gdrive_id,
                    &name,
                    mime
                );
                
                let bytes = desktop_content.as_bytes();
                let start = offset as usize;
                let end = (start + size as usize).min(bytes.len());
                
                if start >= bytes.len() {
                    return Ok(ReplyData { data: vec![].into() });
                }
                
                return Ok(ReplyData {
                    data: bytes[start..end].to_vec().into()
                });
            }
        }

        // 3. Archivo binario normal: estrategia de caché bajo demanda
        let cache_path = self.get_cache_path(&gdrive_id);
        
        // 3a. Asegurar que el rango solicitado esté disponible
        if file_size > 0 {
            // Descargar solo lo necesario
            if let Err(e) = self.ensure_range_cached(inode, &gdrive_id, offset, size, file_size as u64).await {
                error!("Error descargando chunk para inode {}: {}", inode, e);
                return Err(Errno::from(libc::EIO));
            }

            // Leer desde caché
            match self.read_from_cache(&cache_path, offset, size).await {
                Ok(data) => return Ok(ReplyData { data: data.into() }),
                Err(e) => {
                    error!("Error leyendo caché para inode {}: {}", inode, e);
                    return Err(Errno::from(libc::EIO));
                }
            }
        }

        // Archivo vacío
        Ok(ReplyData { data: vec![].into() })
    }

    // Obtener estadísticas del sistema de archivos (requerido por comandos como ls/df)
    async fn statfs(&self, _req: Request, _inode: u64) -> Result<ReplyStatFs> {
        tracing::trace!("statfs");
        Ok(ReplyStatFs {
            blocks: 1024 * 1024 * 1024, // 1 TB ficticio
            bfree: 512 * 1024 * 1024,
            bavail: 512 * 1024 * 1024,
            files: 1000000,
            ffree: 1000000,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        })
    }

    // readdirplus: versión optimizada de readdir que incluye atributos
    // Requerido por herramientas modernas como lsd, nautilus, etc.
    async fn readdirplus(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'_>>> {
        tracing::trace!("👁️ readdirplus: parent={} offset={}", parent, offset);

        let db = self.db.clone();
        
        // 1. Verificación temprana: obtener conteo sin cargar datos
        let child_count = match db.count_children(parent).await {
            Ok(c) => c,
            Err(e) => {
                error!("❌ Error contando hijos de {}: {}", parent, e);
                return Err(Errno::from(libc::EIO));
            }
        };
        
        // Total = hijos + 2 (por . y ..)
        let total_entries = child_count + 2;
        
        // Short-circuit: si ya consumieron todo, retornar vacío sin consultar DB
        if offset >= total_entries {
            tracing::trace!("📊 readdirplus short-circuit: offset={} >= total={}", offset, total_entries);
            return Ok(ReplyDirectoryPlus {
                entries: Box::pin(stream::empty())
            });
        }

        // 2. Solo si hay entradas por retornar, consultar los datos
        let children = match db.list_children_extended(parent).await {
            Ok(c) => c,
            Err(e) => {
                error!("❌ Error listando hijos de {}: {}", parent, e);
                return Err(Errno::from(libc::EIO));
            }
        };

        // 3. Construir lista completa SIEMPRE (. y .. + hijos)
        let mut final_entries: Vec<(u64, String, bool, Option<String>, Option<String>)> = 
            Vec::with_capacity(children.len() + 2);
        final_entries.push((parent, ".".to_string(), true, None, None));
        final_entries.push((1.max(parent), "..".to_string(), true, None, None));

        for (inode, name, is_dir, mime, gdrive_id) in children {
            final_entries.push((inode, name, is_dir, mime, Some(gdrive_id)));
        }

        // 4. Construir stream con atributos completos usando los datos ya cargados
        let stream = stream::iter(final_entries)
            .skip(offset as usize)
            .enumerate()
            .then(move |(index, (inode, name, is_dir, mime, gdrive_id))| {
                let db_clone = db.clone();
                async move {
                    let mut attr = if let Ok(a) = db_clone.get_attrs(inode).await {
                        a.to_file_attr()
                    } else {
                        // Si no hay atributos, crear unos por defecto
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64;
                        crate::fuse::attr::FileAttributes {
                            inode: inode as i64,
                            size: if is_dir { 4096 } else { 0 },
                            mtime: now,
                            ctime: now,
                            mode: if is_dir { 0o755 } else { 0o644 },
                            is_dir,
                            mime_type: None,
                        }.to_file_attr()
                    };

                    // Ajustar tamaño para archivos Workspace si tenemos los datos necesarios
                    if let (Some(m), Some(gid)) = (mime, gdrive_id) {
                        if shortcuts::is_workspace_file(&m) {
                            let desktop_content = shortcuts::generate_desktop_entry(&gid, &name, &m);
                            attr.size = desktop_content.len() as u64;
                        }
                    }

                    Ok(DirectoryEntryPlus {
                        inode,
                        generation: 0,
                        kind: if is_dir { FileType::Directory } else { FileType::RegularFile },
                        name: name.into(),
                        offset: (offset as i64 + index as i64 + 1),
                        attr,
                        entry_ttl: Duration::from_secs(1),
                        attr_ttl: Duration::from_secs(1),
                    })
                }
            });

        Ok(ReplyDirectoryPlus {
            entries: Box::pin(stream)
        })
    }

    // ============================================================
    // WRITE OPERATIONS (Phase 2: Upstream Sync)
    // ============================================================

    // Crear un nuevo archivo
    async fn create(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        let name_str = name.to_str().ok_or(Errno::from(libc::EINVAL))?;
        debug!("✏️ create: parent={} name={} mode={:o} flags={}", parent, name_str, mode, flags);

        // Generar un gdrive_id temporal (será reemplazado al subir)
        let temp_gdrive_id = format!("temp_{}", uuid::Uuid::new_v4());
        
        // Crear inode en la DB
        let inode = self.db.get_or_create_inode(&temp_gdrive_id).await
            .map_err(|e| {
                error!("Error creando inode: {}", e);
                Errno::from(libc::EIO)
            })?;

        // Timestamp actual
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Insertar metadatos del archivo vacío
        self.db.upsert_file_metadata(
            inode,
            0, // size inicial
            now,
            mode,
            false, // no es directorio
            Some("application/octet-stream"),
        ).await.map_err(|e| {
            error!("Error insertando metadatos: {}", e);
            Errno::from(libc::EIO)
        })?;

        // Agregar al dentry
        self.db.upsert_dentry(parent, inode, name_str).await
            .map_err(|e| {
                error!("Error insertando dentry: {}", e);
                Errno::from(libc::EIO)
            })?;

        // Marcar como dirty (pendiente de subida)
        sqlx::query("INSERT INTO sync_state (inode, dirty, version, md5_checksum) VALUES (?, 1, 0, NULL) ON CONFLICT(inode) DO UPDATE SET dirty = 1")
            .bind(inode as i64)
            .execute(self.db.pool())
            .await
            .map_err(|e| {
                error!("Error marcando archivo como dirty: {}", e);
                Errno::from(libc::EIO)
            })?;

        let attrs = self.db.get_attrs(inode).await
            .map_err(|_| Errno::from(libc::EIO))?;

        debug!("✅ Archivo creado: inode={} nombre={}", inode, name_str);

        Ok(ReplyCreated {
            ttl: Duration::from_secs(1),
            attr: attrs.to_file_attr(),
            generation: 0,
            fh: 0,
            flags: 0,
        })
    }

    // Escribir datos en un archivo
    async fn write(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> Result<ReplyWrite> {
        debug!("✏️ write: inode={} offset={} size={}", inode, offset, data.len());

        // Obtener el gdrive_id del archivo
        let gdrive_id = sqlx::query_scalar::<_, String>("SELECT gdrive_id FROM inodes WHERE inode = ?")
            .bind(inode as i64)
            .fetch_one(self.db.pool())
            .await
            .map_err(|e| {
                error!("Error obteniendo gdrive_id: {}", e);
                Errno::from(libc::ENOENT)
            })?;

        // Ruta local de caché
        let cache_path = self.get_cache_path(&gdrive_id);
        
        // Crear directorio de caché si no existe
        if let Some(parent_dir) = cache_path.parent() {
            tokio::fs::create_dir_all(parent_dir).await
                .map_err(|e| {
                    error!("Error creando directorio de caché: {}", e);
                    Errno::from(libc::EIO)
                })?;
        }

        // Escribir datos en el archivo de caché
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&cache_path)
            .await
            .map_err(|e| {
                error!("Error abriendo archivo de caché: {}", e);
                Errno::from(libc::EIO)
            })?;

        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        file.seek(std::io::SeekFrom::Start(offset)).await
            .map_err(|e| {
                error!("Error posicionando en archivo: {}", e);
                Errno::from(libc::EIO)
            })?;

        file.write_all(data).await
            .map_err(|e| {
                error!("Error escribiendo datos: {}", e);
                Errno::from(libc::EIO)
            })?;

        file.flush().await
            .map_err(|e| {
                error!("Error haciendo flush: {}", e);
                Errno::from(libc::EIO)
            })?;

        // Obtener el nuevo tamaño del archivo
        let metadata = file.metadata().await
            .map_err(|e| {
                error!("Error obteniendo metadata: {}", e);
                Errno::from(libc::EIO)
            })?;
        let new_size = metadata.len() as i64;

        // Actualizar tamaño en la base de datos
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        sqlx::query("UPDATE attrs SET size = ?, mtime = ? WHERE inode = ?")
            .bind(new_size)
            .bind(now)
            .bind(inode as i64)
            .execute(self.db.pool())
            .await
            .map_err(|e| {
                error!("Error actualizando attrs: {}", e);
                Errno::from(libc::EIO)
            })?;

        // Marcar como dirty
        sqlx::query("INSERT INTO sync_state (inode, dirty, version, md5_checksum) VALUES (?, 1, 0, NULL) ON CONFLICT(inode) DO UPDATE SET dirty = 1")
            .bind(inode as i64)
            .execute(self.db.pool())
            .await
            .map_err(|e| {
                error!("Error marcando como dirty: {}", e);
                Errno::from(libc::EIO)
            })?;

        debug!("✅ Escritura completada: {} bytes", data.len());

        Ok(ReplyWrite {
            written: data.len() as u32,
        })
    }

    // Cambiar atributos de un archivo (truncate, chmod, etc.)
    async fn setattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        debug!("✏️ setattr: inode={} set_attr={:?}", inode, set_attr);

        // Actualizar solo los campos especificados
        if let Some(size) = set_attr.size {
            // Truncar archivo
            let gdrive_id = sqlx::query_scalar::<_, String>("SELECT gdrive_id FROM inodes WHERE inode = ?")
                .bind(inode as i64)
                .fetch_one(self.db.pool())
                .await
                .map_err(|_| Errno::from(libc::ENOENT))?;

            let cache_path = self.get_cache_path(&gdrive_id);
            
            if cache_path.exists() {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&cache_path)
                    .map_err(|_| Errno::from(libc::EIO))?;
                    
                file.set_len(size)
                    .map_err(|_| Errno::from(libc::EIO))?;
            } else {
                // Crear archivo vacío del tamaño especificado
                std::fs::write(&cache_path, vec![0u8; size as usize])
                    .map_err(|_| Errno::from(libc::EIO))?;
            }

            sqlx::query("UPDATE attrs SET size = ? WHERE inode = ?")
                .bind(size as i64)
                .bind(inode as i64)
                .execute(self.db.pool())
                .await
                .map_err(|_| Errno::from(libc::EIO))?;

            // Marcar como dirty
            sqlx::query("INSERT INTO sync_state (inode, dirty, version, md5_checksum) VALUES (?, 1, 0, NULL) ON CONFLICT(inode) DO UPDATE SET dirty = 1")
                .bind(inode as i64)
                .execute(self.db.pool())
                .await
                .map_err(|_| Errno::from(libc::EIO))?;
        }

        if let Some(mtime) = set_attr.mtime {
            let mtime_secs = mtime.sec;

            sqlx::query("UPDATE attrs SET mtime = ? WHERE inode = ?")
                .bind(mtime_secs)
                .bind(inode as i64)
                .execute(self.db.pool())
                .await
                .map_err(|_| Errno::from(libc::EIO))?;
        }

        if let Some(mode) = set_attr.mode {
            sqlx::query("UPDATE attrs SET mode = ? WHERE inode = ?")
                .bind(mode)
                .bind(inode as i64)
                .execute(self.db.pool())
                .await
                .map_err(|_| Errno::from(libc::EIO))?;
        }

        let attrs = self.db.get_attrs(inode).await
            .map_err(|_| Errno::from(libc::ENOENT))?;

        Ok(ReplyAttr {
            ttl: Duration::from_secs(1),
            attr: attrs.to_file_attr(),
        })
    }

    // Eliminar un archivo (soft delete)
    async fn unlink(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
    ) -> Result<()> {
        let name_str = name.to_str().ok_or(Errno::from(libc::EINVAL))?;
        debug!("🗑️ unlink: parent={} name={}", parent, name_str);

        // Buscar el archivo
        let inode = self.db.lookup(parent, name_str).await
            .map_err(|_| Errno::from(libc::EIO))?
            .ok_or(Errno::from(libc::ENOENT))?;

        // Obtener gdrive_id
        let gdrive_id = sqlx::query_scalar::<_, String>("SELECT gdrive_id FROM inodes WHERE inode = ?")
            .bind(inode as i64)
            .fetch_one(self.db.pool())
            .await
            .map_err(|_| Errno::from(libc::ENOENT))?;

        // Soft delete
        self.db.soft_delete_by_gdrive_id(&gdrive_id).await
            .map_err(|e| {
                error!("Error en soft delete: {}", e);
                Errno::from(libc::EIO)
            })?;

        // Marcar como dirty para que el uploader lo envíe como delete a GDrive
        sqlx::query("INSERT INTO sync_state (inode, dirty, version, md5_checksum) VALUES (?, 1, 0, NULL) ON CONFLICT(inode) DO UPDATE SET dirty = 1")
            .bind(inode as i64)
            .execute(self.db.pool())
            .await
            .map_err(|_| Errno::from(libc::EIO))?;

        debug!("✅ Archivo marcado para eliminación: {}", name_str);

        Ok(())
    }

    // Renombrar/mover un archivo
    async fn rename(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> Result<()> {
        let name_str = name.to_str().ok_or(Errno::from(libc::EINVAL))?;
        let new_name_str = new_name.to_str().ok_or(Errno::from(libc::EINVAL))?;
        debug!("🔄 rename: parent={} name={} -> new_parent={} new_name={}", 
               parent, name_str, new_parent, new_name_str);

        // Buscar el inode del archivo origen
        let inode = self.db.lookup(parent, name_str).await
            .map_err(|_| Errno::from(libc::EIO))?
            .ok_or(Errno::from(libc::ENOENT))?;

        // Si existe un archivo destino, eliminarlo primero (overwite)
        if let Ok(Some(existing_inode)) = self.db.lookup(new_parent, new_name_str).await {
            // Obtener gdrive_id del existente
            if let Ok(gdrive_id) = sqlx::query_scalar::<_, String>("SELECT gdrive_id FROM inodes WHERE inode = ?")
                .bind(existing_inode as i64)
                .fetch_one(self.db.pool())
                .await
            {
                self.db.soft_delete_by_gdrive_id(&gdrive_id).await
                    .map_err(|_| Errno::from(libc::EIO))?;
            }
        }

        // Eliminar la entrada dentry antigua
        sqlx::query("DELETE FROM dentry WHERE parent_inode = ? AND name = ?")
            .bind(parent as i64)
            .bind(name_str)
            .execute(self.db.pool())
            .await
            .map_err(|e| {
                error!("Error eliminando dentry antiguo: {}", e);
                Errno::from(libc::EIO)
            })?;

        // Crear la nueva entrada dentry
        self.db.upsert_dentry(new_parent, inode, new_name_str).await
            .map_err(|e| {
                error!("Error creando nuevo dentry: {}", e);
                Errno::from(libc::EIO)
            })?;

        // Marcar como dirty para sincronizar el cambio de nombre
        sqlx::query("INSERT INTO sync_state (inode, dirty, version, md5_checksum) VALUES (?, 1, 0, NULL) ON CONFLICT(inode) DO UPDATE SET dirty = 1")
            .bind(inode as i64)
            .execute(self.db.pool())
            .await
            .map_err(|_| Errno::from(libc::EIO))?;

        debug!("✅ Archivo renombrado: {} -> {}", name_str, new_name_str);

        Ok(())
    }
}

impl GDriveFS {
    /// Construye la ruta local de caché para un archivo de GDrive
    fn get_cache_path(&self, gdrive_id: &str) -> std::path::PathBuf {
        self.cache_dir.join(gdrive_id)
    }

    /// Obtiene el nombre de un archivo dado su inode
    async fn get_file_name(&self, inode: u64) -> anyhow::Result<String> {
        let name = sqlx::query_scalar::<_, String>(
            "SELECT name FROM dentry WHERE child_inode = ? LIMIT 1"
        )
        .bind(inode as i64)
        .fetch_optional(self.db.pool())
        .await?
        .unwrap_or_else(|| format!("file_{}", inode));
        
        Ok(name)
    }

    /// Obtiene el gdrive_id de un archivo dado su inode
    async fn get_gdrive_id(&self, inode: u64) -> anyhow::Result<String> {
        let gdrive_id = sqlx::query_scalar::<_, String>(
            "SELECT gdrive_id FROM inodes WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_one(self.db.pool())
        .await?;
        
        Ok(gdrive_id)
    }

    /// Lee datos desde un archivo de caché local
    async fn read_from_cache(
        &self,
        cache_path: &std::path::Path,
        offset: u64,
        size: u32,
    ) -> anyhow::Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        
        let mut file = tokio::fs::File::open(cache_path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        
        let mut buffer = vec![0u8; size as usize];
        let bytes_read = file.read(&mut buffer).await?;
        buffer.truncate(bytes_read);
        
        Ok(buffer)
    }


    /// Asegura que un rango específico esté disponible en caché
    /// Descarga solo los chunks faltantes EN PARALELO para mejor performance
    async fn ensure_range_cached(
        &self,
        inode: u64,
        gdrive_id: &str,
        offset: u64,
        size: u32,
        file_size: u64,
    ) -> anyhow::Result<()> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        
        // Calcular el rango solicitado (ajustado al tamaño del archivo)
        let requested_start = offset;
        let requested_end = (offset + size as u64 - 1).min(file_size.saturating_sub(1));
        
        if requested_start >= file_size {
            return Ok(()); // Fuera de rango, nada que hacer
        }

        let cache_path = self.get_cache_path(gdrive_id);
        
        // OPTIMIZACIÓN CRÍTICA: Verificar primero si el archivo caché está COMPLETO
        // Esto evita consultar la DB para archivos ya completamente cacheados
        if let Ok(metadata) = tokio::fs::metadata(&cache_path).await {
            if metadata.len() == file_size {
                // Archivo completo en disco - no necesitamos consultar la DB
                tracing::debug!("✅ Rango ya cacheado (fast-path): inode={} offset={} size={}", inode, offset, size);
                return Ok(());
            }
        }

        // Solo si el archivo no está completo, consultar la DB para rangos faltantes
        let missing_ranges = self.db.get_missing_ranges(inode, requested_start, requested_end).await?;
        

        if missing_ranges.is_empty() {
            tracing::debug!("✅ Rango ya cacheado: inode={} offset={} size={}", inode, offset, size);
            return Ok(());
        }

        
        // Crear directorio de caché si no existe
        if let Some(parent) = cache_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Asegurar que el archivo existe (puede estar vacío o sparse)
        if !cache_path.exists() {
            tokio::fs::File::create(&cache_path).await?;
        }

        // OPTIMIZACIÓN: Descargar todos los rangos EN PARALELO
        tracing::info!("📥 Descargando {} chunks faltantes en paralelo para inode {}", 
                       missing_ranges.len(), inode);

        let drive_client = self.drive_client.clone();
        let db = self.db.clone();
        let gdrive_id_owned = gdrive_id.to_string();
        let cache_path_owned = cache_path.clone();

        // Spawn tasks para descargar cada rango en paralelo
        let download_tasks: Vec<_> = missing_ranges.into_iter().map(|(start, end)| {
            let drive_client = drive_client.clone();
            let db = db.clone();
            let gdrive_id = gdrive_id_owned.clone();
            let cache_path = cache_path_owned.clone();

            tokio::spawn(async move {
                let chunk_size = (end - start + 1) as u32;
                
                tracing::debug!("📥 Descargando chunk: inode={} range={}-{} ({} bytes)", 
                               inode, start, end, chunk_size);
                
                // Descargar chunk
                let data = drive_client.download_chunk(&gdrive_id, start, chunk_size).await?;
                
                // Escribir en el archivo de caché en la posición correcta (con lock)
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&cache_path)
                    .await?;
                
                file.seek(std::io::SeekFrom::Start(start)).await?;
                file.write_all(&data).await?;
                file.flush().await?;
                
                // Registrar el chunk descargado en la DB
                db.add_cached_chunk(inode, start, end).await?;
                
                tracing::debug!("✅ Chunk cacheado: {}-{}", start, end);
                
                Ok::<_, anyhow::Error>((start, end))
            })
        }).collect();

        // Esperar a que todas las descargas completen
        let results = futures_util::future::join_all(download_tasks).await;

        // Verificar errores
        for result in results {
            match result {
                Ok(Ok(_)) => {},
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(anyhow::anyhow!("Task panicked: {}", e)),
            }
        }

        tracing::info!("✅ Todos los chunks descargados para inode {}", inode);
        Ok(())
    }

    /// Detecta si un archivo es multimedia por MIME type
    fn is_multimedia_file(mime_type: &str) -> bool {
        mime_type.starts_with("audio/") || 
        mime_type.starts_with("video/") ||
        mime_type.starts_with("image/")
    }

    /// Pre-descarga un archivo completo en background (para archivos pequeños)
    async fn prefetch_entire_file(
        db: &Arc<MetadataRepository>,
        drive_client: &Arc<DriveClient>,
        inode: u64,
        gdrive_id: &str,
        cache_path: &std::path::Path,
        file_size: u64,
    ) -> anyhow::Result<()> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        
        // Crear directorio de caché si no existe
        if let Some(parent) = cache_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        // Para archivos pequeños (<5MB), descargar en una sola solicitud
        const SINGLE_DOWNLOAD_THRESHOLD: u64 = 5 * 1024 * 1024; // 5MB
        
        if file_size < SINGLE_DOWNLOAD_THRESHOLD {
            // Descargar archivo completo en una solicitud
            tracing::info!("📥 Descargando archivo completo: {} bytes", file_size);
            let data = drive_client.download_chunk(gdrive_id, 0, file_size as u32).await?;
            
            // Escribir a caché
            let mut file = tokio::fs::File::create(cache_path).await?;
            file.write_all(&data).await?;
            file.flush().await?;
            
            // Registrar en DB como completamente cacheado
            db.add_cached_chunk(inode, 0, file_size - 1).await?;
            
            tracing::info!("✅ Archivo multimedia completo cacheado: {} bytes", file_size);
            return Ok(());
        }
        
        // Para archivos grandes, descargar en chunks paralelos
        const CHUNK_SIZE: u64 = 2 * 1024 * 1024; // 2MB chunks para descarga paralela
        const MAX_CONCURRENT: usize = 4; // Máximo 4 descargas simultáneas
        
        tracing::info!("📥 Descargando archivo grande en chunks paralelos: {} bytes", file_size);
        
        // Crear el archivo de caché vacío primero
        tokio::fs::File::create(cache_path).await?;
        
        // Calcular rangos de chunks
        let mut chunks: Vec<(u64, u64)> = Vec::new();
        let mut offset = 0u64;
        while offset < file_size {
            let end = (offset + CHUNK_SIZE - 1).min(file_size - 1);
            chunks.push((offset, end));
            offset = end + 1;
        }
        
        // Descargar en lotes paralelos
        for batch in chunks.chunks(MAX_CONCURRENT) {
            let download_tasks: Vec<_> = batch.iter().map(|&(start, end)| {
                let drive_client = drive_client.clone();
                let gdrive_id = gdrive_id.to_string();
                let db = db.clone();
                let cache_path = cache_path.to_path_buf();
                
                tokio::spawn(async move {
                    let chunk_size = (end - start + 1) as u32;
                    let data = drive_client.download_chunk(&gdrive_id, start, chunk_size).await?;
                    
                    // Escribir en la posición correcta del archivo
                    let mut file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .open(&cache_path)
                        .await?;
                    file.seek(std::io::SeekFrom::Start(start)).await?;
                    file.write_all(&data).await?;
                    file.flush().await?;
                    
                    // Registrar chunk en DB
                    db.add_cached_chunk(inode, start, end).await?;
                    
                    Ok::<_, anyhow::Error>(())
                })
            }).collect();
            
            // Esperar a que el lote complete
            for result in futures_util::future::join_all(download_tasks).await {
                match result {
                    Ok(Ok(_)) => {},
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(anyhow::anyhow!("Task panicked: {}", e)),
                }
            }
        }
        
        tracing::info!("✅ Archivo multimedia grande cacheado: {} bytes", file_size);
        Ok(())
    }

    /// Pre-descarga cabeceras (primeros 1MB) + cola (últimos 256KB) para archivos grandes
    async fn prefetch_headers_and_tail(
        db: &Arc<MetadataRepository>,
        drive_client: &Arc<DriveClient>,
        inode: u64,
        gdrive_id: &str,
        cache_path: &std::path::Path,
        file_size: u64,
    ) -> anyhow::Result<()> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        
        const HEADER_SIZE: u64 = 1024 * 1024; // 1MB
        const TAIL_SIZE: u64 = 256 * 1024;    // 256KB
        
        // Crear directorio de caché si no existe
        if let Some(parent) = cache_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        // Asegurar que el archivo existe
        if !cache_path.exists() {
            tokio::fs::File::create(cache_path).await?;
        }
        
        // Descargar cabeceras (primeros 1MB)
        let header_end = HEADER_SIZE.min(file_size - 1);
        let missing_header = db.get_missing_ranges(inode, 0, header_end).await?;
        
        if !missing_header.is_empty() {
            tracing::info!("📥 Prefetching cabeceras: 0-{}", header_end);
            let header_data = drive_client.download_chunk(gdrive_id, 0, (header_end + 1) as u32).await?;
            
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(cache_path)
                .await?;
            file.seek(std::io::SeekFrom::Start(0)).await?;
            file.write_all(&header_data).await?;
            file.flush().await?;
            
            db.add_cached_chunk(inode, 0, header_end).await?;
        }
        
        // Descargar cola (últimos 256KB)
        if file_size > TAIL_SIZE {
            let tail_start = file_size - TAIL_SIZE;
            let tail_end = file_size - 1;
            let missing_tail = db.get_missing_ranges(inode, tail_start, tail_end).await?;
            
            if !missing_tail.is_empty() {
                tracing::info!("📥 Prefetching cola: {}-{}", tail_start, tail_end);
                let tail_data = drive_client.download_chunk(gdrive_id, tail_start, TAIL_SIZE as u32).await?;
                
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(cache_path)
                    .await?;
                file.seek(std::io::SeekFrom::Start(tail_start)).await?;
                file.write_all(&tail_data).await?;
                file.flush().await?;
                
                db.add_cached_chunk(inode, tail_start, tail_end).await?;
            }
        }
        
        tracing::info!("✅ Prefetch cabeceras+cola completado");
        Ok(())
    }
}

