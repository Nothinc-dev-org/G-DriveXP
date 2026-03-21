use fuse3::raw::prelude::*;
use fuse3::{Errno, Result, Timestamp};
use std::ffi::OsStr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};
use futures_util::stream::{self, BoxStream, StreamExt};
use std::collections::HashMap;
use dashmap::{DashMap, DashSet};

use crate::db::MetadataRepository;
use crate::gdrive::client::DriveClient;
use crate::fuse::shortcuts;
use crate::gui::history::{ActionHistory, TransferOp};


/// Implementación del sistema de archivos FUSE para Google Drive
pub const SHARED_INODE: u64 = 0xFFFF_FFFF_FFFF_FFFE; // Un inodo virtual muy alto
pub struct GDriveFS {
    db: Arc<MetadataRepository>,
    drive_client: Arc<DriveClient>,
    cache_dir: std::path::PathBuf,
    history: Arc<ActionHistory>,
    /// Inodes que tienen un descargo activo en FUSE (Map de Inode -> (Option<Transfer ID>, Open Count, Session Bytes Read))
    fuse_downloads: Arc<tokio::sync::Mutex<HashMap<u64, (Option<u64>, usize, u64)>>>,
    file_locks: Arc<DashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
    /// Inodes que recibieron 403 permanente de Drive API (no reintentar)
    failed_downloads: Arc<DashSet<u64>>,
    /// Seguimiento de la última posición de lectura por inodo (para Smart Streamer)
    read_offsets: Arc<DashMap<u64, u64>>,
}

impl GDriveFS {
    pub fn new(
        db: Arc<MetadataRepository>,
        drive_client: Arc<DriveClient>,
        cache_dir: impl AsRef<std::path::Path>,
        history: Arc<ActionHistory>,
    ) -> Self {
        Self {
            db,
            drive_client,
            cache_dir: cache_dir.as_ref().to_path_buf(),
            history,
            fuse_downloads: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            file_locks: Arc::new(DashMap::new()),
            failed_downloads: Arc::new(DashSet::new()),
            read_offsets: Arc::new(DashMap::new()),
        }
    }
}


impl Filesystem for GDriveFS {
    type DirEntryStream<'a> = BoxStream<'a, Result<DirectoryEntry>>;
    type DirEntryPlusStream<'a> = BoxStream<'a, Result<DirectoryEntryPlus>>;

    // Inicialización del sistema de archivos
    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        tracing::info!("Sistema de archivos inicializado");
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

        // 1. Verificación temprana y carga de datos
        // Caso especial: SHARED_INODE
        let (children, child_count) = if parent == SHARED_INODE {
            let items = self.db.list_non_owned_root_children().await
                .map_err(|e| {
                    error!("❌ Error listando compartidos: {}", e);
                    Errno::from(libc::EIO)
                })?;
            let count = items.len() as u64;
            let simplified = items.into_iter().map(|(inode, name, is_dir, _, _)| (inode, name, is_dir)).collect::<Vec<_>>();
            (simplified, count)
        } else {
            let _count = match self.db.count_children(parent).await {
                Ok(c) => c,
                Err(e) => {
                    error!("❌ Error contando hijos de {}: {}", parent, e);
                    return Err(Errno::from(libc::EIO));
                }
            };
            
            let mut items = match self.db.list_children(parent).await {
                Ok(c) => c,
                Err(e) => {
                    error!("❌ Error listando hijos de {}: {}", parent, e);
                    return Err(Errno::from(libc::EIO));
                }
            };

            // Si es root, filtrar los que NO son propios
            if parent == 1 {
                let mut filtered = Vec::new();
                for (inode, name, is_dir) in items {
                    let attrs = self.db.get_attrs(inode).await.map_err(|_| Errno::from(libc::EIO))?;
                    if attrs.owned_by_me {
                        filtered.push((inode, name, is_dir));
                    }
                }
                items = filtered;
            }

            let real_count = items.len() as u64;
            (items, real_count)
        };
        
        // Total = hijos + 2 (por . y ..) + (1 si es root por el SHARED)
        let mut total_entries = child_count + 2;
        if parent == 1 {
            total_entries += 1;
        }
        
        // Short-circuit: si ya consumieron todo, retornar vacío sin consultar DB
        if offset as u64 >= total_entries {
            tracing::trace!("📊 readdir short-circuit: offset={} >= total={}", offset, total_entries);
            return Ok(ReplyDirectory {
                entries: Box::pin(stream::empty())
            });
        }

        // 3. Construir lista completa SIEMPRE (. y .. + hijos + SHARED)
        let mut entries: Vec<(u64, String, bool)> = Vec::with_capacity(children.len() + 3);
        entries.push((parent, ".".to_string(), true));
        entries.push((if parent == SHARED_INODE { 1 } else { 1.max(parent) }, "..".to_string(), true));
        
        if parent == 1 {
            entries.push((SHARED_INODE, "SHARED".to_string(), true));
        }

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
        
        // Caso especial: Lookup de SHARED en el root
        if parent == 1 && name_str == "SHARED" {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            
            return Ok(ReplyEntry {
                ttl: Duration::from_secs(3600),
                attr: FileAttr {
                    ino: SHARED_INODE,
                    size: 4096,
                    blocks: 8,
                    atime: Timestamp::new(now, 0),
                    mtime: Timestamp::new(now, 0),
                    ctime: Timestamp::new(now, 0),
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: unsafe { libc::getuid() },
                    gid: unsafe { libc::getgid() },
                    rdev: 0,
                    blksize: 4096,
                },
                generation: 0,
            });
        }

        // Para archivos Workspace, el usuario busca con .html pero en DB está sin extensión
        let (lookup_name, is_html_lookup) = if name_str.ends_with(".html") {
            (name_str.trim_end_matches(".html"), true)
        } else {
            (name_str, false)
        };

        // Consultar la base de datos
        // Si el padre es SHARED_INODE, buscamos en el root (1) pero verificamos que sea SHARED
        let search_parent = if parent == SHARED_INODE { 1 } else { parent };

        let inode = self.db.lookup(search_parent, lookup_name)
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

        // Lógica de visibilidad
        if parent == 1 && !attrs.owned_by_me {
            // Un archivo compartido en el root NO debe ser visible mediante lookup directo en /
            return Err(Errno::from(libc::ENOENT));
        }

        if parent == SHARED_INODE && attrs.owned_by_me {
            // Un archivo PROPIO no debe ser visible en SHARED/
            return Err(Errno::from(libc::ENOENT));
        }

        let mut file_attr = attrs.to_file_attr();

        // Si es lookup de archivo Workspace (.html), ajustar tamaño al HTML generado
        if is_html_lookup {
            if let Some(ref mime) = attrs.mime_type {
                if shortcuts::is_workspace_file(mime) {
                    let gdrive_id = self.get_gdrive_id(inode).await
                        .unwrap_or_else(|_| "unknown".to_string());
                    let html_content = shortcuts::generate_desktop_entry(&gdrive_id, lookup_name, mime);
                    file_attr.size = html_content.len() as u64;
                    file_attr.perm = 0o644;
                }
            }
        }



        // DIAGNOSTIC: Log detail for media files to check why OPEN is not called
        // We restore this to confirm if the kernel even LOOKUPS the file.
        if name_str.ends_with(".mp3") || name_str.ends_with(".mkv") || name_str.ends_with(".mp4") {
            //  tracing::warn!("🔎 LOOKUP MEDIA: name={} inode={} size={} perm={:o} kind={:?}", 
            //                name_str, inode, file_attr.size, file_attr.perm, file_attr.kind);
        }

        // tracing::info!("✅ LOOKUP success: parent={} name={} -> inode={} size={} perm={:o} kind={:?}", parent, name_str, inode, file_attr.size, file_attr.perm, file_attr.kind);

        Ok(ReplyEntry {
            ttl: Duration::from_secs(1),
            attr: file_attr,
            generation: 0,
        })
    }

    // Obtener atributos de un archivo (stat)
    async fn getattr(&self, _req: Request, inode: u64, _fh: Option<u64>, _flags: u32) -> Result<ReplyAttr> {
        // tracing::info!("📋 GETATTR called: inode={}", inode);

        // Caso especial: Inodo virtual SHARED
        if inode == SHARED_INODE {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            
            let attr = FileAttr {
                ino: SHARED_INODE,
                size: 4096,
                blocks: 8,
                atime: Timestamp::new(now, 0),
                mtime: Timestamp::new(now, 0),
                ctime: Timestamp::new(now, 0),
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                blksize: 4096,
            };

            return Ok(ReplyAttr {
                ttl: Duration::from_secs(3600), // Directorio virtual estable
                attr,
            });
        }

        let attrs = self.db.get_attrs(inode)
            .await
            .map_err(|e| {
                // Si el inodo es 1 (root) y no está en DB, devolver valores por defecto
                if inode == 1 {
                    tracing::trace!("Devolviendo atributos raíz por defecto");
                    return Errno::from(libc::ENOENT);
                }
                error!("Error en getattr para inode {}: {}", inode, e);
                Errno::from(libc::ENOENT)
            })?;

        let is_audio = attrs.mime_type.as_deref().map(|m| m.starts_with("audio/")).unwrap_or(false);
        if is_audio {
            tracing::warn!("📋 GETATTR for AUDIO: inode={} size={} perm={:o}", inode, attrs.size, attrs.mode);
        }

        // Si es archivo Workspace, ajustar el tamaño reportado al tamaño del HTML
        let mut file_attr = attrs.to_file_attr();
        
        if let Some(ref mime) = attrs.mime_type {
            if shortcuts::is_workspace_file(mime) {
                let name = self.get_file_name(inode).await
                    .unwrap_or_else(|_| "Documento de Google".to_string());
                let gdrive_id = self.get_gdrive_id(inode).await
                    .unwrap_or_else(|_| "unknown".to_string());
                    
                let html_content = shortcuts::generate_desktop_entry(
                    &gdrive_id,
                    &name,
                    mime
                );
                file_attr.size = html_content.len() as u64;
                // HTML no requiere permisos ejecutables
                file_attr.perm = 0o644;
                tracing::trace!("Workspace File (getattr): inode={} size={}", inode, file_attr.size);
            }
        }

        Ok(ReplyAttr {
            ttl: Duration::from_secs(1),
            attr: file_attr,
        })
    }
    
    // Validar permisos de acceso (access)
    async fn access(&self, _req: Request, _inode: u64, mask: u32) -> Result<()> {
        if mask == 0 {
            return Ok(()); // F_OK check, ignore to reduce noise if needed, or log it.
        }
        // tracing::warn!("🛡️ WARNING access check: inode={} mask={:o}", inode, mask);
        // Permitimos todo explícitamente para evitar problemas de permisos de kernel/sandbox
        Ok(())
    }

    // XATTR Support (Crucial for some players/Nautilus)
    async fn getxattr(
        &self,
        _req: Request,
        inode: u64,
        name: &OsStr,
        _size: u32,
    ) -> Result<ReplyXAttr> {
        let name_str = name.to_str().unwrap_or("???");
        tracing::debug!("🏷️ getxattr called: inode={} name={}", inode, name_str);
        // Retornar ENODATA (No attribute) en lugar de ENOSYS (Not implemented)
        // Muchas apps fallan si reciben ENOSYS.
        Err(Errno::from(libc::ENODATA))
    }

    async fn listxattr(
        &self,
        _req: Request,
        inode: u64,
        _size: u32,
    ) -> Result<ReplyXAttr> {
        tracing::debug!("🏷️ listxattr called: inode={}", inode);
        // Retornar lista vacía (0 bytes) - ReplyXAttr es un Enum
        if _size == 0 {
             Ok(ReplyXAttr::Size(0))
        } else {
             Ok(ReplyXAttr::Data(vec![].into()))
        }
    }

    async fn setxattr(
        &self,
        _req: Request,
        inode: u64,
        name: &OsStr,
        _value: &[u8],
        _flags: u32,
        _position: u32,
    ) -> Result<()> {
        let name_str = name.to_str().unwrap_or("???");
        tracing::warn!("🏷️ setxattr called (IGNORED): inode={} name={}", inode, name_str);
        // Ignorar silenciosamente o dar error de permiso?
        // Responder Ok() engaña a la app pensando que guardó metadata.
        Ok(())
    }

    async fn removexattr(
        &self,
        _req: Request,
        inode: u64,
        name: &OsStr,
    ) -> Result<()> {
         let name_str = name.to_str().unwrap_or("???");
         tracing::debug!("🏷️ removexattr called: inode={} name={}", inode, name_str);
         Err(Errno::from(libc::ENODATA))
    }


    // Abrir directorio (requerido antes de readdir)
    async fn opendir(&self, _req: Request, inode: u64, _flags: u32) -> Result<ReplyOpen> {
        tracing::trace!("📂 opendir: inode={}", inode);
        
        // Caso especial: SHARED
        if inode == SHARED_INODE {
            return Ok(ReplyOpen { fh: 0, flags: 0 });
        }

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
        
        // tracing::warn!("🔓 OPEN request: inode={} flags={}", inode, flags);

        // Validar que existe en DB y obtener metadatos
        let attrs = match self.db.get_attrs(inode).await {
            Ok(a) => a,
            Err(e) => {
                tracing::error!("❌ OPEN failed: attributes not found for inode {}: {}", inode, e);
                return Err(Errno::from(libc::ENOENT));
            }
        };

        // Filtered detail logging
        let mime_lower = attrs.mime_type.as_deref().unwrap_or("").to_lowercase();
        let is_media = mime_lower.starts_with("video/") || mime_lower.starts_with("audio/");
        
        if is_media {
             tracing::warn!("🎬 OPEN media detected: inode={} mime={} size={}", inode, mime_lower, attrs.size);
        }

        // SMART PREFETCH (Lazy Eval):
        // Registramos que el archivo fue abierto. No iniciaremos la descarga agresiva
        // inmediatamente, ya que thumbnailers abren el archivo pero nunca leen 
        // volumen real de datos. read() se encargará de promocionarlo a stream oficial.
        let is_workspace = attrs.mime_type.as_deref().map(shortcuts::is_workspace_file).unwrap_or(false);

        if attrs.size > 0 && !is_workspace {
            // Guard: No reintentar descargas que ya fallaron con 403
            if self.failed_downloads.contains(&inode) {
                tracing::debug!("🚫 open() ignorado para inode={} (descarga 403 permanente)", inode);
                return Ok(ReplyOpen { fh: 0, flags: 0 });
            }

            // Sync FD tracking
            let mut f_dls = self.fuse_downloads.lock().await;
            if let Some((_, count, _)) = f_dls.get_mut(&inode) {
                *count += 1;
            } else {
                f_dls.insert(inode, (None, 1, 0));
            }
        }
        
        Ok(ReplyOpen { fh: 0, flags: 0 }) 
    }

    // Cerrar archivo (release)
    async fn release(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        tracing::trace!("release: inode={}", inode);
        
        let mut fuse_downloads = self.fuse_downloads.lock().await;
        let mut should_remove = false;
        let mut completed_transfer_id = None;
        if let Some(entry) = fuse_downloads.get_mut(&inode) {
            entry.1 = entry.1.saturating_sub(1);
            if entry.1 == 0 {
                should_remove = true;
                completed_transfer_id = entry.0;
            }
        }
        if should_remove {
            fuse_downloads.remove(&inode);
            if let Some(t_id) = completed_transfer_id {
                self.history.complete_transfer(t_id);
                tracing::debug!("✅ Transfer FUSE completado y removido de la interfaz: inode={}", inode);
            } else {
                tracing::debug!("✅ Archivo cerrado sin iniciar streaming (Thumbnailer/Metadata): inode={}", inode);
            }
        }

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
        tracing::trace!("flush: inode={}", inode);
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
        tracing::trace!("fsync: inode={}", inode);
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
        // 1. Obtener el gdrive_id del archivo, mime_type y tamaño PRIMERO para logging
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

        let is_audio = mime_type.as_deref().map(|m| m.starts_with("audio/")).unwrap_or(false);
        if is_audio {
             tracing::warn!("📖 READ called for AUDIO: inode={} offset={} size={}", inode, offset, size);
        } else {
             // tracing::info!("📖 READ called: inode={} offset={} size={}", inode, offset, size);
        }

        // GUARDAR OFFSET DE LECTURA para el Smart Streamer
        self.read_offsets.insert(inode, offset + size as u64);

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
        let is_workspace = mime_type.as_deref().map(shortcuts::is_workspace_file).unwrap_or(false);
        
        // 3a. Asegurar que el rango solicitado esté disponible (Solo si no es Workspace Docs)
        if file_size > 0 && !is_workspace {
            // Guard: No reintentar descargas que ya fallaron con 403
            if self.failed_downloads.contains(&inode) {
                tracing::debug!("🚫 read() bloqueado para inode={} (descarga 403 permanente)", inode);
                return Err(Errno::from(libc::EIO));
            }

            // --- HEURÍSTICA DE VOLUMEN (Smart Streamer Lazy Trigger) ---
            // Si el volumen ACUMULADO de lecturas para este descriptor supera 1MB, oficializamos el "Stream".
            // Esto descarta a thumbnailers que saltan rápido por todo el archivo buscando XREFs de PDFs.
            let stream_threshold = 1024 * 1024; // 1 MB
            let is_media = mime_type.as_deref().map(|m| m.starts_with("video/") || m.starts_with("audio/")).unwrap_or(false);
            
            let mut trigger_smart_stream = false;
            
            {
                let mut f_dls = self.fuse_downloads.lock().await;
                if let Some((t_id_opt, _, session_bytes)) = f_dls.get_mut(&inode) {
                    *session_bytes += size as u64; // Acumular bytes leídos
                    
                    if *session_bytes >= stream_threshold && t_id_opt.is_none() {
                        trigger_smart_stream = true;
                        
                        // Oficializamos la transferencia
                        let db = self.db.clone();
                        let file_name = sqlx::query_scalar::<_, String>("SELECT name FROM dentry WHERE child_inode = ? LIMIT 1")
                            .bind(inode as i64).fetch_optional(db.pool()).await.unwrap_or_default().unwrap_or_else(|| format!("file_{}", inode));
                        
                        let op = if is_media { TransferOp::Stream } else { TransferOp::Download };
                        let new_t_id = self.history.start_transfer(&file_name, op, file_size as u64);
                        *t_id_opt = Some(new_t_id);
                    }
                }
            }
            
            if trigger_smart_stream {
                // Lanzar el streamer
                let drive_client = self.drive_client.clone();
                let cache_path_bg = cache_path.clone();
                let file_locks = self.file_locks.clone();
                let history = self.history.clone();
                let fuse_downloads_clone = self.fuse_downloads.clone();
                let failed_downloads = self.failed_downloads.clone();
                let read_offsets = self.read_offsets.clone();
                let gd_id_bg = gdrive_id.clone();
                let db_clone = self.db.clone();

                tokio::spawn(async move {
                    let result = Self::start_background_download_stream(
                        db_clone, drive_client, inode, gd_id_bg, cache_path_bg, file_size as u64,
                        file_locks, history, fuse_downloads_clone, read_offsets, is_media
                    ).await;

                    if let Err(ref e) = result {
                        let err_msg = format!("{}", e);
                        if err_msg.contains("403") && err_msg.contains("cannot") {
                            failed_downloads.insert(inode);
                        }
                    }
                });
                tracing::info!("🚀 Heurística de volumen disparada (>1MB reales leídos). Smart Streamer iniciado para inode={}", inode);
            }

            // Descargar solo lo necesario (con reintento tras corrección 416)
            let mut effective_file_size = file_size as u64;
            let mut attempt = 0u8;
            loop {
                match self.ensure_range_cached(inode, &gdrive_id, offset, size, effective_file_size).await {
                    Ok(()) => break,
                    Err(e) => {
                        let err_msg = format!("{}", e);
                        if err_msg.contains("416") && attempt == 0 {
                            // ensure_range_cached ya corrigió attrs.size en DB. Re-leer y reintentar.
                            if let Ok(new_size) = sqlx::query_scalar::<_, i64>(
                                "SELECT size FROM attrs WHERE inode = ?"
                            )
                            .bind(inode as i64)
                            .fetch_one(self.db.pool())
                            .await
                            {
                                effective_file_size = new_size as u64;
                                if effective_file_size == 0 || offset >= effective_file_size {
                                    return Ok(ReplyData { data: vec![].into() });
                                }
                                tracing::info!("🔄 Reintentando descarga para inode {} con tamaño corregido: {}", inode, effective_file_size);
                                attempt += 1;
                                continue;
                            }
                        }
                        if err_msg.contains("403") && err_msg.contains("cannot") {
                            self.failed_downloads.insert(inode);
                            tracing::warn!("🚫 Inode {} marcado como descarga prohibida (403 en read)", inode);
                        }
                        error!("Error descargando chunk para inode {}: {}", inode, e);
                        return Err(Errno::from(libc::EIO));
                    }
                }
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
        
        // 1. Carga de datos
        let (children, child_count) = if parent == SHARED_INODE {
             let items = db.list_non_owned_root_children().await
                .map_err(|e| {
                    error!("❌ Error listando compartidos (plus): {}", e);
                    Errno::from(libc::EIO)
                })?;
            let count = items.len() as u64;
            (items, count)
        } else {
            let mut items = match db.list_children_extended(parent).await {
                Ok(c) => c,
                Err(e) => {
                    error!("❌ Error listando hijos de {}: {}", parent, e);
                    return Err(Errno::from(libc::EIO));
                }
            };

            // Filtrar si es root
            if parent == 1 {
                let mut filtered = Vec::new();
                for item in items {
                    let attrs = db.get_attrs(item.0).await.map_err(|_| Errno::from(libc::EIO))?;
                    if attrs.owned_by_me {
                        filtered.push(item);
                    }
                }
                items = filtered;
            }

            let real_count = items.len() as u64;
            (items, real_count)
        };
        
        // Total = hijos + 2 (por . y ..) + (1 si es root por el SHARED)
        let mut total_entries = child_count + 2;
        if parent == 1 {
            total_entries += 1;
        }
        
        // Short-circuit: si ya consumieron todo, retornar vacío sin consultar DB
        if offset >= total_entries {
            tracing::trace!("📊 readdirplus short-circuit: offset={} >= total={}", offset, total_entries);
            return Ok(ReplyDirectoryPlus {
                entries: Box::pin(stream::empty())
            });
        }

        // 3. Construir lista completa SIEMPRE (. y .. + hijos + SHARED)
        let mut final_entries: Vec<(u64, String, bool, Option<String>, Option<String>)> = 
            Vec::with_capacity(children.len() + 3);
        final_entries.push((parent, ".".to_string(), true, None, None));
        final_entries.push((if parent == SHARED_INODE { 1 } else { 1.max(parent) }, "..".to_string(), true, None, None));

        if parent == 1 {
            final_entries.push((SHARED_INODE, "SHARED".to_string(), true, None, None));
        }

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
                    let mut attr = if inode == SHARED_INODE {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64;
                        FileAttr {
                            ino: SHARED_INODE,
                            size: 4096,
                            blocks: 8,
                            atime: Timestamp::new(now, 0),
                            mtime: Timestamp::new(now, 0),
                            ctime: Timestamp::new(now, 0),
                            kind: FileType::Directory,
                            perm: 0o755,
                            nlink: 2,
                            uid: unsafe { libc::getuid() },
                            gid: unsafe { libc::getgid() },
                            rdev: 0,
                            blksize: 4096,
                        }
                    } else if let Ok(a) = db_clone.get_attrs(inode).await {
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
                            can_move: true,
                            shared: false,
                            owned_by_me: true,
                        }.to_file_attr()
                    };

                    // Ajustar nombre y tamaño para archivos Workspace - SOLO para ARCHIVOS, no carpetas
                    // Añadimos .html porque Nautilus 3.30+ abre .desktop desde FUSE como texto
                    let mut display_name = name.clone();
                    if !is_dir && inode != SHARED_INODE {
                        if let (Some(m), Some(gid)) = (&mime, &gdrive_id) {
                            if shortcuts::is_workspace_file(m) {
                                display_name = format!("{}.html", name);
                                let html_content = shortcuts::generate_desktop_entry(gid, &name, m);
                                attr.size = html_content.len() as u64;
                                attr.perm = 0o644; // HTML no necesita +x
                                tracing::trace!("Workspace File (readdirplus): inode={} name={} size={}", inode, display_name, attr.size);
                            }
                        }
                    }

                    Ok(DirectoryEntryPlus {
                        inode,
                        generation: 0,
                        kind: if is_dir || inode == SHARED_INODE { FileType::Directory } else { FileType::RegularFile },
                        name: display_name.into(),
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
        _flags: u32,
    ) -> Result<ReplyCreated> {
        let name_str = name.to_str().ok_or(Errno::from(libc::EINVAL))?;
        tracing::info!("📝 CREATE request: parent={} name={} mode={:o}", parent, name_str, mode);

        // Caso especial: SHARED es de solo lectura
        if parent == SHARED_INODE {
            return Err(Errno::from(libc::EROFS));
        }

        // Generar un gdrive_id temporal para el nuevo archivo (será reemplazado al subir)
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
            true, // can_move
            false, // shared (inicialmente falso)
            true, // owned_by_me (archivos creados localmente)
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

        // Marcar como dirty y burbujear estado a ancestros
        self.db.set_dirty_and_bubble(inode).await
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

    // Crear un nuevo directorio
    async fn mkdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        let name_str = name.to_str().ok_or(Errno::from(libc::EINVAL))?;
        debug!("📂 mkdir: parent={} name={} mode={:o}", parent, name_str, mode);

        // Caso especial: SHARED es de solo lectura
        if parent == SHARED_INODE {
            return Err(Errno::from(libc::EROFS));
        }

        // Generar un gdrive_id temporal
        let temp_gdrive_id = format!("temp_{}", uuid::Uuid::new_v4());
        
        // Crear inode en la DB
        let inode = self.db.get_or_create_inode(&temp_gdrive_id).await
            .map_err(|e| {
                error!("Error creando inode para directorio: {}", e);
                Errno::from(libc::EIO)
            })?;

        // Timestamp actual
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
    
        // Insertar metadatos del directorio
        // NOTA: Para directorios mode suele ser S_IFDIR | 0755
        let dir_mode = libc::S_IFDIR as u32 | 0o755;

        self.db.upsert_file_metadata(
            inode,
            0, // size 0 para directorios
            now,
            dir_mode,
            true, // is_dir = true
            Some("application/vnd.google-apps.folder"),
            true, // can_move
            false, // shared
            true, // owned_by_me
        ).await.map_err(|e| {
            error!("Error insertando metadatos de directorio: {}", e);
            Errno::from(libc::EIO)
        })?;

        // Agregar al dentry
        self.db.upsert_dentry(parent, inode, name_str).await
            .map_err(|e| {
                error!("Error insertando dentry de directorio: {}", e);
                Errno::from(libc::EIO)
            })?;

        // Marcar como dirty (pendiente de creación en GDrive)
        // Directorios: set_dirty_and_bubble no burbujea para is_dir=true (correcto)
        self.db.set_dirty_and_bubble(inode).await
            .map_err(|e| {
                error!("Error marcando directorio como dirty: {}", e);
                Errno::from(libc::EIO)
            })?;
        // Asegurar que el nuevo directorio tiene fila en dir_counters
        self.db.ensure_dir_counter(inode).await
            .map_err(|e| {
                error!("Error inicializando dir_counter: {}", e);
                Errno::from(libc::EIO)
            })?;

        let attrs = self.db.get_attrs(inode).await
            .map_err(|_| Errno::from(libc::EIO))?;

        debug!("✅ Directorio creado: inode={} nombre={}", inode, name_str);

        Ok(ReplyEntry {
            ttl: Duration::from_secs(1),
            attr: attrs.to_file_attr(),
            generation: 0,
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
        tracing::trace!("✏️ write: inode={} offset={} size={}", inode, offset, data.len());

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

        // Marcar como dirty y burbujear estado
        self.db.set_dirty_and_bubble(inode).await
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

            // Marcar como dirty y burbujear estado
            self.db.set_dirty_and_bubble(inode).await
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
        tracing::info!("🗑️ UNLINK: parent={} name={}", parent, name_str);

        // Caso especial: SHARED es de solo lectura
        if parent == SHARED_INODE {
            return Err(Errno::from(libc::EROFS));
        }

        // 1. Resolver el archivo para obtener su inode
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

        // Marcar como dirty y burbujear (soft_delete_by_gdrive_id ya burbujea internamente,
        // pero el set_dirty aquí es para el caso donde no hubo soft_delete recursivo)
        self.db.set_dirty_and_bubble(inode).await
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
        let name_str = name.to_str().unwrap_or("???");
        let new_name_str = new_name.to_str().unwrap_or("???");
        tracing::info!("🔄 RENAME: parent={} name={} -> new_parent={} new_name={}", 
                      parent, name_str, new_parent, new_name_str);

        // Caso especial: SHARED es de solo lectura
        if parent == SHARED_INODE || new_parent == SHARED_INODE {
            return Err(Errno::from(libc::EROFS));
        }

        // 1. Obtener inode origen
        let inode = self.db.lookup(parent, name_str).await
            .map_err(|_| Errno::from(libc::EIO))?
            .ok_or(Errno::from(libc::ENOENT))?;

        // VERIFICACIÓN DE PERMISOS (Blocking at Source)
        // Verificar si tenemos permiso para mover este archivo en Google Drive
        let attrs = self.db.get_attrs(inode).await
            .map_err(|_| Errno::from(libc::EIO))?;

        if !attrs.can_move {
            tracing::warn!("⛔ Bloqueando movimiento de archivo de solo lectura (Shared): {}", name_str);
            return Err(Errno::from(libc::EACCES));
        }

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

        // Burbujeo para rename/move
        let is_dir: Option<bool> = sqlx::query_scalar::<_, bool>(
            "SELECT is_dir FROM attrs WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| Errno::from(libc::EIO))?;

        if parent != new_parent {
            // Mover entre directorios: transferir contadores
            if is_dir == Some(true) {
                // Mover un directorio: transferir sus contadores de descendientes
                let counters = sqlx::query_as::<_, (i64, i64)>(
                    "SELECT dirty_desc_count, synced_desc_count FROM dir_counters WHERE inode = ?"
                )
                .bind(inode as i64)
                .fetch_optional(self.db.pool())
                .await
                .map_err(|_| Errno::from(libc::EIO))?;

                if let Some((dirty, synced)) = counters {
                    // Decrementar ancestros del viejo padre
                    let _ = self.db.bubble_state_change(inode, -(dirty as i32), -(synced as i32)).await;
                    // Ahora la dentry apunta al nuevo padre, re-burbujear
                    let _ = self.db.bubble_state_change(inode, dirty as i32, synced as i32).await;
                }
            } else if is_dir == Some(false) {
                // Mover un archivo: determinar su estado y transferir
                let state = sqlx::query_as::<_, (Option<bool>, Option<i64>)>(
                    "SELECT dirty, deleted_at FROM sync_state WHERE inode = ?"
                )
                .bind(inode as i64)
                .fetch_optional(self.db.pool())
                .await
                .map_err(|_| Errno::from(libc::EIO))?;

                let (d_dirty, d_synced) = match state {
                    Some((dirty, deleted_at)) => {
                        let was_dirty = dirty.unwrap_or(false) || deleted_at.map(|v| v > 0).unwrap_or(false);
                        if was_dirty { (1i32, 0i32) } else { (0, 1) }
                    }
                    None => (0, 0),
                };
                // La dentry ya apunta al nuevo padre, así que bubble_state_change
                // actuará sobre los nuevos ancestros. Necesitamos corregir los viejos manualmente.
                // Usamos un UPDATE directo sobre los ancestros del viejo parent.
                if d_dirty != 0 || d_synced != 0 {
                    // Decrementar viejo padre y sus ancestros
                    sqlx::query(
                        r#"
                        WITH RECURSIVE ancestors AS (
                            SELECT ?1 as anc_inode
                            UNION ALL
                            SELECT d.parent_inode FROM dentry d
                            JOIN ancestors a ON d.child_inode = a.anc_inode
                            WHERE a.anc_inode > 0
                        )
                        UPDATE dir_counters
                        SET dirty_desc_count = MAX(0, dirty_desc_count - ?2),
                            synced_desc_count = MAX(0, synced_desc_count - ?3)
                        WHERE inode IN (SELECT anc_inode FROM ancestors)
                        "#
                    )
                    .bind(parent as i64)
                    .bind(d_dirty)
                    .bind(d_synced)
                    .execute(self.db.pool())
                    .await
                    .map_err(|_| Errno::from(libc::EIO))?;

                    // Incrementar nuevo padre y sus ancestros
                    sqlx::query(
                        r#"
                        WITH RECURSIVE ancestors AS (
                            SELECT ?1 as anc_inode
                            UNION ALL
                            SELECT d.parent_inode FROM dentry d
                            JOIN ancestors a ON d.child_inode = a.anc_inode
                            WHERE a.anc_inode > 0
                        )
                        UPDATE dir_counters
                        SET dirty_desc_count = dirty_desc_count + ?2,
                            synced_desc_count = synced_desc_count + ?3
                        WHERE inode IN (SELECT anc_inode FROM ancestors)
                        "#
                    )
                    .bind(new_parent as i64)
                    .bind(d_dirty)
                    .bind(d_synced)
                    .execute(self.db.pool())
                    .await
                    .map_err(|_| Errno::from(libc::EIO))?;
                }
            }
        }

        // Marcar como dirty para sincronizar el cambio de nombre
        self.db.set_dirty_and_bubble(inode).await
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
        
        // SMART BURST: Alinear el rango solicitado a bloques de 2MB para evitar micro-descargas asfixiantes
        const BURST_SIZE: u64 = 2 * 1024 * 1024; // 2MB
        let aligned_start = (offset / BURST_SIZE) * BURST_SIZE;
        let aligned_end_raw = ((offset + size as u64 + BURST_SIZE - 1) / BURST_SIZE) * BURST_SIZE - 1;
        let aligned_end = aligned_end_raw.min(file_size.saturating_sub(1));

        let requested_start = aligned_start;
        let requested_end = aligned_end;
        
        if requested_start >= file_size {
            return Ok(()); // Fuera de rango, nada que hacer
        }

        let cache_path = self.get_cache_path(gdrive_id);
        
        // NOTA: Se ha eliminado la optimización por tamaño (file_size) porque es insegura
         // ZOMBIE / CORRUPTION CHECK:
         // Verificar consistencia entre DB y Archivo Físico.
         if cache_path.exists() {
             let has_chunks = self.db.has_any_chunks(inode).await.unwrap_or(false);
             let meta = tokio::fs::metadata(&cache_path).await;
             let file_size = meta.map(|m| m.len()).unwrap_or(0);
             let max_cached = self.db.get_max_cached_offset(inode).await.unwrap_or(0);

             tracing::trace!("🔍 Zombie Check: inode={} exists=true size={} max_cached_chunk={} has_chunks={}", 
                           inode, file_size, max_cached, has_chunks);
             
             // Caso 1: Archivo existe pero DB dice que no hay chunks -> Zombie (borrar archivo)
             if !has_chunks {
                 tracing::warn!("🧟 Zombie cache detected for inode {}: Deleting corrupt/stale file.", inode);
                 let _ = tokio::fs::remove_file(&cache_path).await;
             } 
             // Caso 2: DB tiene chunks, pero el archivo es más pequeño que el chunk más lejano
             // Esto indica truncamiento o corrupción (ej: file_size=79KB pero tenemos chunk hasta 1MB)
             else if has_chunks && file_size < max_cached {
                  tracing::error!("💀 CORRUPTED cache detected for inode {}: File truncated! (Size={} < DB_Max={}). PURGING.", 
                                 inode, file_size, max_cached);
                  let _ = tokio::fs::remove_file(&cache_path).await;
                  let _ = self.db.clear_chunks(inode).await;
             }
         } else {
              // tracing::info!("🔍 Zombie Check: inode={} exists=false", inode);
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

        // Asegurar que el archivo existe (usando OpenOptions para NO truncar si ganó la carrera el prefetch)
        if !cache_path.exists() {
             let _ = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&cache_path)
                .await;
        }

        // OPTIMIZACIÓN: Descargar todos los rangos EN PARALELO
        tracing::info!("📥 Descargando {} chunks faltantes en paralelo para inode {}",
                       missing_ranges.len(), inode);

        // --- FUSE DOWNLOAD PROGRESS TRACKING ---
        let transfer_id;
        let mut fuse_downloads = self.fuse_downloads.lock().await;

        if let Some((existing_t_id, _, _)) = fuse_downloads.get(&inode) {
            // Ya hay un transfer en curso
            transfer_id = *existing_t_id;
        } else {
            // Registrar nuevo transfer de FUSE Download
            let file_name = self.get_file_name(inode).await.unwrap_or_else(|_| format!("file_{}", inode));
            
            let t_id = self.history.start_transfer(
                &file_name,
                TransferOp::Download,
                file_size
            );
            fuse_downloads.insert(inode, (Some(t_id), 1, 0));
            transfer_id = Some(t_id);
        }

        // Actualizar el progreso inicial
        if let Some(t_id) = transfer_id {
            if let Ok(cached_bytes) = self.db.get_cached_bytes_count(inode).await {
                self.history.update_transfer_progress(t_id, cached_bytes);
            }
        }
        
        drop(fuse_downloads); // Liberar lock antes del stream

        let drive_client = self.drive_client.clone();
        let db = self.db.clone();
        let gdrive_id_owned = gdrive_id.to_string();
        let cache_path_owned = cache_path.clone();
        let history = self.history.clone();

        // Spawn tasks para descargar cada rango en paralelo
        let download_tasks: Vec<_> = missing_ranges.into_iter().map(|(start, end)| {
            let drive_client = drive_client.clone();
            let db = db.clone();
            let gdrive_id = gdrive_id_owned.clone();
            let cache_path = cache_path_owned.clone();
            let history = history.clone();

            let file_locks_clone = self.file_locks.clone();

            tokio::spawn(async move {
                let chunk_size = (end - start + 1) as u32;
                
                tracing::debug!("📥 Descargando chunk: inode={} range={}-{} ({} bytes)", 
                               inode, start, end, chunk_size);
                
                // Descargar chunk
                let data = drive_client.download_chunk(&gdrive_id, start, chunk_size).await?;
                
                // OBTENER LOCK ANTES DE MUTAR EL ARCHIVO CONJUNTO
                let inode_lock = file_locks_clone
                    .entry(inode)
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                    .clone();
                    
                let _guard = inode_lock.lock().await;

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

                // Actualizar progreso visible en GUI
                if let Some(t_id) = transfer_id {
                    let cached_bytes = db.get_cached_bytes_count(inode).await.unwrap_or(0);
                    history.update_transfer_progress(t_id, cached_bytes);
                }

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
                Ok(Err(e)) => {
                    let err_msg = format!("{}", e);
                    if err_msg.contains("416") {
                        // 416 Range Not Satisfiable: el tamaño real en Drive difiere del registrado en DB.
                        // Corregir attrs.size, invalidar caché obsoleto y dejar que el kernel reintente.
                        tracing::warn!("🔄 416 detectado para inode {}: refrescando tamaño desde Drive", inode);
                        if let Ok(remote_file) = self.drive_client.get_file_metadata(gdrive_id).await {
                            let real_size = remote_file.size.unwrap_or(0);
                            let _ = sqlx::query("UPDATE attrs SET size = ? WHERE inode = ?")
                                .bind(real_size)
                                .bind(inode as i64)
                                .execute(self.db.pool())
                                .await;

                            // Invalidar chunks y caché obsoletos para que el reintento descargue limpio
                            let _ = self.db.clear_chunks(inode).await;
                            let _ = tokio::fs::remove_file(&cache_path_owned).await;

                            tracing::info!("✅ attrs.size corregido a {} para inode {} (caché invalidado)", real_size, inode);
                            if real_size == 0 {
                                return Ok(());
                            }
                        }
                    }
                    return Err(e);
                },
                Err(e) => return Err(anyhow::anyhow!("Task panicked: {}", e)),
            }
        }

        tracing::info!("✅ Todos los chunks descargados para inode {}", inode);
        Ok(())
    }



    /// Pre-descarga un archivo completo en background (para archivos pequeños)
    #[allow(dead_code)]
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
        
        // Crear el archivo de caché (sin truncar si ya existe)
        let _ = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(cache_path)
            .await?;
        
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

    /// Descarga continua y agresiva de un archivo en background (Para maximizar el ancho de banda)
    async fn start_background_download_stream(
        db: Arc<MetadataRepository>,
        drive_client: Arc<DriveClient>,
        inode: u64,
        gdrive_id: String,
        cache_path: std::path::PathBuf,
        file_size: u64,
        file_locks: Arc<DashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
        history: Arc<ActionHistory>,
        fuse_downloads_map: Arc<tokio::sync::Mutex<HashMap<u64, (Option<u64>, usize, u64)>>>,
        read_offsets: Arc<DashMap<u64, u64>>,
        is_media: bool,
    ) -> anyhow::Result<()> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        // --- QUICK CACHE CHECK ---
        // Verificar instantáneamente si el archivo ya está 100% descargado
        if file_size > 0 {
            let total_missing = db.get_missing_ranges(inode, 0, file_size - 1).await.unwrap_or_default();
            if total_missing.is_empty() {
                // El archivo ya está local, ni siquiera iniciamos el streamer o creamos transfer.
                tracing::debug!("✅ Archivo ({}) previamente cacheado. Streaming abortado.", file_size);
                return Ok(());
            }
        }

        // Crear directorio de caché si no existe
        if let Some(parent) = cache_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Asegurar que el archivo existe
        if !cache_path.exists() {
             let _ = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&cache_path)
                .await;
        }

        // Obtener nombre del archivo para el transfer
        let file_name = sqlx::query_scalar::<_, String>(
            "SELECT name FROM dentry WHERE child_inode = ? LIMIT 1"
        )
        .bind(inode as i64)
        .fetch_optional(db.pool())
        .await?
        .unwrap_or_else(|| format!("file_{}", inode));

        // --- FUSE DOWNLOAD PROGRESS TRACKING ---
        // La transferencia ya fue inicializada oficialmente por la heurística de read()
        let mut transfer_id = None;
        {
            let f_dls = fuse_downloads_map.lock().await;
            if let Some((Some(t_id), _, _)) = f_dls.get(&inode) {
                transfer_id = Some(*t_id);
            }
        }

        if let Some(t_id) = transfer_id {
            if let Ok(cached_bytes) = db.get_cached_bytes_count(inode).await {
                history.update_transfer_progress(t_id, cached_bytes);
            }
        }

        tracing::info!("🚀 Iniciando {} inteligente para: {} ({} bytes)", 
                      if is_media { "streaming" } else { "descarga" }, file_name, file_size);

        // Bloques grandes para maximizar la velocidad (2MB)
        const CHUNK_SIZE: u64 = 2 * 1024 * 1024; 
        const MAX_CONCURRENT: usize = 4;
        const MEDIA_PREFETCH_LIMIT: u64 = 20 * 1024 * 1024; // 20MB de buffer para media
        
        let mut _iteration = 0;
        
        // El Smart Streamer itera buscando qué descargar basándose en el puntero de lectura
        loop {
            // VERIFICACIÓN ZOMBI: Asegurar que FUSE todavía tiene abierto este archivo
            let is_active = fuse_downloads_map.lock().await.contains_key(&inode);
            if !is_active {
                tracing::debug!("El inode {} fue cerrado. Abortando streamer background (zombi dead).", inode);
                break;
            }

            // 1. Determinar el inicio de la descarga (basado en el offset actual del usuario o 0)
            let user_offset = *read_offsets.get(&inode).as_deref().unwrap_or(&0);
            
            // 2. Buscar rangos faltantes a partir del offset del usuario
            let missing_ranges = db.get_missing_ranges(inode, user_offset, file_size.saturating_sub(1)).await?;
            
            // Si no hay nada más que descargar desde aquí, o ya terminamos el archivo
            if missing_ranges.is_empty() {
                // Si todavía faltan partes al inicio del archivo (antes del puntero del usuario), ir por ellas
                let early_missing = db.get_missing_ranges(inode, 0, user_offset.saturating_sub(1)).await?;
                if early_missing.is_empty() {
                    break; // Archivo 100% completo
                }
                
                // Si es media y ya descargamos todo a partir del puntero, descansamos un poco
                if is_media {
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }

            // 3. Priorizar los primeros N chunks basándose en el puntero
            let mut pending_chunks = Vec::new();
            let mut data_to_fetch = 0;
            
            for (start, end) in missing_ranges {
                let s_aligned = (start / CHUNK_SIZE) * CHUNK_SIZE;
                let mut current = s_aligned;
                
                while current <= end {
                    let c_end = (current + CHUNK_SIZE - 1).min(file_size - 1);
                    
                    // Verificar si este mini-chunk está realmente faltante
                    let m = db.get_missing_ranges(inode, current, c_end).await?;
                    if !m.is_empty() {
                        pending_chunks.push((current, c_end));
                        data_to_fetch += c_end - current + 1;
                    }
                    
                    current += CHUNK_SIZE;
                    
                    // Límite de buffer para media para no asfixiar la red
                    if is_media && data_to_fetch > MEDIA_PREFETCH_LIMIT {
                        break;
                    }
                }
                
                if is_media && data_to_fetch > MEDIA_PREFETCH_LIMIT {
                    break;
                }
                
                if !is_media && pending_chunks.len() >= 20 { // 40MB per iteration for normal files
                    break;
                }
            }

            // Si no hay chunks inmediatos, pero el archivo no está completo, quizás el usuario saltó adelante
            if pending_chunks.is_empty() {
                 // Intentar de nuevo desde 0 si no hay progreso después de un salto
                 let all_missing = db.get_missing_ranges(inode, 0, file_size.saturating_sub(1)).await?;
                 if all_missing.is_empty() { break; }
                 
                 // Pequeña espera para evitar bucle infinito agresivo
                 tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                 continue;
            }

            // 4. Descargar el lote actual
            for batch in pending_chunks.chunks(MAX_CONCURRENT) {
                let mut download_tasks = Vec::new();
                for &(start, end) in batch {
                    let db_clone = db.clone();
                    let client_clone = drive_client.clone();
                    let gdrive_id_clone = gdrive_id.clone();
                    let cache_path_clone = cache_path.clone();
                    let file_locks_clone = file_locks.clone();
                    let history_clone = history.clone();
                    let tid_clone = transfer_id;

                    download_tasks.push(tokio::spawn(async move {
                        let m_size = (end - start + 1) as u32;
                        let data = client_clone.download_chunk(&gdrive_id_clone, start, m_size).await?;
                        
                        let inode_lock = file_locks_clone
                            .entry(inode)
                            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                            .clone();
                        let _guard = inode_lock.lock().await;

                        let mut file = tokio::fs::OpenOptions::new().write(true).open(&cache_path_clone).await?;
                        file.seek(std::io::SeekFrom::Start(start)).await?;
                        file.write_all(&data).await?;
                        file.flush().await?;
                        
                        db_clone.add_cached_chunk(inode, start, end).await?;
                        if let Some(t_id) = tid_clone {
                             if let Ok(cb) = db_clone.get_cached_bytes_count(inode).await {
                                  history_clone.update_transfer_progress(t_id, cb);
                             }
                        }
                        Ok::<_, anyhow::Error>(())
                    }));
                }
                
                for res in futures_util::future::join_all(download_tasks).await {
                    if let Err(e) = res { tracing::error!("Task panic: {}", e); }
                }
            }
            
            _iteration += 1;
            
            // Si es media, pausar para permitir que el reproductor consuma
            if is_media {
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }
        }
        
        tracing::info!("✅ {} inteligente completado para: {}", 
                      if is_media { "Streaming" } else { "Descarga" }, file_name);
        Ok(())
    }
}

