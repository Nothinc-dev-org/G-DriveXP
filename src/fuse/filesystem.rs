
use fuse3::raw::prelude::*;
use fuse3::{Errno, Result};
use std::ffi::OsStr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};
use futures_util::stream::{self, Empty, BoxStream, StreamExt};

use crate::db::MetadataRepository;
use crate::gdrive::client::DriveClient;

/// Implementación del sistema de archivos FUSE para Google Drive
pub struct GDriveFS {
    db: Arc<MetadataRepository>,
    drive_client: Arc<DriveClient>,
}

impl GDriveFS {
    pub fn new(db: Arc<MetadataRepository>, drive_client: Arc<DriveClient>) -> Self {
        Self { db, drive_client }
    }
}


impl Filesystem for GDriveFS {
    type DirEntryStream<'a> = BoxStream<'a, Result<DirectoryEntry>>;
    type DirEntryPlusStream<'a> = Empty<Result<DirectoryEntryPlus>>;

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
        debug!("readdir: parent={} offset={}", parent, offset);

        let mut children = match self.db.list_children(parent).await {
            Ok(c) => c,
            Err(e) => {
                error!("Error listando hijos de {}: {}", parent, e);
                return Err(Errno::from(libc::EIO));
            }
        };

        // Agregar entradas especiales . y ..
        if offset == 0 {
            children.insert(0, (parent, ".".to_string(), true));
            children.insert(1, (1.max(parent), "..".to_string(), true));
        }

        let stream = stream::iter(children)
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
        debug!("lookup: parent={} name={}", parent, name_str);

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
        debug!("getattr: inode={}", inode);

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

        Ok(ReplyAttr {
            ttl: Duration::from_secs(1),
            attr: attrs.to_file_attr(),
        })
    }
    
    // Métodos requeridos adicionales que faltaban (placeholders)
    async fn forget(&self, _req: Request, _inode: u64, _nlookup: u64) {}

    // Abrir archivo (open)
    async fn open(&self, _req: Request, inode: u64, _flags: u32) -> Result<ReplyOpen> {
        debug!("open: inode={}", inode);
        // Validar que existe en DB
        if self.db.get_attrs(inode).await.is_err() {
            return Err(Errno::from(libc::ENOENT));
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

    // Leer contenido (read)
    async fn read(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        debug!("read: inode={} offset={} size={}", inode, offset, size);

        // 1. Obtener los atributos para conseguir el gdrive_id
        // (En una versión optimizada, esto debería estar en una caché de inodos -> file_id)
        let gdrive_id = match sqlx::query_scalar::<_, String>("SELECT gdrive_id FROM inodes WHERE inode = ?")
            .bind(inode as i64)
            .fetch_one(self.db.pool())
            .await 
        {
            Ok(id) => id,
            Err(e) => {
                error!("Error buscando gdrive_id para inode {}: {}", inode, e);
                return Err(Errno::from(libc::ENOENT));
            }
        };

        // 2. Descargar el chunk desde la API
        match self.drive_client.download_chunk(&gdrive_id, offset, size).await {
            Ok(data) => Ok(ReplyData { data: data.into() }),
            Err(e) => {
                error!("Error descargando contenido de {}: {}", gdrive_id, e);
                Err(Errno::from(libc::EIO))
            }
        }
    }
}
