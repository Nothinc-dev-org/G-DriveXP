
use fuse3::raw::prelude::*;
use fuse3::{Errno, Result};
use std::ffi::OsStr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};
use futures_util::stream::Empty;

use crate::db::MetadataRepository;

/// Implementación del sistema de archivos FUSE para Google Drive
pub struct GDriveFS {
    db: Arc<MetadataRepository>,
}

impl GDriveFS {
    pub fn new(db: Arc<MetadataRepository>) -> Self {
        Self { db }
    }
}


impl Filesystem for GDriveFS {
    type DirEntryStream<'a> = Empty<Result<DirectoryEntry>>;
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
}
