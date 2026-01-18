use fuse3::raw::prelude::FileAttr;
use fuse3::Timestamp;
use fuse3::FileType;
use sqlx::FromRow;
use std::time::UNIX_EPOCH;

#[derive(Debug, FromRow)]
pub struct FileAttributes {
    pub inode: i64,
    pub size: i64,
    pub mtime: i64,
    pub ctime: i64,
    pub mode: i64,
    pub is_dir: bool,
    pub mime_type: Option<String>,
}

impl FileAttributes {
    pub fn to_file_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.inode as u64,
            size: self.size as u64,
            blocks: (self.size as u64 + 511) / 512,
            atime: Timestamp::new(self.mtime as i64, 0),
            mtime: Timestamp::new(self.mtime as i64, 0),
            ctime: Timestamp::new(self.ctime as i64, 0),
            kind: if self.is_dir { FileType::Directory } else { FileType::RegularFile },
            perm: (self.mode & 0o7777) as u16,
            nlink: 1,
            uid: unsafe { libc::getuid() }, 
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
        }
    }

    /// Crea atributos por defecto para el directorio raíz
    pub fn root() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
            
        Self {
            inode: 1,
            size: 4096,
            mtime: now,
            ctime: now,
            mode: 0o755,
            is_dir: true,
            mime_type: Some("application/vnd.google-apps.folder".to_string()),
        }
    }
}
