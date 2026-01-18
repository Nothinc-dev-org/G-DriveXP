-- Mapeo bidireccional GDrive ID <-> Inode POSIX
CREATE TABLE IF NOT EXISTS inodes (
    inode INTEGER PRIMARY KEY AUTOINCREMENT,
    gdrive_id TEXT UNIQUE NOT NULL,
    generation INTEGER DEFAULT 0,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gdrive ON inodes(gdrive_id);

-- Estructura jerárquica del árbol (Directory Entry)
CREATE TABLE IF NOT EXISTS dentry (
    parent_inode INTEGER NOT NULL,
    child_inode INTEGER NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (parent_inode, name),
    FOREIGN KEY (parent_inode) REFERENCES inodes(inode),
    FOREIGN KEY (child_inode) REFERENCES inodes(inode)
);

-- Metadatos POSIX cacheados
CREATE TABLE IF NOT EXISTS attrs (
    inode INTEGER PRIMARY KEY,
    size INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    ctime INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    is_dir BOOLEAN NOT NULL,
    mime_type TEXT,
    FOREIGN KEY (inode) REFERENCES inodes(inode)
);

-- Estado de sincronización
CREATE TABLE IF NOT EXISTS sync_state (
    inode INTEGER PRIMARY KEY,
    dirty BOOLEAN DEFAULT 0,
    version INTEGER NOT NULL,
    md5_checksum TEXT,
    deleted_at INTEGER DEFAULT NULL,  -- Timestamp de soft delete
    remote_md5 TEXT,  -- MD5 de la versión remota conocida (para detección de conflictos)
    FOREIGN KEY (inode) REFERENCES inodes(inode)
);
CREATE INDEX IF NOT EXISTS idx_dirty ON sync_state(inode) WHERE dirty=1;


-- Token de sincronización para changes.list
CREATE TABLE IF NOT EXISTS sync_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Tombstones: Entradas de directorio eliminadas (soft delete)
-- Permite recuperación y previene resurrecciones accidentales
CREATE TABLE IF NOT EXISTS dentry_deleted (
    parent_inode INTEGER NOT NULL,
    child_inode INTEGER NOT NULL,
    name TEXT NOT NULL,
    deleted_at INTEGER NOT NULL,
    PRIMARY KEY (child_inode)
);
CREATE INDEX IF NOT EXISTS idx_tombstone_deleted_at ON dentry_deleted(deleted_at);

-- Optimizaciones de WAL (Write-Ahead Logging) para concurrencia
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

