use anyhow::Result;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::path::Path;

/// Repositorio principal de metadatos basado en SQLite
pub struct MetadataRepository {
    pool: SqlitePool,
}

impl MetadataRepository {
    /// Inicializa la conexión a la base de datos y aplica el esquema
    pub async fn new(db_path: &Path) -> Result<Self> {
        // Asegurarse de que el archivo existe (sqlx requiere esto para SQLite)
        if !db_path.exists() {
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::File::create(db_path)?;
        }

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&format!("sqlite://{}", db_path.display()))
            .await?;
        
        // Inicializar esquema (crea tablas si no existen)
        sqlx::query(include_str!("schema.sql"))
            .execute(&pool)
            .await?;
        
        let repo = Self { pool };

        // Aplicar migraciones necesarias para bases de datos existentes
        repo.apply_migrations().await?;
        
        Ok(repo)
    }

    /// Aplica migraciones manuales para asegurar que el esquema está actualizado
    async fn apply_migrations(&self) -> Result<()> {
        // 1. Verificar si la columna deleted_at existe en sync_state
        let has_deleted_at = sqlx::query("PRAGMA table_info(sync_state)")
            .fetch_all(&self.pool)
            .await?
            .iter()
            .any(|row: &sqlx::sqlite::SqliteRow| {
                use sqlx::Row;
                let name: String = row.get("name");
                name == "deleted_at"
            });

        if !has_deleted_at {
            sqlx::query("ALTER TABLE sync_state ADD COLUMN deleted_at INTEGER DEFAULT NULL")
                .execute(&self.pool)
                .await?;
        }

        // 2. Verificar si la columna remote_md5 existe en sync_state
        let has_remote_md5 = sqlx::query("PRAGMA table_info(sync_state)")
            .fetch_all(&self.pool)
            .await?
            .iter()
            .any(|row: &sqlx::sqlite::SqliteRow| {
                use sqlx::Row;
                let name: String = row.get("name");
                name == "remote_md5"
            });

        if !has_remote_md5 {
            sqlx::query("ALTER TABLE sync_state ADD COLUMN remote_md5 TEXT")
                .execute(&self.pool)
                .await?;
        }

        // Asegurar que el índice existe (CREATE INDEX IF NOT EXISTS es seguro)
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_sync_deleted ON sync_state(deleted_at) WHERE deleted_at IS NOT NULL")
            .execute(&self.pool)
            .await?;

        // 3. Crear tabla file_cache_chunks si no existe
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS file_cache_chunks (
                inode INTEGER NOT NULL,
                start_offset INTEGER NOT NULL,
                end_offset INTEGER NOT NULL,
                PRIMARY KEY (inode, start_offset),
                FOREIGN KEY (inode) REFERENCES inodes(inode) ON DELETE CASCADE
            )
            "#
        )
        .execute(&self.pool)
        .await?;

        // 4. Migración: Corregir PRIMARY KEY de dentry_deleted
        // Verificar si la tabla tiene la PK incorrecta
        let has_old_pk = sqlx::query(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='dentry_deleted'"
        )
        .fetch_optional(&self.pool)
        .await?
        .and_then(|row: sqlx::sqlite::SqliteRow| {
            use sqlx::Row;
            let sql: String = row.get("sql");
            Some(sql.contains("PRIMARY KEY (parent_inode, name)"))
        })
        .unwrap_or(false);

        if has_old_pk {
            tracing::info!("Aplicando migración: Corrigiendo PRIMARY KEY de dentry_deleted");
            
            // Renombrar tabla vieja
            sqlx::query("ALTER TABLE dentry_deleted RENAME TO dentry_deleted_old")
                .execute(&self.pool)
                .await?;
            
            // Crear nueva tabla con PK correcto
            sqlx::query(
                r#"
                CREATE TABLE dentry_deleted (
                    parent_inode INTEGER NOT NULL,
                    child_inode INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    deleted_at INTEGER NOT NULL,
                    PRIMARY KEY (child_inode)
                )
                "#
            )
            .execute(&self.pool)
            .await?;
            
            // Migrar datos (eliminando duplicados por child_inode)
            sqlx::query(
                r#"
                INSERT OR IGNORE INTO dentry_deleted (parent_inode, child_inode, name, deleted_at)
                SELECT parent_inode, child_inode, name, deleted_at
                FROM dentry_deleted_old
                "#
            )
            .execute(&self.pool)
            .await?;
            
            // Eliminar tabla vieja
            sqlx::query("DROP TABLE dentry_deleted_old")
                .execute(&self.pool)
                .await?;
            
            // Recrear índice
            sqlx::query("CREATE INDEX IF NOT EXISTS idx_tombstone_deleted_at ON dentry_deleted(deleted_at)")
                .execute(&self.pool)
                .await?;
            
            tracing::info!("Migración de dentry_deleted completada");
        }

        Ok(())
    }

    /// Obtiene el pool de conexiones crudo si es necesario
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    
    /// Buscar inodo por directorio padre y nombre (operación lookup)
    pub async fn lookup(&self, parent: u64, name: &str) -> Result<Option<u64>> {
        let row = sqlx::query_scalar::<_, i64>(
            "SELECT child_inode FROM dentry WHERE parent_inode = ? AND name = ?"
        )
        .bind(parent as i64)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        
        Ok(row.map(|i| i as u64))
    }

    /// Obtener atributos de archivo (operación getattr)
    pub async fn get_attrs(&self, inode: u64) -> Result<crate::fuse::attr::FileAttributes> {
        // Caso especial: Root
        if inode == 1 {
            let row = sqlx::query_as::<_, crate::fuse::attr::FileAttributes>(
                "SELECT * FROM attrs WHERE inode = 1"
            )
            .fetch_optional(&self.pool)
            .await?;

            return Ok(row.unwrap_or_else(crate::fuse::attr::FileAttributes::root));
        }

        let attrs = sqlx::query_as::<_, crate::fuse::attr::FileAttributes>(
            "SELECT * FROM attrs WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_one(&self.pool)
        .await?;
        
        Ok(attrs)
    }
    /// Listar contenido de un directorio con metadatos extendidos (para readdirplus)
    pub async fn list_children_extended(&self, parent_inode: u64) -> Result<Vec<(u64, String, bool, Option<String>, String)>> {
        let children = sqlx::query_as::<_, (i64, String, bool, Option<String>, String)>(
            r#"
            SELECT 
                d.child_inode, 
                d.name, 
                a.is_dir,
                a.mime_type,
                i.gdrive_id
            FROM dentry d
            JOIN attrs a ON d.child_inode = a.inode
            JOIN inodes i ON d.child_inode = i.inode
            WHERE d.parent_inode = ?
            ORDER BY d.name
            "#
        )
        .bind(parent_inode as i64)
        .fetch_all(&self.pool)
        .await?;
        
        Ok(children.into_iter()
            .map(|(inode, name, is_dir, mime, gdrive_id)| (inode as u64, name, is_dir, mime, gdrive_id))
            .collect())
    }

    /// Listar contenido de un directorio (para readdir simple)
    pub async fn list_children(&self, parent_inode: u64) -> Result<Vec<(u64, String, bool)>> {
        let children = sqlx::query_as::<_, (i64, String, bool)>(
            r#"
            SELECT d.child_inode, d.name, a.is_dir 
            FROM dentry d
            JOIN attrs a ON d.child_inode = a.inode
            WHERE d.parent_inode = ?
            ORDER BY d.name
            "#
        )
        .bind(parent_inode as i64)
        .fetch_all(&self.pool)
        .await?;
        
        Ok(children.into_iter()
            .map(|(inode, name, is_dir)| (inode as u64, name, is_dir))
            .collect())
    }

    /// Cuenta el número de hijos de un directorio (para verificación rápida de paginación)
    /// Esta operación es O(1) con el índice de parent_inode
    pub async fn count_children(&self, parent_inode: u64) -> Result<u64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM dentry WHERE parent_inode = ?"
        )
        .bind(parent_inode as i64)
        .fetch_one(&self.pool)
        .await?;
        
        Ok(count as u64)
    }

    /// Verifica si la tabla de inodos está vacía (excepto el root si existe)
    pub async fn is_empty(&self) -> Result<bool> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inodes")
            .fetch_one(&self.pool)
            .await?;
        Ok(count <= 1) // 1 si solo existe el root, 0 si está totalmente vacía
    }

    /// Obtiene o desarrolla un inodo para un gdrive_id dado
    pub async fn get_or_create_inode(&self, gdrive_id: &str) -> Result<u64> {
        // Intentar obtener existente
        let existing = sqlx::query_scalar::<_, i64>("SELECT inode FROM inodes WHERE gdrive_id = ?")
            .bind(gdrive_id)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(inode) = existing {
            return Ok(inode as u64);
        }

        // Crear nuevo
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        let id = sqlx::query("INSERT INTO inodes (gdrive_id, created_at) VALUES (?, ?)")
            .bind(gdrive_id)
            .bind(now)
            .execute(&self.pool)
            .await?
            .last_insert_rowid();

        Ok(id as u64)
    }

    /// Inserta o actualiza metadatos de un archivo
    pub async fn upsert_file_metadata(
        &self,
        inode: u64,
        size: i64,
        mtime: i64,
        mode: u32,
        is_dir: bool,
        mime_type: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO attrs (inode, size, mtime, ctime, mode, is_dir, mime_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(inode) DO UPDATE SET
                size = excluded.size,
                mtime = excluded.mtime,
                mode = excluded.mode,
                is_dir = excluded.is_dir,
                mime_type = excluded.mime_type
            "#
        )
        .bind(inode as i64)
        .bind(size)
        .bind(mtime)
        .bind(mtime) // Usamos mtime como ctime por simplicidad inicial
        .bind(mode as i32)
        .bind(is_dir)
        .bind(mime_type)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Inserta o actualiza una entrada de directorio
    pub async fn upsert_dentry(&self, parent_inode: u64, child_inode: u64, name: &str) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO dentry (parent_inode, child_inode, name)
            VALUES (?, ?, ?)
            ON CONFLICT(parent_inode, name) DO UPDATE SET
                child_inode = excluded.child_inode
            "#
        )
        .bind(parent_inode as i64)
        .bind(child_inode as i64)
        .bind(name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ============================================================
    // Métodos para Sync Meta (persistencia de page tokens)
    // ============================================================

    /// Guarda o actualiza un valor en sync_meta
    pub async fn set_sync_meta(&self, key: &str, value: &str) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        sqlx::query(
            r#"
            INSERT INTO sync_meta (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            "#
        )
        .bind(key)
        .bind(value)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Obtiene un valor de sync_meta
    pub async fn get_sync_meta(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query_scalar::<_, String>(
            "SELECT value FROM sync_meta WHERE key = ?"
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    // ============================================================
    // Métodos para Conflict Detection (Remote MD5 Tracking)
    // ============================================================

    /// Obtiene el MD5 remoto conocido para un archivo
    pub async fn get_remote_md5(&self, inode: u64) -> Result<Option<String>> {
        let row = sqlx::query_scalar::<_, String>(
            "SELECT remote_md5 FROM sync_state WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    /// Actualiza el MD5 remoto conocido para un archivo
    pub async fn set_remote_md5(&self, inode: u64, md5: &str) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO sync_state (inode, dirty, version, remote_md5)
            VALUES (?, 0, 0, ?)
            ON CONFLICT(inode) DO UPDATE SET remote_md5 = excluded.remote_md5
            "#
        )
        .bind(inode as i64)
        .bind(md5)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ============================================================
    // Métodos para Soft Delete (Tombstones)
    // ============================================================

    /// Obtiene el inode asociado a un gdrive_id
    pub async fn get_inode_by_gdrive_id(&self, gdrive_id: &str) -> Result<Option<u64>> {
        let row = sqlx::query_scalar::<_, i64>(
            "SELECT inode FROM inodes WHERE gdrive_id = ?"
        )
        .bind(gdrive_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|i| i as u64))
    }

    /// Marca un archivo como eliminado (soft delete)
    /// Mueve el dentry a dentry_deleted, marca sync_state con deleted_at
    pub async fn soft_delete_by_gdrive_id(&self, gdrive_id: &str) -> Result<bool> {
        let inode = match self.get_inode_by_gdrive_id(gdrive_id).await? {
            Some(i) => i,
            None => return Ok(false), // No existe, nada que eliminar
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        // 1. Mover dentry a dentry_deleted
        sqlx::query(
            r#"
            INSERT INTO dentry_deleted (parent_inode, child_inode, name, deleted_at)
            SELECT parent_inode, child_inode, name, ?
            FROM dentry WHERE child_inode = ?
            "#
        )
        .bind(now)
        .bind(inode as i64)
        .execute(&self.pool)
        .await?;

        // 2. Eliminar de dentry (ya no visible en FUSE)
        sqlx::query("DELETE FROM dentry WHERE child_inode = ?")
            .bind(inode as i64)
            .execute(&self.pool)
            .await?;

        // 3. Marcar deleted_at en sync_state Y dirty=1 para forzar sync
        sqlx::query(
            r#"
            INSERT INTO sync_state (inode, dirty, version, deleted_at)
            VALUES (?, 1, 0, ?)
            ON CONFLICT(inode) DO UPDATE SET 
                deleted_at = excluded.deleted_at,
                dirty = 1
            "#
        )
        .bind(inode as i64)
        .bind(now)
        .execute(&self.pool)
        .await?;

        tracing::debug!("Soft delete aplicado: gdrive_id={}, inode={}", gdrive_id, inode);
        Ok(true)
    }

    /// Restaura un archivo eliminado (quita tombstone)
    /// Mueve el dentry de vuelta, elimina deleted_at
    pub async fn restore_by_gdrive_id(&self, gdrive_id: &str) -> Result<bool> {
        let inode = match self.get_inode_by_gdrive_id(gdrive_id).await? {
            Some(i) => i,
            None => return Ok(false),
        };

        // 1. Restaurar dentry desde dentry_deleted
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO dentry (parent_inode, child_inode, name)
            SELECT parent_inode, child_inode, name
            FROM dentry_deleted WHERE child_inode = ?
            "#
        )
        .bind(inode as i64)
        .execute(&self.pool)
        .await?;

        // 2. Eliminar de dentry_deleted
        sqlx::query("DELETE FROM dentry_deleted WHERE child_inode = ?")
            .bind(inode as i64)
            .execute(&self.pool)
            .await?;

        // 3. Limpiar deleted_at en sync_state
        sqlx::query("UPDATE sync_state SET deleted_at = NULL WHERE inode = ?")
            .bind(inode as i64)
            .execute(&self.pool)
            .await?;

        tracing::debug!("Archivo restaurado: gdrive_id={}, inode={}", gdrive_id, inode);
        Ok(true)
    }

    /// Verifica si un gdrive_id tiene un tombstone activo
    pub async fn has_tombstone(&self, gdrive_id: &str) -> Result<bool> {
        let inode = match self.get_inode_by_gdrive_id(gdrive_id).await? {
            Some(i) => i,
            None => return Ok(false),
        };

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM dentry_deleted WHERE child_inode = ?"
        )
        .bind(inode as i64)
        .fetch_one(&self.pool)
        .await?;

        Ok(count > 0)
    }

    /// Hard delete: elimina permanentemente registros con deleted_at > grace_period
    /// Retorna el número de registros eliminados
    pub async fn purge_expired_tombstones(&self, grace_days: i64) -> Result<u64> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        
        let cutoff = now - (grace_days * 24 * 60 * 60);

        // Obtener inodos a purgar
        let inodes_to_purge: Vec<i64> = sqlx::query_scalar(
            "SELECT child_inode FROM dentry_deleted WHERE deleted_at < ?"
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;

        if inodes_to_purge.is_empty() {
            return Ok(0);
        }

        let count = inodes_to_purge.len() as u64;

        for inode in &inodes_to_purge {
            // Eliminar de todas las tablas relacionadas
            sqlx::query("DELETE FROM dentry_deleted WHERE child_inode = ?")
                .bind(inode)
                .execute(&self.pool)
                .await?;
            
            sqlx::query("DELETE FROM sync_state WHERE inode = ?")
                .bind(inode)
                .execute(&self.pool)
                .await?;
            
            sqlx::query("DELETE FROM attrs WHERE inode = ?")
                .bind(inode)
                .execute(&self.pool)
                .await?;
            
            sqlx::query("DELETE FROM inodes WHERE inode = ?")
                .bind(inode)
                .execute(&self.pool)
                .await?;
        }

        tracing::info!("Purgados {} tombstones expirados (grace_days={})", count, grace_days);
        Ok(count)
    }

    // ============================================================
    // Métodos para File Cache Chunks (On-Demand Caching)
    // ============================================================

    /// Registra un rango descargado en la caché
    pub async fn add_cached_chunk(&self, inode: u64, start: u64, end: u64) -> Result<()> {
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO file_cache_chunks (inode, start_offset, end_offset)
            VALUES (?, ?, ?)
            "#
        )
        .bind(inode as i64)
        .bind(start as i64)
        .bind(end as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Obtiene los rangos faltantes para un archivo en un intervalo dado
    /// Retorna una lista de (start, end) que necesitan descargarse
    pub async fn get_missing_ranges(&self, inode: u64, requested_start: u64, requested_end: u64) -> Result<Vec<(u64, u64)>> {
        // Obtener todos los chunks cacheados para este inode que se solapan con el rango solicitado
        let cached_chunks: Vec<(i64, i64)> = sqlx::query_as(
            r#"
            SELECT start_offset, end_offset
            FROM file_cache_chunks
            WHERE inode = ?
              AND end_offset >= ?
              AND start_offset <= ?
            ORDER BY start_offset
            "#
        )
        .bind(inode as i64)
        .bind(requested_start as i64)
        .bind(requested_end as i64)
        .fetch_all(&self.pool)
        .await?;

        // Si no hay chunks, el rango completo falta
        if cached_chunks.is_empty() {
            return Ok(vec![(requested_start, requested_end)]);
        }

        let mut missing = Vec::new();
        let mut current_pos = requested_start;

        for (start, end) in cached_chunks {
            let start = start as u64;
            let end = end as u64;

            // Si hay un gap antes de este chunk
            if current_pos < start {
                missing.push((current_pos, start - 1));
            }

            // Avanzar más allá del chunk actual
            current_pos = current_pos.max(end + 1);
        }

        // Si queda espacio después del último chunk
        if current_pos <= requested_end {
            missing.push((current_pos, requested_end));
        }

        Ok(missing)
    }


    /// Limpia todos los chunks cacheados para un inode (útil al invalidar caché)
    #[allow(dead_code)]
    pub async fn clear_cached_chunks(&self, inode: u64) -> Result<()> {
        sqlx::query("DELETE FROM file_cache_chunks WHERE inode = ?")
            .bind(inode as i64)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
