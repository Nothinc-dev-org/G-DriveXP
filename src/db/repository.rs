use anyhow::Result;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::path::Path;

/// Repositorio principal de metadatos basado en SQLite
#[derive(Debug)]
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
            .acquire_timeout(std::time::Duration::from_secs(60)) // Add a timeout to wait for a database lock
            .connect(&format!("sqlite://{}", db_path.display()))
            .await?;
        
        // Configurar opciones pragma en la conexión para mejorar la concurrencia en SQLite
        sqlx::query("PRAGMA journal_mode = WAL;")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA synchronous = NORMAL;")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA busy_timeout = 60000;")
            .execute(&pool)
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

        // 5. Crear tabla local_sync_files para Local Sync híbrido
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS local_sync_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_dir_id INTEGER NOT NULL REFERENCES local_sync_dirs(id) ON DELETE CASCADE,
                relative_path TEXT NOT NULL,
                is_dir INTEGER NOT NULL DEFAULT 0,
                
                availability TEXT NOT NULL DEFAULT 'local_online',
                
                local_mtime INTEGER,
                local_size INTEGER,
                local_md5 TEXT,
                
                gdrive_id TEXT,
                remote_md5 TEXT,
                remote_mtime INTEGER,
                
                dirty INTEGER NOT NULL DEFAULT 1,
                last_synced INTEGER,
                
                UNIQUE(sync_dir_id, relative_path)
            )
            "#
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_local_sync_files_dirty ON local_sync_files(dirty) WHERE dirty = 1")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_local_sync_files_gdrive ON local_sync_files(gdrive_id)")
            .execute(&self.pool)
            .await?;

        // 6. Verificar si la columna availability existe en sync_state
        let has_availability = sqlx::query("PRAGMA table_info(sync_state)")
            .fetch_all(&self.pool)
            .await?
            .iter()
            .any(|row: &sqlx::sqlite::SqliteRow| {
                use sqlx::Row;
                let name: String = row.get("name");
                name == "availability"
            });

        if !has_availability {
            // Default a 'online_only' (nube) para no descargar todo por defecto
            sqlx::query("ALTER TABLE sync_state ADD COLUMN availability TEXT DEFAULT 'online_only'")
                .execute(&self.pool)
                .await?;
        }

        // 7. Verificar si la columna can_move existe en attrs
        let has_can_move = sqlx::query("PRAGMA table_info(attrs)")
            .fetch_all(&self.pool)
            .await?
            .iter()
            .any(|row: &sqlx::sqlite::SqliteRow| {
                use sqlx::Row;
                let name: String = row.get("name");
                name == "can_move"
            });

        if !has_can_move {
            sqlx::query("ALTER TABLE attrs ADD COLUMN can_move BOOLEAN DEFAULT 1")
                .execute(&self.pool)
                .await?;
        }

        // 8. Verificar si la columna shared existe en attrs
        let has_shared = sqlx::query("PRAGMA table_info(attrs)")
            .fetch_all(&self.pool)
            .await?
            .iter()
            .any(|row: &sqlx::sqlite::SqliteRow| {
                use sqlx::Row;
                let name: String = row.get("name");
                name == "shared"
            });

        if !has_shared {
            sqlx::query("ALTER TABLE attrs ADD COLUMN shared BOOLEAN DEFAULT 0")
                .execute(&self.pool)
                .await?;
        }

        // 9. Verificar si la columna owned_by_me existe en attrs
        let has_owned_by_me = sqlx::query("PRAGMA table_info(attrs)")
            .fetch_all(&self.pool)
            .await?
            .iter()
            .any(|row: &sqlx::sqlite::SqliteRow| {
                use sqlx::Row;
                let name: String = row.get("name");
                name == "owned_by_me"
            });

        if !has_owned_by_me {
            sqlx::query("ALTER TABLE attrs ADD COLUMN owned_by_me BOOLEAN DEFAULT 1")
                .execute(&self.pool)
                .await?;
        }

        // 10. Crear tabla dir_counters (Protocolo Burbujeo de Estados)
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS dir_counters (
                inode INTEGER PRIMARY KEY,
                dirty_desc_count INTEGER NOT NULL DEFAULT 0,
                synced_desc_count INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (inode) REFERENCES inodes(inode) ON DELETE CASCADE
            )
            "#
        )
        .execute(&self.pool)
        .await?;

        // Si la tabla existe pero está vacía y hay datos en dentry, recalcular contadores
        let counters_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dir_counters")
            .fetch_one(&self.pool)
            .await?;
        let dirs_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attrs WHERE is_dir = 1")
            .fetch_one(&self.pool)
            .await?;
        if counters_count == 0 && dirs_count > 0 {
            tracing::info!("Migrando: recalculando contadores de directorio (dir_counters)...");
            self.rebuild_all_dir_counters().await?;
            tracing::info!("Migración de dir_counters completada");
        }

        Ok(())
    }

    /// Obtiene la disponibilidad de un archivo ('online_only' o 'local_online')
    pub async fn get_availability(&self, inode: u64) -> Result<String> {
        let row = sqlx::query_scalar::<_, String>(
            "SELECT availability FROM sync_state WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.unwrap_or_else(|| "online_only".to_string()))
    }

    /// Establece la disponibilidad de un archivo.
    /// Si `bubble` es true, propaga el cambio de estado hacia los directorios ancestros
    /// (comportamiento normal en runtime). Si es false, solo escribe el UPDATE sin
    /// burbujear — útil durante bootstrap masivo donde se invoca rebuild_all_dir_counters al final.
    pub async fn set_availability(&self, inode: u64, availability: &str, bubble: bool) -> Result<()> {
        // 1. Obtener estado previo
        let prev = sqlx::query_as::<_, (Option<String>, Option<bool>, Option<i64>)>(
            "SELECT s.availability, s.dirty, s.deleted_at FROM sync_state s WHERE s.inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        // OPTIMIZACIÓN CRÍTICA (Cortocircuito):
        // 1. Si existe el registro y coincide, ahorramos los INSERT/UPDATE.
        // 2. Si NO existe el registro (ej. primer arranque), el estado por defecto
        // implícito es 'online_only'. Si nos piden 'online_only', no necesitamos
        // escribir una fila nueva, ahorrando miles de INSERTS en el primer bootstrap.
        match prev {
            Some((Some(ref prev_av), _, _)) if prev_av == availability => {
                return Ok(());
            }
            // Si la DB (sync_state) dice None, tratarlo por defecto como 'online_only'
            None if availability == "online_only" => {
                return Ok(());
            }
            _ => {}
        }

        let was_synced = prev.as_ref().map(|(av, d, del)| {
            let is_local_online = av.as_deref().unwrap_or("online_only") == "local_online";
            let not_dirty = !d.unwrap_or(false);
            let not_deleted = del.unwrap_or(0) == 0;
            is_local_online && not_dirty && not_deleted
        }).unwrap_or(false);

        // 2. Aplicar cambio
        sqlx::query(
            r#"
            INSERT INTO sync_state (inode, availability, dirty, version)
            VALUES (?, ?, 0, 0)
            ON CONFLICT(inode) DO UPDATE SET availability = excluded.availability
            "#
        )
        .bind(inode as i64)
        .bind(availability)
        .execute(&self.pool)
        .await?;

        // 3. Burbujear solo si se solicitó (runtime normal)
        if bubble {
            let is_dir: Option<bool> = sqlx::query_scalar(
                "SELECT is_dir FROM attrs WHERE inode = ?"
            )
            .bind(inode as i64)
            .fetch_optional(&self.pool)
            .await?;

            if is_dir == Some(false) {
                let curr = sqlx::query_as::<_, (Option<String>, Option<bool>, Option<i64>)>(
                    "SELECT s.availability, s.dirty, s.deleted_at FROM sync_state s WHERE s.inode = ?"
                )
                .bind(inode as i64)
                .fetch_one(&self.pool)
                .await?;

                let is_synced = {
                    let is_local_online = curr.0.as_deref().unwrap_or("online_only") == "local_online";
                    let not_dirty = !curr.1.unwrap_or(false);
                    let not_deleted = curr.2.unwrap_or(0) == 0;
                    is_local_online && not_dirty && not_deleted
                };

                if was_synced && !is_synced {
                    self.bubble_state_change(inode, 0, -1).await?;
                } else if !was_synced && is_synced {
                    self.bubble_state_change(inode, 0, 1).await?;
                }
            }
        }

        Ok(())
    }

    /// Obtiene todos los directorios "vivos" para el bootstrapping del Mirror
    /// Incluye directorios vacíos que de otro modo serían invisibles.
    /// Retorna: (inode, path_relativo_desde_root)
    pub async fn get_all_active_dirs(&self) -> Result<Vec<(u64, String)>> {
        let rows = sqlx::query_as::<_, (i64, String)>(
            r#"
            WITH RECURSIVE dir_tree AS (
                SELECT
                    d.child_inode,
                    CASE 
                        WHEN a.owned_by_me = 0 THEN 'SHARED/' || d.name
                        ELSE d.name
                    END as path,
                    a.is_dir
                FROM dentry d
                JOIN attrs a ON d.child_inode = a.inode
                WHERE d.parent_inode = 1

                UNION ALL

                SELECT
                    d.child_inode,
                    dt.path || '/' || d.name,
                    a.is_dir
                FROM dentry d
                JOIN attrs a ON d.child_inode = a.inode
                JOIN dir_tree dt ON d.parent_inode = dt.child_inode
            )
            SELECT
                dt.child_inode,
                dt.path
            FROM dir_tree dt
            LEFT JOIN sync_state s ON dt.child_inode = s.inode
            WHERE dt.is_dir = 1
              AND (s.deleted_at IS NULL OR s.deleted_at = 0)
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut results: Vec<(u64, String)> = rows.into_iter()
            .map(|(inode, path)| (inode as u64, path))
            .collect();

        // Inyectar el directorio virtual SHARED incondicionalmente
        results.insert(0, (0xFFFF_FFFF_FFFF_FFFE, "SHARED".to_string()));

        Ok(results)
    }

    /// Obtiene todos los archivos "vivos" para el Bootstrapping
    /// Retorna: (inode, path_relativo_desde_root, availability)
    pub async fn get_all_active_files(&self) -> Result<Vec<(u64, String, String)>> {
        let rows = sqlx::query_as::<_, (i64, String, String)>(
            r#"
            WITH RECURSIVE file_tree AS (
                -- Caso base: archivos en root (parent_inode = 1)
                SELECT 
                    d.child_inode, 
                    CASE 
                        WHEN a.owned_by_me = 0 THEN 'SHARED/' || d.name
                        ELSE d.name
                    END as path, 
                    a.is_dir
                FROM dentry d
                JOIN attrs a ON d.child_inode = a.inode
                WHERE d.parent_inode = 1
                
                UNION ALL
                
                -- Caso recursivo: hijos de directorios
                SELECT 
                    d.child_inode, 
                    ft.path || '/' || d.name,
                    a.is_dir
                FROM dentry d
                JOIN attrs a ON d.child_inode = a.inode
                JOIN file_tree ft ON d.parent_inode = ft.child_inode
            )
            SELECT 
                ft.child_inode,
                ft.path,
                COALESCE(s.availability, 'online_only') as availability
            FROM file_tree ft
            LEFT JOIN sync_state s ON ft.child_inode = s.inode
            WHERE ft.is_dir = 0 -- Solo archivos
              AND (s.deleted_at IS NULL OR s.deleted_at = 0) -- No eliminados
            "#
        )
        .fetch_all(&self.pool)
        .await?;
        
        Ok(rows.into_iter()
            .map(|(inode, path, availability)| (inode as u64, path, availability))
            .collect())
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

    /// Verifica si un inode tiene al menos una entrada en la tabla dentry.
    pub async fn has_dentry(&self, inode: u64) -> Result<bool> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM dentry WHERE child_inode = ?"
        )
        .bind(inode as i64)
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
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

    /// Listar contenido compartido de la raíz (archivos no propios que cuelgan del inode 1)
    pub async fn list_non_owned_root_children(&self) -> Result<Vec<(u64, String, bool, Option<String>, String)>> {
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
            WHERE d.parent_inode = 1 AND a.owned_by_me = 0
            ORDER BY d.name
            "#
        )
        .fetch_all(&self.pool)
        .await?;
        
        Ok(children.into_iter()
            .map(|(inode, name, is_dir, mime, gdrive_id)| (inode as u64, name, is_dir, mime, gdrive_id))
            .collect())
    }

    /// Resuelve un path relativo (desde el root del mirror) a su inode
    pub async fn resolve_relative_path_to_inode(&self, relative_path: &str) -> Result<Option<u64>> {
        let parts: Vec<&str> = relative_path.split('/').filter(|s| !s.is_empty()).collect();
        
        let mut current_inode = 1u64; // Root inode
        
        // Si el path empieza con "SHARED", saltamos ese segmento virtual
        // y seguimos resolviendo desde root (inode 1)
        let parts_to_resolve = if parts.first() == Some(&"SHARED") {
            &parts[1..]
        } else {
            &parts[..]
        };
        
        for part in parts_to_resolve {
            match self.lookup(current_inode, part).await? {
                Some(child_inode) => current_inode = child_inode,
                None => return Ok(None),
            }
        }
        
        Ok(Some(current_inode))
    }

    /// Resuelve un inode a su path relativo reconstruyendo la jerarquía
    pub async fn resolve_inode_to_relative_path(&self, inode: u64) -> Result<Option<String>> {
        if inode == 1 {
            return Ok(Some("".to_string()));
        }

        let mut current_inode = inode;
        let mut path_parts = Vec::new();

        while current_inode != 1 {
            let row = sqlx::query_as::<_, (i64, String)>(
                "SELECT parent_inode, name FROM dentry WHERE child_inode = ?"
            )
            .bind(current_inode as i64)
            .fetch_optional(&self.pool)
            .await?;

            if let Some((parent_inode, name)) = row {
                path_parts.push(name);
                current_inode = parent_inode as u64;
            } else {
                return Ok(None); // Inodo huérfano
            }
        }

        path_parts.reverse();
        Ok(Some(path_parts.join("/")))
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

    /// Cuenta el número de hijos de un directorio que NO son propiedad del usuario
    /// y que están en el root (usado para la carpeta compartida virtual)
    pub async fn count_non_owned_root_children(&self) -> Result<u64> {
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) 
            FROM dentry d
            JOIN attrs a ON d.child_inode = a.inode
            WHERE d.parent_inode = 1 AND a.owned_by_me = 0
            "#
        )
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

        let insert_result = sqlx::query("INSERT INTO inodes (gdrive_id, created_at) VALUES (?, ?)")
            .bind(gdrive_id)
            .bind(now)
            .execute(&self.pool)
            .await;

        match insert_result {
            Ok(result) => Ok(result.last_insert_rowid() as u64),
            Err(sqlx::Error::Database(err)) if err.is_unique_violation() => {
                // Si hubo una colisión durante la inserción simultánea, simplemente lo leemos
                let existing = sqlx::query_scalar::<_, i64>("SELECT inode FROM inodes WHERE gdrive_id = ?")
                    .bind(gdrive_id)
                    .fetch_one(&self.pool)
                    .await?;
                Ok(existing as u64)
            }
            Err(e) => Err(e.into()),
        }
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
        can_move: bool,
        shared: bool,
        owned_by_me: bool,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO attrs (inode, size, mtime, ctime, mode, is_dir, mime_type, can_move, shared, owned_by_me)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(inode) DO UPDATE SET
                size = excluded.size,
                mtime = excluded.mtime,
                mode = excluded.mode,
                is_dir = excluded.is_dir,
                mime_type = excluded.mime_type,
                can_move = excluded.can_move,
                shared = excluded.shared,
                owned_by_me = excluded.owned_by_me
            "#
        )
        .bind(inode as i64)
        .bind(size)
        .bind(mtime)
        .bind(mtime) // Usamos mtime como ctime por simplicidad inicial
        .bind(mode as i32)
        .bind(is_dir)
        .bind(mime_type)
        .bind(can_move)
        .bind(shared)
        .bind(owned_by_me)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Actualiza específicamente el campo de propiedad (para correcciones masivas)
    pub async fn update_ownership(&self, inode: u64, owned_by_me: bool) -> Result<()> {
        sqlx::query("UPDATE attrs SET owned_by_me = ? WHERE inode = ?")
            .bind(owned_by_me)
            .bind(inode as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Inserta o actualiza una entrada de directorio
    /// IMPORTANTE: Un archivo solo puede tener UN parent. Antes de insertar,
    /// eliminamos cualquier dentry existente para este child_inode.
    pub async fn upsert_dentry(&self, parent_inode: u64, child_inode: u64, name: &str) -> Result<()> {
        // 1. Eliminar cualquier dentry anterior para este child_inode
        //    (un archivo solo puede estar en un directorio a la vez)
        sqlx::query("DELETE FROM dentry WHERE child_inode = ?")
            .bind(child_inode as i64)
            .execute(&self.pool)
            .await?;

        // 2. Insertar el nuevo dentry
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

    /// Elimina una clave de sync_meta
    pub async fn delete_sync_meta(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM sync_meta WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Verifica si existen chunks cacheados para un inodo
    pub async fn has_any_chunks(&self, inode: u64) -> Result<bool> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM file_cache_chunks WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_one(&self.pool)
        .await?;

        Ok(count > 0)
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
    // Protocolo "Burbujeo de Estados" — Contadores pre-calculados
    // ============================================================

    /// Burbujea un cambio de estado desde un archivo hacia todos sus directorios ancestros.
    /// `delta_dirty` y `delta_synced` son incrementos (pueden ser negativos).
    /// Ejemplo: archivo pasa de synced→dirty → delta_dirty=+1, delta_synced=-1
    pub async fn bubble_state_change(
        &self,
        child_inode: u64,
        delta_dirty: i32,
        delta_synced: i32,
    ) -> Result<()> {
        if delta_dirty == 0 && delta_synced == 0 {
            return Ok(());
        }

        sqlx::query(
            r#"
            WITH RECURSIVE ancestors AS (
                SELECT parent_inode FROM dentry WHERE child_inode = ?1
                UNION ALL
                SELECT d.parent_inode FROM dentry d
                JOIN ancestors a ON d.child_inode = a.parent_inode
                WHERE a.parent_inode > 1
            )
            UPDATE dir_counters
            SET dirty_desc_count = MAX(0, dirty_desc_count + ?2),
                synced_desc_count = MAX(0, synced_desc_count + ?3)
            WHERE inode IN (SELECT parent_inode FROM ancestors)
            "#
        )
        .bind(child_inode as i64)
        .bind(delta_dirty)
        .bind(delta_synced)
        .execute(&self.pool)
        .await?;

        // También actualizar root (inode 1) si el archivo cuelga de él
        sqlx::query(
            r#"
            UPDATE dir_counters
            SET dirty_desc_count = MAX(0, dirty_desc_count + ?2),
                synced_desc_count = MAX(0, synced_desc_count + ?3)
            WHERE inode = 1 AND EXISTS (
                WITH RECURSIVE ancestors AS (
                    SELECT parent_inode FROM dentry WHERE child_inode = ?1
                    UNION ALL
                    SELECT d.parent_inode FROM dentry d
                    JOIN ancestors a ON d.child_inode = a.parent_inode
                    WHERE a.parent_inode > 1
                )
                SELECT 1 FROM ancestors WHERE parent_inode = 1
            )
            "#
        )
        .bind(child_inode as i64)
        .bind(delta_dirty)
        .bind(delta_synced)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Inicializa una fila en dir_counters para un directorio si no existe.
    pub async fn ensure_dir_counter(&self, inode: u64) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO dir_counters (inode, dirty_desc_count, synced_desc_count) VALUES (?, 0, 0)"
        )
        .bind(inode as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Marca un inode como dirty y burbujea el cambio a sus ancestros.
    /// Detecta automáticamente el estado previo para calcular el delta correcto.
    /// Solo burbujea para archivos (is_dir=0).
    pub async fn set_dirty_and_bubble(&self, inode: u64) -> Result<()> {
        // Obtener estado previo y si es directorio
        let prev = sqlx::query_as::<_, (Option<String>, Option<bool>, Option<i64>)>(
            "SELECT s.availability, s.dirty, s.deleted_at FROM sync_state s WHERE s.inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        let was_dirty = prev.as_ref().map(|(_, d, del)| {
            d.unwrap_or(false) || del.map(|v| v > 0).unwrap_or(false)
        }).unwrap_or(false);

        let was_synced = prev.as_ref().map(|(av, d, del)| {
            let is_local_online = av.as_deref().unwrap_or("online_only") == "local_online";
            let not_dirty = !d.unwrap_or(false);
            let not_deleted = del.unwrap_or(0) == 0;
            is_local_online && not_dirty && not_deleted
        }).unwrap_or(false);

        // Marcar como dirty
        sqlx::query(
            "INSERT INTO sync_state (inode, dirty, version, md5_checksum) VALUES (?, 1, 0, NULL) ON CONFLICT(inode) DO UPDATE SET dirty = 1"
        )
        .bind(inode as i64)
        .execute(&self.pool)
        .await?;

        // Solo burbujear para archivos
        let is_dir: Option<bool> = sqlx::query_scalar(
            "SELECT is_dir FROM attrs WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        if is_dir == Some(false) {
            // El archivo ahora es dirty seguro
            if was_dirty {
                // Ya era dirty, no hay cambio en contadores
            } else if was_synced {
                // Era synced, ahora es dirty
                self.bubble_state_change(inode, 1, -1).await?;
            } else {
                // Archivo no era dirty ni synced (ej: online_only), ahora es dirty
                self.bubble_state_change(inode, 1, 0).await?;
            }
        }

        Ok(())
    }

    /// Limpia el flag dirty y burbujea el cambio a los ancestros.
    /// Solo burbujea para archivos (is_dir=0).
    pub async fn clear_dirty_and_bubble(&self, inode: u64) -> Result<()> {
        // Verificar estado previo
        let prev = sqlx::query_as::<_, (Option<String>, bool, Option<i64>)>(
            "SELECT availability, dirty, deleted_at FROM sync_state WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        let was_dirty = prev.as_ref().map(|(_, d, del)| {
            *d || del.map(|v| v > 0).unwrap_or(false)
        }).unwrap_or(false);

        let was_synced = prev.as_ref().map(|(av, d, del)| {
            let is_local_online = av.as_deref().unwrap_or("online_only") == "local_online";
            let not_dirty = !d;
            let not_deleted = del.unwrap_or(0) == 0;
            is_local_online && not_dirty && not_deleted
        }).unwrap_or(false);

        // Limpiar dirty
        sqlx::query("UPDATE sync_state SET dirty = 0 WHERE inode = ?")
            .bind(inode as i64)
            .execute(&self.pool)
            .await?;

        // Solo burbujear para archivos
        let is_dir: Option<bool> = sqlx::query_scalar(
            "SELECT is_dir FROM attrs WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        if is_dir == Some(false) {
            let curr = sqlx::query_as::<_, (Option<String>, bool, Option<i64>)>(
                "SELECT availability, dirty, deleted_at FROM sync_state WHERE inode = ?"
            )
            .bind(inode as i64)
            .fetch_one(&self.pool)
            .await?;

            let is_dirty = curr.1 || curr.2.unwrap_or(0) > 0;
            let is_synced = {
                let is_local_online = curr.0.as_deref().unwrap_or("online_only") == "local_online";
                let not_dirty = !curr.1;
                let not_deleted = curr.2.unwrap_or(0) == 0;
                is_local_online && not_dirty && not_deleted
            };

            let delta_dirty = if was_dirty && !is_dirty { -1 } else if !was_dirty && is_dirty { 1 } else { 0 };
            let delta_synced = if was_synced && !is_synced { -1 } else if !was_synced && is_synced { 1 } else { 0 };

            if delta_dirty != 0 || delta_synced != 0 {
                self.bubble_state_change(inode, delta_dirty, delta_synced).await?;
            }
        }

        Ok(())
    }

    /// Recalcula todos los contadores de directorio desde cero.
    /// Usado para migración y como mecanismo de auto-reparación.
    pub async fn rebuild_all_dir_counters(&self) -> Result<()> {
        // Limpiar tabla
        sqlx::query("DELETE FROM dir_counters").execute(&self.pool).await?;

        // Insertar fila para cada directorio
        sqlx::query(
            "INSERT INTO dir_counters (inode, dirty_desc_count, synced_desc_count) SELECT inode, 0, 0 FROM attrs WHERE is_dir = 1"
        )
        .execute(&self.pool)
        .await?;

        // Recalcular usando la CTE recursiva original (one-time cost)
        // Para cada directorio, contar descendientes dirty y synced
        sqlx::query(
            r#"
            UPDATE dir_counters SET
                dirty_desc_count = (
                    WITH RECURSIVE descendants AS (
                        SELECT d.child_inode, a.is_dir
                        FROM dentry d
                        JOIN attrs a ON d.child_inode = a.inode
                        WHERE d.parent_inode = dir_counters.inode
                        UNION ALL
                        SELECT d.child_inode, a.is_dir
                        FROM dentry d
                        JOIN attrs a ON d.child_inode = a.inode
                        JOIN descendants dt ON d.parent_inode = dt.child_inode
                        WHERE dt.is_dir = 1
                    )
                    SELECT COUNT(*) FROM descendants d
                    LEFT JOIN sync_state s ON d.child_inode = s.inode
                    WHERE d.is_dir = 0
                      AND (s.dirty = 1 OR (s.deleted_at IS NOT NULL AND s.deleted_at > 0))
                ),
                synced_desc_count = (
                    WITH RECURSIVE descendants AS (
                        SELECT d.child_inode, a.is_dir
                        FROM dentry d
                        JOIN attrs a ON d.child_inode = a.inode
                        WHERE d.parent_inode = dir_counters.inode
                        UNION ALL
                        SELECT d.child_inode, a.is_dir
                        FROM dentry d
                        JOIN attrs a ON d.child_inode = a.inode
                        JOIN descendants dt ON d.parent_inode = dt.child_inode
                        WHERE dt.is_dir = 1
                    )
                    SELECT COUNT(*) FROM descendants d
                    LEFT JOIN sync_state s ON d.child_inode = s.inode
                    WHERE d.is_dir = 0
                      AND COALESCE(s.availability, 'online_only') = 'local_online'
                      AND COALESCE(s.dirty, 0) = 0
                      AND (s.deleted_at IS NULL OR s.deleted_at = 0)
                )
            "#
        )
        .execute(&self.pool)
        .await?;

        tracing::info!("Contadores de directorio recalculados");
        Ok(())
    }

    /// Verifica si un inode tiene cambios locales pendientes de subir
    pub async fn is_dirty(&self, inode: u64) -> Result<bool> {
        let dirty = sqlx::query_scalar::<_, bool>(
            "SELECT dirty FROM sync_state WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or(false);

        Ok(dirty)
    }

    /// Calcula el estado de sincronización agregado de todos los archivos
    /// descendientes de un directorio, de forma recursiva via CTE.
    /// Retorna (has_local_only, has_synced, total_files).
    pub async fn get_directory_aggregate_status(&self, parent_inode: u64) -> Result<(bool, bool, i64)> {
        // O(1): lectura directa de contadores pre-calculados
        let row = sqlx::query_as::<_, (i64, i64)>(
            "SELECT dirty_desc_count, synced_desc_count FROM dir_counters WHERE inode = ?"
        )
        .bind(parent_inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((dirty, synced)) => {
                let total = dirty + synced;
                Ok((dirty > 0, synced > 0, total))
            }
            None => Ok((false, false, 0)),
        }
    }

    /// Estado agregado para la carpeta virtual SHARED.
    /// Usa contadores pre-calculados con SHARED_INODE como clave.
    pub async fn get_shared_directory_aggregate_status(&self) -> Result<(bool, bool, i64)> {
        // SHARED_INODE = 0xFFFFFFFFFFFFFFFE
        let shared_inode = 0xFFFFFFFFFFFFFFFEu64;
        let row = sqlx::query_as::<_, (i64, i64)>(
            "SELECT dirty_desc_count, synced_desc_count FROM dir_counters WHERE inode = ?"
        )
        .bind(shared_inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((dirty, synced)) => {
                let total = dirty + synced;
                Ok((dirty > 0, synced > 0, total))
            }
            None => Ok((false, false, 0)),
        }
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

    /// Marca un archivo o directorio y todo su contenido (recursivamente) como eliminado
    pub async fn soft_delete_by_gdrive_id(&self, gdrive_id: &str) -> Result<bool> {
        let root_inode = match self.get_inode_by_gdrive_id(gdrive_id).await? {
            Some(i) => i,
            None => return Ok(false),
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        // 0. Burbujeo: contar archivos descendientes que estaban synced (no dirty)
        // antes de marcarlos como dirty. Estos son los que cambian de estado.
        let synced_becoming_dirty: i64 = sqlx::query_scalar(
            r#"
            WITH RECURSIVE subordinates AS (
                SELECT child_inode FROM dentry WHERE child_inode = ?
                UNION ALL
                SELECT d.child_inode FROM dentry d
                JOIN subordinates s ON d.parent_inode = s.child_inode
            )
            SELECT COUNT(*) FROM subordinates sub
            JOIN attrs a ON sub.child_inode = a.inode
            LEFT JOIN sync_state ss ON sub.child_inode = ss.inode
            WHERE a.is_dir = 0
              AND (COALESCE(ss.availability, 'online_only') = 'local_online')
              AND (COALESCE(ss.dirty, 0) = 0)
              AND (ss.deleted_at IS NULL OR ss.deleted_at = 0)
            "#
        )
        .bind(root_inode as i64)
        .fetch_one(&self.pool)
        .await?;



        // Archivos nuevos sin sync_state
        let new_files: i64 = sqlx::query_scalar(
            r#"
            WITH RECURSIVE subordinates AS (
                SELECT child_inode FROM dentry WHERE child_inode = ?
                UNION ALL
                SELECT d.child_inode FROM dentry d
                JOIN subordinates s ON d.parent_inode = s.child_inode
            )
            SELECT COUNT(*) FROM subordinates sub
            JOIN attrs a ON sub.child_inode = a.inode
            WHERE a.is_dir = 0
              AND sub.child_inode NOT IN (SELECT inode FROM sync_state)
            "#
        )
        .bind(root_inode as i64)
        .fetch_one(&self.pool)
        .await?;

        // 1. Identificar recursivamente todos los inodos hijos (incluyendo el raíz)
        let sql_deleted_dentries = r#"
            WITH RECURSIVE subordinates AS (
                SELECT child_inode, parent_inode, name FROM dentry WHERE child_inode = ?
                UNION ALL
                SELECT d.child_inode, d.parent_inode, d.name
                FROM dentry d
                JOIN subordinates s ON d.parent_inode = s.child_inode
            )
            INSERT OR REPLACE INTO dentry_deleted (parent_inode, child_inode, name, deleted_at)
            SELECT parent_inode, child_inode, name, ? FROM subordinates
        "#;

        sqlx::query(sql_deleted_dentries)
            .bind(root_inode as i64)
            .bind(now)
            .execute(&self.pool)
            .await?;

        // 2. Marcar sync_state para todos los inodos afectados
        let sql_update_sync = r#"
            WITH RECURSIVE subordinates AS (
                SELECT child_inode FROM dentry WHERE child_inode = ?
                UNION ALL
                SELECT d.child_inode
                FROM dentry d
                JOIN subordinates s ON d.parent_inode = s.child_inode
            )
            UPDATE sync_state
            SET deleted_at = ?, dirty = 1
            WHERE inode IN (SELECT child_inode FROM subordinates)
        "#;

        sqlx::query(sql_update_sync)
            .bind(root_inode as i64)
            .bind(now)
            .execute(&self.pool)
            .await?;

        // Insertar los que falten
        let sql_insert_sync = r#"
            WITH RECURSIVE subordinates AS (
                SELECT child_inode FROM dentry WHERE child_inode = ?
                UNION ALL
                SELECT d.child_inode
                FROM dentry d
                JOIN subordinates s ON d.parent_inode = s.child_inode
            )
            INSERT INTO sync_state (inode, dirty, version, deleted_at)
            SELECT child_inode, 1, 0, ?
            FROM subordinates
            WHERE child_inode NOT IN (SELECT inode FROM sync_state)
        "#;

        sqlx::query(sql_insert_sync)
            .bind(root_inode as i64)
            .bind(now)
            .execute(&self.pool)
            .await?;

        // 2.5. Burbujear el cambio de estado ANTES de eliminar dentries
        // (bubble_state_change necesita las dentries para caminar hacia los ancestros)
        let delta_dirty = synced_becoming_dirty + new_files; // Nuevos dirty
        let delta_synced = -synced_becoming_dirty; // Dejaron de ser synced
        if delta_dirty != 0 || delta_synced != 0 {
            self.bubble_state_change(root_inode, delta_dirty as i32, delta_synced as i32).await?;
        }

        // 3. Limpiar dentry original para todos los inodos afectados
        // IMPORTANTE: Esto debe ser lo ÚLTIMO porque las CTEs anteriores dependen de dentry.
        let sql_cleanup_dentry = r#"
            WITH RECURSIVE subordinates AS (
                SELECT child_inode FROM dentry WHERE child_inode = ?
                UNION ALL
                SELECT d.child_inode
                FROM dentry d
                JOIN subordinates s ON d.parent_inode = s.child_inode
            )
            DELETE FROM dentry WHERE child_inode IN (SELECT child_inode FROM subordinates)
        "#;

        sqlx::query(sql_cleanup_dentry)
            .bind(root_inode as i64)
            .execute(&self.pool)
            .await?;

        tracing::info!("Recursive soft delete applied for gdrive_id={}, root_inode={}", gdrive_id, root_inode);
        Ok(true)
    }

    /// Restaura un archivo eliminado (quita tombstone)
    /// Mueve el dentry de vuelta, elimina deleted_at
    pub async fn restore_by_gdrive_id(&self, gdrive_id: &str) -> Result<bool> {
        let inode = match self.get_inode_by_gdrive_id(gdrive_id).await? {
            Some(i) => i,
            None => return Ok(false),
        };

        // Estado previo
        let prev = sqlx::query_as::<_, (Option<String>, Option<bool>, Option<i64>)>(
            "SELECT s.availability, s.dirty, s.deleted_at FROM sync_state s WHERE s.inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        let was_dirty = prev.as_ref().map(|(_, d, del)| {
            d.unwrap_or(false) || del.map(|v| v > 0).unwrap_or(false)
        }).unwrap_or(false);

        let was_synced = prev.as_ref().map(|(av, d, del)| {
            let is_local_online = av.as_deref().unwrap_or("online_only") == "local_online";
            let not_dirty = !d.unwrap_or(false);
            let not_deleted = del.unwrap_or(0) == 0;
            is_local_online && not_dirty && not_deleted
        }).unwrap_or(false);

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

        // 4. Burbujear
        let is_dir: Option<bool> = sqlx::query_scalar(
            "SELECT is_dir FROM attrs WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;

        if is_dir == Some(false) {
            let curr = sqlx::query_as::<_, (Option<String>, Option<bool>, Option<i64>)>(
                "SELECT s.availability, s.dirty, s.deleted_at FROM sync_state s WHERE s.inode = ?"
            )
            .bind(inode as i64)
            .fetch_one(&self.pool)
            .await?;

            let is_dirty = curr.1.unwrap_or(false) || curr.2.unwrap_or(0) > 0;
            let is_synced = {
                let is_local_online = curr.0.as_deref().unwrap_or("online_only") == "local_online";
                let not_dirty = !curr.1.unwrap_or(false);
                let not_deleted = curr.2.unwrap_or(0) == 0;
                is_local_online && not_dirty && not_deleted
            };

            let delta_dirty = if was_dirty && !is_dirty { -1 } else if !was_dirty && is_dirty { 1 } else { 0 };
            let delta_synced = if was_synced && !is_synced { -1 } else if !was_synced && is_synced { 1 } else { 0 };

            if delta_dirty != 0 || delta_synced != 0 {
                self.bubble_state_change(inode, delta_dirty, delta_synced).await?;
            }
        } else if is_dir == Some(true) {
            // Restaurar directorio: asegurar que tiene fila en dir_counters
            self.ensure_dir_counter(inode).await?;
        }

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
            self.hard_delete_inode(*inode as u64).await?;
        }

        tracing::info!("Purgados {} tombstones expirados (grace_days={})", count, grace_days);
        Ok(count)
    }

    /// Elimina permanentemente un inode y todos sus registros asociados
    async fn hard_delete_inode(&self, inode: u64) -> Result<()> {
        let inode_i64 = inode as i64;

        // Burbujear: decrementar contadores de ancestros según estado previo del archivo
        // (solo para archivos, no directorios — los directorios eliminados ya tuvieron
        // sus descendientes procesados en soft_delete)
        let is_dir: Option<bool> = sqlx::query_scalar(
            "SELECT is_dir FROM attrs WHERE inode = ?"
        )
        .bind(inode_i64)
        .fetch_optional(&self.pool)
        .await?;

        if is_dir == Some(false) {
            // Verificar estado actual del archivo para calcular delta
            let state = sqlx::query_as::<_, (Option<bool>, Option<i64>)>(
                "SELECT dirty, deleted_at FROM sync_state WHERE inode = ?"
            )
            .bind(inode_i64)
            .fetch_optional(&self.pool)
            .await?;

            if let Some((dirty, deleted_at)) = state {
                let was_dirty = dirty.unwrap_or(false) || deleted_at.map(|v| v > 0).unwrap_or(false);
                if was_dirty {
                    self.bubble_state_change(inode, -1, 0).await?;
                } else {
                    self.bubble_state_change(inode, 0, -1).await?;
                }
            }
        }

        // Eliminar de todas las tablas relacionadas
        sqlx::query("DELETE FROM dentry WHERE child_inode = ?")
            .bind(inode_i64)
            .execute(&self.pool)
            .await?;

        sqlx::query("DELETE FROM dentry_deleted WHERE child_inode = ?")
            .bind(inode_i64)
            .execute(&self.pool)
            .await?;

        sqlx::query("DELETE FROM sync_state WHERE inode = ?")
            .bind(inode_i64)
            .execute(&self.pool)
            .await?;

        sqlx::query("DELETE FROM attrs WHERE inode = ?")
            .bind(inode_i64)
            .execute(&self.pool)
            .await?;

        sqlx::query("DELETE FROM file_cache_chunks WHERE inode = ?")
            .bind(inode_i64)
            .execute(&self.pool)
            .await?;

        // Limpiar dir_counters si era directorio
        sqlx::query("DELETE FROM dir_counters WHERE inode = ?")
            .bind(inode_i64)
            .execute(&self.pool)
            .await?;

        sqlx::query("DELETE FROM inodes WHERE inode = ?")
            .bind(inode_i64)
            .execute(&self.pool)
            .await?;

        tracing::debug!("Hard delete completado para inode={}", inode);
        Ok(())
    }

    /// Hard delete por gdrive_id: elimina permanentemente un archivo de la DB
    /// Usado cuando un archivo es eliminado permanentemente de Google Drive
    pub async fn hard_delete_by_gdrive_id(&self, gdrive_id: &str) -> Result<bool> {
        let inode = match self.get_inode_by_gdrive_id(gdrive_id).await? {
            Some(i) => i,
            None => return Ok(false), // No existe, nada que eliminar
        };

        self.hard_delete_inode(inode).await?;
        tracing::info!("Hard delete aplicado: gdrive_id={}, inode={}", gdrive_id, inode);
        Ok(true)
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

    /// Limpia todos los chunks cacheados de un inodo (usado en caso de corrupción detectada)
    pub async fn clear_chunks(&self, inode: u64) -> Result<()> {
        sqlx::query("DELETE FROM file_cache_chunks WHERE inode = ?")
            .bind(inode as i64)
            .execute(&self.pool)
            .await?;
        
        tracing::warn!("🧹 Chunks limpiados para inode: {}", inode);
        Ok(())
    }

    /// Obtiene el offset máximo registrado en los chunks (para validar consistencia de tamaño)
    pub async fn get_max_cached_offset(&self, inode: u64) -> Result<u64> {
        let max_offset: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(end_offset) FROM file_cache_chunks WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;
        
        Ok(max_offset.unwrap_or(0) as u64)
    }

    /// Obtiene el total de bytes cacheados sumando todos los chunks
    pub async fn get_cached_bytes_count(&self, inode: u64) -> Result<u64> {
        let total: Option<i64> = sqlx::query_scalar(
            "SELECT SUM(end_offset - start_offset + 1) FROM file_cache_chunks WHERE inode = ?"
        )
        .bind(inode as i64)
        .fetch_optional(&self.pool)
        .await?;
        
        Ok(total.unwrap_or(0) as u64)
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

    // ============================================================
    // Métodos para Local Sync Directories
    // ============================================================

    /// Añade un directorio local a la lista de sincronización
    pub async fn add_local_sync_dir(&self, local_path: &Path) -> Result<i64> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        let path_str = local_path.to_string_lossy().to_string();

        let id = sqlx::query(
            r#"
            INSERT INTO local_sync_dirs (local_path, enabled, created_at)
            VALUES (?, 1, ?)
            "#
        )
        .bind(&path_str)
        .bind(now)
        .execute(&self.pool)
        .await?
        .last_insert_rowid();

        tracing::info!("Directorio local añadido: {} (id={})", path_str, id);
        Ok(id)
    }

    /// Elimina un directorio local de la sincronización
    pub async fn remove_local_sync_dir(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM local_sync_dirs WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        tracing::info!("Directorio local eliminado: id={}", id);
        Ok(())
    }

    /// Activa/desactiva la sincronización de un directorio local
    pub async fn toggle_local_sync_dir(&self, id: i64, enabled: bool) -> Result<()> {
        sqlx::query("UPDATE local_sync_dirs SET enabled = ? WHERE id = ?")
            .bind(enabled)
            .bind(id)
            .execute(&self.pool)
            .await?;

        tracing::debug!("Directorio local {} (id={})", if enabled { "activado" } else { "desactivado" }, id);
        Ok(())
    }

    /// Obtiene todos los directorios locales configurados
    pub async fn get_local_sync_dirs(&self) -> Result<Vec<LocalSyncDir>> {
        let dirs = sqlx::query_as::<_, LocalSyncDir>(
            "SELECT id, local_path, gdrive_folder_id, enabled, last_sync, created_at FROM local_sync_dirs ORDER BY created_at"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(dirs)
    }

    /// Obtiene solo los directorios locales habilitados
    pub async fn get_enabled_local_sync_dirs(&self) -> Result<Vec<LocalSyncDir>> {
        let dirs = sqlx::query_as::<_, LocalSyncDir>(
            "SELECT id, local_path, gdrive_folder_id, enabled, last_sync, created_at FROM local_sync_dirs WHERE enabled = 1 ORDER BY created_at"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(dirs)
    }

    /// Establece el gdrive_folder_id para un directorio local
    pub async fn set_gdrive_folder_id(&self, id: i64, gdrive_id: &str) -> Result<()> {
        sqlx::query("UPDATE local_sync_dirs SET gdrive_folder_id = ? WHERE id = ?")
            .bind(gdrive_id)
            .bind(id)
            .execute(&self.pool)
            .await?;

        tracing::debug!("GDrive folder ID {} asociado a local_sync_dir id={}", gdrive_id, id);
        Ok(())
    }

    /// Actualiza el timestamp de última sincronización
    pub async fn update_last_sync(&self, id: i64) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        sqlx::query("UPDATE local_sync_dirs SET last_sync = ? WHERE id = ?")
            .bind(now)
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    // ============================================================
    // Métodos para Local Sync Files (Hybrid Local Sync)
    // ============================================================

    /// Inserta o actualiza un archivo en local_sync_files
    pub async fn upsert_local_sync_file(
        &self,
        sync_dir_id: i64,
        relative_path: &str,
        is_dir: bool,
        availability: &str,
        local_mtime: Option<i64>,
        local_size: Option<i64>,
        local_md5: Option<&str>,
    ) -> Result<i64> {
        let id = sqlx::query(
            r#"
            INSERT INTO local_sync_files 
                (sync_dir_id, relative_path, is_dir, availability, local_mtime, local_size, local_md5, dirty)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(sync_dir_id, relative_path) DO UPDATE SET
                is_dir = excluded.is_dir,
                availability = excluded.availability,
                local_mtime = excluded.local_mtime,
                local_size = excluded.local_size,
                local_md5 = excluded.local_md5,
                dirty = 1
            "#
        )
        .bind(sync_dir_id)
        .bind(relative_path)
        .bind(is_dir)
        .bind(availability)
        .bind(local_mtime)
        .bind(local_size)
        .bind(local_md5)
        .execute(&self.pool)
        .await?
        .last_insert_rowid();

        Ok(id)
    }

    /// Obtiene archivos dirty para subir
    pub async fn get_dirty_local_sync_files(&self) -> Result<Vec<LocalSyncFile>> {
        let files = sqlx::query_as::<_, LocalSyncFile>(
            "SELECT * FROM local_sync_files WHERE dirty = 1 ORDER BY id"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(files)
    }

    /// Obtiene un archivo local por sync_dir_id y relative_path
    pub async fn get_local_sync_file(&self, sync_dir_id: i64, relative_path: &str) -> Result<Option<LocalSyncFile>> {
        let file = sqlx::query_as::<_, LocalSyncFile>(
            "SELECT * FROM local_sync_files WHERE sync_dir_id = ? AND relative_path = ?"
        )
        .bind(sync_dir_id)
        .bind(relative_path)
        .fetch_optional(&self.pool)
        .await?;

        Ok(file)
    }

    /// Busca un archivo local por gdrive_id
    pub async fn find_local_sync_file_by_gdrive_id(&self, gdrive_id: &str) -> Result<Option<LocalSyncFile>> {
        let file = sqlx::query_as::<_, LocalSyncFile>(
            "SELECT * FROM local_sync_files WHERE gdrive_id = ?"
        )
        .bind(gdrive_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(file)
    }

    /// Obtiene un directorio local por ID
    pub async fn get_local_sync_dir(&self, id: i64) -> Result<LocalSyncDir> {
        let dir = sqlx::query_as::<_, LocalSyncDir>(
            "SELECT * FROM local_sync_dirs WHERE id = ?"
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Ok(dir)
    }

    /// Cambia el modo de disponibilidad de un archivo
    pub async fn set_file_availability(&self, sync_dir_id: i64, relative_path: &str, availability: &str) -> Result<()> {
        sqlx::query(
            "UPDATE local_sync_files SET availability = ? WHERE sync_dir_id = ? AND relative_path = ?"
        )
        .bind(availability)
        .bind(sync_dir_id)
        .bind(relative_path)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Actualiza metadatos locales de un archivo
    pub async fn update_local_file_metadata(
        &self,
        sync_dir_id: i64,
        relative_path: &str,
        availability: &str,
        local_size: i64,
        local_mtime: i64,
        local_md5: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE local_sync_files 
            SET availability = ?, local_size = ?, local_mtime = ?, local_md5 = ?
            WHERE sync_dir_id = ? AND relative_path = ?
            "#
        )
        .bind(availability)
        .bind(local_size)
        .bind(local_mtime)
        .bind(local_md5)
        .bind(sync_dir_id)
        .bind(relative_path)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Actualiza metadatos remotos desde un cambio de Drive
    pub async fn update_local_file_from_remote(
        &self,
        file_id: i64,
        remote_md5: Option<&str>,
        remote_mtime: Option<i64>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE local_sync_files SET remote_md5 = ?, remote_mtime = ?, dirty = 0 WHERE id = ?"
        )
        .bind(remote_md5)
        .bind(remote_mtime)
        .bind(file_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Actualiza solo metadatos remotos (para archivos online_only)
    pub async fn update_local_file_remote_metadata(&self, file_id: i64, remote_md5: Option<&str>) -> Result<()> {
        sqlx::query(
            "UPDATE local_sync_files SET remote_md5 = ? WHERE id = ?"
        )
        .bind(remote_md5)
        .bind(file_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Limpia el flag dirty de un archivo después de una subida exitosa
    pub async fn clear_local_file_dirty(&self, file_id: i64) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        sqlx::query(
            "UPDATE local_sync_files SET dirty = 0, last_synced = ? WHERE id = ?"
        )
        .bind(now)
        .bind(file_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Establece el gdrive_id para un archivo local después de crearlo en Drive
    pub async fn set_local_file_gdrive_id(&self, file_id: i64, gdrive_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE local_sync_files SET gdrive_id = ? WHERE id = ?"
        )
        .bind(gdrive_id)
        .bind(file_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Busca un archivo de local sync por su path absoluto del filesystem
    /// Útil para comandos IPC que reciben file:// URIs de Nautilus
    pub async fn find_local_sync_file_by_absolute_path(
        &self,
        absolute_path: &str,
    ) -> Result<Option<LocalSyncFile>> {
        // Obtener todas las carpetas locales habilitadas
        let sync_dirs = self.get_enabled_local_sync_dirs().await?;
        
        // Intentar encontrar qué carpeta contiene este path
        for dir in sync_dirs {
            let base_path = &dir.local_path;
            
            // Si el absolute_path empieza con este base_path
            if absolute_path.starts_with(base_path) {
                // Calcular relative_path
                let relative_path = absolute_path
                    .strip_prefix(base_path)
                    .unwrap_or("")
                    .trim_start_matches('/');
                
                // Buscar el archivo en la DB
                if let Some(file) = self.get_local_sync_file(dir.id, relative_path).await? {
                    return Ok(Some(file));
                }
            }
        }
        
        Ok(None)
    }

    /// Resuelve un path absoluto a (sync_dir_id, relative_path)
    /// Lanza error si el path no pertenece a ninguna carpeta Local Sync
    pub async fn resolve_local_sync_path(
        &self,
        absolute_path: &str,
    ) -> Result<(i64, String)> {
        let sync_dirs = self.get_enabled_local_sync_dirs().await?;
        
        for dir in sync_dirs {
            let base_path = &dir.local_path;
            
            if absolute_path.starts_with(base_path) {
                let relative_path = absolute_path
                    .strip_prefix(base_path)
                    .unwrap_or("")
                    .trim_start_matches('/')
                    .to_string();
                
                return Ok((dir.id, relative_path));
            }
        }
        
        Err(anyhow::anyhow!("Path no pertenece a ninguna carpeta Local Sync: {}", absolute_path))
    }
}
/// Struct que representa un directorio local sincronizado
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LocalSyncDir {
    pub id: i64,
    pub local_path: String,
    pub gdrive_folder_id: Option<String>,
    pub enabled: bool,
    pub last_sync: i64,
    pub created_at: i64,
}

/// Struct que representa un archivo individual en Local Sync
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LocalSyncFile {
    pub id: i64,
    pub sync_dir_id: i64,
    pub relative_path: String,
    pub is_dir: bool,
    
    pub availability: String,  // 'local_online' | 'online_only'
    
    pub local_mtime: Option<i64>,
    pub local_size: Option<i64>,
    pub local_md5: Option<String>,
    
    pub gdrive_id: Option<String>,
    pub remote_md5: Option<String>,
    pub remote_mtime: Option<i64>,
    
    pub dirty: bool,
    pub last_synced: Option<i64>,
}
