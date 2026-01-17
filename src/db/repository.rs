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
        
        // Inicializar esquema
        sqlx::query(include_str!("schema.sql"))
            .execute(&pool)
            .await?;
        
        Ok(Self { pool })
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
    /// Listar contenido de un directorio (para readdir)
    pub async fn list_children(&self, parent_inode: u64) -> Result<Vec<(u64, String, bool)>> {
        // Hacemos un JOIN para saber si es directorio (de la tabla attrs)
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
}
