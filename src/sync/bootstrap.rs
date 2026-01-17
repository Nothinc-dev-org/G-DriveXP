use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use crate::db::MetadataRepository;
use crate::gdrive::client::DriveClient;

/// Asegura que el inode raíz (1) exista en la base de datos.
/// Esto es necesario porque GDrive no tiene un "archivo" para el root,
/// pero FUSE siempre consulta inode=1 como punto de entrada.
async fn ensure_root_exists(db: &Arc<MetadataRepository>) -> Result<()> {
    let pool = db.pool();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;

    // Insertar en tabla inodes (el root tiene gdrive_id = "root")
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO inodes (inode, gdrive_id, generation, created_at)
        VALUES (1, 'root', 0, ?)
        "#
    )
    .bind(now)
    .execute(pool)
    .await?;

    // Insertar en tabla attrs
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO attrs (inode, size, mtime, ctime, mode, is_dir, mime_type)
        VALUES (1, 4096, ?, ?, 493, 1, 'application/vnd.google-apps.folder')
        "#
    )
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;

    tracing::debug!("Inode raíz (1) verificado/creado en la base de datos");
    Ok(())
}

/// Ejecuta la sincronización inicial de metadatos
pub async fn sync_all_metadata(
    db: &Arc<MetadataRepository>,
    client: &Arc<DriveClient>,
) -> Result<()> {
    tracing::info!("Iniciando bootstrapping de metadatos...");

    // 1. Obtener todos los archivos de Drive
    let files = client.list_all_files().await?;
    
    // 2. Mapeo temporal de DriveID -> Inode
    // Esto nos ayudará a resolver los padres
    let mut drive_id_to_inode = HashMap::new();
    
    // 3. Primero, asegurar que el root existe en la base de datos
    // Esto es CRÍTICO: el inode 1 debe existir como registro en `inodes` y `attrs`
    // para que las referencias foreign key en `dentry` sean válidas
    ensure_root_exists(db).await?;
    drive_id_to_inode.insert("root".to_string(), 1u64);

    // 4. Procesar archivos en dos pasadas o con recursión
    // Primera pasada: Crear todos los inodos y guardar sus metadatos básicos
    for file in &files {
        if let Some(id) = &file.id {
            let inode = db.get_or_create_inode(id).await?;
            drive_id_to_inode.insert(id.clone(), inode);

            // Determinar si es directorio
            let is_dir = file.mime_type.as_deref() == Some("application/vnd.google-apps.folder");
            
            // Metadatos
            let size = file.size.unwrap_or(0);
            let mtime = file.modified_time
                .as_ref()
                .map(|t| t.timestamp())
                .unwrap_or(0);
            
            // Modo POSIX básico
            let mode = if is_dir { 0o755 } else { 0o644 };

            db.upsert_file_metadata(
                inode,
                size,
                mtime,
                mode,
                is_dir,
                file.mime_type.as_deref()
            ).await?;
        }
    }

    // Segunda pasada: Construir el árbol (dentries)
    for file in &files {
        if let (Some(id), Some(name)) = (&file.id, &file.name) {
            let child_inode = drive_id_to_inode.get(id).cloned().context("Inode no encontrado para ID")?;
            
            if let Some(parents) = &file.parents {
                for parent_id in parents {
                    if let Some(&parent_inode) = drive_id_to_inode.get(parent_id) {
                        db.upsert_dentry(parent_inode, child_inode, name).await?;
                    } else {
                        // Si el padre no está en nuestro set (ej. compartido fuera del drive principal)
                        // lo colgamos del root por ahora
                        db.upsert_dentry(1, child_inode, name).await?;
                    }
                }
            } else {
                // Sin padres explícitos -> Colgar del root
                db.upsert_dentry(1, child_inode, name).await?;
            }
        }
    }

    tracing::info!("Bootstrapping completado exitosamente");
    Ok(())
}
