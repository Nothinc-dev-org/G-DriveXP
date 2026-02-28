use anyhow::Result;
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

    // Inicializar dir_counters para root
    db.ensure_dir_counter(1).await?;

    tracing::debug!("Inode raíz (1) verificado/creado en la base de datos");

    // FIX MIGRATION: Update existing incorrect permissions
    // Flatpak/Audio players fail if directories don't have execute bits (0o755 / 493)
    let _ = sqlx::query("UPDATE attrs SET mode = 493 WHERE is_dir = 1 AND mode = 420")
        .execute(pool)
        .await;

    let _ = sqlx::query("UPDATE attrs SET mode = 420 WHERE is_dir = 0 AND mode != 420")
        .execute(pool)
        .await;

    Ok(())
}

/// Helper: procesa un archivo de Drive e inserta inode + attrs.
/// Retorna (inode, is_dir).
async fn insert_file_metadata(
    db: &Arc<MetadataRepository>,
    file: &google_drive3::api::File,
) -> Result<Option<(u64, bool)>> {
    let id = match &file.id {
        Some(id) => id,
        None => return Ok(None),
    };

    let inode = db.get_or_create_inode(id).await?;
    let is_dir = file.mime_type.as_deref() == Some("application/vnd.google-apps.folder");
    let size = file.size.unwrap_or(0);
    let mtime = file.modified_time
        .as_ref()
        .map(|t| t.timestamp())
        .unwrap_or(0);
    let mode = if is_dir { 0o755 } else { 0o644 };
    let can_move = file.capabilities.as_ref()
        .and_then(|c| c.can_move_item_within_drive)
        .unwrap_or(true);
    let shared = file.shared.unwrap_or(false);

    db.upsert_file_metadata(
        inode, size, mtime, mode, is_dir,
        file.mime_type.as_deref(), can_move, shared,
        file.owned_by_me.unwrap_or(true),
    ).await?;

    // Inicializar dir_counters para directorios
    if is_dir {
        db.ensure_dir_counter(inode).await?;
    }

    Ok(Some((inode, is_dir)))
}

/// Bootstrap Fase 1: Solo los hijos directos del root.
/// Retorna rápidamente (~1 segundo) permitiendo que la app funcione de inmediato.
pub async fn bootstrap_level1(
    db: &Arc<MetadataRepository>,
    client: &Arc<DriveClient>,
    root_id: &str,
) -> Result<()> {
    tracing::info!("Bootstrap nivel 1: cargando hijos directos del root...");

    ensure_root_exists(db).await?;

    // Fetch rápido: solo hijos de root
    let root_children = client.list_root_children(root_id).await?;
    tracing::info!("Bootstrap nivel 1: {} items encontrados en root", root_children.len());

    // Insertar inodes + attrs + dentries para nivel 1
    for file in &root_children {
        if let Some((inode, _is_dir)) = insert_file_metadata(db, file).await? {
            if let Some(name) = &file.name {
                db.upsert_dentry(1, inode, name).await?;
            }
        }
    }

    // Recalcular contadores del root después de insertar nivel 1
    // (todos los archivos nuevos del bootstrap son synced por defecto)
    db.rebuild_all_dir_counters().await?;

    tracing::info!("Bootstrap nivel 1 completado");
    Ok(())
}

/// Bootstrap Fase 2: Completa el resto del árbol usando BFS.
/// Diseñado para ejecutarse en un tokio::spawn dedicado (no bloquea el flujo principal).
pub async fn bootstrap_remaining_bfs(
    db: &Arc<MetadataRepository>,
    client: &Arc<DriveClient>,
    root_id: &str,
) -> Result<()> {
    tracing::info!("Bootstrap BFS: descargando árbol completo en segundo plano...");

    // Obtener TODOS los archivos via list_all_files (más eficiente que una llamada por carpeta)
    let all_files = client.list_all_files().await?;
    tracing::info!("Bootstrap BFS: {} archivos totales obtenidos de Drive", all_files.len());

    // Mapeo de DriveID -> Inode
    let mut drive_id_to_inode: HashMap<String, u64> = HashMap::new();
    drive_id_to_inode.insert("root".to_string(), 1u64);
    if !root_id.is_empty() {
        drive_id_to_inode.insert(root_id.to_string(), 1u64);
    }

    // Agrupar archivos por padre para procesarlos en BFS
    let mut by_parent: HashMap<String, Vec<&google_drive3::api::File>> = HashMap::new();
    let mut root_level_dir_ids: Vec<String> = Vec::new();

    // Primera pasada: crear inodes y metadata para TODOS los archivos
    // y agrupar por padre
    for file in &all_files {
        if let Some(id) = &file.id {
            // INSERT OR IGNORE: si ya existe del nivel 1, no lo sobreescribe
            let inode = db.get_or_create_inode(id).await?;
            drive_id_to_inode.insert(id.clone(), inode);

            let is_dir = file.mime_type.as_deref() == Some("application/vnd.google-apps.folder");
            let size = file.size.unwrap_or(0);
            let mtime = file.modified_time
                .as_ref()
                .map(|t| t.timestamp())
                .unwrap_or(0);
            let mode = if is_dir { 0o755 } else { 0o644 };
            let can_move = file.capabilities.as_ref()
                .and_then(|c| c.can_move_item_within_drive)
                .unwrap_or(true);
            let shared = file.shared.unwrap_or(false);

            db.upsert_file_metadata(
                inode, size, mtime, mode, is_dir,
                file.mime_type.as_deref(), can_move, shared,
                file.owned_by_me.unwrap_or(true),
            ).await?;

            if is_dir {
                db.ensure_dir_counter(inode).await?;
            }

            // Agrupar por padre
            if let Some(parents) = &file.parents {
                for parent_id in parents {
                    by_parent.entry(parent_id.clone()).or_default().push(file);

                    // Identificar directorios de nivel 1 (hijos del root)
                    if (parent_id == "root" || parent_id == root_id) && is_dir {
                        root_level_dir_ids.push(id.clone());
                    }
                }
            }
        }
    }

    // BFS: construir dentries nivel por nivel
    // Nivel 1 ya se insertó en bootstrap_level1, pero usamos INSERT OR IGNORE
    // para manejar el caso idempotente
    let mut current_level_ids = vec!["root".to_string()];
    if !root_id.is_empty() {
        current_level_ids.push(root_id.to_string());
    }
    let mut level = 1u32;
    let mut processed_parents: std::collections::HashSet<String> = std::collections::HashSet::new();

    while !current_level_ids.is_empty() {
        let mut next_level_ids = Vec::new();

        for parent_gdrive_id in &current_level_ids {
            if processed_parents.contains(parent_gdrive_id) {
                continue;
            }
            processed_parents.insert(parent_gdrive_id.clone());

            if let Some(children) = by_parent.get(parent_gdrive_id) {
                let parent_inode = drive_id_to_inode.get(parent_gdrive_id)
                    .cloned()
                    .unwrap_or(1);

                for file in children {
                    if let (Some(id), Some(name)) = (&file.id, &file.name) {
                        if let Some(&child_inode) = drive_id_to_inode.get(id.as_str()) {
                            // INSERT OR IGNORE: idempotente si ya existe del nivel 1
                            db.upsert_dentry(parent_inode, child_inode, name).await?;

                            // Si es directorio, añadir a la cola BFS
                            let is_dir = file.mime_type.as_deref() == Some("application/vnd.google-apps.folder");
                            if is_dir {
                                next_level_ids.push(id.clone());
                            }
                        }
                    }
                }
            }
        }

        if level > 1 {
            tracing::debug!("Bootstrap BFS: nivel {} procesado ({} directorios)", level, next_level_ids.len());
        }

        current_level_ids = next_level_ids;
        level += 1;

        // Yield periódico para no acaparar el executor
        if level % 5 == 0 {
            tokio::task::yield_now().await;
        }
    }

    // Recalcular TODOS los contadores de directorio ahora que el árbol está completo
    tracing::info!("Bootstrap BFS: recalculando contadores de directorio...");
    db.rebuild_all_dir_counters().await?;

    tracing::info!("Bootstrap BFS completado: {} niveles procesados", level - 1);
    Ok(())
}

/// Repara específicamente los metadatos de propiedad (owned_by_me)
/// Útil cuando la base de datos tiene datos antiguos o incompletos
pub async fn repair_ownership_metadata(
    db: &Arc<MetadataRepository>,
    client: &Arc<DriveClient>,
) -> Result<()> {
    tracing::info!("Iniciando REPARACIÓN de metadatos de propiedad...");

    // 1. Obtener lista mínima de Google Drive (solo IDs y propiedad)
    let files = client.list_all_files().await?;
    let total = files.len();

    let mut repaired_count = 0;
    for file in files {
        if let Some(id) = file.id {
            // Solo actualizamos si el inodo existe localmente
            if let Some(inode) = db.get_inode_by_gdrive_id(&id).await? {
                let owned = file.owned_by_me.unwrap_or(true);
                db.update_ownership(inode, owned).await?;
                repaired_count += 1;
            }
        }
    }

    tracing::info!("Reparación completada: {}/{} archivos actualizados", repaired_count, total);
    Ok(())
}
