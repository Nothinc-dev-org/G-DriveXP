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

/// Escanea Drive progresivamente, procesando cada página (~1000 archivos) de inmediato.
/// Elimina el blackout del bootstrap: los archivos aparecen en el espejo a medida que se escanean.
pub async fn bootstrap_remaining_bfs(
    db: &Arc<MetadataRepository>,
    client: &Arc<DriveClient>,
    root_id: &str,
    history: &crate::gui::history::ActionHistory,
    mirror_sender: &tokio::sync::mpsc::Sender<crate::mirror::MirrorCommand>,
) -> Result<()> {
    tracing::info!("Escaneo progresivo: iniciando...");
    ensure_root_exists(db).await?;

    // Mapa acumulativo gdrive_id → inode (crece con cada página)
    let mut drive_id_to_inode: HashMap<String, u64> = HashMap::new();
    drive_id_to_inode.insert("root".to_string(), 1u64);
    if !root_id.is_empty() {
        drive_id_to_inode.insert(root_id.to_string(), 1u64);
    }

    // Archivos compartidos no propios para resolución de huérfanos al final
    let mut shared_non_owned: Vec<(u64, String)> = Vec::new();

    let mut page_token: Option<String> = None;
    let mut total_scanned: usize = 0;
    let mut page_number: u32 = 0;

    loop {
        // Obtener una página de la API
        let (page_files, next_token) = client.fetch_files_page(page_token.as_deref()).await?;
        if page_files.is_empty() && next_token.is_none() {
            break;
        }

        page_number += 1;
        let page_count = page_files.len();
        total_scanned += page_count;

        // 1. Recoger TODOS los drive_ids referenciados (archivos + padres)
        let mut all_ids_in_page: Vec<String> = Vec::with_capacity(page_count * 2);
        for file in &page_files {
            if let Some(id) = &file.id {
                all_ids_in_page.push(id.clone());
            }
            if let Some(parents) = &file.parents {
                for pid in parents {
                    if pid != "root" && pid != root_id {
                        all_ids_in_page.push(pid.clone());
                    }
                }
            }
        }

        // 2. Obtener/crear inodes en bloque para esta página
        let page_inodes = db.get_or_create_inodes_bulk(&all_ids_in_page).await?;
        drive_id_to_inode.extend(page_inodes);

        // 3. Upsert metadatos + dentries para archivos de esta página
        let mut metadata_buffer = Vec::with_capacity(page_count);
        let mut dentry_buffer = Vec::with_capacity(page_count);

        for file in &page_files {
            let id = match &file.id {
                Some(id) => id,
                None => continue,
            };

            let inode = match drive_id_to_inode.get(id.as_str()) {
                Some(&i) => i,
                None => continue,
            };

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
            let owned = file.owned_by_me.unwrap_or(true);

            metadata_buffer.push(crate::db::BulkFileMetadata {
                inode, size, mtime, mode, is_dir,
                mime_type: file.mime_type.clone(),
                can_move, shared,
                owned_by_me: owned,
            });

            // Dentry: vincular hijo con padre
            if let Some(parents) = &file.parents {
                if let Some(name) = &file.name {
                    for parent_id in parents {
                        let parent_inode = if parent_id == "root" || parent_id == root_id {
                            1u64
                        } else {
                            match drive_id_to_inode.get(parent_id.as_str()) {
                                Some(&pi) => pi,
                                None => continue,
                            }
                        };
                        dentry_buffer.push(crate::db::BulkDentry {
                            parent_inode, child_inode: inode, name: name.clone(),
                        });
                    }
                }
            }

            // Acumular compartidos no propios para resolución posterior
            if !owned {
                if let Some(name) = &file.name {
                    shared_non_owned.push((inode, name.clone()));
                }
            }
        }

        // Flush metadatos y dentries de esta página
        if !metadata_buffer.is_empty() {
            db.upsert_bulk_file_metadata(&metadata_buffer).await?;
        }
        if !dentry_buffer.is_empty() {
            db.upsert_bulk_dentries(&dentry_buffer).await?;
        }

        // Reportar progreso a GUI
        tracing::info!("Escaneo progresivo: página {}, {} archivos escaneados", page_number, total_scanned);
        history.set_scanning_total(total_scanned);

        page_token = next_token;
        if page_token.is_none() {
            break;
        }

        tokio::task::yield_now().await;
    }

    // Post-procesamiento: archivos compartidos huérfanos (sin dentry)
    if !shared_non_owned.is_empty() {
        tracing::info!("Escaneo: vinculando {} archivos compartidos huérfanos...", shared_non_owned.len());
        let mut orphan_buffer = Vec::new();
        for (inode, name) in &shared_non_owned {
            if !db.has_dentry(*inode).await.unwrap_or(true) {
                orphan_buffer.push(crate::db::BulkDentry {
                    parent_inode: 1, child_inode: *inode, name: name.clone(),
                });
                if orphan_buffer.len() >= 500 {
                    db.upsert_bulk_dentries(&orphan_buffer).await?;
                    orphan_buffer.clear();
                }
            }
        }
        if !orphan_buffer.is_empty() {
            db.upsert_bulk_dentries(&orphan_buffer).await?;
        }
    }

    // Recalcular contadores y enviar refresh final
    db.rebuild_all_dir_counters().await?;
    let _ = mirror_sender.send(crate::mirror::MirrorCommand::Refresh).await;

    // Señalar fin de escaneo
    history.set_scanning_total(0);
    tracing::info!("Escaneo progresivo completado: {} archivos en total.", total_scanned);
    history.log(
        crate::gui::history::ActionType::Sync,
        format!("Escaneo completado: {} archivos", total_scanned),
    );

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
    let mut buffer = Vec::with_capacity(500);

    for file in files {
        if let Some(id) = file.id {
            // Solo actualizamos si el inodo existe localmente
            if let Some(inode) = db.get_inode_by_gdrive_id(&id).await? {
                let owned = file.owned_by_me.unwrap_or(true);
                buffer.push((inode, owned));
                
                if buffer.len() >= 500 {
                    db.update_bulk_ownership(&buffer).await?;
                    repaired_count += buffer.len();
                    buffer.clear();
                }
            }
        }
    }

    if !buffer.is_empty() {
        repaired_count += buffer.len();
        db.update_bulk_ownership(&buffer).await?;
    }

    tracing::info!("Reparación completada: {}/{} archivos procesados", repaired_count, total);
    Ok(())
}
