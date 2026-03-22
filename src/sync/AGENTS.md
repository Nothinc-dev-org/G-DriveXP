# AGENTS.md — Módulo `sync/`

## Propósito

Sincronización bidireccional entre la base de datos local y Google Drive. Comprende tres subsistemas: bootstrap inicial, sincronización continua de cambios remotos, y subida de cambios locales.

## Archivos

| Archivo        | Responsabilidad |
|----------------|----------------|
| `mod.rs`       | Re-exporta submódulos. |
| `bootstrap.rs` | Inicialización del árbol de metadatos. `bootstrap_level1` carga el primer nivel. `bootstrap_remaining_bfs` recorre todo el árbol en BFS background. `repair_ownership_metadata` corrige propiedad de archivos compartidos. `resolve_shortcut_info` detecta shortcuts y extrae target_id/target_mime. |
| `syncer.rs`    | `BackgroundSyncer`: polling periódico via `changes.list` de Google Drive API. Exponential backoff (máx 300s). Procesa cambios incrementales y notifica al MirrorManager. Gestiona tombstones con período de gracia de 7 días. |
| `uploader.rs`  | `Uploader`: escanea `sync_state WHERE dirty=1` y `local_sync_files WHERE dirty=1`. Sube archivos via Resumable Upload con exponential backoff. |

## Dependencias

- **Externas**: `futures` (stream).
- **Internas**: `db::MetadataRepository`, `gdrive::client::DriveClient`, `gui::history::ActionHistory`, `mirror::MirrorCommand`.

## Notas para Agentes

- **Orden de arranque**: Bootstrap → Syncer → Uploader. El bootstrap BFS corre en background (tokio::spawn).
- **sync_meta**: tabla clave-valor para almacenar state persistente (ej: `bootstrap_complete`, `changes_page_token`).
- **Pausa de sync**: controlada por `Arc<AtomicBool>` compartido con la GUI.
- **MirrorManager**: el Syncer envía `MirrorCommand::Refresh` cuando hay cambios remotos que afectan al espejo.
- **Shortcuts de Drive**: Tanto el bootstrap como el syncer resuelven shortcuts usando `resolve_shortcut_info()`. El MIME efectivo del target se usa para clasificación (is_dir, workspace). El `shortcut_target_id` se almacena en `attrs` y los sizes se resuelven post-indexación via `resolve_shortcut_sizes()`.
