# ADR-011: Bootstrap Progresivo Page-by-Page

## Estado
Aceptada

## Contexto

El proceso de bootstrap original (`bootstrap_remaining_bfs`) descargaba la totalidad del árbol de archivos de Google Drive en memoria antes de escribir a SQLite. Esto provocaba un "apagón visual" de varios minutos en cuentas grandes (~26,000 archivos): el directorio espejo permanecía vacío hasta que el BFS completaba íntegramente.

Adicionalmente, el algoritmo requería mantener todos los objetos `google_drive3::api::File` en un `Vec` en memoria simultáneamente, elevando el consumo de RAM durante el arranque.

El método `fetch_files_page` no existía; solo existía `list_all_files` que acumulaba todas las páginas antes de retornar.

## Decisión

Se reemplazó `bootstrap_remaining_bfs` por un algoritmo de **escaneo progresivo page-by-page**:

1. **Nuevo método `DriveClient::fetch_files_page`**: Obtiene una sola página de la API de Drive (`pageSize=1000`) retornando `(Vec<File>, Option<String>)` — archivos de la página y token de continuación.

2. **Procesamiento por página**: Por cada página obtenida se ejecuta el pipeline completo de forma inmediata:
   - `get_or_create_inodes_bulk` para archivos y padres de la página.
   - `upsert_bulk_file_metadata` para metadatos.
   - `upsert_bulk_dentries` para vínculos padre-hijo.
   - `history.set_scanning_total(total_scanned)` para reportar progreso a la GUI.

3. **Señal inmediata a MirrorManager**: `bfs_ready_tx.send(true)` se emite antes del escaneo (no al final). El `MirrorCommand::Refresh` final reconstruye el espejo con el árbol completo una vez finalizado el escaneo.

4. **Indicador de progreso en GUI** (`AppModel::scanning_total`): Nuevo campo en `SyncProgress` y `AppModel`. Mientras `scanning_total > 0`, el icono de estado muestra `emblem-synchronizing-symbolic` y el hint de sincronización muestra "Escaneados N archivos". Al finalizar, `set_scanning_total(0)` devuelve la GUI al estado normal.

5. **Limpieza de huérfanos diferida en MirrorManager**: `run_bootstrap` ahora recibe `skip_orphan_cleanup: bool`. Al iniciar (`MirrorCommand::Start`), se pasa `true` para evitar eliminar archivos del espejo que aún no existen en DB (escaneo en progreso). Al recibir `MirrorCommand::Refresh` (post-escaneo), se pasa `false` ejecutando la limpieza completa.

6. **Protección del punto de montaje FUSE durante limpieza**: Se añadió un `HashSet<PathBuf>` de directorios a excluir (`skip_dirs`) que incluye `fuse_mount_path`, previniendo que la limpieza de huérfanos intente recorrer o eliminar el punto de montaje FUSE.

7. **`soft_delete_remote`**: Nueva variante de `soft_delete_by_gdrive_id` que NO propaga `dirty=1` al nodo eliminado ni a sus descendientes. Se usa desde `BackgroundSyncer` cuando un archivo se detecta como `trashed` en la API remota (la eliminación ya ocurrió en Drive; marcarla como `dirty` causaría que el Uploader la re-enviase a la papelera innecesariamente).

8. **`clear_stale_dirty_deletes`**: Nuevo método que limpia al inicio de sesión las entradas con `dirty=1 AND deleted_at IS NOT NULL` que quedaron de sesiones anteriores (e.g., eliminaciones de huérfanos del mirror que nunca debieron marcarse como dirty).

## Justificación

- **Sin blackout visual**: Los primeros archivos aparecen en el espejo tras la primera página (~1–3s), no al final del BFS completo (~5 minutos en cuentas grandes).
- **Memoria acotada**: Solo una página (~1,000 archivos) reside en memoria a la vez en lugar de todo el árbol.
- **Integridad del espejo**: `skip_orphan_cleanup=true` en el arranque evita que el mirror elimine archivos válidos que aún no han sido escaneados. La limpieza completa ocurre solo cuando el árbol está completo.
- **Consistencia del ciclo dirty**: Separar `soft_delete_remote` de `soft_delete_by_gdrive_id` elimina una fuente de corrección circular donde eliminaciones remotas generaban uploads innecesarios.

## Archivos Modificados

| Archivo | Cambio |
|---------|--------|
| `g-drive-xp/src/sync/bootstrap.rs` | Reescritura de `bootstrap_remaining_bfs` (algoritmo page-by-page). Añade parámetros `history` y `mirror_sender`. |
| `g-drive-xp/src/gdrive/client.rs` | Nuevo método `fetch_files_page`. Migración de `list_all_files` a query param `q=trashed = false` (URL-encoded). |
| `g-drive-xp/src/db/repository.rs` | Nuevos métodos `soft_delete_remote` y `clear_stale_dirty_deletes`. |
| `g-drive-xp/src/mirror/manager.rs` | `run_bootstrap` acepta `skip_orphan_cleanup`. Protección de `fuse_mount_path` en limpieza. Lógica de pausa/reanudación de watcher en `RemoteDeleted`. |
| `g-drive-xp/src/gui/history.rs` | Nuevo campo `scanning_total` en `SyncProgress`. Método `set_scanning_total`. |
| `g-drive-xp/src/gui/app_model.rs` | Nuevo campo `scanning_total` en `AppModel`. Lógica de indicador visual de escaneo en curso. |
| `g-drive-xp/src/main.rs` | Refactor completo de la fase de bootstrap de `run_backend`. Llama a `clear_stale_dirty_deletes` al inicio. |
| `g-drive-xp/src/sync/syncer.rs` | Usa `soft_delete_remote` en lugar de `soft_delete_by_gdrive_id` para archivos trashed remotos. |

## Consecuencias

- El primer arranque muestra datos incrementalmente: root visible en ~2s, árbol completo en background.
- Las cuentas con muchos archivos compartidos externos siguen requiriendo el paso de vinculación de huérfanos post-escaneo (O(n) adicional).
- El `MirrorCommand::Refresh` final puede tardar varios segundos en reconstruir symlinks para cuentas grandes; el indicador `scanning_total` cubre este periodo visualmente.
- La dependencia `ctrlc` fue eliminada (ver ADR-010). Las señales OS se manejan íntegramente en el runtime Tokio.
