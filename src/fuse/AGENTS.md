# AGENTS.md — Módulo `fuse/`

## Propósito

Implementa el sistema de archivos virtual FUSE que monta Google Drive como un directorio local. Toda operación del kernel (lookup, read, write, etc.) pasa por este módulo.

## Archivos

| Archivo         | Responsabilidad |
|-----------------|----------------|
| `mod.rs`        | Re-exporta `GDriveFS`. |
| `filesystem.rs` | Implementación completa del trait `fuse3::raw::Filesystem`. Gestiona descargas bajo demanda, caché en disco, locks por inodo, y streaming inteligente. |
| `attr.rs`       | Conversión de filas SQLite a `FileAttr` de FUSE (permisos, tamaños, timestamps). |
| `shortcuts.rs`  | Genera archivos HTML de redirección para documentos Google Workspace (Docs, Sheets, Slides, etc.) que no tienen contenido descargable. `is_workspace_file()` clasifica MIME types con lista explícita (no incluye shortcuts ni carpetas). |

## Dependencias

- **Externas**: `fuse3` (tokio-runtime, unprivileged), `futures-util`, `dashmap`.
- **Internas**: `db::MetadataRepository`, `gdrive::client::DriveClient`, `gui::history::ActionHistory`.

## Notas para Agentes

- **Inodo virtual**: `SHARED_INODE = 0xFFFF_FFFF_FFFF_FFFE` es un directorio virtual para "Shared with me".
- **Concurrencia**: `fuse_downloads` (Mutex), `file_locks` (DashMap), `failed_downloads` (DashSet) gestionan el estado de descargas activas.
- **Montaje**: Se monta con `allow_other`, `default_permissions`, `exec` y `max_read=1048576`. Se monta en `~/GoogleDrive/FUSE_Mount/` (oculto al usuario).
- **Post-FUSE**: El `MirrorManager` se inicia DESPUÉS de montar FUSE para evitar deadlocks.
- Las operaciones de escritura marcan el archivo como `dirty=1` en `sync_state` para que el `Uploader` lo procese.
- **Shortcuts de Drive**: `read()` consulta `attrs.shortcut_target_id` y usa el `target_id` como `gdrive_id` efectivo para descargar el archivo destino real. `lookup()` y `getattr()` deben reportar tamaños consistentes para evitar que el kernel cachee `size=0`.
- **`is_workspace_file()`**: Usa lista explícita `matches!` con 9 tipos MIME. No usar `starts_with("application/vnd.google-apps.")` ya que capturaría shortcuts y carpetas erróneamente.
