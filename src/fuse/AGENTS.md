# AGENTS.md — Módulo `fuse/`

## Propósito

Implementa el sistema de archivos virtual FUSE que monta Google Drive como un directorio local. Toda operación del kernel (lookup, read, write, etc.) pasa por este módulo.

## Archivos

| Archivo         | Responsabilidad |
|-----------------|----------------|
| `mod.rs`        | Re-exporta `GDriveFS`. |
| `filesystem.rs` | Implementación completa del trait `fuse3::raw::Filesystem`. Gestiona descargas bajo demanda, caché en disco, locks por inodo, y streaming inteligente. |
| `attr.rs`       | Conversión de filas SQLite a `FileAttr` de FUSE (permisos, tamaños, timestamps). |
| `shortcuts.rs`  | Genera archivos `.desktop` para documentos Google nativos (Docs, Sheets, Slides) que no tienen contenido descargable. |

## Dependencias

- **Externas**: `fuse3` (tokio-runtime, unprivileged), `futures-util`, `dashmap`.
- **Internas**: `db::MetadataRepository`, `gdrive::client::DriveClient`, `gui::history::ActionHistory`.

## Notas para Agentes

- **Inodo virtual**: `SHARED_INODE = 0xFFFF_FFFF_FFFF_FFFE` es un directorio virtual para "Shared with me".
- **Concurrencia**: `fuse_downloads` (Mutex), `file_locks` (DashMap), `failed_downloads` (DashSet) gestionan el estado de descargas activas.
- **Montaje**: Se monta con `allow_other`, `default_permissions`, `exec` y `max_read=1048576`. Se monta en `~/GoogleDrive/FUSE_Mount/` (oculto al usuario).
- **Post-FUSE**: El `MirrorManager` se inicia DESPUÉS de montar FUSE para evitar deadlocks.
- Las operaciones de escritura marcan el archivo como `dirty=1` en `sync_state` para que el `Uploader` lo procese.
