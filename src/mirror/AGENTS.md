# AGENTS.md — Módulo `mirror/`

## Propósito

Implementa la "Arquitectura Espejo": el directorio `~/GoogleDrive/` visible al usuario es un espejo del montaje FUSE oculto. Los archivos pueden ser symlinks (Online Only) o copias reales (Local & Online).

## Archivos

| Archivo      | Responsabilidad |
|--------------|----------------|
| `mod.rs`     | Re-exporta `MirrorManager`, `MirrorCommand`. |
| `manager.rs` | `MirrorManager`: gestiona el estado del espejo. Recibe comandos via `mpsc::channel` (SetOnlineOnly, SetLocalOnline, Refresh, RemoteDeleted). Procesa eventos del watcher. |
| `watcher.rs` | `MirrorWatcher`: monitorea `~/GoogleDrive/` con `notify` (debounced). Detecta Create, Modify, Rename y Remove en el directorio espejo. |

## Dependencias

- **Externas**: `notify`, `notify-debouncer-full`.
- **Internas**: `db::MetadataRepository`, `gui::history::ActionHistory`.

## Notas para Agentes

- **CRÍTICO**: El MirrorManager se inicia DESPUÉS de montar FUSE. Iniciar antes causa deadlock porque intenta acceder al mount point FUSE antes de que esté listo.
- **MirrorCommand**: los comandos vienen del IPC Server (Nautilus) y del Syncer (cambios remotos).
- **Symlinks vs Copias**: Online Only = symlink a `FUSE_Mount/<path>`. Local & Online = archivo real copiado desde FUSE.
- El watcher ignora eventos dentro de `FUSE_Mount/` para evitar loops.
