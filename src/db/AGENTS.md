# AGENTS.md — Módulo `db/`

## Propósito

Capa de persistencia SQLite que almacena metadatos de archivos de Google Drive, estado de sincronización y configuración del espejo local.

## Archivos

| Archivo         | Responsabilidad |
|-----------------|----------------|
| `mod.rs`        | Re-exporta `MetadataRepository`, `LocalSyncDir`, `LocalSyncFile`. |
| `repository.rs` | Pool SQLite (`sqlx`), inicialización de esquema, migraciones automáticas, operaciones CRUD. |
| `schema.sql`    | DDL embebido: tablas `inodes`, `dentry`, `attrs`, `sync_state`, `local_sync_dirs`, `local_sync_files`, `sync_meta`, `dir_counters`. |

## Dependencias

- **Externas**: `sqlx` (sqlite, runtime-tokio).
- **Internas**: Ninguna (módulo base, consumido por todos los demás).

## Notas para Agentes

- **WAL mode**: Habilitado para concurrencia lectura/escritura. No cambiar a otro journal mode.
- **Migraciones**: Se aplican manualmente en `apply_migrations()` verificando columnas con `PRAGMA table_info`. Al agregar columnas, seguir este patrón.
- **Inodo raíz**: Siempre `inode=1`, `gdrive_id="root"`. Es invariante del sistema.
- **Pool**: Máximo 5 conexiones con `busy_timeout=60s`. Compartido via `Arc<MetadataRepository>`.
- Los archivos `schema.sql` se embeben en compilación. Cambios al esquema requieren recompilación.
- **Shortcuts**: La columna `attrs.shortcut_target_id` almacena el `gdrive_id` del archivo destino. Métodos `set_shortcut_target_id`, `set_bulk_shortcut_targets` y `resolve_shortcut_sizes` gestionan la resolución.
