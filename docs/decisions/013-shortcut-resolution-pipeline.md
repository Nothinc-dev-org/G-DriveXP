# ADR-013: Pipeline de Resolución de Shortcuts de Google Drive

## Estado
Aceptada

## Contexto

Los archivos de Google Classroom (y otros contextos compartidos) aparecen como **shortcuts** de Google Drive (`application/vnd.google-apps.shortcut`). Un shortcut es una entidad distinta al archivo destino: tiene su propio `fileId`, `size=0`, y no tiene contenido descargable. La información del destino real está en `shortcutDetails.targetId` y `shortcutDetails.targetMimeType`.

Se identificaron tres fallos que impedían abrir estos archivos:

1. **Clasificación errónea como Workspace**: `is_workspace_file()` usaba `starts_with("application/vnd.google-apps.")`, capturando shortcuts y carpetas además de documentos Workspace. Los shortcuts se renderizaban como archivos HTML de redirección en vez de descargarse.
2. **Inconsistencia lookup/getattr**: `lookup()` solo ajustaba el tamaño para archivos Workspace bajo una condición (`is_html_lookup`), mientras `getattr()` lo hacía incondicionalmente. El kernel cacheaba `size=0` del `lookup()` y nunca invocaba `read()`.
3. **Ausencia de resolución de shortcuts**: No se solicitaban `shortcutDetails` a la API, no se almacenaba el `targetId`, y `read()` intentaba descargar el shortcut en vez del archivo destino.

### Alternativas descartadas

- **Heredar el `gdrive_id` del target**: Descartado porque `inodes.gdrive_id` tiene constraint UNIQUE y `changes.list` envía cambios con el `fileId` del shortcut, no del target.
- **Modelo hardlink** (dentry del shortcut → inodo del target): Descartado porque `upsert_dentry` aplica one-parent-per-child, lo que "movería" el target de su ubicación original.

## Decisión

Se implementó un pipeline de resolución en cuatro capas:

### 1. API: Solicitar `shortcutDetails` (`gdrive/client.rs`)

Se añadió `shortcutDetails(targetId,targetMimeType)` al campo `fields` de las cuatro llamadas a la API:
- `list_root_children` (bootstrap nivel 1)
- `list_all_files` (bootstrap BFS)
- `fetch_files_page` (bootstrap progresivo)
- `list_changes` (syncer incremental)

### 2. Almacenamiento: Columna `shortcut_target_id` (`db/`)

- **Esquema** (`schema.sql`): Nueva columna `shortcut_target_id TEXT` en tabla `attrs`.
- **Migración** (`repository.rs`): Paso 10 — `ALTER TABLE attrs ADD COLUMN shortcut_target_id TEXT` si no existe.
- **Métodos nuevos**:
  - `set_shortcut_target_id(inode, target_id)`: Guarda el target para un shortcut individual.
  - `set_bulk_shortcut_targets(items)`: Batch insert para bootstrap.
  - `resolve_shortcut_sizes()`: UPDATE con subquery que copia el `size` del target al shortcut (solo si el target ya fue indexado y tiene `size > 0`).

### 3. Indexación: Resolución en Bootstrap y Syncer (`sync/`)

- **Helper** `resolve_shortcut_info(file)` en `bootstrap.rs`: Detecta shortcuts y extrae `(target_id, target_mime_type)`.
- **Bootstrap** (`bootstrap_remaining_bfs`): Usa `effective_mime` (del target) para clasificar shortcuts correctamente. Acumula targets y resuelve sizes en batch al final.
- **Syncer** (`process_change`): Aplica la misma lógica para cambios incrementales, guardando `shortcut_target_id` y resolviendo sizes.

### 4. FUSE: Descarga del target real (`fuse/`)

- **`shortcuts.rs`**: `is_workspace_file()` cambiado a lista explícita con `matches!` (9 tipos). Shortcuts y carpetas ya no se clasifican erróneamente.
- **`filesystem.rs` lookup()**: Ajuste de tamaño para archivos Workspace ahora es incondicional (consistente con `getattr()`).
- **`filesystem.rs` read()**: Consulta `shortcut_target_id` de `attrs`. Si existe, usa el `target_id` como `gdrive_id` efectivo para la descarga, resolviendo la indirección transparentemente.

## Justificación

- El shortcut mantiene su propia identidad en `inodes` (su propio `gdrive_id`), respetando el constraint UNIQUE y la semántica de `changes.list`.
- La indirección se resuelve solo en el punto de descarga (`read()`), minimizando la complejidad en el resto del sistema.
- `resolve_shortcut_sizes()` se ejecuta post-bootstrap y en cada cambio incremental, asegurando que el kernel siempre vea el tamaño correcto del archivo destino.
- La lista explícita en `is_workspace_file()` es más segura que un `starts_with`, evitando falsos positivos ante nuevos tipos MIME de Google.

## Archivos Modificados

| Archivo | Cambio |
|---------|--------|
| `g-drive-xp/src/gdrive/client.rs` | `shortcutDetails` en `fields` de 4 endpoints. |
| `g-drive-xp/src/db/schema.sql` | Columna `shortcut_target_id` en `attrs`. |
| `g-drive-xp/src/db/repository.rs` | Migración paso 10. Métodos `set_shortcut_target_id`, `set_bulk_shortcut_targets`, `resolve_shortcut_sizes`. |
| `g-drive-xp/src/sync/bootstrap.rs` | Helper `resolve_shortcut_info`. Resolución en `insert_file_metadata` y `bootstrap_remaining_bfs`. |
| `g-drive-xp/src/sync/syncer.rs` | Resolución de shortcuts en `process_change`. |
| `g-drive-xp/src/fuse/shortcuts.rs` | `is_workspace_file` con `matches!` explícito. |
| `g-drive-xp/src/fuse/filesystem.rs` | `lookup()` incondicional. `read()` usa `shortcut_target_id` para descargas. |

## Consecuencias

- Los archivos de accesos directos ahora se abren correctamente como archivos regulares (PDF, imágenes, etc.).
- Requiere re-bootstrap (eliminar `metadata.db`) para indexar shortcuts existentes con la nueva columna.
- Si el target de un shortcut no ha sido indexado aún (e.g., archivo externo al Drive del usuario), `resolve_shortcut_sizes` no tendrá efecto y el shortcut puede mostrarse con `size=0` hasta que el target sea descubierto.