# ADR-007: Resiliencia post-crash y coordinación BFS → MirrorManager

## Estado
Aceptada

## Contexto
Tras un cierre no limpio (crash, corte de energía, `kill -9`), la aplicación arrancaba con todas las carpetas vacías. La causa raíz involucra dos factores:

1. **Pérdida de WAL**: SQLite con `PRAGMA synchronous=NORMAL` + WAL puede perder transacciones no checkpointed tras un crash. El flag `bootstrap_complete` en `sync_meta` se pierde, forzando al BFS a re-correr.
2. **Race condition**: El BFS re-corre en background (`tokio::spawn`), pero el MirrorManager arrancaba independientemente y consultaba la tabla `dentry` antes de que BFS la repoblara. Resultado: directorios existen pero sus contenidos faltan.

## Decisión

### A. Detección de cierre no limpio (`session_active` flag)
- Al iniciar el backend: si `session_active` existe en `sync_meta`, el cierre previo fue abrupto.
- Acción: limpiar `bootstrap_complete` y `changes_page_token` para forzar re-bootstrap fresco.
- Marcar `session_active = "true"` al inicio; eliminarlo al cerrar limpiamente (antes de `HARD_RESET_IN_PROGRESS` check).

### B. Coordinación BFS → MirrorManager (`tokio::sync::watch` channel)
- `main.rs` crea un `watch::channel(false)`.
- El `Receiver` se pasa a `MirrorManager::new()`.
- `bfs_ready_tx.send(true)` se envía **inmediatamente después de level 1** (o inmediatamente si `bootstrap_complete` ya existe), NO al final del BFS completo.
- MirrorManager espera a que el canal tenga `true` antes de ejecutar su bootstrap.
- Cuando BFS completa, envía `MirrorCommand::Refresh` para que MirrorManager re-bootstrapee con el árbol completo. **El handler de Refresh pausa el watcher** (drop → drain → bootstrap síncrono → recrear watcher) para evitar que los rename atómicos de symlinks generen falsos `dirty=1`.

### C. Método `delete_sync_meta` en repositorio
- Necesario para limpiar claves corruptas (`bootstrap_complete`, `changes_page_token`) al detectar crash.

## Justificación
- **`watch` sobre `Notify`**: `watch` retiene estado. Si el sender envía `true` antes de que el receiver haga `poll`, el valor ya está disponible. `Notify` pierde la señal si `notified()` no estaba polled.
- **Señal inmediata (no al final del BFS)**: En primer arranque, level 1 inserta ~82 items del root en <1s. MirrorManager puede mostrar esos items de inmediato. El `Refresh` post-BFS actualiza con el árbol completo (~26,000 archivos). Esperar al BFS completo causaba ~5 minutos de directorio vacío.
- **`session_active` como dirty flag**: Patrón auto-healing. Un doble crash simplemente re-detecta el flag y re-bootstrapea.

## Archivos modificados
- `g-drive-xp/src/db/repository.rs` — `delete_sync_meta()`.
- `g-drive-xp/src/main.rs` — Detección crash, creación watch channel, señal inmediata, limpieza en shutdown.
- `g-drive-xp/src/mirror/manager.rs` — Campo `bfs_ready_rx`, espera en `spawn()`.

## Rev 2: Purga de caché física post-crash (2026-03-22)

### Problema
`clear_all_chunks()` vaciaba la tabla `file_cache_chunks` pero los archivos físicos en `~/.cache/fedoradrive/` sobrevivían. Esto creaba "zombies" persistentes: archivos en disco sin registros en DB. Al leer cualquiera de estos archivos vía FUSE, `ensure_range_cached()` detectaba la inconsistencia (archivo existe, `has_any_chunks()` = false), borraba el archivo y lo re-descargaba. Los zombies persistían entre sesiones hasta ser leídos, causando descargas innecesarias en cada arranque.

### Corrección
Después de `clear_all_chunks()`, se purga `cache_dir` con `remove_dir_all` + `create_dir_all`. Esto mantiene la invariante: si la DB no tiene registros de chunks, el disco tampoco tiene archivos de caché. El patrón es idéntico al usado en Hard Reset (`utils/cleanup.rs`).

### Seguridad
La purga ocurre antes de que cualquier consumidor de `cache_dir` esté activo (FUSE, Uploader, IPC, MirrorManager se inicializan después).

## Consecuencias
- En startup normal (`bootstrap_complete` existe): zero latencia adicional. El canal ya tiene `true` cuando MirrorManager lo consulta.
- En primer arranque: MirrorManager muestra items del root en ~2s (level 1 + 1s sleep FUSE). BFS completa en background y `Refresh` actualiza el espejo.
- Post-crash: la detección fuerza re-bootstrap. Los datos previos en DB (que sobrevivieron al crash) permiten a MirrorManager mostrar contenido parcial mientras BFS repopula.
- Si BFS falla: el sender se dropea y el receiver recibe `Err` en `changed()`, procediendo con lo disponible en DB. No hay bloqueo indefinido.
