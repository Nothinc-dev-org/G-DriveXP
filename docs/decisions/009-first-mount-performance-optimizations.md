# ADR-009: Optimizaciones de Rendimiento del Primer Montaje

## Estado
Aceptado

## Contexto

El primer montaje de G-DriveXP (cuando `bootstrap_complete` no existe en `sync_meta`) presentaba un delay excesivo de ~9+ minutos para ~27,000 archivos. El cuello de botella abarcaba múltiples capas: SQLite, red, y operaciones de filesystem.

## Decisión

Se implementaron 5 rondas de optimización incremental, atacando cada capa del pipeline:

### Ronda 1 — SQLite PRAGMA propagation + transaction batching
**Problema**: Errores "database is locked" y escrituras individuales sin transacciones.
**Solución**: `SqliteConnectOptions` con `journal_mode(Wal)`, `pragma("busy_timeout", "60000")`, `pragma("synchronous", "NORMAL")`. Las operaciones masivas de bootstrap usan transacciones agrupadas (bloques de 500).

### Ronda 2 — Índice `idx_dentry_child`
**Problema**: Búsquedas O(n²) en la tabla `dentry` durante la resolución de padres en BFS.
**Solución**: `CREATE INDEX IF NOT EXISTS idx_dentry_child ON dentry(child_inode)` en `schema.sql`. Reduce lookup de padres a O(log n).

### Ronda 3 — MirrorManager batch symlink creation
**Problema**: El bootstrap del espejo creaba symlinks uno a uno (~9 min para ~24K symlinks).
**Solución**: Acumular rutas en memoria y crear todos los symlinks en un único `spawn_blocking`. Reducción a ~2 segundos.

### Ronda 4 — `pageSize=1000` + `reqwest::Client` reuse
**Problema**: `list_all_files()` usaba `pageSize=100` (default) = ~273 requests HTTP. Cada request instanciaba un nuevo `reqwest::Client` (TLS handshake repetido).
**Solución**: `pageSize=1000` reduce a ~28 requests. Un único `reqwest::Client` persistido como campo `http` en `DriveClient` reutiliza conexiones TCP/TLS. Reducción de ~9 min a ~53 segundos.

### Ronda 5 — Compresión gzip transparente
**Problema**: ~28 requests HTTP transfiriendo JSON sin comprimir (~300-500KB por página = ~8-14MB total).
**Solución**: Feature `gzip` en reqwest (`Cargo.toml`). Envía automáticamente `Accept-Encoding: gzip` y descomprime respuestas. JSON comprime ~5-10x, reduciendo transferencia a ~1.5-3MB.

## Consecuencias

- **Positivas**:
  - Reducción acumulada de ~9+ minutos a tiempos significativamente menores.
  - Todas las optimizaciones son transparentes: no cambian interfaces ni flujos de control.
  - Dependencias adicionales mínimas: `flate2` y `async-compression` (transitivas via reqwest `gzip`).

- **Negativas**:
  - El feature `gzip` agrega ~5 crates transitivos al árbol de dependencias.

## Archivos Afectados

| Archivo | Cambio |
|---------|--------|
| `g-drive-xp/Cargo.toml` | Feature `gzip` en reqwest |
| `g-drive-xp/src/db/repository.rs` | `SqliteConnectOptions` con PRAGMAs, transacciones bulk |
| `g-drive-xp/src/db/schema.sql` | Índice `idx_dentry_child` |
| `g-drive-xp/src/gdrive/client.rs` | Campo `http: reqwest::Client`, `pageSize=1000` |
| `g-drive-xp/src/mirror/manager.rs` | Bootstrap batch con `spawn_blocking` |
| `g-drive-xp/src/sync/bootstrap.rs` | Operaciones bulk (inodes, metadata, dentry) |
