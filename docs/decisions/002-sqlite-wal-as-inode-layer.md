# ADR-002: SQLite en modo WAL como capa de traducción de inodos

## Estado
Aceptada

## Contexto
Linux/FUSE requiere inodos numéricos `u64` persistentes. Google Drive usa IDs de cadena opacos. Se necesita un mapeo bidireccional rápido y persistente.

## Decisión
SQLite con `sqlx` en modo WAL (Write-Ahead Logging), con tablas `inodes`, `dentry`, `attrs`, `sync_state`.

## Justificación
- **Persistencia ACID**: transacciones atómicas para operaciones como `rename` (actualización de `dentry`).
- **WAL mode**: permite lectura concurrente desde el hilo FUSE mientras el Syncer escribe, minimizando bloqueos.
- **Consultas rápidas**: índices sobre `gdrive_id` (lookup por API) y `(parent_inode, name)` (lookup POSIX).
- **busy_timeout=60s**: tolera contención transitoria sin fallar.

## Consecuencias
- Los inodos son inmutables una vez asignados a un `gdrive_id`. Nunca se reutilizan.
- El esquema se embebe con `include_str!("schema.sql")` y requiere recompilación al cambiar.
- Las migraciones se gestionan manualmente en `apply_migrations()`.
