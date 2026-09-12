# ADR-001: Usar `fuse3` asíncrono en lugar de `fuser`

## Estado
Aceptada

## Contexto
Se necesita montar Google Drive como un sistema de archivos local. Las opciones principales eran `fuser` (síncrono) y `fuse3` (asíncrono con Tokio).

## Decisión
Se eligió `fuse3` con feature `tokio-runtime` y `unprivileged`.

## Justificación
- **Async nativo**: permite suspender operaciones de I/O de red sin bloquear otras operaciones del filesystem. Con `fuser`, un `read()` esperando datos de Google Drive congelaría `getattr()` en otro archivo.
- **`readdirplus`**: reduce syscalls al listar directorios grandes (evita `lookup` individual por entrada).
- **Montaje unprivileged**: no requiere root ni SUID, usa `fusermount3`.
- **Integración Tokio**: todo el backend ya usa Tokio; `fuse3` se integra nativamente.

## Consecuencias
- Toda la implementación del trait `Filesystem` es asíncrona.
- Las descargas bajo demanda se gestionan con locks por inodo (`DashMap<u64, Arc<Mutex<()>>>`).
