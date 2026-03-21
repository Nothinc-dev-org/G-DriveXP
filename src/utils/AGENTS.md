# AGENTS.md — Módulo `utils/`

## Propósito

Utilidades compartidas para gestión de montajes, hashing y limpieza de datos temporales.

## Archivos

| Archivo      | Responsabilidad |
|--------------|----------------|
| `mod.rs`     | Re-exporta submódulos. |
| `mount.rs`   | `cleanup_if_needed()`: detecta y desmonta puntos FUSE huérfanos (stale mounts). `unmount_and_wait()`: desmonta limpiamente con `fusermount3 -u`. |
| `hash.rs`    | Cálculo de hash MD5 de archivos para verificación de integridad contra `md5Checksum` de Google Drive API. |
| `cleanup.rs` | Limpieza de caché y datos temporales del directorio `~/.cache/fedoradrive/`. |
| `shutdown.rs` | Coordinación de cierre graceful. `SHUTDOWN_REQUESTED` (AtomicBool global), `request_shutdown()` para señalizar desde GUI, `wait_for_shutdown()` async para integrar en `tokio::select!`. Consumido por la macro `or_shutdown!` en `main.rs` para cancelar fases de inicialización pre-FUSE. |

## Dependencias

- **Externas**: `md-5`, `libc`, `ctrlc`.
- **Internas**: Ninguna (módulo utilitario puro).

## Notas para Agentes

- `cleanup_if_needed` se ejecuta ANTES de montar FUSE para evitar errores "Transport endpoint is not connected".
- El hash MD5 se usa para detectar si un archivo local difiere del remoto, no para seguridad criptográfica.
- **Shutdown coordinado**: La GUI NO debe llamar `process::exit()` directamente. Debe usar `request_shutdown()` para que el backend ejecute la secuencia completa: ocultar archivos → desmontar FUSE → exit. Ver ADR-006.
