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

## Dependencias

- **Externas**: `md-5`, `libc`.
- **Internas**: Ninguna (módulo utilitario puro).

## Notas para Agentes

- `cleanup_if_needed` se ejecuta ANTES de montar FUSE para evitar errores "Transport endpoint is not connected".
- El hash MD5 se usa para detectar si un archivo local difiere del remoto, no para seguridad criptográfica.
