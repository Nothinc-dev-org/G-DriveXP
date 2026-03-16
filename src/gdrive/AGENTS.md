# AGENTS.md — Módulo `gdrive/`

## Propósito

Cliente wrapper para la API v3 de Google Drive. Abstrae las operaciones HTTP (listado, descarga, subida, eliminación) detrás de una interfaz Rust idiomática.

## Archivos

| Archivo     | Responsabilidad |
|-------------|----------------|
| `mod.rs`    | Re-exporta `DriveError`. |
| `client.rs` | `DriveClient`: wrapper sobre `google-drive3::DriveHub`. Métodos para listar, descargar, subir (Resumable Upload), crear carpetas, eliminar y obtener cambios. `ProgressReader` para reporting de progreso de upload. |
| `error.rs`  | `DriveError`: enum de errores tipados (quota, auth, network, not_found). |

## Dependencias

- **Externas**: `google-drive3`, `yup-oauth2`, `hyper`, `hyper-rustls`, `reqwest`, `mime_guess`.
- **Internas**: Ninguna directa (consumido por `sync/` y `fuse/`).

## Notas para Agentes

- El `DriveClient` se comparte via `Arc<DriveClient>` por múltiples tasks de Tokio.
- **Resumable Upload**: para archivos grandes, usa el protocolo de subida resumible de Google.
- **Exponential Backoff**: debe implementarse en los consumidores, no en este módulo directamente.
- **Root ID**: se obtiene con `get_root_file_id()` y se cachea en el caller.
