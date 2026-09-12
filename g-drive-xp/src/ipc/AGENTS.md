# AGENTS.md — Módulo `ipc/`

## Propósito

Comunicación inter-procesos entre el daemon G-DriveXP y extensiones externas (Nautilus). Protocolo binario sobre Unix Domain Sockets.

## Archivos

| Archivo     | Responsabilidad |
|-------------|----------------|
| `mod.rs`    | Define el protocolo: `IpcRequest`, `IpcResponse`, `SyncStatus`, `FileAvailability`, `FileStatusData`. Función `get_socket_path()`. |
| `server.rs` | `IpcServer`: escucha en `/run/user/<uid>/gdrivexp.sock`. Procesa peticiones: `GetFileStatus`, `Ping`, `SetOnlineOnly`, `SetLocalOnline`, `GetFileAvailability`. |

## Dependencias

- **Externas**: `serde`, `bincode`, `libc`.
- **Internas**: `db::MetadataRepository`, `mirror::MirrorCommand` (via sender).

## Notas para Agentes

- **Protocolo compartido**: Los tipos `SyncStatus`, `FileAvailability` y `FileStatusData` están duplicados en `nautilus-ext/src/lib.rs`. Cualquier cambio en el protocolo debe sincronizarse manualmente en ambos lados.
- **Serialización**: `bincode` con prefijo de longitud (4 bytes u32 LE + payload).
- El socket se elimina al iniciar si ya existe (stale).
