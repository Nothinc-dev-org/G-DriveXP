# ADR-004: IPC via Unix Domain Socket con bincode

## Estado
Aceptada

## Contexto
La extensión de Nautilus (cargada en un proceso separado) necesita consultar el estado de sincronización de archivos al daemon G-DriveXP.

## Decisión
Protocolo binario sobre Unix Domain Socket en `/run/user/<uid>/gdrivexp.sock`. Serialización con `bincode` y framing con prefijo de longitud (4 bytes u32 LE).

## Justificación
- **Baja latencia**: bincode es significativamente más rápido que JSON para serialización/deserialización.
- **Unix Socket**: no requiere puertos TCP, aislado al usuario, limpio con el ciclo de vida de la sesión.
- **Simplicidad**: protocolo request-response síncrono por conexión.

## Consecuencias
- Los tipos del protocolo (`SyncStatus`, `FileAvailability`, `IpcRequest`, `IpcResponse`) están **duplicados** entre `g-drive-xp/src/ipc/mod.rs` y `nautilus-ext/src/lib.rs`. Deben mantenerse sincronizados manualmente.
- Cambios en el protocolo requieren recompilar ambos crates.
