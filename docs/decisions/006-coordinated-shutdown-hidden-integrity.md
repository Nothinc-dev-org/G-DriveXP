# ADR-006: Shutdown coordinado y ocultación de archivos OnlineOnly

## Estado
Aceptada — Revisión 2 (2026-03-16): corregido bug de sincronización de `.hidden`

## Contexto
Cuando el daemon G-DriveXP se cierra, los archivos OnlineOnly (symlinks a `~/GoogleDrive/FUSE_Mount/`) se convierten en symlinks rotos. Nautilus los muestra como "Broken Link" con opciones destructivas ("Move to Trash", "Delete"), lo que puede causar pérdida de datos involuntaria.

Además, el flujo de cierre anterior tenía una race condition: la GUI (hilo GTK) llamaba `unmount_and_wait()` y `process::exit(0)` directamente, matando el proceso antes de que el runtime Tokio pudiera ejecutar la ocultación de archivos.

### Bug descubierto (Rev 2)
La secuencia original ejecutaba `hide_online_only_files()` con el MirrorWatcher aún activo. Los archivos `.hidden` y `.gdrivexp_hidden_manifest` escritos en el mirror path eran detectados por el watcher como cambios del usuario, registrados como dirty en la DB, y encolados para upload a Google Drive. Esto causaba que la app no terminara de cerrarse, quedando atrapada en un loop de sincronización infinito intentando subir archivos `.hidden` de 0 bytes.

## Decisión

### Ocultación de archivos
- Al cerrar: `hide_online_only_files()` consulta la DB para obtener todos los archivos OnlineOnly activos, agrupa por directorio, y escribe entradas en archivos `.hidden` (mecanismo nativo de Nautilus/GLib).
- Se genera un manifiesto `.gdrivexp_hidden_manifest` por directorio para rastrear qué entradas fueron añadidas por G-DriveXP.
- Al arrancar: `restore_hidden_online_only_files()` lee los manifiestos y elimina las entradas de `.hidden`, restaurando la visibilidad.

### Shutdown coordinado (Rev 2)
La secuencia de shutdown sigue un orden estricto con 5 barreras defensivas:

1. **Señalización global**: `request_shutdown()` activa `AtomicBool`. El Syncer, Uploader y Progress Monitor detectan el flag y terminan sus loops.
2. **`MirrorCommand::Shutdown`**: enviado al MirrorManager, que dropea el `MirrorWatcher` (deteniendo la vigilancia del filesystem) y sale de `run_loop()`.
3. **Gracia de 600ms**: permite drenar el último batch del debouncer (timeout de 500ms).
4. **`hide_online_only_files()`**: escribe `.hidden` y manifiestos sin que nadie los observe.
5. **`unmount_and_wait()`** → **`process::exit(0)`**.

### Barreras defensivas contra sincronización de `.hidden` (Rev 2)
Defensa en profundidad para que los archivos de control interno nunca se sincronicen:

| Capa | Ubicación | Mecanismo |
|------|-----------|-----------|
| 1 | `handle_fs_events` | Filtro por `file_name` en el loop de procesamiento de eventos FS |
| 2 | `process_local_change` | Guard al inicio: rechaza `.hidden` y manifiesto antes de tocar la DB |
| 3 | Escaneo recursivo de directorios | Exclusión explícita en el scan iterativo de hijos |
| 4 | `MirrorCommand::Shutdown` | El watcher se destruye antes de crear los `.hidden` |
| 5 | `Uploader::upload_file` | Última defensa: si un `.hidden` ya está dirty en la DB, limpia su flag y lo ignora |

## Justificación
- **`.hidden`**: mecanismo estándar de GLib/Nautilus, no requiere prefijo `.` ni modificar nombres de archivo.
- **Manifiesto**: permite restauración limpia sin asumir que todas las entradas en `.hidden` son de G-DriveXP.
- **Señalización desacoplada**: evita la race condition entre el hilo GTK (que no puede hacer async) y el runtime Tokio (que ejecuta operaciones async de DB y filesystem).
- **Defensa en profundidad (Rev 2)**: un solo punto de filtrado es frágil. Las 5 barreras garantizan que incluso si una falla (ej: el watcher no se detiene a tiempo), las capas interiores previenen la sincronización.

## Consecuencias
- Los archivos OnlineOnly no son visibles en Nautilus cuando el daemon está apagado. `ls` en terminal sí los muestra (`.hidden` es exclusivo de GLib).
- La señal de shutdown tiene latencia de hasta 100ms (polling interval de `wait_for_shutdown()`).
- El shutdown ahora tiene 600ms adicionales de latencia (gracia para el debouncer). Imperceptible para el usuario.
- `HARD_RESET_IN_PROGRESS` sigue siendo un caso especial manejado con su propio flujo (loop infinito cediendo control al hilo de limpieza).
