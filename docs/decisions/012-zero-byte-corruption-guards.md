# ADR-012: Protecciones Anti-Corrupción de Archivos Vacíos (0-bytes)

## Estado
Aceptada

## Contexto

Se identificaron múltiples vectores donde un archivo podía quedar corrupto (truncado a 0 bytes) a causa de condiciones de carrera o respuestas anómalas de la API de Google Drive:

1. **Descarga FUSE fallida**: Si la descarga o el streaming fallaban tras crear el archivo de caché, el archivo vacío quedaba en disco. Las lecturas FUSE subsecuentes devolvían 0 bytes en lugar de datos reales o un error claro.
2. **API Drive reporta `size=0` para archivo existente**: La API puede retornar metadatos con `size=0` transitoriamente (e.g., tras un cambio reciente, limitación de cuota, o error temporal). El Syncer podría interpretar esto como una actualización legítima y sobrescribir el archivo local con un archivo vacío.
3. **Cache de FUSE vacío sube a Drive**: El Uploader lee el archivo de caché para subirlo. Si el caché fue creado pero no rellenado (e.g., descarga parcial interrumpida), sube 0 bytes al archivo remoto borrando su contenido.
4. **Copia LocalOnline vacía**: En `MirrorManager`, al materializar un symlink como copia local, si la fuente FUSE devolvía 0 bytes, se reemplazaba el symlink por un archivo vacío.

## Decisión

Se implementaron cuatro guardias defensivas independientes, siguiendo el principio de "defensa en profundidad":

### 1. Limpieza de cache vacío post-descarga fallida (`fuse/filesystem.rs`)

Se introdujo la variable `cache_was_created: bool` (verdadero si el archivo de caché no existía antes de la descarga). En los manejadores de error de las descargas y streaming FUSE, si `cache_was_created && tamaño == 0`, el archivo de caché se elimina. Esto evita que lecturas posteriores devuelvan datos vacíos.

```
let cache_was_created = !cache_path.exists();
// ... intento de descarga ...
// En rama Err:
if cache_was_created && meta.len() == 0 {
    tokio::fs::remove_file(&cache_path).await;
}
```

### 2. Protección del Syncer contra `size=0` de la API (`sync/syncer.rs`)

Nueva función auxiliar `should_protect_local_file(api_size, local_path)`: Si la API reporta `size=0` pero el archivo local existe y tiene contenido, se omite la descarga y se registra una advertencia. La función es async y accede al filesystem solo cuando `api_size == 0`.

### 3. Bloqueo de uploads de 0 bytes en el Uploader (`sync/uploader.rs`)

Nueva función auxiliar `should_block_zero_byte_upload(local_size, remote_size)`: Si el archivo local mide 0 bytes pero el archivo remoto tiene contenido (`remote_size > 0`), se bloquea el upload. Además:
- Se elimina el archivo de caché corrupto localmente.
- Se llama a `clear_dirty_and_bubble(inode)` para limpiar el flag `dirty=1` y evitar reintentos infinitos.

Esta guardia aplica tanto a uploads por caché FUSE como a uploads de Local Sync.

### 4. Aborto de copia vacía en MirrorManager (`mirror/manager.rs`)

Al completar la copia de archivo desde FUSE al espejo, se verifica que `bytes_copiados > 0`. Si la copia resultó en 0 bytes, se elimina el archivo temporal y se aborta el reemplazo del symlink, preservando la accesibilidad online del archivo.

## Justificación

- Las cuatro guardias son independientes y ortogonales: ninguna depende de las demás. Esto garantiza cobertura ante fallos en cualquier subsistema.
- El patrón de "proteger si local tiene contenido y API dice 0" asume que la API está en fallo transitorio; los archivos genuinamente vacíos (tamaño conocido = 0) pasan sin obstrucción.
- La eliminación del caché corrupto, en lugar de ignorarlo, fuerza una nueva descarga limpia en la siguiente apertura.
- El patrón `remote_size < 0` (valor `-1` por parseo de JSON) se trata como "remoto desconocido" y no bloquea la subida, evitando falsos positivos en archivos de Workspace (que no tienen `size` en la API).

## Archivos Modificados

| Archivo | Cambio |
|---------|--------|
| `g-drive-xp/src/fuse/filesystem.rs` | Variable `cache_was_created`, limpieza en ramas de error de descarga y streaming. |
| `g-drive-xp/src/sync/syncer.rs` | Función `should_protect_local_file`. Guardia en `apply_remote_change`. |
| `g-drive-xp/src/sync/uploader.rs` | Función `should_block_zero_byte_upload`. Guardias en `update_file` y `upload_local_sync_file`. |
| `g-drive-xp/src/mirror/manager.rs` | Verificación de bytes copiados (`copied == 0`) antes de intercambio de symlink. |

## Consecuencias

- Los archivos corruptos existentes en caché que sean 0 bytes serán detectados y eliminados en la próxima operación de descarga o subida, forzando una recuperación limpia.
- Archivos genuinamente vacíos (documentos nuevos sin contenido) no se ven afectados: `remote_size == 0 && local_size == 0` no activa ninguna guardia.
- Se añade una llamada extra a la API (`get_file_metadata`) en el Uploader de Local Sync solo cuando el archivo local mide 0 bytes, minimizando el impacto en el caso nominal.
