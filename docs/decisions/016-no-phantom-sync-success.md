# ADR-016: Sin contenido no hay éxito — fantasmas dados de baja, caché restaurada

- **Fecha**: 2026-09-11
- **Estado**: Aceptado
- **Contexto**: el uploader tenía dos ramas que registraban éxito sin haber subido
  nada (`g-drive-xp/src/sync/uploader.rs`):
  1. `create_file`: sin caché ni mirror, escribía `attrs.size = 0`, limpiaba dirty
     y retornaba `Ok` (contado como "subido"). Fabricaba un registro sincronizado
     de 0 bytes de un archivo que no existe ni local ni en Drive.
  2. `update_file`: con dirty=1 pero sin caché, limpiaba dirty y lo llamaba "Estado
     corregido", sin siquiera intentar recuperar desde el mirror (que `create_file`
     sí intentaba). Deltas locales pendientes quedaban des-encolados en silencio.
- **Decisión**:
  1. Creación sin contenido en ningún lado y nunca vista por Drive: `remove_lost_phantom`
     da de baja sus filas vía `hard_delete` (con ajuste de contadores). Si la baja
     falla, propaga el error para mantener dirty=1 y reintentar. Nunca `size = 0`.
  2. Update sin caché: primero `restore_cache_from_mirror` (solo archivos reales,
     nunca symlinks; errores de copia degradan a "no recuperable"). Si recupera,
     el flujo normal continúa (incluida la optimización de contenido idéntico, que
     cura limpiamente). Si no hay nada en ningún lado, converge a la verdad remota
     (que no se tocó) pero se registra como `Conflict` ("delta local descartado"),
     con `warn`, nunca como sync exitosa.
- **Consecuencias**: ningún path del uploader escribe tamaños fabricados; lo único
  que figura como sincronizado es lo verificado contra contenido real o remoto.
- **Tests**: `sync::uploader::tests`: `restore_cache_from_mirror` (recupera bytes
  exactos; rechaza ausente/None/directorio/symlink roto sin crear nada),
  `remove_lost_phantom` (dentry/inode/estado desaparecen, sin fila `size=0`).
