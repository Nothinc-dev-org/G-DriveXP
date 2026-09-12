# ADR-014: La limpieza de huérfanos del espejo nunca borra archivos reales

- **Fecha**: 2026-09-11
- **Estado**: Aceptado
- **Contexto**: `MirrorCommand::Refresh` pausa el watcher (`watcher.take()` + drenado de
  eventos pendientes) y ejecuta `run_bootstrap(ctx, false)`, que incluía limpieza de
  huérfanos. La limpieza borraba del disco toda entrada no registrada en la DB
  (`remove_file` / `remove_dir_all`). Un archivo creado segundos antes del Refresh —
  o durante el bootstrap, que tarda segundos en cuentas grandes — aún no estaba en
  la DB y se borraba como "huérfano". Pérdida de datos (P0). El Refresh se dispara
  solo (`sync/syncer.rs`, `sync/bootstrap.rs`), sin acción del usuario.
- **Decisión**: `cleanup_orphans` (`g-drive-xp/src/mirror/manager.rs`) solo elimina
  **symlinks obsoletos** (artefactos OnlineOnly cuyo contenido vive en Drive y son
  recreables). Las entradas **reales** desconocidas se **ingestan** en la DB
  (`static_ingest_unknown`: cadena de padres + inode temporal + metadata + dentry +
  `local_online` + dirty, con `bubble=false` porque el bootstrap reconstruye
  `dir_counters` al final), replicando lo que el watcher habría hecho vía
  `process_local_change`. Los directorios reales nuevos se ingieren y se desciende
  en ellos. Artefactos internos/temporales (`.hidden`, manifiesto, `.gdrive*`,
  `.cloud*`, `*.part/tmp/crdownload`) se dejan intactos. Ante error de ingesta, el
  archivo se conserva en disco (se falla hacia la preservación).
- **Consecuencias**:
  - Se elimina la pérdida de datos del P0; el archivo nuevo sobrevive al Refresh y
    sube a Drive como dirty en el siguiente ciclo del uploader.
  - La limpieza sigue cumpliendo su propósito original (symlinks de archivos
    eliminados remotamente).
  - Caso límite aceptado: un archivo `local_online` borrado remotamente mientras el
    cliente estaba offline se re-ingiere como nuevo y puede resubirse. Se prefiere
    este riesgo menor (recuperable, visible) sobre el borrado silencioso (P0).
- **Tests**: `mirror::manager::tests` (4 tests):
  `orphan_cleanup_preserves_new_unregistered_file`,
  `orphan_cleanup_preserves_new_unregistered_tree`,
  `orphan_cleanup_registers_unknown_file_instead_of_deleting` (fallaban antes del
  fix), `orphan_cleanup_still_removes_stale_symlink` (guardia de regresión).
