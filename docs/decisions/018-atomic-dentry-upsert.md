# ADR-018: `upsert_dentry` atómico con orden invertido + perdedores que se autolimpian

- **Fecha**: 2026-09-11
- **Estado**: Aceptado
- **Contexto**: `upsert_dentry` (`db/repository.rs`) hacía DELETE y luego INSERT en
  statements sueltos sobre un pool de 5 conexiones, con 6+ escritores concurrentes
  (FUSE ×3, syncer ×2, bootstrap, mirror ×3). Ventana donde el nombre no existe
  (lectores ven `None` → inodos duplicados), crash deja huérfano permanente, y el
  `ON CONFLICT DO UPDATE` roba el nombre dejando al perdedor sin dentry. Esos
  huérfanos `temp_*` suben a Drive como `file_N` en raíz (fallback de nombre +
  `unwrap_or(root)` en el uploader).
- **Decisión**:
  1. `upsert_dentry` en transacción con orden invertido: INSERT primero (roba si
     ocupado), DELETE del enlace viejo después excluyendo el recién escrito.
     Lo peor visible pasa de "nombre inexistente" a "enlace viejo aún
     resolviendo". Mismo reorden en `upsert_bulk_dentries` (ya tenía tx).
     Contención `BUSY` propaga a reintento/cuarentena (ADR-017), nunca a pérdida.
  2. `drop_losing_temp` (`mirror/manager.rs`): si un inode recién creado pierde
     el nombre frente a otro y es `temp_*`, se da de baja vía `hard_delete`.
     Jamás toca reales ni al ganador. Cableado en `process_local_change`
     (solo `is_new`) y `static_ingest_unknown`.
- **Consecuencias**: cero nombres inexistentes observables; cero huérfanos nuevos
  por robo (verificado por tests). El robo como tal persiste por diseño del
  modelo (un `(parent,name)` → un hijo); los preexistentes en producción requieren
  triage manual en la web si ya subieron como `file_N`.
- **Tests**: `db::repository::tests`: equivalencia de movimiento + martilleo
  concurrente con observador (caza el código viejo 3/3 con 8-11 lecturas rotas
  de 5000; 0 con el fix). `mirror::manager::tests`: perdedor temporal se
  autolimpia, ganador intacto, real nunca tocado.
