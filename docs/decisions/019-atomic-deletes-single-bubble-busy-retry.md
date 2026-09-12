# ADR-019 — Borrados atómicos + burbujeo simple + reintento ante BUSY

Fecha: 2026-09-12. Estado: aceptado e implementado (`db/repository.rs`).

## Contexto

El claim 6 de la auditoría decía: borrados sin transacción + `soft_delete` retorna
`true` aunque no borre nada. Verificación:

- **Cierto (1):** `soft_delete_by_gdrive_id` eran ~7 statements sueltos
  (2 conteos, tumbas, 2 escrituras a `sync_state`, burbujeo, borrado de dentry)
  y la ruta `hard_delete` otras ~7. Sin tx.
- **Cierto (2), matiz:** el vector realista no es el crash sino la concurrencia:
  los conteos del paso 0 se calculan antes de mutar, así que cualquier escritor
  concurrente entre el paso 0 y el burbujeo corrompe `dir_counters` en silencio
  (solo un rebuild lo cura); dos borrados solapados burbujean doble; un recreate
  concurrente parte el subárbol entre `dentry` y `dentry_deleted`.
- **Cierto pero benigno (3):** `Ok(true)` incondicional si el `gdrive_id` resuelve
  (casos ya-borrado/huérfano no tocan filas). Es idempotencia por diseño y
  `soft_delete_remote` depende de ella para converger; ningún caller ramifica por
  el booleano. **Se deja como está.**

## Hallazgo adicional (bug real, determinista)

`bubble_state_change` contaba la raíz **dos veces**: la CTE de ancestros ya
alcanza al inode 1 (el `WHERE parent_inode > 1` solo detiene la recursión, no
excluye la fila que vale 1), y el segundo `UPDATE` explícito para root volvía a
sumar. Enmascarado porque la fila `dir_counters(1)` suele no existir (`UPDATE`
sin fila = no-op). Detectado por `soft_delete_estado_final_completo`
(esperaba `(2,0)`, obtuvo `(4,0)`). Se eliminó el segundo `UPDATE`; la sentencia
única vive en la constante `BUBBLE_UPDATE_SQL`, compartida entre pool y tx.

## Decisión

1. `soft_delete_by_gdrive_id` = wrapper fino sobre `soft_delete_tree_tx`
   (todo en una tx: resolve, conteos, tumbas, `sync_state`, burbujeo inline,
   borrado de dentry, commit). `soft_delete_remote` reutiliza el mismo núcleo
   y añade su limpieza de `dirty` **dentro de la misma tx** (antes eran dos
   fases separadas con una re-resolución entremedias).
2. `hard_delete_inode` = núcleo `hard_delete_inode_tx` (burbujeo + 7 borrados
   en una tx). El bucle de purga sigue llamando por inodo (una tx por inodo).
3. `retry_on_busy` (~20 s, 50 ms): en WAL, dos tx DEFERRED que escriben a la
   vez fallan con `BUSY_SNAPSHOT` (el `busy_timeout` del pool no cubre ese
   caso). El perdedor reintenta con tx fresca y converge por idempotencia.
   Sin esto, la tx **empeoraba** la concurrencia respecto al autocommit
   (detectado en rojo: `database is locked` en el test concurrente). Se aplica
   a los 4 entry points de borrado y a `upsert_dentry` (misma clase, claim 5).
   Agotado el presupuesto, el error sube al llamador (reintento/cuarentena).
4. Semántica de `true` sin cambios (idempotencia documentada, no éxito falso).

## Tests

- `soft_delete_estado_final_completo`: dentry fuera, 3 tumbas, flags,
  contadores exactos `(2,0)`, segunda llamada idempotente sin cambios.
- `hard_delete_limpia_todas_las_tablas`: 7 tablas limpias.
- `soft_deletes_concurrentes_cuadran_contadores`: 2 borrados con barrera,
  contadores exactos `(4,0)` (en rojo: doble-burbujeo + BUSY).
- Suite: 152 passed, 0 failed, 1 ignored. Build sin warnings.

## Nota de formato

Los cuerpos envueltos en `retry_on_busy(|| async { … })` quedan un nivel de
indentación por debajo de lo que pediría rustfmt. No se corrió `cargo fmt`
porque el repo ya tiene suciedad de formato preexistente sin gate; un futuro
`cargo fmt` lo normaliza.
