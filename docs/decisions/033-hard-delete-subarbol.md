# ADR-033 — Hard delete recursivo del subárbol (hojas primero, misma tx)

Fecha: 2026-09-12. Estado: aceptado e implementado
(`db/repository.rs`).

## Contexto

Auditoría de FKs: el enforcement SÍ está activo (default de sqlx 0.8 por
conexión, verificado en runtime), así que el hueco real no eran huérfanos
silenciosos sino lo contrario: `hard_delete_inode_tx` borraba un solo nodo
y, ante un directorio con hijos con edges vivos (REMOVED de carpeta en el
syncer, temp-dir desplazado en mirror), el `DELETE FROM inodes` fallaba con
FK constraint → rollback total → el cambio entraba en loop de cuarentena sin
converger jamás. Añadir CASCADE a `dentry.parent_inode` habría sido peor
(borraría los edges dejando inodes/attrs/state varados).

## Decisión

- Nueva `hard_delete_subtree_tx`: CTE con `depth DESC` (hojas primero) +
  reutilización de `hard_delete_inode_tx` por nodo, todo en la misma tx
  (se preserva el contrato "todo o nada" y el burbujeo por nodo: cuando cada
  archivo ajusta contadores, sus edges y ancestros aún existen).
- CTE con `UNION` (no `UNION ALL` como `soft_delete_tree_tx`): `dentry`
  puede contener ciclos y la recolección debe terminar igual; los
  re-alcances son no-ops idempotentes.
- Guardia raíz (`inode == 1` → error ruidoso): por este camino, borrar el 1
  vaciaría el árbol entero; Drive nunca emite eso.
- `purge_expired_tombstones` ya no aborta al primer fallo: un item
  patológico se registra y la purga sigue (antes, un solo tombstone malo
  atascaba la purga entera en cada ciclo). Retorna purgados reales.
- Purga, `hard_delete_by_gdrive_id`, phantom y mirror heredan el fix sin
  tocarlos; `hard_delete_inode_tx` queda intacta.

## Verificación

3 tests nuevos (subárbol con edges vivos → 0 filas + `foreign_key_check`
vacío; guard de raíz intacta; purga que continúa). `cargo check`
0 warnings; `cargo test` 200 passed, 0 failed; clippy sin avisos en lo
tocado.
