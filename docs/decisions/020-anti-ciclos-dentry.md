# ADR-020 — Anti-ciclos en dentry: guards + EINVAL en rename

Fecha: 2026-09-12. Estado: aceptado e implementado
(`db/repository.rs`, `fuse/filesystem.rs`).

## Contexto

El claim 7 de la auditoría decía: el `while current != 1` de
`resolve_inode_to_relative_path` no detecta ciclos → loop infinito en thread
FUSE. Verificación:

- **Cierto el loop sin guarda**, con un matiz: ningún thread FUSE ejecuta ese
  `while` (solo lo llaman uploader y syncer). Pero el cuelgue del filesystem
  es real por vía indirecta: los CTE recursivos (`bubble_state_change`,
  `subordinates` de borrados, el de rename) tampoco tienen detección de ciclos
  (SQLite no la tiene) → una query girando eternamente frena una conexión del
  pool; a las 5, todo acceso a DB —o sea, todo FUSE— se bloquea. Además
  `rebuild_all_dir_counters` giraba al 100% CPU en memoria sobre ciclos.
- **Cierto y alcanzable:** FUSE `rename` hacía DELETE + `upsert_dentry`
  sin validar que `new_parent` no fuera el propio inodo o un descendiente.
  Un `rename(2)` directo de un dir a su subárbol creaba el ciclo (falta EINVAL).
  La vía mirror está cubierta (el kernel/`mv` lo rechaza) y la remota también
  (Drive lo prohíbe). Producción verificada sin ciclos (walk con cota 50000:
  0 cadenas largas).

## Decisión

1. `resolve_inode_to_relative_path`: visited-set; ante revisita, `error!` +
   `Ok(None)` (los callers ya tratan `None` como ausente: uploader
   `.ok().flatten()`, syncer `if let Ok(Some)` → convergen sin colgarse).
2. Nuevo `is_descendant_or_self(ancestor, node)` (también con visited-set) y
   chequeo en FUSE `rename` antes de mutar nada: mover bajo sí mismo o bajo
   descendiente → `EINVAL`. Mismo-dir y movimientos normales no cambian.
3. `rebuild_all_dir_counters`: corta con `debug!` ante revisita (contadores
   parciales para ese archivo, sin hang).
4. Fuera de alcance: endurecer cada CTE recursivo con cota de profundidad
   (ante corrupción preexistente, un CTE truncado parcial sería peor que un
   error visible; con las tx del ADR-019 el error haría rollback). Si alguna
   vez aparece un ciclo legacy, el resolver lo reporta en logs y las rutas
   convergen a `None`.

## Tests

- `resolve_inode_ciclo_no_cuelga` / `resolve_inode_self_loop_no_cuelga` /
  `rebuild_con_ciclo_termina`: con `timeout` (en rojo: cuelgue; en verde:
  instantáneos). El run paralelo pre-fix se atraganta — correr en serie.
- `is_descendant_or_self_casos`: self, hijo, nieto, padre, ramas, root,
  ancestro inexistente.
- Suite: 156 passed, 0 failed, 1 ignored. Build sin warnings.
