# ADR-015: Los conflictos resueltos actualizan `remote_md5` y usan milisegundos

- **Fecha**: 2026-09-11
- **Estado**: Aceptado
- **Contexto**: `Uploader::handle_conflict` (`g-drive-xp/src/sync/uploader.rs`) subía la
  copia `(Conflicto local …)` y solo hacía `clear_dirty_and_bubble`, sin actualizar
  `remote_md5`. El `known_md5` quedaba obsoleto para siempre: cada dirty posterior
  (cada edición local, por trivial que fuera) volvía a detectarse como conflicto y
  generaba OTRA copia en lugar de un update normal. Además el timestamp del nombre
  tenía resolución de 1 segundo y Drive permite duplicados, así que dos conflictos
  seguidos colisionaban nombre.
- **Decisión**:
  1. `handle_conflict` recibe el MD5 remoto actual y, tras subir la copia, llama a
     `mark_conflict_resolved` (`set_remote_md5` + `clear_dirty_and_bubble`). El
     original remoto no se tocó, así que su MD5 pasa a ser la base conocida: el
     próximo dirty sigue el path normal de update salvo que el remoto haya vuelto
     a cambiar (conflicto genuino nuevo, correcto).
  2. `build_conflict_name` usa fecha de calendario real con milisegundos
     (`%Y-%m-%d-%H%M%S-%3f`): sin colisiones dentro del mismo segundo.
  3. Predicado `is_real_conflict` extraído (sin cambio de semántica) para fijar con
     tests cuándo hay conflicto real.
- **Consecuencias**: una copia de conflicto por evento de conflicto, no una por
  edición posterior. Si el remoto vuelve a cambiar, el siguiente conflicto se
  detecta y resuelve igual (correcto).
- **Tests**: `sync::uploader::tests`: `is_real_conflict` (6 casos),
  `build_conflict_name` (extensiones + sufijo de ms), unicidad mismo-segundo
  (fallaba antes del fix), `mark_conflict_resolved` (curación real contra SQLite:
  tras resolver, el mismo estado ya no es conflicto y dirty=0).
