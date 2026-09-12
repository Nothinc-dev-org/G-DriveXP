# ADR-017: El page token solo avanza sobre páginas aplicadas (cuarentena visible)

- **Fecha**: 2026-09-11
- **Estado**: Aceptado
- **Contexto**: `BackgroundSyncer::sync_once` (`g-drive-xp/src/sync/syncer.rs`) registraba
  solo `warn` por cambio fallido y guardaba el page token de la página de todos
  modos. Los tokens de `changes.list` son acuses de recibo: avanzar sobre fallos
  descarta esos cambios para siempre (divergencia silenciosa; los deletes perdidos
  ni siquiera los cura el BFS de arranque, que no reconcilia ausentes). Además
  retornaba `total_fetched` (incluía fallidos) como "sincronizados".
- **Decisión**:
  1. El token de página solo se guarda si todos sus cambios aplicaron. Si alguno
     falla, la página se retiene (el token guardado no se mueve) y se re-lista en
     el próximo ciclo; aplicar dos veces es seguro (upserts/`hard_delete`
     idempotentes). Se sale del loop de páginas al retener para no re-pedir en
     bucle dentro del mismo `sync_once`.
  2. Cuarentena visible contra cambios envenenados: tabla `failed_changes`
     (`file_id`, `failures`, `last_error`, `updated_at`; migración 12 + schema).
     Cada fallo suma; al llegar a `MAX_CHANGE_FAILURES = 5` el cambio pasa a
     cuarentena con `error` + entrada de historial y el token puede avanzar sobre
     él (salto explícito y registrado, nunca silencioso). Un éxito posterior
     limpia su fila.
  3. `sync_once` retorna cambios aplicados con éxito, no listados; el `Refresh`
     al mirror solo se envía si hubo aplicaciones reales.
- **Consecuencias**: ningún cambio remoto se pierde en silencio; lo peor posible
  es una página retenida ~5 ciclos (~5 min) ante un cambio envenenado, luego
  cuarentena visible y consultable (`SELECT * FROM failed_changes`).
- **Tests**: `sync::syncer::tests`: `should_advance_token` (vacío/cuarentena
  total/mixto/retención) y conteo+limpieza de cuarentena contra SQLite real.
