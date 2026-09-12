# ADR-026 — Extensión: daemon caído = rojo, no "no rastreado"

Fecha: 2026-09-12. Estado: aceptado e implementado (`nautilus-ext/`,
commit pendiente abajo).

## Contexto

Claim 16: tres estados distintos (daemon muerto, transitorio, no rastreado)
colapsaban a `Ok(Unknown, NotTracked)` en tres capas (ipc_client, worker,
emblema). Daemon caído se veía igual que "archivo no sincronizado" y las
acciones del menú eran no-ops silenciosos (peor: `Ok(false)` se logueaba
como "Success").

## Decisión

- Sin cambio de protocolo (bincode indexa variantes por posición;
  daemon/ext se versionan por separado). Se reutiliza `SyncStatus::Error`
  (rojo), que ya existe en ambos lados; el mapeo ocurre solo en la extensión.
- `send_request`: fallos de transporte → `Err` (conservando el kind),
  no `Ok(Error)`. El `Error` respondido por el daemon sigue yendo a Unknown.
- Worker: solo lo PROBADO-muerto va a rojo (`is_unreachable_kind`:
  NotFound/ConnectionRefused/ConnectionReset/BrokenPipe/UnexpectedEof/
  ConnectionAborted/NotConnected). Timeouts y cola llena → Unknown.
  Sin contadores de fallos consecutivos: con drenado lento los timeouts son
  rutinarios bajo carga y contarlos parpadearía rojos en cada sync masivo.
  Se mantiene `try_send` (el `send` bloqueante reintroduciría el freeze
  del claim 9).
- Menú: `Ok(true)` éxito / `Ok(false)` rechazo logueado como tal /
  `Err` con mención a daemon inalcanzable. Sin libnotify (sin dependencia):
  el feedback es gating de visibilidad (ya saltaba lo no-Synced/CloudOnly)
  + logs precisos.

## Verificación

2 tests nuevos del clasificador (muerte probada → rojo; transitorio jamás).
`cargo test` 2 passed; `cargo check`/`clippy` sin warnings en líneas nuevas.
