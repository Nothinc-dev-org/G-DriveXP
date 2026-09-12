# ADR-021 — Supervisor con reinicio para servicios background

Fecha: 2026-09-12. Estado: aceptado e implementado
(`utils/supervisor.rs`, `main.rs`).

## Contexto

Riesgo residual del claim 8: los loops background (syncer, uploader, mirror,
IPC, monitor) se lanzaban con handles descartados (`let _handle`). Cualquier
panic futuro en esas tareas = muerte silenciosa del subsistema, sin alarma ni
restart. Además `DriveClient::new` tenía `.expect()` en certs TLS (ya fijado:
retorna `Result`, commit `69ccb68`).

## Decisión

- Nuevo `utils::supervisor`: cada servicio se registra con una factoría
  (`FnMut() -> JoinHandle`). `supervise_once` cosecha tareas terminadas y las
  relanza (distingue panic vs salida limpia); tope `max_restarts` (5) con
  perdón tras racha sana de 10 min; al agotarse emite `GaveUp` y no relanza.
  Eventos por callback (`Died`/`Restarted`/`GaveUp`) para no acoplar utils
  con GUI/tracing. Tick de 5 s + salida ante shutdown global.
- `main` cablea el sink a tracing + historial + estado de UI. Reinicio
  automático para syncer, uploader, IPC y monitor (loops idempotentes).
- **Mirror sin auto-restart (`max_restarts = 0`):** su canal de comandos y el
  watcher no son re-creables en caliente (los remitentes previos apuntarían al
  canal muerto). Ante caída: alarma ruidosa en logs, historial y UI.

## Tests

- `panic_relanza`, `agotado_se_rinde` (tras GaveUp, silencio total),
  `salida_limpia_relanza`, `racha_sana_perdona`. Sin tocar el flag global de
  shutdown (envenenaría la suite).
- Suite: 160 passed, 0 failed, 1 ignored. Build sin warnings.
