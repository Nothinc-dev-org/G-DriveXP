# ADR-030 — GUI usa el runtime de relm4, errores visibles

Fecha: 2026-09-12. Estado: aceptado e implementado (`gui/app_model.rs`).

## Contexto

Claim 20: los 4 handlers de carpetas sync (Load/Add/Remove/ToggleSyncDir)
creaban `thread::spawn` + `tokio::runtime::Runtime::new()` por clic y
tragaban errores con doble `if let Ok` sin `else` (clic sin efecto y sin
explicación).

## Decisión

- Los 4 sitios usan `_sender.oneshot_command(async move { ... })`: relm4
  0.9 ya mantiene un runtime Tokio global compartido para comandos, así que
  no hay threads manuales ni runtimes por clic, y el futuro se cancela solo
  al cerrar el componente.
- Errores con `match`: `warn!` con contexto + `AppMsg::UpdateStatus`
  (variante existente, visible vía `status_message`) en vez de silencio.
  DB aún `None` (backend iniciando) también avisa en log.
- NO runtime propio en el modelo (duplicaría el de relm4), NO Handle del
  backend (acopla ciclos de vida sin ganar nada), NO `spawn_blocking`
  (hilo bloqueado por clic para queries que ya son async).

## Verificación

Build 0 warnings; suite 183 passed, 0 failed; `grep Runtime::new
src/gui/` vacío.
