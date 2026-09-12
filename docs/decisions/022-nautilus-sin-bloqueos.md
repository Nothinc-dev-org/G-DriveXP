# ADR-022 — Extensión Nautilus: sin bloqueos ni muerte silenciosa

Fecha: 2026-09-12. Estado: aceptado e implementado (`nautilus-ext/`,
commit `c800d20`).

## Contexto

El claim 9 citaba `static mut` ×2 y un NULL-deref como vectores de segfault.
Verificación: los tres eran olor real pero no alcanzables (inits single-thread,
punteros que Nautilus nunca entrega corruptos). Los problemas reales estaban
al lado:

1. **Freeze de Nautilus:** `send` bloqueante sobre canal acotado (32) desde el
   main thread, con worker secuencial de 200 ms/req. Daemon colgado pero
   aceptando → cola llena → main thread bloqueado indefinidamente.
2. **Muerte silenciosa permanente:** worker muerto (panic) jamás se
   re-creaba (`OnceLock` sin reintento) → emblemas muertos hasta reiniciar
   Nautilus. Callbacks con `.unwrap()` en el runtime → no-op silencioso.

## Decisión

- `try_send` + fallback `Unknown`: cola llena = sin emblema, jamás freeze.
- Worker global con re-creación (`Mutex<Option<IpcWorker>>` + `alive()` por
  `JoinHandle::is_finished`): la siguiente consulta tras una muerte
  reconstruye el hilo.
- `static mut` → `OnceLock` (TYPE_LIST, GType); guards NULL en `file`/`data`;
  construcción de runtime en callbacks con log + return.
- No se tocaron los cambios preexistentes del worktree (migración edition
  2024 en Cargo.toml/ffi.rs/lib.rs): el commit solo lleva estos hunks
  (staging parcial por patch).

## Verificación

`cargo check --lib` limpio; clippy sin warnings en líneas nuevas. Sin tests
en el crate (sin harness para FFI/Nautilus); la evidencia es construcción
exitosa + revisión de pares productor/consumidor de cada puntero/canal.
