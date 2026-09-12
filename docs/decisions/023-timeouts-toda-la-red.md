# ADR-023 — Timeouts en toda la red: ningún hang es eterno

Fecha: 2026-09-12. Estado: aceptado e implementado
(`gdrive/client.rs`, `sync/*`, `fuse/filesystem.rs`, `main.rs`).

## Contexto

Claim 11 y su familia: ningún cliente HTTP tenía timeout
(`reqwest::Client::new()`, `hyper::Client::builder().build()`, yup sin
timeout). Una red de agujero negro (acepta TCP, nunca responde) colgaba
cualquier futuro de red para siempre: syncer atascado a mitad de ciclo con
la tarea "viva" (el supervisor no podía verlo), repair/BFS zombies, hilos
FUSE bloqueados en lecturas bajo demanda. El gateo citado (repair antes que
mirror) ya no existía tras el supervisor, pero el hang sí.

## Decisión

- `DriveClient::timed(what, limit, fut)`: convierte hang en error
  recuperable (reintento/backoff/cuarentena, que ya existían). Genérico
  sobre el error interno (`E: Into<anyhow::Error>`), así sirve para
  `anyhow` y `DriveError`.
- Plano de control (listados, páginas, changes, metadatos, carpetas,
  md5, root id, token OAuth): 60 s en cada call site (25 sites en
  syncer/uploader/bootstrap/main/FUSE + token de `trash_file` in situ
  para preservar el `match` de `InsufficientPermissions`).
- Descargas: reqwest con timeout total 120 s + 180 s por chunk en sites.
- Subidas: timeout por tamaño dentro de `upload_file`/`update_file_content`
  (simple 300 s; resumable 600 s + 1 s/50 KB, tope 1 h). Un timeout deja el
  archivo dirty y se reintenta en el ciclo siguiente: nunca pérdida.

## Verificación

4 tests nuevos (`timed` convierte `pending()` en error marcado, passthrough
de ok/error interno, escala y tope de `upload_timeout`, compatibilidad con
`DriveError`). Suite 164 passed, 0 failed; build 0 warnings.
