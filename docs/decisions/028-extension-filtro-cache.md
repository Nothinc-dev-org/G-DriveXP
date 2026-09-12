# ADR-028 — Extensión: filtro por mirror + caché TTL (adiós 50 ms/archivo)

Fecha: 2026-09-12. Estado: aceptado e implementado (`nautilus-ext/`,
commit pendiente abajo).

## Contexto

Claim 18: `update_file_info_impl` (main thread de Nautilus, sincrónico por
archivo) pagaba hasta 50 ms de `recv_timeout` por archivo — y el tope se
volvía costo fijo bajo carga (worker secuencial más lento que el deadline).
Sin filtro por path (hasta `/tmp` y USBs pagaban IPC) y sin caché
(scroll/refresh re-pagaban todo). El worker además procesaba consultas ya
abandonadas.

## Decisión

- Filtro por prefijo del mirror antes de consultar: el daemon solo resuelve
  estado bajo el mirror (responde Unknown fuera de todos modos), así que el
  filtro no cambia ningún comportamiento visible. `mirror_path` sale del
  `config.json` del daemon (misma fuente, sin skew), fallback
  `~/GoogleDrive`. Comparación por `strip_prefix` (borde de componente:
  `GoogleDrive2` no cuela) + percent-decode del URI.
- Caché TTL 3 s, tope 5000 entradas (al llenarse se vacía; la oleada
  siguiente lo rellena fresco). Hit = solo un lock de mutex en el main
  thread. Staleness invisible (syncs cada 60 s).
- NO se implementa "saltar trabajo abandonado": crossbeam 0.5 no expone
  `is_disconnected` en el `Sender` (solo en los errores) y no hay pre-vuelo
  limpio. Con filtro+caché el volumen colapsa y el punto es irrelevante.
- NO proveedor asíncrono real (10x riesgo FFI por el último 5%), NO bajar el
  timeout (más Unknowns), NO batch con protocolo (skew), NO worker paralelo
  (el main thread seguiría pagando 50 ms/archivo).

## Verificación

3 tests nuevos (borde del filtro, decode de URI, TTL/expiración/tope).
`cargo test` 5 passed; check/clippy sin warnings en líneas nuevas.
