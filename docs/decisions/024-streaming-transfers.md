# ADR-024 — Streaming en transfers: RAM acotada, publicación atómica

Fecha: 2026-09-12. Estado: aceptado e implementado
(`gdrive/client.rs`, `sync/syncer.rs`, `fuse/filesystem.rs`).

## Contexto

Claim 12: descargas acumulando el archivo completo en un `Vec` + `fs::write`
directo al destino, subidas con `fs::read` completo, y `vec![0u8; size]` en el
truncate de FUSE (alcanzable con `truncate -s 100G`).

## Por qué esta solución y no otra

- **No reimplementar resumable:** `google-apis-common` ya implementa el
  protocolo (sesiones, reintentos con seek, chunks de 8 MB por request) y ya
  lee acotado por chunk. El único problema era que nosotros lo alimentábamos
  con un `Cursor<Vec>` precargado. Pasarle el `File` de disco lo vuelve
  O(8 MB) de RAM con ~10 líneas cambiadas. Reescribir sesiones resumables a
  mano sobre reqwest duplicaría protocolo probado en batalla: peor.
- **No `mmap`:** misma presión de residente + riesgo de SIGBUS. Peor.
- **No solo temp+rename:** curaba el truncado pero dejaba el OOM. Insuficiente.
- **No topes arbitrarios de tamaño:** un archivo legítimo de 30 GB debe
  funcionar; con streaming funciona con 10 MB de RAM.
- **Simple (< 5 MB) sin cambios:** el `Vec` acotado es barato y el upload
  simple lo necesita de una pieza. Menos churn, mismo techo de RAM.
- **I/O síncrono aceptado en resumable:** el bound `ReadSeek` de la librería
  lo exige; el runtime es multi-thread y las subidas corren en tareas
  dedicadas (hay precedente: `std::fs` ya se usa en paths FUSE).

## Decisión

- Subidas ≥ 5 MB: `File` de disco (+ `ProgressReader` encima, conserva
  progreso; el seek rebobina el contador, sin doble conteo en reintentos).
- Descargas local_sync: `stream_chunks_to_file` (chunks de 10 MB a tmp en el
  mismo dir) + `flush`/`sync_all` + `commit_temp_file` (rename atómico).
  Fallo = tmp eliminado, reintento parte de cero. Chunk vacío = error
  (antes: loop infinito quemando un timeout por iteración).
- Truncate FUSE: `set_len` disperso en vez de materializar ceros.

## Verificación

7 tests nuevos (streaming por chunks, aborto ante chunk vacío, commit
publica/limpia y no toca dest si falta tmp, naming del tmp, progreso desde
disco con rebobinado, truncate disperso de 10 GB). Suite 171 passed,
0 failed; build 0 warnings.
