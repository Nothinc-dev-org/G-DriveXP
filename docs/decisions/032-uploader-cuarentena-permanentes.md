# ADR-032 — Uploader distingue errores permanentes (cuarentena + backoff por archivo)

Fecha: 2026-09-12. Estado: aceptado e implementado
(`gdrive/error.rs`, `gdrive/client.rs`, `sync/uploader.rs`).

## Contexto

Auditoría: `DriveError::is_permanent()` solo existía en `#[cfg(test)]`
(`gdrive/error.rs:26-27`, cero callers en producción). El syncer lo compensa
con cuarentena agnóstica (`MAX_CHANGE_FAILURES = 5`), pero el uploader solo
logueaba el fallo por archivo y conservaba `dirty`: un archivo condenado
reintentaba cada 30 s eternamente, sin que el backoff global del ciclo
(que solo salta si falla el ciclo entero) siquiera se activara.

## Decisión

- `is_permanent()` des-gateado + `DriveError::from_http_status(status, body)`:
  404 → `NotFound`; 403 solo es `InsufficientPermissions` si el body lo
  confirma (`quota` y `rate-limit` quedan como `ApiError`: se reintentan).
  `permanent_of(&anyhow)` extrae el permanente a través de `.context()`.
- Clasificación donde nace el error, sin cambiar firmas `anyhow::Result`:
  `timed()` mapea el `Failure` del hub vía `classify_hub_error` (cubre
  upload/update/listados de una vez); `create_folder` (hub directo) y
  `get_file_metadata` (reqwest) clasifican en su sitio.
- Uploader, por archivo: `NotFound` sobre id real reconcilia de inmediato
  (gana la verdad remota, como el delete); otro permanente cuenta en
  `failed_changes` (reúsa la tabla del syncer, claves `up:*`/`uplocal:*`,
  umbral 10 ≈ 5 min) con baja visible al umbral; transitorio/desconocido
  entra en backoff exponencial por archivo en memoria (30 s→1 h) sin tocar
  `dirty`. `DEFERRED_PARENT_TEMP` no cuenta (es orden, no fallo).
- NO se clona la cuarentena ciega del syncer a propósito: un outage largo
  limpiaría `dirty` de ediciones reales (pérdida del intento de subida).
  Aquí solo lo clasificado-permanente da de baja; lo demás reintenta
  espaciado. El archivo local nunca se toca: re-editarlo lo revive
  (`dirty=1` de nuevo).

## Verificación

10 tests nuevos (mapeo 404/403-quota/429/500, `permanent_of` bajo contexto,
clasificador del hub con `Failure` construidos, delay exponencial con tope,
`DEFERRED_PARENT_TEMP`, claves). `cargo check` 0 warnings; `cargo test`
197 passed, 0 failed; clippy sin avisos en lo tocado.
