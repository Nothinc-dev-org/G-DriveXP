# ADR-029 — Conflictos en local_sync: el uploader no pisa, el syncer no borra

Fecha: 2026-09-12. Estado: aceptado e implementado
(`sync/uploader.rs`, `sync/syncer.rs`).

## Contexto

Claim 19: `upload_local_file` (rama `Some(gdrive_id)`) subía a ciegas con
un `TODO` abierto — edición remota entre ciclos + edición local = la remota
se perdía en silencio. Peor aún, el otro sentido no estaba citado:
`process_local_sync_change` descargaba y sobrescribía el archivo local sin
mirar `dirty`, y `update_local_file_from_remote` ponía `dirty = 0` después
de pisar — pérdida local permanente sin historial que la salvara.

## Decisión

- Uploader: un solo `get_file_metadata` (sirve al check y a la guardia
  anti-0-bytes); si `is_real_conflict(remote_md5_conocido, md5_actual)`,
  sube el local como archivo nuevo con `build_conflict_name` al mismo
  padre, remoto intacto, y cura con `update_local_file_from_remote`
  (base = md5 actual + dirty limpio). Mismo patrón que update_file/FUSE.
- Syncer (`local_online`): si `dirty == 1`, warn + return sin descargar y
  sin tocar la fila (actualizar `remote_md5` "de paso" destruiría la base
  del conflicto). Dueño único de la resolución: el uploader; sin dobles
  copias ni escrituras sorpresa en el FS del usuario.
- NO merge a 3 vías (binarios arbitrarios), NO copia local del remoto
  desde el syncer.

## Límite asumido

Drive no tiene upload condicional por md5: queda una race mínima entre el
check y el update (igual que el path FUSE). Reduce la pérdida al caso raro,
no la elimina.

## Verificación

Test `test_local_sync_conflicto_no_pisa_y_se_cura` (contrato: dirty +
remoto cambiado = conflicto; curar deja de detectarlo; dirty=1 presente).
Suite 183 passed, 0 failed; build 0 warnings.
