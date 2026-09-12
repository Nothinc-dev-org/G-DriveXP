# ADR-027 — Refresh token en GNOME Keyring (nunca en disco plano)

Fecha: 2026-09-12. Estado: aceptado e implementado
(`auth/keyring.rs`, `auth/oauth.rs`, `Cargo.toml`).

## Contexto

Claim 17: `keyring.rs` existía pero jamás se llamaba (`save/load` con cero
callers bajo `#[allow(dead_code)]`); el secreto vivo era `tokens.json`
plano con access+refresh+id_token. Además `is_authenticated()` consultaba el
keyring vacío → siempre falso.

## Hallazgo durante el fix (importante)

`keyring = "3.6"` sin features usa el **mock en memoria por instancia**:
`set` retorna `Ok` sin guardar nada y otro `Entry` no ve nada (cero D-Bus,
verificado con dbus-monitor). Mi primer instrumentado me engañó (leía del
mismo objeto). Cualquiera que "probara" el keyring así concluiría que
funciona. La solución exige `features = ["sync-secret-service",
"crypto-rust"]` (backend D-Bus real + sesión cifrada).

## Decisión

- `KeyringTokenStore: yup::storage::TokenStorage` (vía `with_storage`, sin
  reimplementar protocolo): solo persiste el **refresh token**; el access se
  re-deriva por refresh en cada arranque (menos escrituras, menos superficie).
- Arranque: migra `tokens.json` → keyring y lo **tritura** (overwrite ceros +
  sync + unlink), SOLO si todo se guardó (fallo parcial = JSON intacto, sin
  lockout). Fallo total o sin secret-service → warn + disco plano (comporta-
  miento anterior, entornos headless no se rompen).
- `logout` borra keyring (real ahora) + JSON; `is_authenticated` consulta
  keyring primero, JSON pre-migración después.

## Verificación

10 tests auth (parse yup-disk, migración ok/fallo-conserva/noop, shred,
cuentas deterministas, TokenInfo solo-refresh, e2e adaptativo contra el
backend que haya + roundtrip real `#[ignore]`). E2E verificado contra el
daemon GNOME Keyring vivo (store→load→delete). Suite 182 passed, 0 failed;
build 0 warnings.
