# ADR-031 — Logout revoca la sesión en Google (no solo borra lo local)

Fecha: 2026-09-12. Estado: aceptado e implementado
(`auth/oauth.rs`, `auth/keyring.rs`, `auth/mod.rs`, `gui/app_model.rs`).

## Contexto

Auditoría del TODO en `auth/oauth.rs:136`: "Cerrar sesión" borraba keyring
+ `tokens.json` pero jamás llamaba a `https://oauth2.googleapis.com/revoke`.
El refresh seguía válido en Google: un backup de `tokens.json` pre-migración
seguía canjeando access tokens. Veredicto: confirmado.

Hallazgo extra en el barrido: la ruta real de la GUI
(`AppMsg::Logout` → `clear_all_auth_data()`) borraba la cuenta de keyring
`"refresh_token"`, distinta de la real
`"refresh_token https://www.googleapis.com/auth/drive"`
(`keyring.rs:account_for_scopes`). El refresh sobrevivía hasta en local:
`is_authenticated()` seguía dando `true` al rearrancar. `perform_hard_reset()`
tampoco tocaba el keyring.

## Decisión

- Nuevo `auth::logout()` async (única fuente de verdad): snapshot de tokens
  (keyring + fallback `tokens.json`) → `POST oauth2.googleapis.com/revoke`
  del refresh (o del access si no hay refresh) con timeout 10 s → borrado
  local. Revocación best-effort: sin red también se borra lo local; 200 y 400
  (`invalid_token`) cuentan como resuelto idempotente.
- `clear_all_auth_data()` queda como borrado LOCAL correcto (sin red):
  `delete_all()` (cuenta actual + legado) + triturado de `tokens.json`
  (reúsa `shred_file`, antes `remove_file` simple). Se elimina el método
  muerto `OAuth2Manager::logout` (cero callers, contenía el TODO).
- GUI en dos fases: `AppMsg::Logout` dispara revoke+clear por
  `oneshot_command` y el shutdown coordinado espera a `LogoutDone`
  (apagar antes cortaría el revoke a medias). Sin dependencias nuevas:
  `reqwest` + `urlencoding` ya estaban en el árbol.
- `HardReset` limpia además el keyring vía `clear_all_auth_data()`
  (`utils::cleanup` sigue puro, sin depender de `auth`). Límite conocido:
  HardReset es purga local, no revoca en Google; para revocar usar
  "Cerrar sesión".

## Verificación

4 tests nuevos en `auth/oauth.rs` (mapeo 200/400→resuelto, 401/429/500→no,
parse de access del JSON yup, JSON malo) + 10 tests auth preexistentes.
`cargo test` y `cargo check` limpios.
