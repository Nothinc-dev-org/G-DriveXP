# AGENTS.md — Módulo `auth/`

## Propósito

Gestiona la autenticación OAuth2 contra Google Drive y el almacenamiento seguro de credenciales.

## Archivos

| Archivo      | Responsabilidad |
|--------------|-----------------------------------------------------------|
| `mod.rs`     | Re-exporta `OAuth2Manager`, `clear_all_auth_data`. |
| `oauth.rs`   | Flujo OAuth2 "Installed App" con `yup-oauth2`. Servidor TCP efímero para callback. `LoginUrlDelegate` envía URL a la GUI via `ComponentSender`. |
| `keyring.rs` | `KeyringTokenStore`: implementa `yup_oauth2::storage::TokenStorage` sobre GNOME Keyring (solo refresh token). Migración una sola vez desde `tokens.json` + triturado. |

## Dependencias

- **Externas**: `yup-oauth2`, `keyring`, `hyper`, `hyper-rustls`.
- **Internas**: `gui::app_model::AppModel` (para enviar URL de login a la GUI).

## Notas para Agentes

- Los refresh tokens viven en GNOME Keyring (cifrado). `tokens.json` plano solo
  existe pre-migración o donde no hay secret-service (fallback con warn).
  `migrate_tokens_json_to_keyring()` lo tritura tras migrar (solo si todo se guardó).
- `clear_all_auth_data()` es borrado LOCAL (keyring actual+legado y `tokens.json`
  triturado). La revocación en Google la hace `auth::logout()` (revoke best-effort
  + borrado); la GUI lo corre en background y apaga en `LogoutDone`.
- El scope OAuth2 es `https://www.googleapis.com/auth/drive` (acceso completo a Drive).
