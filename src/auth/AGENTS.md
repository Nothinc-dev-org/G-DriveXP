# AGENTS.md — Módulo `auth/`

## Propósito

Gestiona la autenticación OAuth2 contra Google Drive y el almacenamiento seguro de credenciales.

## Archivos

| Archivo      | Responsabilidad |
|--------------|----------------|
| `mod.rs`     | Re-exporta `OAuth2Manager`, `TokenStorage`, `clear_all_auth_data`. |
| `oauth.rs`   | Flujo OAuth2 "Installed App" con `yup-oauth2`. Servidor TCP efímero para callback. `LoginUrlDelegate` envía URL a la GUI via `ComponentSender`. |
| `keyring.rs` | Wrapper sobre el crate `keyring` para almacenar/recuperar refresh tokens en GNOME Keyring. |

## Dependencias

- **Externas**: `yup-oauth2`, `keyring`, `hyper`, `hyper-rustls`.
- **Internas**: `gui::app_model::AppModel` (para enviar URL de login a la GUI).

## Notas para Agentes

- Los tokens se persisten en `~/.config/fedoradrive/tokens.json` (via `yup-oauth2`) y opcionalmente en GNOME Keyring.
- `clear_all_auth_data()` es una función independiente usada por la GUI para "Hard Reset" sin necesidad de instanciar `OAuth2Manager`.
- El scope OAuth2 es `https://www.googleapis.com/auth/drive` (acceso completo a Drive).
