# AGENTS.md — Módulo `gui/`

## Propósito

Interfaz gráfica de usuario construida con Relm4 (patrón MVU) sobre GTK4 y Libadwaita. Muestra estado de sincronización, transferencias activas, historial de acciones y configuración.

## Archivos

| Archivo        | Responsabilidad |
|----------------|----------------|
| `mod.rs`       | Re-exporta submódulos. |
| `app_model.rs` | `AppModel`: componente Relm4 principal. Gestiona estado completo de la aplicación. Recibe mensajes `AppMsg` desde el backend thread. Renderiza con widgets Libadwaita (HeaderBar, ListBox, ProgressBar, etc.). |
| `history.rs`   | `ActionHistory`: registro thread-safe (`Arc<Mutex>`) de acciones (descargas, subidas, errores) y transferencias activas con progreso. |
| `tray.rs`      | `TrayIcon`: icono en bandeja del sistema via `ksni` (StatusNotifierItem sobre DBus). |

## Dependencias

- **Externas**: `relm4`, `gtk4`, `libadwaita`, `ksni`.
- **Internas**: `db::MetadataRepository`, `auth::clear_all_auth_data`, `mirror::MirrorCommand`.

## Notas para Agentes

- **Thread safety**: GTK4 NO es thread-safe. Todas las actualizaciones de widgets deben pasar por `ComponentSender<AppModel>` (mensajes `AppMsg`).
- **run_backend()**: se ejecuta en `std::thread::spawn` desde `AppModel::init`. El runtime Tokio vive en ese hilo.
- **Hard Reset**: la GUI puede limpiar toda la autenticación y base de datos. Usa `HARD_RESET_IN_PROGRESS` (AtomicBool global) para coordinar el cierre.
- **ViewMode**: Main (dashboard) y Activity (detalle de transferencias).
