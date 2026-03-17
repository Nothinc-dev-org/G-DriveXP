//! Icono de bandeja del sistema usando StatusNotifierItem (ksni)
//!
//! Muestra el historial de acciones recientes y permite controlar la aplicación.

use ksni::{menu::*, Tray, TrayService, ToolTip};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use super::history::{ActionHistory, TransferOp};



/// Servicio del icono de bandeja
pub struct TrayIcon {
    history: ActionHistory,
    sync_paused: Arc<AtomicBool>,
}

impl TrayIcon {
    pub fn new(history: ActionHistory, sync_paused: Arc<AtomicBool>) -> Self {
        Self { history, sync_paused }
    }

    /// Inicia el servicio del icono de bandeja en un thread separado
    pub fn spawn(self) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let history = self.history.clone();
            let service = TrayService::new(GDriveXPTray {
                history: self.history,
                sync_paused: self.sync_paused,
            });

            // Obtener handle para forzar actualizaciones del menú
            let handle = service.handle();

            // Canal de notificación: history.push() → watcher → handle.update()
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            history.set_notifier(tx);

            // Thread watcher: escucha cambios en el historial y fuerza refresh del menú
            std::thread::spawn(move || {
                while rx.recv().is_ok() {
                    handle.update(|_| {});
                }
            });

            // Ejecutar el loop de eventos de ksni (blocking)
            if let Err(e) = service.run() {
                tracing::error!("Error en servicio de icono de bandeja: {:?}", e);
            }
        })
    }
}

/// Implementación del trait Tray para ksni
struct GDriveXPTray {
    history: ActionHistory,
    sync_paused: Arc<AtomicBool>,
}

impl Tray for GDriveXPTray {
    fn id(&self) -> String {
        "gdrivexp-tray".to_string()
    }

    fn icon_name(&self) -> String {
        "org.gnome.FedoraDrive".to_string()
    }

    fn icon_theme_path(&self) -> String {
        // Ruta adicional para que el host SNI encuentre el icono instalado por install-icons.sh
        dirs::home_dir()
            .map(|h| h.join(".local/share/icons").to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    fn title(&self) -> String {
        "G-DriveXP".to_string()
    }

    fn tool_tip(&self) -> ToolTip {
        let status = if self.sync_paused.load(Ordering::Relaxed) {
            "Sincronización pausada"
        } else {
            "Sincronizando"
        };

        ToolTip {
            icon_name: self.icon_name(),
            icon_pixmap: Vec::new(),
            title: "G-DriveXP".to_string(),
            description: status.to_string(),
        }
    }

    fn menu(&self) -> Vec<MenuItem<Self>> {
        let mut items: Vec<MenuItem<Self>> = Vec::new();

        let progress = self.history.get_sync_progress();
        let active_transfers = self.history.active_transfers();

        let has_pending_downloads = progress.changes_detected != progress.changes_applied;
        let has_pending_uploads = progress.pending_uploads > 0;

        let has_active_real_transfers = active_transfers.iter().any(|t| t.operation != TransferOp::Stream);

        // Determinar estado de sincronización
        if has_active_real_transfers || has_pending_downloads || has_pending_uploads {
            items.push(StandardItem {
                label: "🔄 Syncronizando".to_string(),
                enabled: false,
                ..Default::default()
            }.into());

            if has_pending_downloads {
                items.push(StandardItem {
                    label: format!("Descargas pendientes: {}/{}", progress.changes_applied, progress.changes_detected),
                    enabled: false,
                    ..Default::default()
                }.into());
            }
            if has_pending_uploads {
                items.push(StandardItem {
                    label: format!("Subidas/Cambios pendientes: {}", progress.pending_uploads),
                    enabled: false,
                    ..Default::default()
                }.into());
            }
            let non_stream_transfers: Vec<_> = active_transfers.iter().filter(|t| t.operation != TransferOp::Stream).collect();
            if !non_stream_transfers.is_empty() {
                items.push(StandardItem {
                    label: format!("{} transferencias activas", non_stream_transfers.len()),
                    enabled: false,
                    ..Default::default()
                }.into());
            }
        } else {
            items.push(StandardItem {
                label: "Todo en Orden".to_string(),
                enabled: false,
                ..Default::default()
            }.into());
        }

        items.push(MenuItem::Separator);

        // Abrir panel principal
        items.push(StandardItem {
            label: "Abrir Panel".to_string(),
            activate: Box::new(|_| {
                // Enviar señal D-Bus para activar la ventana GTK
                // Por ahora usamos xdg-open como fallback
                let _ = std::process::Command::new("gdbus")
                    .args([
                        "call", "--session",
                        "--dest", "org.gnome.FedoraDrive",
                        "--object-path", "/org/gnome/FedoraDrive",
                        "--method", "org.gtk.Actions.Activate",
                        "show-window", "[]", "{}"
                    ])
                    .spawn();
            }),
            ..Default::default()
        }.into());

        // Abrir en Archivos
        items.push(StandardItem {
            label: "Abrir en Archivos".to_string(),
            activate: Box::new(|_| {
                let mount_point = dirs::home_dir()
                    .map(|h| h.join("GoogleDrive"))
                    .unwrap_or_else(|| std::path::PathBuf::from("/tmp/GoogleDrive"));
                let _ = std::process::Command::new("xdg-open")
                    .arg(mount_point)
                    .spawn();
            }),
            ..Default::default()
        }.into());

        items.push(MenuItem::Separator);

        // Pausar/Reanudar sincronización
        let is_paused = self.sync_paused.load(Ordering::Relaxed);
        items.push(CheckmarkItem {
            label: "Pausar sincronización".to_string(),
            checked: is_paused,
            activate: Box::new(|this: &mut Self| {
                let current = this.sync_paused.load(Ordering::Relaxed);
                this.sync_paused.store(!current, Ordering::Relaxed);
                if current {
                    tracing::info!("🔄 Sincronización reanudada");
                } else {
                    tracing::info!("⏸️ Sincronización pausada");
                }
            }),
            ..Default::default()
        }.into());

        items.push(MenuItem::Separator);

        // Salir
        items.push(StandardItem {
            label: "Salir".to_string(),
            activate: Box::new(|_| {
                tracing::info!("👋 Cerrando aplicación desde bandeja...");
                // Desmontar FUSE antes de salir para evitar zombie del kernel
                let fuse_path = dirs::home_dir()
                    .map(|h| h.join("GoogleDrive/FUSE_Mount"))
                    .unwrap_or_else(|| std::path::PathBuf::from("/tmp/GoogleDrive/FUSE_Mount"));
                let _ = crate::utils::mount::unmount_and_wait(&fuse_path);
                std::process::exit(0);
            }),
            ..Default::default()
        }.into());

        items
    }
}
