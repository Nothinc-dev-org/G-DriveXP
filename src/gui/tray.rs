//! Icono de bandeja del sistema usando StatusNotifierItem (ksni)
//!
//! Muestra el historial de acciones recientes y permite controlar la aplicación.

use ksni::{menu::*, Tray, TrayService, ToolTip};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use super::history::ActionHistory;

/// Número de entradas recientes a mostrar en el menú
const RECENT_ENTRIES_COUNT: usize = 10;

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
            let service = TrayService::new(GDriveXPTray {
                history: self.history,
                sync_paused: self.sync_paused,
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
        // Usar icono del sistema o uno personalizado
        if self.sync_paused.load(Ordering::Relaxed) {
            "folder-cloud-offline".to_string()
        } else {
            "folder-cloud".to_string()
        }
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

        // Historial reciente
        let recent = self.history.recent(RECENT_ENTRIES_COUNT);
        if !recent.is_empty() {
            for entry in recent {
                items.push(StandardItem {
                    label: entry.format_for_menu(),
                    enabled: false, // Solo informativo
                    ..Default::default()
                }.into());
            }
            items.push(MenuItem::Separator);
        } else {
            items.push(StandardItem {
                label: "Sin actividad reciente".to_string(),
                enabled: false,
                ..Default::default()
            }.into());
            items.push(MenuItem::Separator);
        }

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
                std::process::exit(0);
            }),
            ..Default::default()
        }.into());

        items
    }
}
