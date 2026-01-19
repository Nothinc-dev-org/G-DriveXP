use relm4::prelude::*;
use gtk::prelude::*;
use libadwaita as adw;
use adw::prelude::*;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use super::history::{ActionHistory, ActionType};
use super::tray::TrayIcon;

pub struct AppModel {
    pub status_message: String,
    pub is_connected: bool,
    pub mirror_path: Option<std::path::PathBuf>,
    pub fuse_mount_path: Option<std::path::PathBuf>,
    pub sync_paused: Arc<AtomicBool>,
    pub history: ActionHistory,
    pub db: Option<Arc<crate::db::MetadataRepository>>,
}

#[derive(Debug)]
pub enum AppMsg {
    UpdateStatus(String),
    SetConnected(bool),
    SetPaths { mirror: std::path::PathBuf, fuse: std::path::PathBuf },
    SetDatabase(Arc<crate::db::MetadataRepository>),
    OpenInNautilus,
    TogglePauseSync,
    Logout,
    Hide,
    Quit,
    ShowWindow,
    // Mensajes para el historial
    LogAction(ActionType, String),
}

#[relm4::component(pub)]
#[allow(unused_assignments)]
impl Component for AppModel {
    type Init = ();
    type Input = AppMsg;
    type Output = ();
    type CommandOutput = ();

    view! {
        adw::ApplicationWindow {
            set_title: Some("G-DriveXP"),
            set_default_size: (450, 560),

            #[wrap(Some)]
            set_content = &gtk::Box {
                set_orientation: gtk::Orientation::Vertical,

                append = &adw::HeaderBar {
                    #[wrap(Some)]
                    set_title_widget = &gtk::Box {
                        set_orientation: gtk::Orientation::Horizontal,
                        set_spacing: 12,
                        set_halign: gtk::Align::Center,

                        #[name = "logo_image"]
                        append = &gtk::Image {
                            set_pixel_size: 32,
                        },

                        append = &adw::WindowTitle {
                            set_title: "G-DriveXP",
                            set_subtitle: "Cliente de Google Drive",
                        },
                    },
                },

                append = &gtk::ScrolledWindow {
                    set_vexpand: true,
                    set_hscrollbar_policy: gtk::PolicyType::Never,

                    #[wrap(Some)]
                    set_child = &adw::Clamp {
                        set_maximum_size: 600,
                        set_margin_all: 16,

                        #[wrap(Some)]
                        set_child = &gtk::Box {
                            set_orientation: gtk::Orientation::Vertical,
                            set_spacing: 24,

                            // Botón principal: Abrir en Archivos
                            append = &gtk::Button {
                                set_css_classes: &["suggested-action", "pill"],
                                set_halign: gtk::Align::Center,
                                set_margin_top: 8,
                                set_margin_bottom: 16,

                                #[wrap(Some)]
                                set_child = &gtk::Box {
                                    set_orientation: gtk::Orientation::Horizontal,
                                    set_spacing: 8,

                                    append = &gtk::Image {
                                        set_icon_name: Some("folder-open-symbolic"),
                                    },

                                    append = &gtk::Label {
                                        set_label: "Abrir en Archivos",
                                    },
                                },

                                connect_clicked[sender] => move |_| {
                                    sender.input(AppMsg::OpenInNautilus);
                                },
                            },

                            // Estado actual
                            append = &adw::PreferencesGroup {
                                set_title: "Estado",

                                add = &adw::ActionRow {
                                    set_title: "Conexión",
                                    #[watch]
                                    set_subtitle: if model.is_connected { "Conectado a Google Drive" } else { "Desconectado" },

                                    add_suffix = &gtk::Image {
                                        #[watch]
                                        set_icon_name: Some(if model.is_connected { "emblem-ok-symbolic" } else { "dialog-error-symbolic" }),
                                        #[watch]
                                        set_css_classes: if model.is_connected { &["success"] } else { &["error"] },
                                    },
                                },

                                add = &adw::ActionRow {
                                    set_title: "Estado",
                                    #[watch]
                                    set_subtitle: &model.status_message,
                                },
                            },

                            // Sección Configuración
                            append = &adw::PreferencesGroup {
                                set_title: "Configuración",

                                add = &adw::SwitchRow {
                                    set_title: "Pausar sincronización",
                                    set_subtitle: "Detiene temporalmente la sincronización",
                                    #[watch]
                                    set_active: model.sync_paused.load(Ordering::Relaxed),

                                    connect_active_notify[sender] => move |_| {
                                        sender.input(AppMsg::TogglePauseSync);
                                    },
                                },
                            },

                            // Sección Cuenta
                            append = &adw::PreferencesGroup {
                                set_title: "Account",

                                add = &adw::ActionRow {
                                    set_title: "Cerrar sesión",
                                    set_subtitle: "Desvincula esta cuenta de Google",
                                    set_activatable: true,

                                    add_suffix = &gtk::Image {
                                        set_icon_name: Some("system-log-out-symbolic"),
                                    },

                                    connect_activated[sender] => move |_| {
                                        sender.input(AppMsg::Logout);
                                    },
                                },
                            },
                        },
                    },
                },
            }
        }
    }

    fn init(
        _init: Self::Init,
        root: Self::Root,
        sender: ComponentSender<Self>,
    ) -> ComponentParts<Self> {
        // Forzar tema oscuro
        adw::StyleManager::default().set_color_scheme(adw::ColorScheme::ForceDark);

        let sync_paused = Arc::new(AtomicBool::new(false));
        let history = ActionHistory::new();

        let model = AppModel {
            status_message: "Iniciando G-DriveXP...".to_string(),
            is_connected: false,
            mirror_path: None,
            fuse_mount_path: None,
            sync_paused: sync_paused.clone(),
            history: history.clone(),
            db: None,
        };

        // Iniciar icono de bandeja
        let tray = TrayIcon::new(history.clone(), sync_paused.clone());
        let _tray_handle = tray.spawn();

        // Registrar acción para mostrar ventana desde el tray (D-Bus)
        let app = relm4::main_application();
        let sender_show = sender.clone();
        let show_action: gtk::gio::SimpleAction = gtk::gio::SimpleAction::new("show-window", None);
        show_action.connect_activate(move |_, _| {
            sender_show.input(AppMsg::ShowWindow);
        });
        app.add_action(&show_action);

        // Spawnear el backend en un hilo separado
        let sender_clone = sender.clone();
        let history_clone = history.clone();
        let sync_paused_clone = sync_paused.clone();
        std::thread::spawn(move || {
            if let Err(e) = crate::run_backend(sender_clone, history_clone, sync_paused_clone) {
                tracing::error!("Error en el backend: {:?}", e);
            }
        });

        let widgets = view_output!();

        // Cargar logo embebido y asignarlo al widget
        let logo_bytes = include_bytes!("../../assets/logo.png");
        if let Ok(pixbuf) = gtk::gdk_pixbuf::Pixbuf::from_read(std::io::Cursor::new(logo_bytes)) {
            let texture = gtk::gdk::Texture::for_pixbuf(&pixbuf);
            widgets.logo_image.set_paintable(Some(&texture));
        } else {
            widgets.logo_image.set_icon_name(Some("drive-harddisk-symbolic"));
        }
        
        // Configurar manejador de cierre de ventana: Ocultar en lugar de Cerrar
        let sender_clone = sender.clone();
        root.connect_close_request(move |window| {
            window.set_visible(false);
            sender_clone.input(AppMsg::Hide);
            gtk::glib::Propagation::Stop // Detener propagación para que no se destruya la ventana
        });

        ComponentParts { model, widgets }
    }

    fn update(&mut self, msg: Self::Input, _sender: ComponentSender<Self>, root: &Self::Root) {
        match msg {
            AppMsg::UpdateStatus(msg) => {
                self.status_message = msg;
            }
            AppMsg::SetConnected(connected) => {
                self.is_connected = connected;
            }
            AppMsg::SetPaths { mirror, fuse } => {
                self.mirror_path = Some(mirror);
                self.fuse_mount_path = Some(fuse);
            }
            AppMsg::OpenInNautilus => {
                if let Some(ref path) = self.mirror_path {
                    let _ = std::process::Command::new("xdg-open")
                        .arg(path)
                        .spawn();
                } else {
                    // Fallback al directorio por defecto
                    let default = dirs::home_dir()
                        .map(|h| h.join("GoogleDrive"))
                        .unwrap_or_else(|| std::path::PathBuf::from("/tmp/GoogleDrive"));
                    let _ = std::process::Command::new("xdg-open")
                        .arg(default)
                        .spawn();
                }
            }
            AppMsg::TogglePauseSync => {
                let current = self.sync_paused.load(Ordering::Relaxed);
                self.sync_paused.store(!current, Ordering::Relaxed);
                if current {
                    tracing::info!("🔄 Sincronización reanudada");
                    self.history.log(ActionType::Sync, "Sincronización reanudada");
                } else {
                    tracing::info!("⏸️ Sincronización pausada");
                    self.history.log(ActionType::Sync, "Sincronización pausada");
                }
            }
            AppMsg::Logout => {
                tracing::info!("🚪 Cerrando sesión...");
                
                // Limpiar todos los datos de autenticación
                if let Err(e) = crate::auth::clear_all_auth_data() {
                    tracing::error!("Error al limpiar datos de autenticación: {:?}", e);
                }
                
                // Desmontar el filesystem FUSE
                if let Some(ref path) = self.fuse_mount_path {
                    let _ = crate::utils::mount::unmount(path);
                }
                
                // Terminar la aplicación
                std::process::exit(0);
            }
            AppMsg::LogAction(action_type, description) => {
                self.history.log(action_type, description);
            }
            AppMsg::Hide => {
                tracing::info!("Ventana oculta, la aplicación sigue en background...");
            }
            AppMsg::ShowWindow => {
                root.present();
            }
            AppMsg::SetDatabase(db) => {
                self.db = Some(db);
            }
            AppMsg::Quit => {
                tracing::info!("Cerrando aplicación...");
                
                // Intentar desmontar si tenemos el mount point
                if let Some(ref path) = self.fuse_mount_path {
                    if let Err(e) = crate::utils::mount::unmount(path) {
                        tracing::error!("Error al desmontar en cierre: {:?}", e);
                    }
                }
                
                std::process::exit(0);
            }
        }
    }
}


