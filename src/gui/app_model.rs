use relm4::prelude::*;
use gtk::prelude::*;
use libadwaita as adw;
use adw::prelude::*;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use super::history::{ActionHistory, ActionType, ActionEntry, ActiveTransfer};
use super::tray::TrayIcon;

pub struct AppModel {
    pub status_message: String,
    pub is_connected: bool,
    pub mirror_path: Option<std::path::PathBuf>,
    pub fuse_mount_path: Option<std::path::PathBuf>,
    pub sync_paused: Arc<AtomicBool>,
    pub history: ActionHistory,
    pub db: Option<Arc<crate::db::MetadataRepository>>,
    pub login_url: Option<String>,
    // Actividad reciente
    pub activity_entries: Vec<ActionEntry>,
    pub active_transfers: Vec<ActiveTransfer>,
    pub sync_detected: usize,
    pub sync_applied: usize,
    pub pending_uploads: usize,
    // Referencias a widgets dinámicos
    pub transfers_listbox: Option<gtk::ListBox>,
    pub history_listbox: Option<gtk::ListBox>,
    // Navegación
    pub current_view: ViewMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewMode {
    Main,
    Activity,
}

impl AppModel {
    fn sync_hint_text(&self) -> String {
        let has_pending_downloads = self.sync_detected != self.sync_applied;
        let has_pending_uploads = self.pending_uploads > 0;
        
        if !self.active_transfers.is_empty() || has_pending_downloads || has_pending_uploads {
            if has_pending_downloads {
                format!("{}/{} Cambios aplicados", self.sync_applied, self.sync_detected)
            } else if has_pending_uploads {
                format!("{} Cambios pendientes", self.pending_uploads)
            } else {
                "Sincronizando...".to_string()
            }
        } else {
            "Todo Sincronizado".to_string()
        }
    }

    /// Reconstruye el contenido del listbox de transfers activos
    fn rebuild_transfers_box(transfers_box: &gtk::ListBox, transfers: &[ActiveTransfer]) {
        // Limpiar
        while let Some(child) = transfers_box.first_child() {
            transfers_box.remove(&child);
        }

        for transfer in transfers {
            let row = gtk::Box::new(gtk::Orientation::Vertical, 4);
            row.set_margin_top(4);
            row.set_margin_bottom(4);
            row.set_margin_start(8);
            row.set_margin_end(8);

            let label = gtk::Label::new(Some(&format!(
                "{} {}",
                transfer.operation.emoji(),
                transfer.file_name
            )));
            label.set_halign(gtk::Align::Start);
            label.set_ellipsize(gtk::pango::EllipsizeMode::Middle);
            row.append(&label);

            let progress = gtk::ProgressBar::new();
            progress.set_fraction(transfer.progress_fraction());
            if transfer.total_bytes > 0 {
                let mb_done = transfer.bytes_transferred as f64 / (1024.0 * 1024.0);
                let mb_total = transfer.total_bytes as f64 / (1024.0 * 1024.0);
                progress.set_text(Some(&format!("{:.1}/{:.1} MB", mb_done, mb_total)));
                progress.set_show_text(true);
            }
            row.append(&progress);

            transfers_box.append(&row);
        }
    }

    /// Reconstruye el contenido del listbox de historial
    fn rebuild_history_listbox(history_listbox: &gtk::ListBox, entries: &[ActionEntry]) {
        // Limpiar
        while let Some(child) = history_listbox.first_child() {
            history_listbox.remove(&child);
        }

        if entries.is_empty() {
            let label = gtk::Label::new(Some("Sin actividad reciente"));
            label.set_css_classes(&["dim-label"]);
            label.set_margin_top(8);
            label.set_margin_bottom(8);
            history_listbox.append(&label);
            return;
        }

        for entry in entries {
            let label = gtk::Label::new(Some(&entry.format_for_menu()));
            label.set_halign(gtk::Align::Start);
            label.set_margin_top(2);
            label.set_margin_bottom(2);
            label.set_margin_start(8);
            label.set_margin_end(8);
            label.set_ellipsize(gtk::pango::EllipsizeMode::End);
            history_listbox.append(&label);
        }
    }
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
    HardReset,
    Login,
    SetLoginUrl(String),
    // Refresco periódico de actividad
    RefreshActivity,
    // Navegación
    ShowActivityView,
    ShowMainView,
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

                // Header con navegación
                append = &adw::HeaderBar {
                    pack_start = &gtk::Button {
                        set_icon_name: "go-previous-symbolic",
                        #[watch]
                        set_visible: model.current_view == ViewMode::Activity,
                        connect_clicked[sender] => move |_| {
                            sender.input(AppMsg::ShowMainView);
                        },
                    },

                    #[wrap(Some)]
                    set_title_widget = &gtk::Box {
                        set_orientation: gtk::Orientation::Horizontal,
                        set_spacing: 12,
                        set_halign: gtk::Align::Center,

                        #[name = "logo_image"]
                        append = &gtk::Image {
                            set_pixel_size: 32,
                            #[watch]
                            set_visible: model.current_view == ViewMode::Main,
                        },

                        append = &adw::WindowTitle {
                            set_title: "G-DriveXP",
                            #[watch]
                            set_subtitle: match model.current_view {
                                ViewMode::Main => "Cliente de Google Drive",
                                ViewMode::Activity => "Actividad Reciente",
                            },
                        },
                    },
                },

                // Stack para alternar vistas
                #[name = "main_stack"]
                append = &gtk::Stack {
                    set_vexpand: true,
                    set_transition_type: gtk::StackTransitionType::SlideLeftRight,
                    #[watch]
                    set_visible_child_name: match model.current_view {
                        ViewMode::Main => "main",
                        ViewMode::Activity => "activity",
                    },

                    // ========== VISTA PRINCIPAL ==========
                    add_named[Some("main")] = &gtk::ScrolledWindow {
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
                                    #[watch]
                                    set_visible: model.is_connected,
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
                                    #[watch]
                                    set_visible: model.is_connected,
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

                                // Botón para ver actividad reciente
                                append = &adw::PreferencesGroup {
                                    #[watch]
                                    set_visible: model.is_connected,

                                    add = &adw::ActionRow {
                                        #[watch]
                                        set_title: &model.sync_hint_text(),
                                        set_subtitle: "Actividad Reciente",
                                        set_activatable: true,

                                        add_prefix = &gtk::Image {
                                            #[watch]
                                            set_icon_name: Some(
                                                if !model.active_transfers.is_empty() || model.sync_detected != model.sync_applied {
                                                    "emblem-synchronizing-symbolic"
                                                } else {
                                                    "emblem-ok-symbolic"
                                                }
                                            ),
                                            #[watch]
                                            set_css_classes: if !model.active_transfers.is_empty() || model.sync_detected != model.sync_applied {
                                                &["accent"]
                                            } else {
                                                &["success"]
                                            },
                                        },

                                        add_suffix = &gtk::Image {
                                            set_icon_name: Some("go-next-symbolic"),
                                        },

                                        connect_activated[sender] => move |_| {
                                            sender.input(AppMsg::ShowActivityView);
                                        },
                                    },
                                },

                                // Sección Configuración
                                append = &adw::PreferencesGroup {
                                    #[watch]
                                    set_visible: model.is_connected,
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
                                    #[watch]
                                    set_visible: model.is_connected,
                                    set_title: "Cuenta",

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

                                    add = &adw::ActionRow {
                                        set_title: "Hard Reset",
                                        set_subtitle: "BORRADO TOTAL: Reinicia DB, Cache y Archivos.",
                                        set_activatable: true,
                                        set_css_classes: &["destructive-action"],

                                        add_suffix = &gtk::Image {
                                            set_icon_name: Some("user-trash-symbolic"),
                                        },

                                        connect_activated[sender] => move |_| {
                                            sender.input(AppMsg::HardReset);
                                        },
                                    },
                                },

                                // PANTALLA DE LOGIN (SOLO SI NO ESTÁ CONECTADO)
                                append = &gtk::Box {
                                    set_orientation: gtk::Orientation::Vertical,
                                    set_valign: gtk::Align::Center,
                                    set_halign: gtk::Align::Center,
                                    set_spacing: 16,
                                    set_margin_top: 40,
                                    #[watch]
                                    set_visible: !model.is_connected,

                                    append = &gtk::Image {
                                        set_pixel_size: 96,
                                        set_icon_name: Some("avatar-default-symbolic"),
                                        set_css_classes: &["dim-label"],
                                    },

                                    append = &gtk::Label {
                                        set_label: "Inicie sesión para continuar",
                                        set_css_classes: &["title-1"],
                                    },

                                    append = &gtk::Button {
                                        #[watch]
                                        set_label: if model.login_url.is_some() { "Iniciar Sesión" } else { "Generando enlace..." },
                                        #[watch]
                                        set_sensitive: model.login_url.is_some(),
                                        set_css_classes: &["suggested-action", "pill"],
                                        set_margin_top: 16,
                                        connect_clicked[sender] => move |_| {
                                            sender.input(AppMsg::Login);
                                        },
                                    },
                                },
                            },
                        },
                    } -> {
                        set_name: "main",
                    },

                    // ========== VISTA DE ACTIVIDAD ==========
                    add_named[Some("activity")] = &gtk::Box {
                        set_orientation: gtk::Orientation::Vertical,
                        set_spacing: 16,
                        set_margin_all: 16,

                        // Transfers activos (barras de progreso)
                        #[name = "transfers_box"]
                        append = &gtk::ListBox {
                            set_css_classes: &["boxed-list"],
                            set_selection_mode: gtk::SelectionMode::None,
                            #[watch]
                            set_visible: !model.active_transfers.is_empty(),
                        },

                        // Label "Historial"
                        append = &gtk::Label {
                            set_label: "Historial",
                            set_halign: gtk::Align::Start,
                            set_css_classes: &["heading"],
                            #[watch]
                            set_visible: !model.active_transfers.is_empty(),
                        },

                        // Historial scrolleable (pantalla completa)
                        append = &gtk::ScrolledWindow {
                            set_vexpand: true,
                            set_hscrollbar_policy: gtk::PolicyType::Never,
                            set_propagate_natural_height: false,

                            #[wrap(Some)]
                            #[name = "history_listbox"]
                            set_child = &gtk::ListBox {
                                set_css_classes: &["boxed-list"],
                                set_selection_mode: gtk::SelectionMode::None,
                            },
                        },
                    } -> {
                        set_name: "activity",
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

        let mut model = AppModel {
            status_message: "Iniciando G-DriveXP...".to_string(),
            is_connected: false,
            mirror_path: None,
            fuse_mount_path: None,
            sync_paused: sync_paused.clone(),
            history: history.clone(),
            db: None,
            login_url: None,
            activity_entries: Vec::new(),
            active_transfers: Vec::new(),
            sync_detected: 0,
            sync_applied: 0,
            pending_uploads: 0,
            transfers_listbox: None,
            history_listbox: None,
            current_view: ViewMode::Main,
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

        // Timer de refresco de actividad cada 2 segundos
        let sender_timer = sender.clone();
        gtk::glib::timeout_add_local(std::time::Duration::from_secs(2), move || {
            sender_timer.input(AppMsg::RefreshActivity);
            gtk::glib::ControlFlow::Continue
        });

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

        // Guardar referencias a widgets dinámicos en el model
        model.transfers_listbox = Some(widgets.transfers_box.clone());
        model.history_listbox = Some(widgets.history_listbox.clone());

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
                    tracing::info!("Sincronización reanudada");
                    self.history.log(ActionType::Sync, "Sincronización reanudada");
                } else {
                    tracing::info!("Sincronización pausada");
                    self.history.log(ActionType::Sync, "Sincronización pausada");
                }
            }
            AppMsg::Logout => {
                tracing::info!("Cerrando sesión...");

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
            AppMsg::HardReset => {
                tracing::warn!("Ejecutando Hard Reset y reiniciando...");

                // 1. Programar el reinicio (delayed)
                // Usamos sh para dormir un poco y permitir que esta instancia se cierre y libere recursos
                if let Ok(exe) = std::env::current_exe() {
                    let _ = std::process::Command::new("sh")
                        .arg("-c")
                        .arg(format!("sleep 3; {:?} &", exe)) // 3 segundos para asegurar limpieza
                        .spawn();
                }

                // 2. Ejecutar limpieza
                if let Err(e) = crate::utils::cleanup::perform_hard_reset() {
                    tracing::error!("Error durante el Hard Reset: {:?}", e);
                }

                // 3. Salir de la aplicación forzosamente
                std::process::exit(0);
            }
            AppMsg::Login => {
                if let Some(ref url) = self.login_url {
                    tracing::info!("[System] Abriendo navegador para login: {}", url);
                    let _ = std::process::Command::new("xdg-open")
                        .arg(url)
                        .spawn();
                } else {
                    tracing::warn!("[System] Intento de login sin URL disponible. Por favor espere.");
                }
            }
            AppMsg::SetLoginUrl(url) => {
                tracing::info!("URL de login recibida: {}", url);
                self.login_url = Some(url);
            }
            AppMsg::RefreshActivity => {
                // Leer datos del historial compartido
                self.activity_entries = self.history.recent(20);
                self.active_transfers = self.history.active_transfers();
                let progress = self.history.get_sync_progress();
                self.sync_detected = progress.changes_detected;
                self.sync_applied = progress.changes_applied;
                self.pending_uploads = progress.pending_uploads;

                // Rebuild imperativo de los listbox dinámicos
                if let Some(ref transfers_box) = self.transfers_listbox {
                    Self::rebuild_transfers_box(transfers_box, &self.active_transfers);
                }
                if let Some(ref history_box) = self.history_listbox {
                    Self::rebuild_history_listbox(history_box, &self.activity_entries);
                }
            }
            AppMsg::ShowActivityView => {
                self.current_view = ViewMode::Activity;
            }
            AppMsg::ShowMainView => {
                self.current_view = ViewMode::Main;
            }
        }
    }
}
