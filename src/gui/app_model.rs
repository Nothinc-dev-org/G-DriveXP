use relm4::prelude::*;
use gtk::prelude::*;
use gtk::glib;
use libadwaita as adw;
use adw::prelude::*;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use super::history::{ActionHistory, ActionType};
use super::tray::TrayIcon;

pub struct AppModel {
    pub status_message: String,
    pub is_connected: bool,
    pub mount_point: Option<std::path::PathBuf>,
    pub sync_paused: Arc<AtomicBool>,
    pub history: ActionHistory,
    pub local_dirs: Vec<crate::db::LocalSyncDir>,
    pub db: Option<Arc<crate::db::MetadataRepository>>,
    pub local_sync_sender: Option<crate::sync::local_sync_manager::LocalSyncCommandSender>,
}

#[derive(Debug)]
pub enum AppMsg {
    UpdateStatus(String),
    SetConnected(bool),
    SetMountPoint(std::path::PathBuf),
    SetDatabase(Arc<crate::db::MetadataRepository>),
    OpenInNautilus,
    TogglePauseSync,
    Logout,
    Hide,
    Quit,
    ShowWindow,
    // Mensajes para el historial
    LogAction(ActionType, String),
    // Mensajes para sincronización local
    AddLocalDir,
    RemoveLocalDir(i64),
    ToggleLocalDir(i64, bool),
    RefreshLocalDirs,
    SetLocalDirs(Vec<crate::db::LocalSyncDir>),
    SetLocalSyncSender(crate::sync::local_sync_manager::LocalSyncCommandSender),
}

#[relm4::component(pub)]
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

                            // Sección Sincronización Local
                            append = &adw::PreferencesGroup {
                                set_title: "Sincronización Local",
                                set_description: Some("Sincronice carpetas locales con Google Drive"),

                                #[wrap(Some)]
                                set_header_suffix = &gtk::Button {
                                    set_icon_name: "list-add-symbolic",
                                    set_valign: gtk::Align::Center,
                                    add_css_class: "flat",
                                    set_tooltip_text: Some("Añadir directorio"),

                                    connect_clicked[sender] => move |_| {
                                        sender.input(AppMsg::AddLocalDir);
                                    },
                                },

                                // Mostrar lista de directorios configurados
                                add = &adw::ActionRow {
                                    #[watch]
                                    set_title: &if model.local_dirs.is_empty() {
                                        "No hay directorios configurados".to_string()
                                    } else {
                                        format!("{} directorio(s) configurado(s)", model.local_dirs.len())
                                    },
                                    
                                    #[watch]
                                    set_subtitle: &model.local_dirs.iter()
                                        .map(|d| d.local_path.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", "),

                                    add_prefix = &gtk::Image {
                                        set_icon_name: Some("folder-symbolic"),
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
            mount_point: None,
            sync_paused: sync_paused.clone(),
            history: history.clone(),
            local_dirs: Vec::new(),
            db: None,
            local_sync_sender: None,
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

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>, root: &Self::Root) {
        match msg {
            AppMsg::UpdateStatus(msg) => {
                self.status_message = msg;
            }
            AppMsg::SetConnected(connected) => {
                self.is_connected = connected;
            }
            AppMsg::SetMountPoint(path) => {
                self.mount_point = Some(path);
            }
            AppMsg::OpenInNautilus => {
                if let Some(ref mount_point) = self.mount_point {
                    let _ = std::process::Command::new("xdg-open")
                        .arg(mount_point)
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
                if let Some(ref mount_point) = self.mount_point {
                    let _ = crate::utils::mount::unmount(mount_point);
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
                // Cargar directorios al recibir la base de datos
                sender.input(AppMsg::RefreshLocalDirs);
            }
            AppMsg::AddLocalDir => {
                #[allow(deprecated)]
                let dialog = gtk::FileChooserDialog::new(
                    Some("Seleccionar directorio"),
                    gtk::Window::NONE,
                    gtk::FileChooserAction::SelectFolder,
                    &[("Cancelar", gtk::ResponseType::Cancel), ("Añadir", gtk::ResponseType::Accept)],
                );

                let sender_clone = sender.clone();
                let db_clone = self.db.clone();
                let local_sync_sender_clone = self.local_sync_sender.clone();
                let existing_dirs = self.local_dirs.iter()
                    .map(|d| std::path::PathBuf::from(&d.local_path))
                    .collect::<Vec<_>>();

                #[allow(deprecated)]
                dialog.connect_response(move |dialog, response| {
                    if response == gtk::ResponseType::Accept {
                        #[allow(deprecated)]
                        if let Some(file) = dialog.file() {
                            if let Some(path) = file.path() {
                                // Validar anidamiento
                                if let Err(e) = validate_local_dir(&path, &existing_dirs) {
                                    tracing::error!("Validación falló: {}", e);
                                    return;
                                }

                                // Añadir a la base de datos (async)
                                if let Some(db) = &db_clone {
                                    let db_clone2 = db.clone();
                                    let sender_clone2 = sender_clone.clone();
                                    let path_clone = path.clone();
                                    let local_sync_sender_inner = local_sync_sender_clone.clone();
                                    
                                    glib::spawn_future_local(async move {
                                        match db_clone2.add_local_sync_dir(&path_clone).await {
                                            Ok(id) => {
                                                tracing::info!("Directorio añadido: {} (id={})", path_clone.display(), id);
                                                
                                                // Disparar inicialización en GDrive mediante el LocalSyncManager
                                                if let Some(sync_sender) = &local_sync_sender_inner {
                                                    let cmd = crate::sync::local_sync_manager::LocalSyncCommand::InitializeFolder {
                                                        sync_dir_id: id,
                                                        local_path: path_clone.clone(),
                                                    };
                                                    if let Err(e) = sync_sender.send(cmd).await {
                                                        tracing::error!("Error enviando comando de inicialización: {:?}", e);
                                                    }
                                                }
                                                
                                                sender_clone2.input(AppMsg::RefreshLocalDirs);
                                            }
                                            Err(e) => {
                                                tracing::error!("Error al añadir directorio: {:?}", e);
                                            }
                                        }
                                    });
                                }
                            }
                        }
                    }
                    dialog.close();
                });

                dialog.set_modal(true);
                dialog.set_visible(true);
            }
            AppMsg::RemoveLocalDir(id) => {
                let dir = self.local_dirs.iter().find(|d| d.id == id);
                if let Some(dir) = dir {
                    let dialog = adw::AlertDialog::new(
                        Some("¿Eliminar directorio?"),
                        Some(&format!(
                            "¿Desea eliminar '{}' de la sincronización?\n\nEsto NO eliminará los archivos locales.",
                            dir.local_path
                        )),
                    );
                    dialog.add_response("cancel", "Cancelar");
                    dialog.add_response("remove", "Eliminar");
                    dialog.set_response_appearance("remove", adw::ResponseAppearance::Destructive);
                    dialog.set_default_response(Some("cancel"));

                    let sender_clone = sender.clone();
                    let db_clone = self.db.clone();

                    dialog.connect_response(None, move |_, response| {
                        if response == "remove" {
                            if let Some(db) = &db_clone {
                                let db_clone2 = db.clone();
                                let sender_clone2 = sender_clone.clone();

                                glib::spawn_future_local(async move {
                                    match db_clone2.remove_local_sync_dir(id).await {
                                        Ok(()) => {
                                            sender_clone2.input(AppMsg::RefreshLocalDirs);
                                        }
                                        Err(e) => {
                                            tracing::error!("Error al eliminar directorio: {:?}", e);
                                        }
                                    }
                                });
                            }
                        }
                    });

                   dialog.present(Some(root));
                }
            }
            AppMsg::ToggleLocalDir(id, enabled) => {
                if let Some(db) = &self.db {
                    let db_clone = db.clone();
                    let sender_clone = sender.clone();

                    glib::spawn_future_local(async move {
                        match db_clone.toggle_local_sync_dir(id, enabled).await {
                            Ok(()) => {
                                sender_clone.input(AppMsg::RefreshLocalDirs);
                            }
                            Err(e) => {
                                tracing::error!("Error al toggle directorio: {:?}", e);
                            }
                        }
                    });
                }
            }
            AppMsg::RefreshLocalDirs => {
                if let Some(db) = &self.db {
                    let db_clone = db.clone();
                    let sender_clone = sender.clone();

                    glib::spawn_future_local(async move {
                        match db_clone.get_local_sync_dirs().await {
                            Ok(dirs) => {
                                tracing::debug!("Directorios locales: {}", dirs.len());
                                sender_clone.input(AppMsg::SetLocalDirs(dirs));
                            }
                            Err(e) => {
                                tracing::error!("Error al cargar directorios: {:?}", e);
                            }
                        }
                    });
                }
            }
            AppMsg::SetLocalDirs(dirs) => {
                tracing::info!("📋 Actualizando lista de directorios: {} elementos", dirs.len());
                self.local_dirs = dirs;
            }
            AppMsg::SetLocalSyncSender(sync_sender) => {
                tracing::info!("🔗 LocalSyncSender configurado");
                self.local_sync_sender = Some(sync_sender);
            }
            AppMsg::Quit => {
                tracing::info!("Cerrando aplicación...");
                
                // Intentar desmontar si tenemos el mount point
                if let Some(ref mount_point) = self.mount_point {
                    if let Err(e) = crate::utils::mount::unmount(mount_point) {
                        tracing::error!("Error al desmontar en cierre: {:?}", e);
                    }
                }
                
                std::process::exit(0);
            }
        }
    }
}

/// Valida que un directorio nuevo no esté anidado con los existentes
fn validate_local_dir(
    new_path: &std::path::Path,
    existing: &[std::path::PathBuf],
) -> Result<(), String> {
    // 1. Verificar que no sea subdirectorio de uno existente
    for existing_path in existing {
        if new_path.starts_with(existing_path) {
            return Err(format!(
                "'{}' ya está contenido en '{}'",
                new_path.display(),
                existing_path.display()
            ));
        }
        // 2. Verificar que ninguno existente sea subdirectorio del nuevo
        if existing_path.starts_with(new_path) {
            return Err(format!(
                "'{}' contiene a '{}' que ya está sincronizado",
                new_path.display(),
                existing_path.display()
            ));
        }
    }
    Ok(())
}
