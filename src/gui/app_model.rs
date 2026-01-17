use relm4::prelude::*;
use gtk::prelude::*;
use libadwaita as adw;
use adw::prelude::*;

pub struct AppModel {
    pub status_message: String,
    pub is_connected: bool,
    pub mount_point: Option<std::path::PathBuf>,
}

#[derive(Debug)]
pub enum AppMsg {
    UpdateStatus(String),
    SetConnected(bool),
    SetMountPoint(std::path::PathBuf),
    Quit,
}

#[relm4::component(pub)]
impl SimpleComponent for AppModel {
    type Init = ();
    type Input = AppMsg;
    type Output = ();

    view! {
        adw::ApplicationWindow {
            set_title: Some("FedoraDrive"),
            set_default_size: (800, 450),

            #[wrap(Some)]
            set_content = &gtk::Box {
                set_orientation: gtk::Orientation::Vertical,

                append = &adw::HeaderBar {},

                append = &gtk::Box {
                    set_orientation: gtk::Orientation::Vertical,
                    set_spacing: 24,
                    set_margin_all: 32,
                    set_valign: gtk::Align::Center,
                    set_vexpand: true,

                    append = &gtk::Label {
                        #[watch]
                        set_label: &model.status_message,
                        add_css_class: "title-1",
                    },

                    append = &gtk::Box {
                        set_orientation: gtk::Orientation::Horizontal,
                        set_spacing: 8,
                        set_halign: gtk::Align::Center,

                        append = &gtk::Label {
                            #[watch]
                            set_label: if model.is_connected { "🟢" } else { "🔴" },
                        },

                        append = &gtk::Label {
                            #[watch]
                            set_label: if model.is_connected { "Conectado" } else { "Desconectado" },
                            add_css_class: "caption",
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

        let model = AppModel {
            status_message: "Iniciando FedoraDrive...".to_string(),
            is_connected: false,
            mount_point: None,
        };

        // Spawnear el backend en un hilo separado
        let sender_clone = sender.clone();
        std::thread::spawn(move || {
            if let Err(e) = crate::run_backend(sender_clone) {
                tracing::error!("Error en el backend: {:?}", e);
            }
        });

        let widgets = view_output!();
        
        // Configurar manejador de cierre de ventana
        let sender_clone = sender.clone();
        root.connect_close_request(move |_| {
            sender_clone.input(AppMsg::Quit);
            gtk::glib::Propagation::Proceed
        });

        ComponentParts { model, widgets }
    }

    fn update(&mut self, msg: Self::Input, _sender: ComponentSender<Self>) {
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
