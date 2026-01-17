use relm4::prelude::*;
use gtk::prelude::*;
use libadwaita as adw;
use adw::prelude::*;

pub struct AppModel {
    pub status_message: String,
    pub is_connected: bool,
}

#[derive(Debug)]
pub enum AppMsg {
    UpdateStatus(String),
    SetConnected(bool),
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
        };

        // Spawnear el backend en un hilo separado
        let sender_clone = sender.clone();
        std::thread::spawn(move || {
            if let Err(e) = crate::run_backend(sender_clone) {
                tracing::error!("Error en el backend: {:?}", e);
            }
        });

        let widgets = view_output!();

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
            AppMsg::Quit => {
                // TODO: Graceful shutdown logic
                std::process::exit(0);
            }
        }
    }
}
