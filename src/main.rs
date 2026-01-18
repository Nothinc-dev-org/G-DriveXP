mod auth;
mod config;
mod db;
mod fuse;
mod gdrive;
mod sync;
mod gui;
mod ipc;
mod utils;

use anyhow::{Context, Result};
use fuse3::MountOptions;
use fuse3::raw::Session;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use relm4::{RelmApp, ComponentSender};

use config::Config;
use fuse::GDriveFS;

fn main() -> Result<()> {
    // Inicializar sistema de logging
    init_logging()?;
    
    tracing::info!("🚀 Iniciando FedoraDrive-rs v{}", env!("CARGO_PKG_VERSION"));

    // Iniciar la aplicación Relm4
    tracing::info!("🖥️ Iniciando interfaz gráfica...");
    let app = RelmApp::new("org.gnome.FedoraDrive");
    app.run::<gui::app_model::AppModel>(());

    Ok(())
}

/// Ejecuta toda la lógica de backend (asíncrona)
pub fn run_backend(
    ui_sender: ComponentSender<gui::app_model::AppModel>,
    history: gui::history::ActionHistory,
    sync_paused: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Inicializando backend...".to_string()));
    // Crear runtime de Tokio
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Error al construir Tokio Runtime")?;

    rt.block_on(async {
        // Cargar o crear configuración
        let config = Config::load().unwrap_or_else(|_| {
            tracing::warn!("No se pudo cargar configuración, usando valores predeterminados");
            Config::default().expect("Error al crear configuración predeterminada")
        });
        
        // Crear directorios necesarios
        config
            .ensure_directories()
            .context("Error al crear directorios de configuración")?;
        
        // Guardar configuración
        config.save().context("Error al guardar configuración")?;
        
        tracing::info!("Punto de montaje: {:?}", config.mount_point);
        tracing::info!("Directorio de caché: {:?}", config.cache_dir);
        tracing::info!("Base de datos: {:?}", config.db_path);
        
        // Fase 1: Autenticación OAuth2
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Verificando autenticación...".to_string()));
        
        // Buscar archivo de credenciales
        let cred_path = "credentials.json";
        if !std::path::Path::new(cred_path).exists() {
            tracing::error!("No se encontró el archivo '{}'. Por favor siga las instrucciones de instalación.", cred_path);
            ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Error: credentials.json no encontrado".to_string()));
            anyhow::bail!("Archivo de credenciales no encontrado");
        }

        let oauth_manager = auth::OAuth2Manager::new_from_file(cred_path)
            .await
            .context("Error al inicializar gestor OAuth2")?;

        tracing::info!("Verificando estado de autenticación (esto puede abrir su navegador)...");
        oauth_manager.authenticate()
            .await
            .context("Fallo crítico en autenticación")?;
            
        tracing::info!("✅ Autenticación correcta");
        ui_sender.input(gui::app_model::AppMsg::SetConnected(true));
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Autenticación correcta".to_string()));
        
        // Inicializar base de datos SQLite
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Cargando base de datos...".to_string()));
        let db = Arc::new(db::MetadataRepository::new(&config.db_path).await?);
        
        // Inicializar cliente de Google Drive
        let authenticator = oauth_manager.get_authenticator().await?;
        let drive_client = Arc::new(gdrive::client::DriveClient::new(authenticator));
        
        // Inicializar sistema de archivos
        let fs = GDriveFS::new(db.clone(), drive_client.clone(), &config.cache_dir);
        
        // Fase 2.1: Bootstrapping (Sincronización de metadatos)
        if db.is_empty().await? {
            ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Sincronización inicial (esto puede tardar)...".to_string()));
            sync::bootstrap::sync_all_metadata(&db, &drive_client).await?;
        }
        
        // Fase 2.2: Background Syncer (sincronización continua)
        tracing::info!("Iniciando sincronizador en background...");
        let syncer = sync::syncer::BackgroundSyncer::new(
            db.clone(),
            drive_client.clone(),
            60, // Intervalo base: 60 segundos
            history.clone(),
            sync_paused.clone(),
        );
        let _syncer_handle = syncer.spawn();
        
        // Fase 2.3: Uploader (subida de archivos dirty)
        tracing::info!("Iniciando uploader en background...");
        let uploader = sync::uploader::Uploader::new(
            db.clone(),
            drive_client.clone(),
            30, // Intervalo: 30 segundos
            &config.cache_dir,
            history.clone(),
        );
        let _uploader_handle = uploader.spawn();
        
        // Fase 2.4: Servidor IPC para extensiones externas (Nautilus)
        tracing::info!("Iniciando servidor IPC...");
        let socket_path = ipc::get_socket_path();
        let ipc_server = ipc::server::IpcServer::new(
            socket_path,
            db.clone(),
            config.mount_point.clone(),
            config.cache_dir.clone(),
        );
        let _ipc_handle = ipc_server.spawn();
        
        // CRITICAL: Limpiar punto de montaje huérfano antes de intentar montar
        utils::mount::cleanup_if_needed(&config.mount_point)
            .context("Error al limpiar punto de montaje huérfano")?;
        
        // Informar a la GUI del punto de montaje para cleanup
        ui_sender.input(gui::app_model::AppMsg::SetMountPoint(config.mount_point.clone()));
        
        // Configurar opciones de montaje
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        
        let mut mount_options = MountOptions::default();
        mount_options
            .uid(uid)
            .gid(gid)
            .fs_name("fedoradrive")
            .custom_options("exec"); // CRÍTICO: Permitir ejecución de binarios y .desktop
            
        tracing::info!("Montando sistema de archivos en {:?}...", config.mount_point);
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus(format!("Montando en {:?}...", config.mount_point)));
        
        let mut handle = Session::new(mount_options)
            .mount_with_unprivileged(fs, &config.mount_point)
            .await
            .context("Error al montar sistema de archivos FUSE")?;
        
        tracing::info!("✅ Sistema de archivos montado exitosamente");
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Sistema de archivos montado y activo".to_string()));
        
        // Esperar a que termine la sesión O sea interrumpida por Ctrl+C
        tokio::select! {
            res = &mut handle => {
                if let Err(e) = res {
                    tracing::error!("Error en la sesión FUSE: {:?}", e);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("🛑 Recibida señal de interrupción (Ctrl+C)");
                ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Cerrando por señal...".to_string()));
            }
        }
        
        tracing::info!("🛑 Desmontando sistema de archivos y cerrando...");
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Desmontando...".to_string()));
        
        // El drop de 'handle' debería intentar desmontar, pero lo forzamos por seguridad
        let _ = utils::mount::unmount(&config.mount_point);
        
        // Forzar salida del proceso (GTK no responde a señales del backend)
        tracing::info!("👋 Cerrando aplicación...");
        std::process::exit(0);
    })
}

/// Inicializa el sistema de logging con tracing
fn init_logging() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "g_drive_xp=debug,info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    Ok(())
}
