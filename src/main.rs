mod auth;
mod config;
mod db;
mod fuse;
mod gdrive;

use anyhow::{Context, Result};
use fuse3::MountOptions;
use fuse3::raw::Session;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use config::Config;
use fuse::GDriveFS;

#[tokio::main]
async fn main() -> Result<()> {
    // Inicializar sistema de logging
    init_logging()?;
    
    tracing::info!("🚀 Iniciando FedoraDrive-rs v{}", env!("CARGO_PKG_VERSION"));
    
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
    tracing::info!("Iniciando sistema de autenticación...");
    
    // Buscar archivo de credenciales
    let cred_path = "credentials.json";
    if !std::path::Path::new(cred_path).exists() {
        tracing::error!("No se encontró el archivo '{}'. Por favor siga las instrucciones de instalación.", cred_path);
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
    
    // Inicializar base de datos SQLite
    tracing::info!("Inicializando repositorio de metadatos...");
    let db = Arc::new(db::MetadataRepository::new(&config.db_path).await?);
    
    // Inicializar cliente de Google Drive
    let authenticator = oauth_manager.get_authenticator().await?;
    let drive_client = Arc::new(gdrive::client::DriveClient::new(authenticator));
    
    // Inicializar sistema de archivos
    let fs = GDriveFS::new(db.clone(), drive_client);
    
    // Configurar opciones de montaje
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    
    let mut mount_options = MountOptions::default();
    mount_options
        .uid(uid)
        .gid(gid)
        .fs_name("fedoradrive");
        
    tracing::info!("Montando sistema de archivos en {:?}...", config.mount_point);
    
    // Crear handler de montaje
    let handle = Session::new(mount_options)
        .mount_with_unprivileged(fs, &config.mount_point)
        .await
        .context("Error al montar sistema de archivos FUSE")?;
    
    tracing::info!("✅ Sistema de archivos montado exitosamente");
    
    // Esperar a que termine la sesión (bloqueante hasta unmount o Ctrl+C)
    handle.await.context("Error durante la sesión FUSE")?;
    
    tracing::info!("🛑 Desmontando sistema de archivos y cerrando...");
    
    Ok(())
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

