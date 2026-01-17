mod auth;
mod config;

use anyhow::{Context, Result};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// OAuth2Manager se usará cuando implementemos el flujo de autenticación completo
#[allow(unused_imports)]
use auth::OAuth2Manager;
use config::Config;

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
    
    // TODO: Fase 1 - Implementar flujo de autenticación OAuth2
    // TODO: Fase 2 - Inicializar base de datos SQLite
    // TODO: Fase 2 - Montar sistema de archivos FUSE
    // TODO: Fase 3 - Lanzar interfaz GTK4
    
    tracing::info!("✅ Inicialización completada. Presione Ctrl+C para detener.");
    
    // Mantener el proceso activo
    tokio::signal::ctrl_c()
        .await
        .context("Error al esperar señal de interrupción")?;
    
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

