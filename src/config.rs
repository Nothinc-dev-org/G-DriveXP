use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;

/// Configuración persistente de la aplicación
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Punto de montaje del sistema de archivos FUSE
    pub mount_point: PathBuf,
    
    /// Directorio de caché para contenido de archivos
    pub cache_dir: PathBuf,
    
    /// Ruta de la base de datos SQLite
    pub db_path: PathBuf,
    
    /// Intervalo de sincronización en segundos
    pub sync_interval_secs: u64,
    
    /// Tamaño máximo de caché en MB
    pub max_cache_size_mb: u64,
}

impl Config {
    /// Crea una configuración con valores predeterminados
    pub fn default() -> Result<Self> {
        let home = env::var("HOME")?;
        
        Ok(Self {
            mount_point: PathBuf::from(format!("{}/GoogleDrive", home)),
            cache_dir: PathBuf::from(format!("{}/.cache/fedoradrive", home)),
            db_path: PathBuf::from(format!("{}/.config/fedoradrive/metadata.db", home)),
            sync_interval_secs: 60,
            max_cache_size_mb: 1024, // 1GB predeterminado
        })
    }
    
    /// Carga la configuración desde el archivo
    pub fn load() -> Result<Self> {
        let config_path = Self::config_path()?;
        
        if config_path.exists() {
            let contents = fs::read_to_string(&config_path)?;
            let config: Config = serde_json::from_str(&contents)?;
            tracing::info!("Configuración cargada desde {:?}", config_path);
            Ok(config)
        } else {
            tracing::info!("Configuración no encontrada, usando valores predeterminados");
            Self::default()
        }
    }
    
    /// Guarda la configuración en el archivo
    pub fn save(&self) -> Result<()> {
        let config_path = Self::config_path()?;
        
        // Crear el directorio si no existe
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        let contents = serde_json::to_string_pretty(self)?;
        fs::write(&config_path, contents)?;
        
        tracing::info!("Configuración guardada en {:?}", config_path);
        Ok(())
    }
    
    /// Retorna la ruta del archivo de configuración
    fn config_path() -> Result<PathBuf> {
        let home = env::var("HOME")?;
        Ok(PathBuf::from(format!("{}/.config/fedoradrive/config.json", home)))
    }
    
    /// Crea todos los directorios necesarios
    pub fn ensure_directories(&self) -> Result<()> {
        fs::create_dir_all(&self.cache_dir)?;
        
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        // Crear el punto de montaje si no existe
        // Si ya existe (incluso en estado stale por crash anterior), ignorar el error EEXIST
        match fs::create_dir_all(&self.mount_point) {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                tracing::debug!("Punto de montaje ya existe, continuando...");
            },
            Err(e) => {
                // Verificar si es accesible (stale mount devuelve error al acceder)
                if fs::read_dir(&self.mount_point).is_err() {
                    tracing::warn!(
                        "Punto de montaje {:?} existe pero no es accesible. \
                         Por favor ejecute: fusermount3 -u {:?}",
                        self.mount_point, self.mount_point
                    );
                }
                return Err(e.into());
            }
        }
        
        tracing::info!("Directorios de configuración y montaje creados");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = Config::default().unwrap();
        assert!(config.sync_interval_secs > 0);
        assert!(config.max_cache_size_mb > 0);
    }
}
