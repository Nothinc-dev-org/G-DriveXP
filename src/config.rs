use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;

/// Configuración persistente de la aplicación
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Punto de montaje del sistema de archivos FUSE (Oculto)
    pub fuse_mount_path: PathBuf,

    /// Directorio espejo visible para el usuario (~/GoogleDrive)
    pub mirror_path: PathBuf,
    
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
            // FUSE_Mount en lugar de .cloud_mount para que Flatpak pueda atravesarlo
            fuse_mount_path: PathBuf::from(format!("{}/GoogleDrive/FUSE_Mount", home)),
            mirror_path: PathBuf::from(format!("{}/GoogleDrive", home)),
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
            let mut config: Config = serde_json::from_str(&contents)?;
            
            // MIGRATION: Check if using restricted paths (.local) or unstable (/tmp) or hidden (.cloud_mount) and migrate to visible mount
            let home = env::var("HOME")?;
            let current_path = config.fuse_mount_path.to_string_lossy();
            
            let needs_migration = current_path.contains(".local/share/g-drive-xp") || 
                                  current_path.contains("/tmp/g-drive-xp-mount") ||
                                  current_path.contains(".cloud_mount");

            if needs_migration {
                tracing::warn!("⚠️ MIGRACIÓN: Moviendo punto de montaje a ~/GoogleDrive/FUSE_Mount para compatibilidad total con Flatpak (Sandbox).");
                let new_mount = PathBuf::from(format!("{}/GoogleDrive/FUSE_Mount", home));
                config.fuse_mount_path = new_mount;
                config.ensure_directories()?;
                config.save()?;
            }
            
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
        
        // Crear el directorio espejo (visible) si no existe
        fs::create_dir_all(&self.mirror_path)?;

        // Crear el punto de montaje FUSE (oculto visualmente con .hidden)
        // Si ya existe ignorar el error EEXIST
        match fs::create_dir_all(&self.fuse_mount_path) {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                tracing::debug!("Punto de montaje FUSE ya existe, continuando...");
            },
            Err(e) => {
                // Verificar si es accesible (stale mount)
                if fs::read_dir(&self.fuse_mount_path).is_err() {
                    tracing::warn!(
                        "Punto de montaje {:?} existe pero no es accesible. \
                         Por favor ejecute: fusermount3 -u {:?}",
                        self.fuse_mount_path, self.fuse_mount_path
                    );
                }
                return Err(e.into());
            }
        }
        
        // Ocultar FUSE_Mount en Nautilus usando un archivo .hidden
        let hidden_file_path = self.mirror_path.join(".hidden");
        let mount_name = self.fuse_mount_path.file_name().unwrap_or_default().to_string_lossy();
        if let Ok(contents) = fs::read_to_string(&hidden_file_path) {
            if !contents.contains(mount_name.as_ref()) {
                let new_contents = format!("{}\n{}", contents, mount_name);
                let _ = fs::write(&hidden_file_path, new_contents);
            }
        } else {
            let _ = fs::write(&hidden_file_path, format!("{}\n", mount_name));
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
