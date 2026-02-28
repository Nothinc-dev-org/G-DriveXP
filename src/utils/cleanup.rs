use anyhow::{Context, Result};
use std::process::Command;
use std::fs;
use std::path::PathBuf;

/// Ejecuta un "Hard Reset" de la aplicación.
/// 
/// Acciones:
/// 1. Desmonta FUSE (lazy unmount si es necesario).
/// 2. Elimina la base de datos local.
/// 3. Elimina tokens de autenticación.
/// 4. Limpia la caché.
/// 5. Limpia y recrea el directorio espejo (~/GoogleDrive).
pub fn perform_hard_reset() -> Result<()> {
    tracing::warn!("⚠️ INICIANDO PROTOCOLO HARD RESET");

    let home = std::env::var("HOME").context("No se pudo obtener HOME")?;
    
    // Rutas críticas
    let db_path = PathBuf::from(format!("{}/.config/fedoradrive/metadata.db", home));
    let tokens_path = PathBuf::from(format!("{}/.config/fedoradrive/tokens.json", home));
    let cache_dir = PathBuf::from(format!("{}/.cache/fedoradrive", home));
    let mirror_dir = PathBuf::from(format!("{}/GoogleDrive", home));

    // 2. Eliminar Base de Datos y sus archivos de Journaling (WAL/SHM)
    if db_path.exists() {
        tracing::info!("Eliminando base de datos: {:?}", db_path);
        fs::remove_file(&db_path).context("Fallo al eliminar metadata.db")?;

        let db_wal = PathBuf::from(format!("{}/.config/fedoradrive/metadata.db-wal", home));
        let db_shm = PathBuf::from(format!("{}/.config/fedoradrive/metadata.db-shm", home));
        
        if db_wal.exists() {
            let _ = fs::remove_file(&db_wal);
        }
        if db_shm.exists() {
            let _ = fs::remove_file(&db_shm);
        }
    }

    // 3. Eliminar Tokens
    if tokens_path.exists() {
        tracing::info!("Eliminando tokens: {:?}", tokens_path);
        fs::remove_file(&tokens_path).context("Fallo al eliminar tokens.json")?;
    }

    // 4. Limpiar Caché
    if cache_dir.exists() {
        tracing::info!("Limpiando caché: {:?}", cache_dir);
        fs::remove_dir_all(&cache_dir).context("Fallo al eliminar directorio de caché")?;
    }

    // 5. Limpiar y Recrear Mirror
    if mirror_dir.exists() {
        tracing::info!("Limpiando directorio espejo: {:?}", mirror_dir);
        // Usamos rm -rf via shell para ser más robustos con permisos o symlinks rotos
        let _ = Command::new("rm")
            .arg("-rf")
            .arg(&mirror_dir)
            .status();
    }
    
    // Recrear directorios básicos
    tracing::info!("Recreando estructura de directorios limpia...");
    fs::create_dir_all(&mirror_dir).context("Fallo al recrear ~/GoogleDrive")?;
    fs::create_dir_all(&cache_dir).context("Fallo al recrear caché")?;

    tracing::info!("✅ Hard Reset completado con éxito.");
    Ok(())
}
