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
    let mount_point = PathBuf::from(format!("{}/GoogleDrive/.cloud_mount", home));

    // 1. Desmontar FUSE
    // Intentamos fusermount3 primero, luego umount -l como fallback
    tracing::info!("Desmontando sistema de archivos...");
    let umount_status = Command::new("fusermount3")
        .arg("-u")
        .arg(&mount_point)
        .status();
        
    if umount_status.is_err() || !umount_status.unwrap().success() {
        tracing::warn!("fusermount3 falló, intentando lazy unmount...");
        let _ = Command::new("pkexec")
            .arg("umount")
            .arg("-l")
            .arg(&mount_point)
            .status();
    }

    // 2. Eliminar Base de Datos
    if db_path.exists() {
        tracing::info!("Eliminando base de datos: {:?}", db_path);
        fs::remove_file(&db_path).context("Fallo al eliminar metadata.db")?;
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
