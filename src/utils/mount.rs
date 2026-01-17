//! Utilidades para gestión de puntos de montaje FUSE

use anyhow::{Context, Result};
use std::path::Path;
use std::process::Command;

/// Verifica si un directorio está montado como punto de montaje FUSE
pub fn is_mounted<P: AsRef<Path>>(path: P) -> bool {
    let path_str = path.as_ref().to_string_lossy();
    
    Command::new("mountpoint")
        .arg("-q")
        .arg(path_str.as_ref())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Intenta desmontar un punto de montaje FUSE usando fusermount3
/// Retorna Ok incluso si el punto no estaba montado
pub fn unmount<P: AsRef<Path>>(path: P) -> Result<()> {
    let path_str = path.as_ref().to_string_lossy();
    
    // Si no está montado, no hay nada que hacer
    if !is_mounted(&path) {
        tracing::debug!("Punto de montaje {:?} no está montado, omitiendo desmontaje", path_str);
        return Ok(());
    }
    
    tracing::info!("Desmontando {:?}...", path_str);
    
    let output = Command::new("fusermount3")
        .arg("-uz") // -u: unmount, -z: lazy unmount
        .arg(path_str.as_ref())
        .output()
        .context("Error al ejecutar fusermount3")?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::warn!("fusermount3 retornó error: {}", stderr);
        // No fallar aquí, intentar continuar
    } else {
        tracing::info!("Punto de montaje {:?} desmontado correctamente", path_str);
    }
    
    Ok(())
}

/// Limpia un punto de montaje potencialmente "huérfano"
/// Verifica si está montado y lo desmonta si es necesario
pub fn cleanup_if_needed<P: AsRef<Path>>(path: P) -> Result<()> {
    let path_ref = path.as_ref();
    
    if is_mounted(path_ref) {
        tracing::warn!(
            "Detectado punto de montaje huérfano en {:?}, limpiando...",
            path_ref
        );
        unmount(path_ref)?;
    }
    
    Ok(())
}
