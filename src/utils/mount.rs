//! Utilidades para gestión de puntos de montaje FUSE

use anyhow::Result;
use std::path::Path;
use std::process::Command;

/// Verifica si un directorio está montado como punto de montaje FUSE
/// Es más robusto que mountpoint -q ya que detecta estados de error (endpoint no conectado)
pub fn is_mounted<P: AsRef<Path>>(path: P) -> bool {
    let path_ref = path.as_ref();
    if !path_ref.exists() {
        return false;
    }

    // El comando 'mountpoint' es confiable, pero si el endpoint no está conectado,
    // a veces retorna error. En ese caso, IGUAL queremos intentar desmontar.
    let status = Command::new("mountpoint")
        .arg("-q")
        .arg(path_ref)
        .status();

    match status {
        Ok(s) if s.success() => true,
        _ => {
            // Si mountpoint falla, verificamos /proc/mounts como respaldo
            match std::fs::read_to_string("/proc/mounts") {
                Ok(content) => {
                    let path_abs = path_ref.canonicalize()
                        .unwrap_or_else(|_| path_ref.to_path_buf());
                    let path_str = path_abs.to_string_lossy();
                    content.lines().any(|line| line.contains(&*path_str))
                },
                Err(_) => false
            }
        }
    }
}

/// Intenta desmontar un punto de montaje FUSE de forma agresiva (Lazy)
pub fn unmount<P: AsRef<Path>>(path: P) -> Result<()> {
    let path_str = path.as_ref().to_string_lossy();
    
    tracing::info!("Iniciando protocolo de desmontaje para {:?}...", path_str);
    
    // Intentar con fusermount3 primero, luego fusermount
    let binaries = ["fusermount3", "fusermount"];
    let mut success = false;

    for bin in binaries {
        let output = Command::new(bin)
            .arg("-uz") // -u: unmount, -z: lazy unmount (CRÍTICO para Nautilus ocupado)
            .arg(path_str.as_ref())
            .output();

        if let Ok(out) = output {
            if out.status.success() {
                tracing::info!("✅ Desmontado exitosamente con {}", bin);
                success = true;
                break;
            } else {
                let stderr = String::from_utf8_lossy(&out.stderr);
                tracing::debug!("{} no pudo desmontar (quizás no montado): {}", bin, stderr.trim());
            }
        }
    }

    if !success {
        // Como último recurso, intentar umount estándar (requiere sudo usualmente, pero lo intentamos)
        let _ = Command::new("umount")
            .arg("-l") // lazy
            .arg(path_str.as_ref())
            .status();
    }
    
    Ok(())
}

/// Limpia un punto de montaje potencialmente "huérfano"
/// Se asegura de que el directorio esté limpio antes de intentar un nuevo montaje
pub fn cleanup_if_needed<P: AsRef<Path>>(path: P) -> Result<()> {
    let path_ref = path.as_ref();
    
    if is_mounted(path_ref) {
        tracing::warn!("Detectado montaje previo en {:?}, aplicando purga...", path_ref);
        unmount(path_ref)?;
        
        // Pequeña espera para permitir que el kernel limpie el inodo
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    
    Ok(())
}
