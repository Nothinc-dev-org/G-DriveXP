//! Utilidades para gestión de puntos de montaje FUSE

use anyhow::Result;
use std::path::Path;
use std::process::Command;

/// Verifica si un directorio está montado como punto de montaje FUSE
/// Detecta TANTO montajes normales COMO endpoints rotos (error 107 / ENOTCONN)
pub fn is_mounted<P: AsRef<Path>>(path: P) -> bool {
    let path_ref = path.as_ref();
    
    // Primero: detectar si el path tiene un endpoint roto (ENOTCONN)
    // Esto ocurre cuando el proceso FUSE anterior murió sin desmontar
    match std::fs::metadata(path_ref) {
        Ok(_) => {
            // El path es accesible, verificar si es un mountpoint
            let status = Command::new("mountpoint")
                .arg("-q")
                .arg(path_ref)
                .status();
            
            if let Ok(s) = status {
                if s.success() {
                    return true;
                }
            }
        }
        Err(e) => {
            // Si el error es "Transport endpoint is not connected" (ENOTCONN = 107),
            // entonces HAY un montaje zombie que necesita limpiarse
            if e.raw_os_error() == Some(107) {
                tracing::warn!("Detectado endpoint FUSE zombi en {:?} (ENOTCONN)", path_ref);
                return true; // Reportar como montado para que se intente desmontar
            }
        }
    }
    
    // Fallback: verificar /proc/mounts sin depender de canonicalize
    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        let path_str = path_ref.to_string_lossy();
        if content.lines().any(|line| line.contains(&*path_str)) {
            return true;
        }
    }
    
    false
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
        
        // Fallback final: abortar conexiones FUSE si todo lo demás falla
        tracing::warn!("Desmontaje normal falló, intentando abort de conexiones FUSE...");
        let _ = abort_fuse_connections();
    }
    
    Ok(())
}

/// Aborta conexiones FUSE huérfanas para liberar hilos bloqueados
/// 
/// Esta función es un último recurso cuando el desmontaje normal falla.
/// Escribe "1" en los archivos de abort del subsistema FUSE para forzar
/// la terminación de conexiones bloqueadas.
/// 
/// **ADVERTENCIA**: Esto puede causar pérdida de datos en operaciones pendientes.
/// Solo usar cuando el desmontaje normal ha fallado.
pub fn abort_fuse_connections() -> Result<()> {
    let connections_dir = std::path::Path::new("/sys/fs/fuse/connections");
    
    if !connections_dir.exists() {
        tracing::debug!("Directorio de conexiones FUSE no existe");
        return Ok(());
    }
    
    tracing::warn!("⚠️ Abortando conexiones FUSE residuales...");
    
    for entry in std::fs::read_dir(connections_dir)? {
        let entry = entry?;
        let abort_path = entry.path().join("abort");
        
        if abort_path.exists() {
            match std::fs::write(&abort_path, "1") {
                Ok(_) => {
                    tracing::info!("✅ Abortada conexión FUSE: {:?}", entry.file_name());
                }
                Err(e) => {
                    tracing::debug!(
                        "No se pudo abortar {:?}: {} (puede requerir privilegios)",
                        abort_path,
                        e
                    );
                }
            }
        }
    }
    
    Ok(())
}

/// Desmonta FUSE y espera a que el kernel confirme que ya no está montado.
/// Reintenta con estrategias cada vez más agresivas si el mount persiste.
pub fn unmount_and_wait<P: AsRef<Path>>(path: P) -> Result<()> {
    let path_ref = path.as_ref();

    if !is_mounted(path_ref) {
        return Ok(());
    }

    tracing::info!("🛑 Desmontando FUSE y esperando confirmación del kernel...");
    unmount(path_ref)?;

    // Polling: verificar que el mount realmente se fue
    let start = std::time::Instant::now();
    let max_wait = std::time::Duration::from_secs(10);

    while is_mounted(path_ref) {
        if start.elapsed() > max_wait {
            tracing::error!(
                "⚠️ FUSE sigue montado después de {}s. Forzando salida de todas formas.",
                max_wait.as_secs()
            );
            break;
        }

        // Esperar brevemente antes de re-verificar
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Si ya pasaron 2 segundos y sigue montado, reintentar unmount
        if start.elapsed() > std::time::Duration::from_secs(2) && is_mounted(path_ref) {
            tracing::warn!("FUSE persiste, reintentando desmontaje...");
            let _ = unmount(path_ref);
        }
    }

    if !is_mounted(path_ref) {
        tracing::info!("✅ FUSE desmontado correctamente en {:?}", start.elapsed());
    }

    Ok(())
}

/// Limpia un punto de montaje potencialmente "huérfano"
/// Se asegura de que el directorio esté limpio antes de intentar un nuevo montaje
pub fn cleanup_if_needed<P: AsRef<Path>>(path: P) -> Result<()> {
    let path_ref = path.as_ref();

    if is_mounted(path_ref) {
        tracing::warn!("Detectado montaje previo en {:?}, aplicando purga...", path_ref);
        unmount_and_wait(path_ref)?;
    }

    Ok(())
}
