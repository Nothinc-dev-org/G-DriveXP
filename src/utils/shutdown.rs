//! Gestión de shutdown coordinado para G-DriveXP
//! 
//! Este módulo proporciona utilidades para coordinar el cierre graceful de la aplicación,
//! evitando race conditions entre hilos de GTK y la sesión FUSE.

use std::sync::atomic::{AtomicBool, Ordering};

/// Estado global de shutdown
/// 
/// Este flag se activa cuando se recibe una señal de terminación (SIGTERM, SIGINT)
/// y se consulta periódicamente para detener operaciones antes del cierre.
pub static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Registra manejadores de señales para cierre graceful
/// 
/// Captura SIGTERM y SIGINT para establecer el flag de shutdown en lugar de
/// terminar abruptamente la aplicación.
/// 
/// # Panics
/// 
/// Si no se puede registrar el handler de señales.
pub fn register_shutdown_handlers() {
    if let Err(e) = ctrlc::set_handler(move || {
        tracing::info!("🛑 Señal de cierre recibida (SIGTERM/SIGINT)");
        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
    }) {
        tracing::error!("Error registrando handler de ctrl-c: {:?}", e);
        // No entramos en pánico, pero el cierre será menos graceful
    }
}

/// Verifica si se solicitó shutdown
/// 
/// Consulta el estado del flag global de shutdown.
/// 
/// # Returns
/// 
/// `true` si se recibió una señal de terminación.
#[inline]
pub fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

/// Marca manualmente el inicio del shutdown
///
/// Útil para iniciar el proceso de cierre desde código interno
/// (por ejemplo, desde un botón "Salir" en la GUI).
pub fn request_shutdown() {
    tracing::info!("🛑 Shutdown solicitado programáticamente");
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// Espera asíncronamente hasta que se solicite shutdown.
///
/// Permite integrar la señal de shutdown en un `tokio::select!`
/// para coordinar el cierre desde el runtime async.
pub async fn wait_for_shutdown() {
    loop {
        if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
