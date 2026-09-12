//! Gestión de shutdown coordinado para G-DriveXP
//! 
//! Este módulo proporciona utilidades para coordinar el cierre graceful de la aplicación,
//! evitando race conditions entre hilos de GTK y la sesión FUSE.

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

/// Estado global de shutdown
/// 
/// Este flag se activa cuando se recibe una señal (SIGTERM, SIGINT) o se solicita por GUI,
/// y se consulta periódicamente en contextos síncronos (Progress Monitor).
pub static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Primitiva de notificación async de Tokio
/// Despierta a todos los select! que estén esperando sin consumir CPU activa.
pub static SHUTDOWN_NOTIFY: Notify = Notify::const_new();

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

/// Marca coordinadamente el inicio del shutdown
///
/// Despierta a todas las tareas asíncronas bloqueadas en `wait_for_shutdown()`.
pub fn request_shutdown() {
    // Evita notificaciones en cadena si múltiples eventos disparan al mismo tiempo
    if !SHUTDOWN_REQUESTED.swap(true, Ordering::SeqCst) {
        tracing::info!("🛑 Estado de shutdown activado, notificando listeners asíncronos");
        SHUTDOWN_NOTIFY.notify_waiters();
    }
}

/// Espera asíncronamente de forma reactiva hasta que se solicite el shutdown.
///
/// Permite integrar la señal de shutdown en un `tokio::select!`
/// con 0 latencia y 0 consumo de ciclos de CPU en polling.
pub async fn wait_for_shutdown() {
    let notified = SHUTDOWN_NOTIFY.notified();
    if is_shutdown_requested() {
        return;
    }
    notified.await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Mutex global para evitar data races entre tests paralelos
    // que manipulan el estado estático global.
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    /// Resetea el estado global entre tests
    fn reset_shutdown() {
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_initial_state_not_shutdown() {
        let _guard = TEST_MUTEX.lock().unwrap();
        reset_shutdown();
        assert!(!is_shutdown_requested());
    }

    #[test]
    fn test_request_shutdown_sets_flag() {
        let _guard = TEST_MUTEX.lock().unwrap();
        reset_shutdown();
        request_shutdown();
        assert!(is_shutdown_requested());
        reset_shutdown();
    }

    #[tokio::test]
    async fn test_wait_for_shutdown_returns_reactively() {
        let _guard = TEST_MUTEX.lock().unwrap();
        reset_shutdown();

        let task = tokio::spawn(async {
            // Emular que otro hilo solicita el shutdown tras un ínfimo delay
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            request_shutdown();
        });

        // Garantiza que la iteración asíncrona lo captura enseguida mediante Notify
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            wait_for_shutdown(),
        )
        .await;

        assert!(result.is_ok(), "wait_for_shutdown failed to awaken immediately");
        let _ = task.await;
        reset_shutdown();
    }
}
