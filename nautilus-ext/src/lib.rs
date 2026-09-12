//! Extensión de Nautilus para G-DriveXP (GTK4 / libnautilus-extension-4)
//!
//! Muestra emblemas de sincronización en archivos montados por G-DriveXP.

mod ffi;
mod ipc_client;
mod provider;
pub mod menu_provider;

use glib_sys::GType;
use gobject_sys::GTypeModule;
use std::os::raw::c_int;
use std::sync::OnceLock;

/// Estado de sincronización (debe coincidir con src/ipc/mod.rs del daemon)
/// - Synced: Local + Drive (verde)
/// - CloudOnly: Solo en Drive, no descargado (azul)
/// - LocalOnly: Solo local, pendiente de subir (naranja)
/// - Error: Error de sincronización (rojo)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SyncStatus {
    Synced,      // Verde: en local y en drive
    CloudOnly,   // Azul: solo en drive
    LocalOnly,   // Naranja: solo local (pending upload)
    Error,       // Rojo: error de sincronización
    Unknown,     // Sin emblema
}

/// Disponibilidad de un archivo (debe coincidir con src/ipc/mod.rs del daemon)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FileAvailability {
    LocalOnline,
    OnlineOnly,
    NotTracked,
}

/// Datos completos de estado del archivo (debe coincidir con src/ipc/mod.rs del daemon)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileStatusData {
    pub status: SyncStatus,
    pub availability: FileAvailability,
    pub is_shared: bool,
}

// ============================================================
// Funciones exportadas requeridas por Nautilus
// ============================================================

// Helper de logging
pub fn log_debug(msg: &str) {
    use std::io::Write;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/gdrivexp-nautilus-init.log")
    {
        let _ = writeln!(file, "{}", msg);
    }
}

/// Llamada cuando la extensión es cargada
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nautilus_module_initialize(module: *mut GTypeModule) {
    log_debug("nautilus_module_initialize called");
    // Registrar nuestro tipo GDriveXPProvider
    provider::register_type(module);
    log_debug("provider registered");
}

/// Llamada cuando la extensión es descargada
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nautilus_module_shutdown() {
    log_debug("nautilus_module_shutdown called");
    // Cleanup si es necesario
}

/// Nautilus llama esto para obtener los tipos que exportamos
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nautilus_module_list_types(
    types: *mut *const GType,
    num_types: *mut c_int,
) {
    log_debug("nautilus_module_list_types called");
    // OnceLock en vez de static mut: Nautilus conserva el puntero, pero solo
    // se escribe una vez en carga single-thread; así no hay data race posible.
    static TYPE_LIST: OnceLock<[GType; 1]> = OnceLock::new();
    let list = TYPE_LIST.get_or_init(|| [provider::get_type()]);

    *types = list.as_ptr();
    *num_types = 1;
    log_debug("types listed");
}
