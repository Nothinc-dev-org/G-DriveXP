//! Implementación del InfoProvider para G-DriveXP
//!
//! Registra un GType que implementa NautilusInfoProvider y consulta
//! el estado de sincronización vía IPC.

use crate::ffi::*;
use crate::ipc_client::IpcClient;
use gobject_sys::{GObject, GTypeInfo, GInterfaceInfo, GTypeModule, g_type_module_register_type, g_type_module_add_interface};
use glib_sys::GType;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use crossbeam_channel::{bounded, Sender, Receiver};
use percent_encoding::percent_decode_str;

// ============================================================
// IPC Worker Thread Architecture
// ============================================================

/// Request to the IPC worker
struct IpcRequest {
    uri: String,
    response_tx: Sender<crate::FileStatusData>,
}

/// IPC worker that runs a dedicated thread with multi-threaded Tokio runtime
struct IpcWorker {
    request_tx: Sender<IpcRequest>,
    thread: std::thread::JoinHandle<()>,
}

impl IpcWorker {
    fn new() -> Self {
        let (request_tx, request_rx): (Sender<IpcRequest>, Receiver<IpcRequest>) = bounded(32);

        // Spawn dedicated worker thread
        let thread = thread::spawn(move || {
            crate::log_debug("Worker thread started");
            let result = std::panic::catch_unwind(move || {
                // Single-threaded runtime is sufficient here because:
                // 1. This runs in its own dedicated thread (not Nautilus main thread)
                // 2. Requests are processed sequentially from the channel
                // 3. More lightweight than multi-threaded runtime
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create IPC worker runtime");
                
                rt.block_on(async {
                    crate::log_debug("Worker LocalSet started");
                    let client = IpcClient::new();
                    
                    while let Ok(req) = request_rx.recv() {
                        crate::log_debug(&format!("Worker received request: {}", req.uri));
                        // Query IPC with timeout
                        let status_data = match tokio::time::timeout(
                            Duration::from_millis(200),
                            client.get_extended_status(&req.uri)
                        ).await {
                            Ok(Ok(data)) => data,
                            Ok(Err(e)) => {
                                crate::log_debug(&format!("Client Error: {}", e));
                                // Solo lo PROBADO-muerto va a rojo (Error): socket
                                // ausente, conexión rechazada o daemon caído a mitad
                                // de la conversación. El resto (transitorio o un
                                // Error respondido por el daemon) va a Unknown.
                                // Sin contadores: con drenado lento, los timeouts
                                // son rutinarios bajo carga y contarlos parpadearía
                                // rojos en cada sync masivo.
                                if is_unreachable_kind(e.kind()) {
                                    unreachable_status()
                                } else {
                                    crate::FileStatusData {
                                        status: crate::SyncStatus::Unknown,
                                        availability: crate::FileAvailability::NotTracked,
                                        is_shared: false,
                                    }
                                }
                            },
                            Err(_) => {
                                crate::log_debug("Worker timeout");
                                crate::FileStatusData {
                                    status: crate::SyncStatus::Unknown,
                                    availability: crate::FileAvailability::NotTracked,
                                    is_shared: false,
                                }
                            }
                        };
                        
                        // Send response back (ignore error if receiver dropped)
                        let _ = req.response_tx.send(status_data);
                    }
                    crate::log_debug("Worker channel closed");
                });
            });
            
            if let Err(e) = result {
                crate::log_debug(&format!("WORKER PANIC: {:?}", e));
            }
        });

        Self { request_tx, thread }
    }

    /// ¿Sigue vivo el hilo? (para re-crear el worker si murió)
    fn alive(&self) -> bool {
        !self.thread.is_finished()
    }

    fn sender(&self) -> Sender<IpcRequest> {
        self.request_tx.clone()
    }
    
    /// Query file status with timeout from main thread.
    /// USA try_send (no bloqueante): si la cola está llena (daemon colgado
    /// drenando a 5 req/s), responde Unknown en vez de congelar el main
    /// thread de Nautilus. Con caché TTL: los re-listados/scrolls dentro de
    /// la ventana no tocan IPC en absoluto.
    fn query_extended_status(
        sender: &Sender<IpcRequest>,
        uri: &str,
        timeout: Duration,
    ) -> crate::FileStatusData {
        if let Some(hit) = status_cache_get(uri) {
            return hit;
        }
        let (response_tx, response_rx) = bounded(1);

        let request = IpcRequest {
            uri: uri.to_string(),
            response_tx,
        };

        // Send request to worker (sin bloquear jamás al llamador)
        if sender.try_send(request).is_err() {
            return unknown_status();
        }

        // Wait for response with timeout
        let data = match response_rx.recv_timeout(timeout) {
            Ok(data) => data,
            Err(_) => unknown_status(),
        };
        status_cache_put(uri, data.clone());
        data
    }
}

/// ¿Este kind de io::Error prueba que el daemon está muerto (no solo lento)?
/// Solo señales de transporte: socket ausente, rechazo, corte a mitad de la
/// conversación o reintentos agotados. Timeouts y colas llenas se manejan
/// aparte como transitorios (Unknown).
fn is_unreachable_kind(kind: std::io::ErrorKind) -> bool {
    use std::io::ErrorKind::*;
    matches!(kind, NotFound | ConnectionRefused | ConnectionReset | BrokenPipe | UnexpectedEof | ConnectionAborted | NotConnected)
}

/// Estado "daemon inalcanzable": emblema rojo de error (distinto de Unknown,
/// que es "sin emblema"). Requiere prueba de muerte (ver arriba), nunca timing.
fn unreachable_status() -> crate::FileStatusData {
    crate::FileStatusData {
        status: crate::SyncStatus::Error,
        availability: crate::FileAvailability::NotTracked,
        is_shared: false,
    }
}

/// Ventana de frescura de emblemas: los re-listados/scrolls dentro del TTL no
/// tocan IPC (el main thread solo toma un lock). Invisible en la práctica:
/// los ciclos de sync son de 60 s.
const STATUS_TTL: Duration = Duration::from_secs(3);
/// Tope de entradas para acotar memoria en árboles gigantes.
const STATUS_CACHE_MAX: usize = 5000;

type StatusCache = HashMap<String, (crate::FileStatusData, Instant)>;

static STATUS_CACHE: OnceLock<std::sync::Mutex<StatusCache>> = OnceLock::new();

fn status_cache() -> &'static std::sync::Mutex<StatusCache> {
    STATUS_CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Lookup puro (testeable): hit solo si existe y está dentro del TTL.
fn cache_lookup(cache: &StatusCache, key: &str, now: Instant) -> Option<crate::FileStatusData> {
    cache.get(key).and_then(|(data, at)| {
        if now.duration_since(*at) <= STATUS_TTL {
            Some(data.clone())
        } else {
            None
        }
    })
}

/// Store puro (testeable): si se supera el tope, se vacía (simple y acotado;
/// la siguiente oleada lo rellena con datos frescos).
fn cache_store(cache: &mut StatusCache, key: String, data: crate::FileStatusData, now: Instant) {
    if cache.len() >= STATUS_CACHE_MAX {
        cache.clear();
    }
    cache.insert(key, (data, now));
}

fn status_cache_get(uri: &str) -> Option<crate::FileStatusData> {
    let cache = status_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache_lookup(&cache, uri, Instant::now())
}

fn status_cache_put(uri: &str, data: crate::FileStatusData) {
    let mut cache = status_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache_store(&mut cache, uri.to_string(), data, Instant::now());
}

/// Raíz del mirror (para filtrar consultas): la de config.json del daemon,
/// fallback a ~/GoogleDrive. Se lee una vez por sesión de Nautilus.
static MIRROR_ROOT: OnceLock<PathBuf> = OnceLock::new();

fn mirror_root() -> &'static Path {
    MIRROR_ROOT.get_or_init(|| {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let cfg_path = format!("{}/.config/fedoradrive/config.json", home);
        if let Ok(text) = std::fs::read_to_string(&cfg_path) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(p) = v.get("mirror_path").and_then(|m| m.as_str()) {
                    return PathBuf::from(p);
                }
            }
        }
        PathBuf::from(format!("{}/GoogleDrive", home))
    })
}

/// ¿Este path local vive bajo el mirror? `strip_prefix` respeta el borde de
/// componente (GoogleDrive2 NO cuela). Puro (testeable).
fn path_under_mirror(mirror: &Path, path: &Path) -> bool {
    path.strip_prefix(mirror).is_ok()
}

/// Extrae el path local de un URI file:// (con percent-decode). None si no aplica.
fn local_path_of_uri(uri: &str) -> Option<PathBuf> {
    let path_str = uri.strip_prefix("file://")?;
    let decoded = percent_decode_str(path_str).decode_utf8().ok()?;
    Some(PathBuf::from(decoded.as_ref()))
}

/// Estado "desconocido" (cola llena o timeout = transitorio): sin emblema,
/// pero sin bloquear ni romper nada.
fn unknown_status() -> crate::FileStatusData {
    crate::FileStatusData {
        status: crate::SyncStatus::Unknown,
        availability: crate::FileAvailability::NotTracked,
        is_shared: false,
    }
}

// Worker global con re-creación: si el hilo murió (panic), el siguiente
// llamado lo reconstruye en vez de dejar emblemas muertos para siempre
// (el OnceLock solo, sin esto, jamás reintentaba).
static IPC_WORKER: OnceLock<std::sync::Mutex<Option<IpcWorker>>> = OnceLock::new();

/// Sender al worker vivo (crea o re-crea según haga falta). Nunca bloquea.
fn worker_sender() -> Sender<IpcRequest> {
    let cell = IPC_WORKER.get_or_init(|| std::sync::Mutex::new(None));
    let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    let needs_new = guard.as_ref().map(|w| !w.alive()).unwrap_or(true);
    if needs_new {
        *guard = Some(IpcWorker::new());
    }
    guard.as_ref().unwrap().sender()
}

/// Public API for querying IPC status (used by menu_provider)
pub fn ipc_query_status(uri: &str) -> Result<crate::SyncStatus, ()> {
    Ok(IpcWorker::query_extended_status(&worker_sender(), uri, Duration::from_millis(50)).status)
}

// ============================================================

/// GType de nuestra extensión (se registra en nautilus_module_initialize).
/// OnceLock en vez de static mut: se escribe una vez en init single-thread,
/// pero así ni siquiera existe la superficie de data race.
static GDRIVEXP_PROVIDER_TYPE: OnceLock<GType> = OnceLock::new();

// ============================================================
// Struct que representa nuestra extensión (hereda de GObject)
// ============================================================

#[repr(C)]
pub struct GDriveXPProvider {
    parent: GObject,
}

#[repr(C)]
pub struct GDriveXPProviderClass {
    parent_class: gobject_sys::GObjectClass,
}

// ============================================================
// Implementación de update_file_info
// ============================================================

// ============================================================
// Implementación de update_file_info
// ============================================================

unsafe extern "C" fn update_file_info_impl(
    _provider: *mut GObject,
    file: *mut NautilusFileInfo,
    _update_complete: *mut gobject_sys::GClosure,
    _handle: *mut *mut NautilusOperationHandle,
) -> NautilusOperationResult {
    if file.is_null() {
        return NautilusOperationResult::Complete;
    }
    // Obtener URI del archivo
    let uri_ptr = nautilus_file_info_get_uri(file);
    let uri = match gchar_to_string_free(uri_ptr) {
        Some(u) => u,
        None => return NautilusOperationResult::Complete,
    };

    crate::log_debug(&format!("update_file_info_impl called for: {}", uri));
    
    // Solo procesar archivos file://
    if !uri.starts_with("file://") {
        return NautilusOperationResult::Complete;
    }

    // Filtro por prefijo del mirror: el daemon solo resuelve estado bajo el
    // mirror (fuera de ahí responde Unknown de todos modos), así que ni se
    // consulta. Esto elimina ~todas las consultas al navegar fuera de Drive
    // (Descargas, USBs, /tmp) y con ellas el grueso de los 50 ms/archivo.
    let under_mirror = local_path_of_uri(&uri)
        .map(|p| path_under_mirror(mirror_root(), &p))
        .unwrap_or(false);
    if !under_mirror {
        return NautilusOperationResult::Complete;
    }

    // Consultar estado usando worker (no bloquea el main thread más de 50ms)
    let data = IpcWorker::query_extended_status(&worker_sender(), &uri, Duration::from_millis(50));
    crate::log_debug(&format!("Status: {:?}, Shared: {}", data.status, data.is_shared));
    
    // NUEVO: Aplicar emblema de compartido si corresponde
    // Se añade primero para que quede visualmente "abajo" del emblema de estado (el último añadido queda arriba)
    if data.is_shared {
        let emblem = str_to_cstring("emblem-shared");
        nautilus_file_info_add_emblem(file, emblem.as_ptr());
    }

    // Aplicar emblema según estado de sincronización
    match data.status {
        crate::SyncStatus::Synced => {
            // Verde: sincronizado (local + drive)
            let emblem = str_to_cstring("emblem-gdrivexp-synced");
            nautilus_file_info_add_emblem(file, emblem.as_ptr());
        }
        crate::SyncStatus::CloudOnly => {
            // Azul: solo en drive
            let emblem = str_to_cstring("emblem-gdrivexp-cloud");
            nautilus_file_info_add_emblem(file, emblem.as_ptr());
        }
        crate::SyncStatus::LocalOnly => {
            // Naranja: solo local (pendiente de subir)
            let emblem = str_to_cstring("emblem-gdrivexp-local");
            nautilus_file_info_add_emblem(file, emblem.as_ptr());
        }
        crate::SyncStatus::Error => {
            // Rojo: error
            let emblem = str_to_cstring("emblem-gdrivexp-error");
            nautilus_file_info_add_emblem(file, emblem.as_ptr());
        }
        crate::SyncStatus::Unknown => {
            // Sin emblema
        }
    }


    
    NautilusOperationResult::Complete
}

unsafe extern "C" fn cancel_update_impl(
    _provider: *mut GObject,
    _handle: *mut NautilusOperationHandle,
) {
    // No-op: nuestras operaciones son síncronas
}

// ============================================================
// Inicialización de la interface
// ============================================================

// ============================================================
// Inicialización de la interface
// ============================================================

unsafe extern "C" fn info_provider_iface_init(iface: glib_sys::gpointer, _data: glib_sys::gpointer) {
    crate::log_debug("info_provider_iface_init called");
    let iface = iface as *mut NautilusInfoProviderInterface;
    (*iface).update_file_info = Some(update_file_info_impl);
    (*iface).cancel_update = Some(cancel_update_impl);
    crate::log_debug("info_provider_iface_init finished");
}

unsafe extern "C" fn class_init(class: glib_sys::gpointer, _data: glib_sys::gpointer) {
    crate::log_debug("class_init called");
    // Peek parent class just to be sure we touch it and compiler doesn't optimize away
    let parent = gobject_sys::g_type_class_peek_parent(class);
    if !parent.is_null() {
        crate::log_debug("class_init: parent class found");
    } else {
        crate::log_debug("class_init: parent class is null (unexpected for GObject derived)");
    }
}

unsafe extern "C" fn instance_init(_instance: *mut gobject_sys::GTypeInstance, _class: glib_sys::gpointer) {
    crate::log_debug("instance_init called");
}

// ============================================================
// Registro del tipo con GObject
// ============================================================

pub unsafe fn register_type(module: *mut GTypeModule) {
    // Debug: Check parent type validity
    let parent_type = gobject_sys::g_object_get_type();
    crate::log_debug(&format!("Parent GType (GObject): {}", parent_type));

    // Intentar un nombre único para evitar colisiones con versiones anteriores cargadas en memoria
    let type_name = str_to_cstring("GDriveXPProviderFixed");
    
    // Debug size
    crate::log_debug(&format!("sizeof(GTypeInfo) = {}", std::mem::size_of::<GTypeInfo>()));
    crate::log_debug(&format!("sizeof(GDriveXPProviderClass) = {}", std::mem::size_of::<GDriveXPProviderClass>()));
    crate::log_debug(&format!("sizeof(GDriveXPProvider) = {}", std::mem::size_of::<GDriveXPProvider>()));

    // Info del tipo
    let type_info = GTypeInfo {
        class_size: std::mem::size_of::<GDriveXPProviderClass>() as u16,
        base_init: None,
        base_finalize: None,
        class_init: Some(class_init),
        class_finalize: None,
        class_data: std::ptr::null(),
        instance_size: std::mem::size_of::<GDriveXPProvider>() as u16,
        n_preallocs: 0,
        instance_init: Some(instance_init),
        value_table: std::ptr::null(),
    };
    
    // Registrar tipo derivado de GObject
    let gtype_val = g_type_module_register_type(
        module,
        parent_type,
        type_name.as_ptr(),
        &type_info,
        0, // GTypeFlags (u32)
    );

    crate::log_debug(&format!("Registered GType: {}", gtype_val));

    if gtype_val == 0 {
        crate::log_debug("CRITICAL: Failed to register GType! (Name collision or invalid parent?)");
        return;
    }
    let _ = GDRIVEXP_PROVIDER_TYPE.set(gtype_val);

    // Info de la interface NautilusInfoProvider
    let iface_info = GInterfaceInfo {
        interface_init: Some(info_provider_iface_init),
        interface_finalize: None,
        interface_data: std::ptr::null_mut(),
    };
    
    // Registrar que implementamos NautilusInfoProvider
    let info_type = nautilus_info_provider_get_type();
    crate::log_debug(&format!("NautilusInfoProvider Type: {}", info_type));
    
    g_type_module_add_interface(
        module,
        gtype_val,
        info_type,
        &iface_info,
    );
    
    // Registrar NautilusMenuProvider
    let menu_iface_info = GInterfaceInfo {
        interface_init: Some(menu_provider_iface_init),
        interface_finalize: None,
        interface_data: std::ptr::null_mut(),
    };
    
    let menu_type = nautilus_menu_provider_get_type();
    crate::log_debug(&format!("NautilusMenuProvider Type: {}", menu_type));

    g_type_module_add_interface(
        module,
        gtype_val,
        menu_type,
        &menu_iface_info,
    );
}

unsafe extern "C" fn menu_provider_iface_init(
    iface: glib_sys::gpointer,
    _data: glib_sys::gpointer,
) {
    crate::log_debug("menu_provider_iface_init called");
    let iface = iface as *mut NautilusMenuProviderInterface;
    (*iface).get_file_items = Some(crate::menu_provider::get_file_items_impl);
    (*iface).get_background_items = None; // No implementamos background items
}

pub fn get_type() -> GType {
    GDRIVEXP_PROVIDER_TYPE.get().copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{is_unreachable_kind, path_under_mirror, local_path_of_uri, cache_lookup, cache_store, STATUS_TTL};
    use std::io::ErrorKind::*;

    /// Muerte probada del daemon → rojo.
    #[test]
    fn unreachable_solo_muerte_probada() {
        for k in [NotFound, ConnectionRefused, ConnectionReset, BrokenPipe, UnexpectedEof, ConnectionAborted, NotConnected] {
            assert!(is_unreachable_kind(k), "{:?} debería ser inalcanzable", k);
        }
    }

    /// Lo transitorio o ambiguo jamás va a rojo (evita parpadeos bajo carga).
    #[test]
    fn transitorio_nunca_a_rojo() {
        for k in [TimedOut, WouldBlock, Interrupted, InvalidData, PermissionDenied, Other] {
            assert!(!is_unreachable_kind(k), "{:?} no debería ser inalcanzable", k);
        }
    }

    /// El filtro respeta el borde de componente: GoogleDrive2 no cuela,
    /// el propio mirror y sus hijos sí.
    #[test]
    fn filtro_mirror_borde_exact() {
        use std::path::Path;
        let m = Path::new("/home/u/GoogleDrive");
        assert!(path_under_mirror(m, Path::new("/home/u/GoogleDrive")));
        assert!(path_under_mirror(m, Path::new("/home/u/GoogleDrive/a/b.txt")));
        assert!(!path_under_mirror(m, Path::new("/home/u/GoogleDrive2/x")));
        assert!(!path_under_mirror(m, Path::new("/home/u/Descargas/x")));
        assert!(!path_under_mirror(m, Path::new("/")));
    }

    /// URIs con espacios codificados resuelven al path real.
    #[test]
    fn uri_decode_para_filtro() {
        let p = local_path_of_uri("file:///home/u/GoogleDrive/Mi%20Doc.txt").unwrap();
        assert!(path_under_mirror(std::path::Path::new("/home/u/GoogleDrive"), &p));
        assert!(local_path_of_uri("sftp://x/y").is_none());
    }

    /// Caché: hit fresco, miss expirado, miss ausente; el store respeta el tope.
    #[test]
    fn cache_ttl_y_tope() {
        use std::collections::HashMap;
        use std::time::Instant;
        let data = crate::FileStatusData {
            status: crate::SyncStatus::Synced,
            availability: crate::FileAvailability::LocalOnline,
            is_shared: false,
        };
        let now = Instant::now();
        let mut c: HashMap<String, (crate::FileStatusData, Instant)> = HashMap::new();
        assert!(cache_lookup(&c, "k", now).is_none());
        cache_store(&mut c, "k".to_string(), data, now);
        assert!(cache_lookup(&c, "k", now).is_some());
        assert!(cache_lookup(&c, "k", now + STATUS_TTL + std::time::Duration::from_secs(1)).is_none());
        for i in 0..super::STATUS_CACHE_MAX + 10 {
            cache_store(&mut c, format!("k{}", i), crate::FileStatusData {
                status: crate::SyncStatus::Unknown,
                availability: crate::FileAvailability::NotTracked,
                is_shared: false,
            }, now);
        }
        assert!(c.len() <= super::STATUS_CACHE_MAX, "caché acotado, len={}", c.len());
    }
}
