//! Implementación del MenuProvider para acciones contextuales

use crate::ffi::*;
use crate::ipc_client::IpcClient;
use crate::SyncStatus;
use glib_sys::gpointer;
use gobject_sys::GObject;
use percent_encoding::percent_decode_str;
use std::thread;

// Helper de logging
fn log_debug(msg: &str) {
    eprintln!("G-DriveXP-Ext: {}", msg);
    use std::io::Write;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/gdrivexp-nautilus-v3.log")
    {
        let _ = writeln!(file, "{}", msg);
    }
}

/// Callback para get_file_items (soporta selección múltiple)
pub unsafe extern "C" fn get_file_items_impl(
    _provider: *mut GObject,
    files: *mut glib_sys::GList,
) -> *mut glib_sys::GList {
    log_debug("v4: get_file_items_impl called");

    if files.is_null() {
        return std::ptr::null_mut();
    }

    let file_count = g_list_length(files);
    if file_count == 0 {
        return std::ptr::null_mut();
    }

    log_debug(&format!("v4: file_count = {}", file_count));

    // 1. Recolectar URIs de todos los archivos seleccionados
    let mut free_uris: Vec<String> = Vec::new();   // Synced → pueden liberar espacio
    let mut download_uris: Vec<String> = Vec::new(); // CloudOnly → pueden descargar

    let mut node = files;
    while !node.is_null() {
        let file = (*node).data as *mut NautilusFileInfo;
        if file.is_null() {
            node = (*node).next;
            continue;
        }
        let uri_ptr = nautilus_file_info_get_uri(file);
        if let Some(uri) = gchar_to_string_free(uri_ptr) {
            if uri.starts_with("file://") {
                let path_str = uri.strip_prefix("file://").unwrap_or(&uri);
                // Validar que el path se puede decodificar
                if percent_decode_str(path_str).decode_utf8().is_ok() {
                    match crate::provider::ipc_query_status(&uri).ok() {
                        Some(SyncStatus::Synced) => {
                            log_debug(&format!("v4: {} -> Synced (can free)", uri));
                            free_uris.push(uri);
                        }
                        Some(SyncStatus::CloudOnly) => {
                            log_debug(&format!("v4: {} -> CloudOnly (can download)", uri));
                            download_uris.push(uri);
                        }
                        other => {
                            log_debug(&format!("v4: {} -> {:?} (skip)", uri, other));
                        }
                    }
                }
            }
        }
        node = (*node).next;
    }

    // 2. Construir menú según los estados encontrados
    let mut items: *mut glib_sys::GList = std::ptr::null_mut();

    if !free_uris.is_empty() {
        log_debug(&format!("v4: Showing 'Liberar espacio' for {} files", free_uris.len()));
        let item = create_menu_item(
            "gdrivexp::free_space",
            "Liberar espacio",
            "Eliminar copia local, mantener en la nube",
            "weather-few-clouds-symbolic",
        );

        let uris_boxed = Box::into_raw(Box::new(free_uris)) as gpointer;
        connect_activate(item, free_space_callback, uris_boxed);
        items = g_list_append(items, item as gpointer);
    }

    if !download_uris.is_empty() {
        log_debug(&format!("v4: Showing 'Mantener siempre local' for {} files", download_uris.len()));
        let item = create_menu_item(
            "gdrivexp::keep_local",
            "Mantener siempre local",
            "Descargar y mantener copia local",
            "folder-download-symbolic",
        );

        let uris_boxed = Box::into_raw(Box::new(download_uris)) as gpointer;
        connect_activate(item, keep_local_callback, uris_boxed);
        items = g_list_append(items, item as gpointer);
    }

    items
}

// === Helpers ===

// Helper Runtime removed


// IPC Client helper removed (now instantiated per thread)

unsafe fn create_menu_item(
    name: &str,
    label: &str,
    tip: &str,
    icon: &str,
) -> *mut NautilusMenuItem {
    let name_c = str_to_cstring(name);
    let label_c = str_to_cstring(label);
    let tip_c = str_to_cstring(tip);
    let icon_c = str_to_cstring(icon);
    
    nautilus_menu_item_new(
        name_c.as_ptr(),
        label_c.as_ptr(),
        tip_c.as_ptr(),
        icon_c.as_ptr(),
    )
}

unsafe fn connect_activate(
    item: *mut NautilusMenuItem,
    callback: unsafe extern "C" fn(*mut NautilusMenuItem, gpointer),
    user_data: gpointer,
) {
    let signal = str_to_cstring("activate");
    log_debug(&format!("v3: Connecting signal 'activate' to item {:?} with user_data {:?}", item, user_data));
    
    // Transmute callback to generic function pointer
    let cb_ptr: unsafe extern "C" fn() = std::mem::transmute(callback);

    let handler_id = g_signal_connect_data(
        item as gpointer,
        signal.as_ptr(),
        Some(cb_ptr),
        user_data,
        Some(free_user_data),
        0, // G_CONNECT_DEFAULT
    );
    
    log_debug(&format!("v3: g_signal_connect_data returned handler_id: {}", handler_id));
}

unsafe extern "C" fn free_user_data(data: gpointer, _closure: *mut gobject_sys::GClosure) {
    if !data.is_null() {
        let ptr = data as *mut Vec<String>;
        log_debug(&format!("v4: free_user_data called for {:?}", ptr));
        drop(Box::from_raw(ptr));
    } else {
        log_debug("v4: free_user_data called with NULL");
    }
}

// === Callbacks de Acción ===

unsafe extern "C" fn free_space_callback(
    _item: *mut NautilusMenuItem,
    user_data: gpointer,
) {
    if user_data.is_null() { return; }

    let uris = unsafe { &*(user_data as *const Vec<String>) }.clone();

    // Spawn thread to avoid blocking Nautilus UI
    thread::spawn(move || {
        log_debug(&format!("v4: Action Thread Started: Free space for {} files", uris.len()));
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                log_debug(&format!("v4: no se pudo crear runtime: {}", e));
                return;
            }
        };

        rt.block_on(async {
            let client = IpcClient::new();
            for uri in &uris {
                match client.set_online_only(uri).await {
                    Ok(true) => log_debug(&format!("IPC Success: Set Online Only for {}", uri)),
                    Ok(false) => log_debug(&format!("IPC rechazado por el daemon para {} (sin cambios)", uri)),
                    Err(e) => log_debug(&format!("IPC Error for {}: {:?} (daemon inalcanzable?)", uri, e)),
                }
            }
        });
    });
}

unsafe extern "C" fn keep_local_callback(
    _item: *mut NautilusMenuItem,
    user_data: gpointer,
) {
    if user_data.is_null() { return; }

    let uris = unsafe { &*(user_data as *const Vec<String>) }.clone();

    // Spawn thread to avoid blocking Nautilus UI
    thread::spawn(move || {
        log_debug(&format!("v4: Action Thread Started: Keep local for {} files", uris.len()));
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                log_debug(&format!("v4: no se pudo crear runtime: {}", e));
                return;
            }
        };

        rt.block_on(async {
            let client = IpcClient::new();
            for uri in &uris {
                match client.set_local_online(uri).await {
                    Ok(true) => log_debug(&format!("IPC Success: Set Local Online for {}", uri)),
                    Ok(false) => log_debug(&format!("IPC rechazado por el daemon para {} (sin cambios)", uri)),
                    Err(e) => log_debug(&format!("IPC Error for {}: {:?} (daemon inalcanzable?)", uri, e)),
                }
            }
        });
    });
}
