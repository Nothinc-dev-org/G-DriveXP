//! Bindings FFI manuales para libnautilus-extension-4
//!
//! Estos bindings cubren solo las funciones necesarias para implementar
//! un InfoProvider que añade emblemas a archivos.

use glib_sys::{GType, gpointer};
use gobject_sys::{GClosure, GObject, GTypeInterface, GTypeModule, GTypeInfo, GInterfaceInfo};
use std::os::raw::c_char;

// ============================================================
// Tipos opacos de Nautilus
// ============================================================

/// Opaco: representa un NautilusFileInfo
#[repr(C)]
pub struct NautilusFileInfo {
    _private: [u8; 0],
}

/// Opaco: handle para operaciones asíncronas
#[repr(C)]
pub struct NautilusOperationHandle {
    _private: [u8; 0],
}

/// Opaco: representa un item de menú contextual
#[repr(C)]
pub struct NautilusMenuItem {
    _private: [u8; 0],
}

// ============================================================
// Enums
// ============================================================

/// Resultado de operaciones de InfoProvider
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum NautilusOperationResult {
    Complete = 0,
    Failed = 1,
    InProgress = 2,
}

// ============================================================
// Interface: NautilusInfoProvider
// ============================================================

/// VTable para NautilusInfoProvider interface
#[repr(C)]
pub struct NautilusInfoProviderInterface {
    pub g_iface: GTypeInterface,
    
    pub update_file_info: Option<
        unsafe extern "C" fn(
            provider: *mut GObject,
            file: *mut NautilusFileInfo,
            update_complete: *mut GClosure,
            handle: *mut *mut NautilusOperationHandle,
        ) -> NautilusOperationResult,
    >,
    
    pub cancel_update: Option<
        unsafe extern "C" fn(
            provider: *mut GObject,
            handle: *mut NautilusOperationHandle,
        ),
    >,
}

// ============================================================
// Interface: NautilusMenuProvider
// ============================================================

/// VTable para NautilusMenuProvider interface
#[repr(C)]
pub struct NautilusMenuProviderInterface {
    pub g_iface: GTypeInterface,
    
    pub get_file_items: Option<
        unsafe extern "C" fn(
            provider: *mut GObject,
            files: *mut glib_sys::GList,
        ) -> *mut glib_sys::GList,
    >,
    
    pub get_background_items: Option<
        unsafe extern "C" fn(
            provider: *mut GObject,
            current_folder: *mut NautilusFileInfo,
        ) -> *mut glib_sys::GList,
    >,
}

// ============================================================
// Funciones externas de libnautilus-extension
// ============================================================

#[link(name = "nautilus-extension")]
unsafe extern "C" {
    // Funciones de NautilusFileInfo
    pub fn nautilus_file_info_get_uri(file_info: *mut NautilusFileInfo) -> *mut c_char;
    pub fn nautilus_file_info_add_emblem(file_info: *mut NautilusFileInfo, emblem_name: *const c_char);
    #[allow(dead_code)]
    pub fn nautilus_file_info_is_directory(file_info: *mut NautilusFileInfo) -> glib_sys::gboolean;
    
    // Obtener el GType de NautilusInfoProvider
    pub fn nautilus_info_provider_get_type() -> GType;
    
    // Obtener el GType de NautilusMenuProvider
    pub fn nautilus_menu_provider_get_type() -> GType;
    
    // Crear un nuevo menu item
    pub fn nautilus_menu_item_new(
        name: *const c_char,
        label: *const c_char,
        tip: *const c_char,
        icon: *const c_char,
    ) -> *mut NautilusMenuItem;
}

// ============================================================
// Funciones de GLib/GObject que necesitamos
// ============================================================

#[link(name = "glib-2.0")]
unsafe extern "C" {
    pub fn g_free(mem: gpointer);
}

#[link(name = "gobject-2.0")]
unsafe extern "C" {
    #[allow(dead_code)]
    pub fn g_type_module_register_type(
        module: *mut GTypeModule,
        parent_type: GType,
        type_name: *const c_char,
        type_info: *const GTypeInfo,
        flags: u32, // GTypeFlags
    ) -> GType;
    
    #[allow(dead_code)]
    pub fn g_type_module_add_interface(
        module: *mut GTypeModule,
        instance_type: GType,
        interface_type: GType,
        interface_info: *const GInterfaceInfo,
    );
    
    pub fn g_signal_connect_data(
        instance: gpointer,
        detailed_signal: *const c_char,
        c_handler: Option<unsafe extern "C" fn()>,
        data: gpointer,
        destroy_data: Option<unsafe extern "C" fn(gpointer, *mut GClosure)>,
        connect_flags: u32,
    ) -> u64;
}

// ============================================================
// Funciones adicionales de GLib para manipulación de listas
// ============================================================

#[link(name = "glib-2.0")]
unsafe extern "C" {
    pub fn g_list_append(
        list: *mut glib_sys::GList,
        data: gpointer,
    ) -> *mut glib_sys::GList;
    
    pub fn g_list_length(list: *mut glib_sys::GList) -> u32;
}

// ============================================================
// Macros de utilidad
// ============================================================

/// Convierte un *mut c_char a String y libera la memoria
pub unsafe fn gchar_to_string_free(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let s = std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned();
    g_free(ptr as gpointer);
    Some(s)
}

/// Convierte un &str a *const c_char (temporal, no usar fuera del scope)
pub fn str_to_cstring(s: &str) -> std::ffi::CString {
    std::ffi::CString::new(s).expect("CString conversion failed")
}
