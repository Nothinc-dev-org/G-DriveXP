# Plan de Implementación: Extensión de Nautilus con FFI GTK4

## Estado: Implementación Core Completada / Refinando Acciones

---

## Plan de Implementación

### Fase 1: Generar Bindings FFI (Completada)
- [x] Instalar nautilus-devel
- [x] Examinar headers de libnautilus-extension
- [x] Crear bindings (implementados en `ffi.rs`)
- [x] Verificar compilación (RFC ABI C correcta)

### Fase 2: Implementar InfoProvider (Completada)
- [x] Implementar `nautilus_module_initialize`
- [x] Implementar `nautilus_module_list_types`
- [x] Implementar interface `NautilusInfoProvider`
- [x] Implementar `update_file_info` con consulta IPC (Worker Thread asíncrono)
- [x] Implementar emblemas dinámicos (`synced`, `cloud`, `local`, `error`, `shared`)

### Fase 3: Acciones Contextuales (Completada)
- [x] Implementar interface `NautilusMenuProvider`
- [x] Acción: "Liberar espacio" (Vía IPC)
- [x] Acción: "Mantener siempre local" (Vía IPC)
- [x] Gestión de seguridad multihilo para callbacks de GObject

### Fase 4: Compilar y Probar (Completada)
- [x] Compilar como cdylib (.so) optimizada
- [x] Script de instalación automatizado (`install_extension.sh`)
- [x] Verificar persistencia de emblemas tras reinicio de Nautilus

---

## Estructura de nautilus-ext/

```
nautilus-ext/
├── Cargo.toml
├── build.rs           # Usar pkg-config para obtener flags
└── src/
    ├── lib.rs         # Exportar funciones FFI
    ├── ffi.rs         # Bindings a libnautilus-extension-4
    ├── provider.rs    # Implementación de InfoProvider
    └── ipc_client.rs  # Cliente Unix Socket
```

---

## Notas Técnicas

1. **GObject System**: Nautilus usa GObject para interfaces. Necesitamos registrar un GType.
2. **Async**: `update_file_info` puede retornar `NAUTILUS_OPERATION_IN_PROGRESS` si la operación es async.
3. **build.rs**: Usar `pkg-config` para obtener flags de compilación automáticamente.
