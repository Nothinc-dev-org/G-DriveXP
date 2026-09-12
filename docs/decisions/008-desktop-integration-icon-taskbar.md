# ADR-008: Integración con el escritorio GNOME (icono en barra de tareas)

## Estado
Aceptada

## Contexto
La aplicación mostraba su icono en la bandeja del sistema (via `ksni`/SNI) pero no aparecía en la barra de tareas de GNOME Shell como aplicación abierta. Esto impedía al usuario ver, cambiar y gestionar la ventana desde el dock.

Se identificaron dos causas raíz independientes que debían resolverse conjuntamente.

## Problema 1: Relm4 sin feature `libadwaita`

La dependencia `relm4 = "0.9"` se declaraba sin el feature `libadwaita`. Esto causaba:

1. **`adw::init()` nunca se ejecutaba** — la inicialización interna de libadwaita no ocurría. Relm4 solo llamaba `gtk::init()`.
2. **Se creaba `gtk::Application` en lugar de `adw::Application`** — la ventana `adw::ApplicationWindow` operaba sobre una Application de GTK puro, sin la integración completa con el escritorio GNOME.

### Evidencia
Archivo `relm4-0.9.1/src/lib.rs`:
```rust
fn init() {
    gtk::init().unwrap();
    #[cfg(feature = "libadwaita")]
    adw::init().unwrap();  // Solo con el feature habilitado
}

// Sin el feature:
fn new_application() -> gtk::Application {
    gtk::Application::default()  // NO adw::Application
}
```

### Corrección
```toml
# Antes
relm4 = "0.9"

# Después
relm4 = { version = "0.9", features = ["libadwaita"] }
```

## Problema 2: GIO descartaba el `.desktop` file por `Exec` inválido

El archivo `org.gnome.FedoraDrive.desktop` declaraba `Exec=g-drive-xp`, pero el binario no estaba en `$PATH`. GIO (`g_desktop_app_info_new()`) valida el campo `Exec` y **descarta silenciosamente** todo el archivo si el ejecutable no se encuentra.

Sin un `.desktop` file válido, GNOME Shell no podía:
- Asociar la ventana con la aplicación
- Mostrar el nombre "G-DriveXP" (mostraba el app-id crudo `org.gnome.FedoraDrive`)
- Mostrar el icono personalizado en el dock

### Evidencia
```python
# Con Exec=g-drive-xp (no en PATH)
Gio.DesktopAppInfo.new('org.gnome.FedoraDrive.desktop')  # → NULL

# Con Exec=/bin/true (existe)
Gio.DesktopAppInfo.new_from_filename('/tmp/test.desktop')  # → Name: G-DriveXP
```

### Corrección
Se crea un symlink en `~/.local/bin/` (que sí está en `$PATH`):
```bash
ln -sf /ruta/al/target/release/g-drive-xp ~/.local/bin/g-drive-xp
```
El script `install-icons.sh` fue actualizado para crear este symlink automáticamente.

## Decisión
1. Habilitar el feature `libadwaita` en la dependencia de Relm4.
2. Mantener el symlink del binario en `~/.local/bin/` como parte del proceso de instalación.
3. El archivo `.desktop` se versiona en `data/org.gnome.FedoraDrive.desktop` dentro del repositorio.

## Consecuencias
- La aplicación aparece correctamente en el dock de GNOME con icono y nombre.
- `adw::init()` se ejecuta automáticamente, garantizando la inicialización completa de libadwaita.
- El script `install-icons.sh` es responsable de instalar iconos, `.desktop` file y symlink del binario.
- Futuras compilaciones requieren re-ejecutar `install-icons.sh` si cambia la ubicación del binario.
