# Instalación de Dependencias del Sistema - FedoraDrive-rs

## Dependencias Requeridas para Fedora

Para compilar este proyecto en Fedora Workstation, necesita instalar las bibliotecas de desarrollo del sistema:

### Comando de Instalación Completo

```bash
sudo dnf install -y \
    rust \
    cargo \
    sqlite-devel \
    gtk4-devel \
    libadwaita-devel \
    fuse3-devel \
    glib2-devel \
    gobject-introspection-devel \
    cairo-gobject-devel \
    pango-devel \
    gdk-pixbuf2-devel \
    graphene-devel \
    openssl-devel \
    pkg-config
```

### Desglose de Paquetes

| Paquete | Propósito |
|---------|-----------|
| `rust`, `cargo` | Compilador y gestor de paquetes Rust |
| `sqlite-devel` | Bibliotecas de desarrollo SQLite (base de datos de metadatos) |
| `gtk4-devel` | Bibliotecas de desarrollo GTK4 (interfaz gráfica) |
| `libadwaita-devel` | Widgets modernos de GNOME |
| `fuse3-devel` | Bibliotecas de desarrollo FUSE3 (sistema de archivos) |
| `glib2-devel` | Bibliotecas base de GLib |
| `gobject-introspection-devel` | Sistema de objetos de GLib |
| `cairo-gobject-devel` | Gráficos vectoriales |
| `pango-devel` | Renderizado de texto |
| `gdk-pixbuf2-devel` | Carga y manipulación de imágenes |
| `graphene-devel` | Matemáticas 3D/2D para gráficos |
| `openssl-devel` | Bibliotecas SSL/TLS |
| `pkg-config` | Herramienta de configuración de paquetes |

### Verificación Post-Instalación

Después de instalar los paquetes, verifique que `pkg-config` puede encontrar las bibliotecas:

```bash
pkg-config --modversion glib-2.0 gtk4 libadwaita-1
```

Debería ver versiones como:
```
2.78.x
4.12.x
1.5.x
```

### Compilación del Proyecto

Una vez instaladas las dependencias del sistema:

```bash
# Limpiar compilaciones anteriores si es necesario
cargo clean

# Verificar que todo compila correctamente
cargo check

# Compilar en modo debug
cargo build

# Compilar optimizado para producción
cargo build --release
```

### Configuración de FUSE (Acceso para Apps/Flatpak)

Para que aplicaciones como reproductores de música (GNOME Music, Rhythmbox) o navegadores puedan acceder a los archivos, especialmente si funcionan bajo **Flatpak**, es necesario permitir el acceso a otros usuarios en FUSE:

1. **Habilitar `user_allow_other`**:
   Edite el archivo `/etc/fuse.conf` como superusuario:
   ```bash
   sudo nano /etc/fuse.conf
   ```
   Descomente (elimine el `#`) la línea que dice `user_allow_other`.

2. **Asegurar pertenencia al grupo `fuse`**:
   ```bash
   sudo usermod -a -G fuse $USER
   # Reinicie su sesión para que los cambios surtan efecto
   ```

#### Compatibilidad con Aplicaciones (Restricciones de Symlinks)

G-DriveXP utiliza una arquitectura híbrida donde los archivos en línea son **enlaces simbólicos** hacia el montaje FUSE. Algunas aplicaciones GNOME modernas (como **GNOME Decibels**) tienen políticas de seguridad estrictas que les impiden seguir enlaces simbólicos hacia sistemas de archivos virtuales.

- **Síntoma**: La aplicación dice "Archivo no admitido" al intentar abrir desde la carpeta principal.
- **Workaround**:
  1. Acceda al archivo directamente desde el punto de montaje virtual: `~/GoogleDrive/FUSE_Mount/`.
  2. O descargue el archivo ("Sincronizar a Local") para que se convierta en un archivo real compatible con el portal de archivos de GNOME.

### Notas Específicas de Fedora

- **Versión mínima de Fedora**: 39+ (para GTK4 4.12 y Libadwaita 1.5)
- **Edición de Rust**: Este proyecto usa Rust Edition 2024, requiere `rustc >= 1.85`

### Solución de Problemas Comunes

**Error: `glib-2.0.pc` no encontrado**
```bash
sudo dnf install glib2-devel pkg-config
```

**Error: `gtk4 >= 4.12` no encontrado**
```bash
# Actualizar Fedora si usa versión antigua
sudo dnf upgrade --refresh
```

**Error de permisos FUSE**
```bash
# Verificar que FUSE está habilitado
lsmod | grep fuse
# Si no aparece, cargar el módulo
sudo modprobe fuse
```
