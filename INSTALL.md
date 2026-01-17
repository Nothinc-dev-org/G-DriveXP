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

### Notas Específicas de Fedora

- **Versión mínima de Fedora**: 39+ (para GTK4 4.12 y Libadwaita 1.5)
- **Edición de Rust**: Este proyecto usa Rust Edition 2024, requiere `rustc >= 1.85`
- **FUSE3**: Asegúrese de que su usuario pertenece al grupo `fuse`:
  ```bash
  sudo usermod -a -G fuse $USER
  newgrp fuse
  ```

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
