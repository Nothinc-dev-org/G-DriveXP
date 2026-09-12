# G-DriveXP Nautilus Extension

Extensión para el administrador de archivos Nautilus (GNOME) que muestra emblemas de estado de sincronización para G-DriveXP.

## Instalación

La extensión se instala automáticamente junto con el cliente principal. Consulta las instrucciones en el [repo de G-DriveXP](https://github.com/Nothinc-dev-org/G-DriveXP).

### Instalación manual (desarrollo)

#### Requisitos

- Rust (stable)
- `libnautilus-extension` y `glib2` (cabeceras de desarrollo)

```bash
sudo dnf install nautilus-devel glib2-devel pkg-config
```

#### Compilar e instalar

```bash
cargo build --release

# Instalar emblemas
mkdir -p ~/.local/share/icons/hicolor/scalable/emblems/
cp icons/*.svg ~/.local/share/icons/hicolor/scalable/emblems/
gtk-update-icon-cache -f -t ~/.local/share/icons/hicolor/

# Instalar la librería
sudo cp target/release/libgdrivexp_nautilus.so /usr/lib64/nautilus/extensions-4/libgdrivexp-nautilus.so

# Reiniciar Nautilus
nautilus -q
```

## Estados soportados

| Emblema | Color | Significado |
|---------|-------|-------------|
| Verde   | Sincronizado | El archivo existe localmente y coincide con Drive |
| Azul    | Solo en Drive | No descargado localmente |
| Naranja | Pendiente | Cambios locales esperando subida |
| Rojo    | Error | Problema de permisos o conflicto |

## Arquitectura

```
Nautilus ──► nautilus-ext (InfoProvider + MenuProvider) ──► g-drive-xp (IPC Server)
                          Unix Socket: /run/user/UID/gdrivexp.sock
```

1. **InfoProvider**: Nautilus solicita información para cada archivo visible.
2. **MenuProvider**: Menú contextual para cambiar entre Online Only / Local & Online.
3. **IPC Client**: Consulta al daemon via Unix socket para obtener el estado de cada archivo.

## Depuración

```bash
cargo run --bin debug_ipc
```

## Licencia

GPL-3.0
