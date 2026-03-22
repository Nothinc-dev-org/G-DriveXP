# G-DriveXP

Cliente nativo de Google Drive para Fedora Workstation/GNOME, escrito en Rust.

Monta un sistema de archivos virtual FUSE, sincroniza metadatos y contenido bidireccionalmente, y se integra con el explorador Nautilus mediante emblemas de estado.

## Características

- Sistema de archivos virtual FUSE3 asíncrono
- Sincronización bidireccional con caché de metadatos SQLite
- Interfaz nativa GNOME con GTK4/Libadwaita
- Autenticación OAuth2 con almacenamiento en GNOME Keyring
- Extensión de Nautilus con emblemas de estado de sincronización
- Icono en la bandeja del sistema (SNI/DBus)

## Instalación

### Opción 1: Script de instalación (recomendado)

```bash
curl -fsSL https://raw.githubusercontent.com/Nothinc-dev-org/G-DriveXP/v1.0.0/packaging/install.sh | bash
```

Descarga los binarios pre-compilados de GitHub Releases e instala el cliente, la extensión de Nautilus, iconos y archivo `.desktop`.

### Opción 2: Paquete RPM

Descarga el RPM desde la [página de Releases](https://github.com/Nothinc-dev-org/G-DriveXP/releases/latest):

```bash
sudo dnf install ./g-drive-xp-1.0.0-1.fc*.x86_64.rpm
```

### Opción 3: Compilar desde fuente

#### Requisitos

- Fedora Workstation 39+
- Rust 1.85+ (Edition 2024)

#### Dependencias de compilación

```bash
sudo dnf install -y \
    rust cargo sqlite-devel gtk4-devel \
    libadwaita-devel fuse3-devel glib2-devel \
    gobject-introspection-devel cairo-gobject-devel \
    pango-devel gdk-pixbuf2-devel graphene-devel \
    openssl-devel pkg-config nautilus-devel
```

#### Compilar e instalar

```bash
git clone https://github.com/Nothinc-dev-org/G-DriveXP.git
cd G-DriveXP
cargo build --release
./scripts/install-icons.sh
```

Para instalar también la extensión de Nautilus, clona el repo hermano y ejecuta el instalador:

```bash
cd ..
git clone https://github.com/Nothinc-dev-org/G-DriveXp-nautilus-ext.git
cd G-DriveXp-nautilus-ext
cargo build --release
cd ..
./G-DriveXP/packaging/build-release.sh
```

## Configuración

### Credenciales OAuth2

1. Ve a [Google Cloud Console](https://console.cloud.google.com)
2. Crea un proyecto y habilita la API de Google Drive
3. Crea credenciales OAuth2 para "Aplicación de escritorio"
4. Descarga `credentials.json` y colócalo en `~/.config/fedoradrive/credentials.json`

### FUSE (opcional)

Para que aplicaciones de terceros accedan al sistema de archivos virtual:

1. Descomenta `user_allow_other` en `/etc/fuse.conf`
2. Asegúrate de que tu usuario esté en el grupo `fuse`

## Uso

```bash
g-drive-xp
```

El sistema de archivos se monta en `~/GoogleDrive/`. Los archivos aparecen como:
- **Online Only**: symlinks al punto de montaje FUSE (sin ocupar espacio)
- **Local & Online**: copias reales sincronizadas bidireccionalmente

## Limitaciones conocidas

- Algunas aplicaciones GNOME basadas en GTK4/GJS pueden fallar al abrir archivos Online Only debido a restricciones de seguridad con symlinks. Solución: navega directamente a `~/GoogleDrive/FUSE_Mount/` o sincroniza el archivo a local.

## Extensión de Nautilus

La extensión se instala automáticamente con el RPM o el script de instalación. Muestra emblemas de estado en los archivos dentro de `~/GoogleDrive/`:

| Emblema | Significado |
|---------|-------------|
| Verde   | Sincronizado |
| Azul    | Solo en Drive |
| Naranja | Pendiente de subida |
| Rojo    | Error |

Repo de la extensión: [G-DriveXp-nautilus-ext](https://github.com/Nothinc-dev-org/G-DriveXp-nautilus-ext)

## Licencia

GPL-3.0
