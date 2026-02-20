# FedoraDrive-rs

Cliente nativo de Google Drive para Fedora Workstation desarrollado en Rust.

## 🚀 Características

- **Sistema de archivos virtual** usando FUSE3 asíncrono
- **Sincronización inteligente** con caché de metadatos SQLite
- **Interfaz nativa GNOME** con GTK4 y Libadwaita
- **Autenticación segura** OAuth2 con almacenamiento en GNOME Keyring
- **Integración con Nautilus** mediante emblemas de estado
- **Alto rendimiento** gracias a Rust y arquitectura asíncrona

## 📋 Requisitos Previos

- **Fedora Workstation** 39 o superior
- **Rust** 1.85+ (Edition 2024)
- Bibliotecas de desarrollo del sistema (ver INSTALL.md)

## 🔧 Instalación

### 1. Instalar dependencias del sistema

```bash
sudo dnf install -y \
    rust cargo sqlite-devel gtk4-devel \
    libadwaita-devel fuse3-devel glib2-devel \
    gobject-introspection-devel cairo-gobject-devel \
    pango-devel gdk-pixbuf2-devel graphene-devel \
    openssl-devel pkg-config
```

Para más detalles, consulte [INSTALL.md](./INSTALL.md).

### 2. Compilar el proyecto

```bash
git clone <repository-url>
cd g-drive-xp
cargo build --release
```

### 3. Configurar credenciales OAuth2

1. Vaya a [Google Cloud Console](https://console.cloud.google.com)
2. Cree un nuevo proyecto
3. Habilite la API de Google Drive
4. Cree credenciales OAuth2 para "Aplicación de escritorio"
5. Descargue el archivo `credentials.json`
6. Colóquelo en la raíz del proyecto como `credentials.json`

### 4. Configurar FUSE (Opcional, recomendado para Flatpak)

Para que aplicaciones de terceros (reproductores, navegadores) puedan acceder al sistema de archivos:

1. Descomente `user_allow_other` en `/etc/fuse.conf`.
2. Asegúrese de que su usuario esté en el grupo `fuse`.

Consulte [INSTALL.md](./INSTALL.md) para instrucciones detalladas.

## ⚠️ Limitaciones Conocidas

- **Resolución de Enlaces Simbólicos**: Algunas aplicaciones modernas de GNOME (especialmente aquellas basadas en GTK4/GJS como **GNOME Decibels**) pueden fallar al abrir archivos desde la carpeta principal del espejo. Esto se debe a restricciones de seguridad que impiden seguir enlaces simbólicos (`OnlineOnly`) hacia el sistema de archivos FUSE.
  - **Solución**: Navegue directamente a `~/GoogleDrive/FUSE_Mount/` para una compatibilidad total, o sincronice el archivo a local para convertirlo en un archivo real.

## 🎯 Uso

```bash
# Ejecutar el daemon
./target/release/g-drive-xp

# El sistema de archivos se montará automáticamente en:
~/GoogleDrive
```

## 📁 Estructura del Proyecto

```
g-drive-xp/
├── src/
│   ├── auth/           # Autenticación OAuth2 y Keyring
│   │   ├── oauth.rs
│   │   ├── keyring.rs
│   │   └── mod.rs
│   ├── db/             # Gestión de base de datos SQLite
│   ├── fuse/           # Implementación del sistema de archivos
│   ├── sync/           # Lógica de sincronización
│   ├── ui/             # Interfaz GTK4/Relm4
│   ├── config.rs       # Configuración persistente
│   └── main.rs         # Punto de entrada
├── Cargo.toml
├── INSTALL.md          # Guía de instalación detallada
└── README.md
```

## 🏗️ Estado del Desarrollo

**Fase Actual**: Fase 5 - Pulido, Documentación y Empaquetado 🚀

- [x] Fase 1: Autenticación OAuth2 y GNOME Keyring ✅
- [x] Fase 2: Núcleo FUSE Asíncrono (Lectura/Escritura/readdirplus) ✅
- [x] Fase 3: Interfaz GTK4/Libadwaita (Relm4, Tray Icon, Historial) ✅
- [x] Fase 4: Sincronización Bidireccional y Gestión de Conflictos ✅
- [ ] Fase 5: Empaquetado RPM/Flatpak y optimizaciones finales 🏗️

## 📚 Documentación

- [Plan de Desarrollo](./Plan-Desarrollo.md) - Documento técnico exhaustivo
- [INSTALL.md](./INSTALL.md) - Instalación de dependencias
- [Implementación](/.gemini/antigravity/brain/.../implementation_plan.md) - Plan detallado

## 🤝 Contribución

Este proyecto está en desarrollo activo. Las contribuciones son bienvenidas.

## 📄 Licencia

GNU General Public License v3.0 (GPL-3.0)

## 🔗 Referencias

- [Documentación de FUSE3](https://docs.rs/fuse3)
- [GTK4 para Rust](https://gtk-rs.org/)
- [Google Drive API v3](https://developers.google.com/drive/api/v3)
- [Relm4](https://relm4.org/)
