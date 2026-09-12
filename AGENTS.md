# AGENTS.md — Contexto Global del Proyecto G-DriveXP

## Descripción

G-DriveXP es un cliente nativo de Google Drive para Fedora Workstation/GNOME escrito en Rust. Monta un sistema de archivos virtual FUSE asíncrono, sincroniza metadatos y contenido bidireccionalmente, y se integra con el explorador Nautilus mediante una extensión nativa.

## Estructura del Repositorio

```
G-DriveXP/
├── g-drive-xp/          # Crate principal (binario daemon + GUI)
│   └── src/
│       ├── main.rs       # Punto de entrada, orquestación
│       ├── config.rs     # Configuración persistente
│       ├── auth/         # OAuth2 + GNOME Keyring
│       ├── db/           # SQLite (metadatos, sync state)
│       ├── fuse/         # Sistema de archivos FUSE (fuse3)
│       ├── gdrive/       # Cliente Google Drive API v3
│       ├── sync/         # Bootstrap + Syncer + Uploader
│       ├── mirror/       # Arquitectura Espejo (visible al usuario)
│       ├── ipc/          # Servidor IPC (Unix socket)
│       ├── gui/          # Relm4 / GTK4 / Libadwaita
│       └── utils/        # Mount, hash, cleanup
├── nautilus-ext/         # Extensión de Nautilus (cdylib)
│   └── src/
│       ├── lib.rs        # Puntos de entrada FFI
│       ├── ffi.rs        # Bindings libnautilus-extension-4
│       ├── provider.rs   # InfoProvider (emblemas)
│       ├── menu_provider.rs  # MenuProvider (acciones)
│       └── ipc_client.rs # Cliente IPC al daemon
├── docs/                 # Documentación técnica
│   ├── architecture.md   # Arquitectura completa
│   └── decisions/        # Registro de Decisiones Arquitectónicas
├── .ai/                  # Configuración de IA (skills, agents)
│   └── skills/
├── scripts/              # Scripts de utilidad
├── Plan-Desarrollo.md    # Plan técnico exhaustivo
└── install_extension.sh  # Instalador de la extensión Nautilus
```

## Reglas para Agentes de IA

1. **Lee antes de actuar**: Antes de modificar cualquier módulo, lee su `AGENTS.md` local para entender su contexto y dependencias.
2. **Separación estricta**: Código en `src/`, documentación en `docs/`, configuración de IA en `.ai/`.
3. **Nuevos módulos**: Al crear un directorio en `src/`, genera obligatoriamente un `AGENTS.md` dentro de él.
4. **Decisiones**: Registra cualquier decisión estructural en `docs/decisions/`.
5. **Fuente de verdad**: La arquitectura canónica está en `docs/architecture.md`.

## Dependencias Clave entre Módulos

```
main.rs
  ├── gui/app_model  (lanza UI, recibe AppMsg del backend)
  ├── config         (rutas, persistencia)
  ├── auth/          (OAuth2 → get_authenticator)
  ├── db/            (MetadataRepository → compartido por todos)
  ├── gdrive/        (DriveClient → compartido por sync/ y fuse/)
  ├── fuse/          (GDriveFS → monta sobre db + gdrive)
  ├── sync/          (bootstrap, syncer, uploader → db + gdrive)
  ├── mirror/        (MirrorManager → db + fuse mount)
  └── ipc/           (IpcServer → db + mirror commands)

nautilus-ext → ipc/ (protocolo compartido via Unix socket)
```

## Convenciones

- **Async**: Todo el I/O de red y filesystem es asíncrono sobre Tokio.
- **Arc compartido**: `MetadataRepository` y `DriveClient` se comparten via `Arc<T>`.
- **Mensajería**: La GUI recibe actualizaciones via `ComponentSender<AppModel>`. Los subsistemas se comunican por `mpsc::channel`.
- **Idioma**: Comentarios y logs en español. Código y nombres de variables/tipos en inglés.
