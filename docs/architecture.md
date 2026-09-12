# Arquitectura de G-DriveXP

## Visión General

G-DriveXP es un cliente nativo de Google Drive para Fedora Workstation/GNOME, escrito en Rust. Implementa un sistema de archivos virtual FUSE asíncrono, una capa de sincronización bidireccional, y una GUI reactiva con GTK4/Relm4. Se compone de dos crates:

- **`g-drive-xp`** — Binario principal (daemon + GUI)
- **`nautilus-ext`** — Extensión cdylib para el explorador de archivos Nautilus

## Diagrama de Componentes

```
┌─────────────────────────────────────────────────────────────┐
│                        Usuario                              │
│    ~/GoogleDrive/ (Mirror)          Nautilus (emblemas)      │
└────────┬────────────────────────────────────┬───────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────┐                  ┌─────────────────┐
│  MirrorManager  │                  │  nautilus-ext    │
│  (symlinks +    │                  │  (cdylib FFI)    │
│   watcher)      │                  │                  │
└────────┬────────┘                  └────────┬─────────┘
         │                                    │ IPC (Unix Socket)
         ▼                                    ▼
┌─────────────────┐                  ┌─────────────────┐
│  FUSE Mount     │                  │  IPC Server      │
│  (fuse3/tokio)  │◄────────────────►│  (bincode)       │
│  ~/GoogleDrive/ │                  └─────────────────┘
│   FUSE_Mount/   │                           │
└────────┬────────┘                           │
         │                                    │
         ▼                                    ▼
┌─────────────────────────────────────────────────────────────┐
│                  SQLite (WAL mode)                           │
│  inodes │ dentry │ attrs │ sync_state │ local_sync_*        │
└────────────────────────────┬────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Bootstrap   │   │  Syncer      │   │  Uploader    │
│  (BFS init)  │   │  (changes.   │   │  (Resumable  │
│              │   │   list poll) │   │   Upload)    │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────┐
│           Google Drive API v3 (DriveClient)                 │
│           OAuth2 (yup-oauth2 + GNOME Keyring)               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    GUI (Relm4 / GTK4 / Libadwaita)          │
│  AppModel ◄─── ComponentSender ◄─── Backend Thread          │
│  History  │  TrayIcon (ksni)                                │
└─────────────────────────────────────────────────────────────┘
```

## Módulos del Crate Principal (`g-drive-xp`)

### `main.rs` — Punto de entrada
- Inicializa logging (`tracing`).
- Lanza la aplicación Relm4 (`org.gnome.FedoraDrive`) con `adw::Application` (feature `libadwaita`).
- Expone `run_backend()`, que arranca el runtime Tokio y orquesta todos los subsistemas.

### `config` — Configuración persistente
- Rutas: `fuse_mount_path`, `mirror_path`, `cache_dir`, `db_path`.
- Persistencia en `~/.config/fedoradrive/config.json`.
- Lógica de migración automática de rutas legacy.

### `auth/` — Autenticación OAuth2
| Archivo    | Responsabilidad |
|------------|----------------|
| `oauth.rs` | Flujo OAuth2 "Installed App" con `yup-oauth2`. Servidor TCP efímero para captura de callback. Delegado `LoginUrlDelegate` que envía la URL de login a la GUI. |
| `keyring.rs` | Almacenamiento seguro de refresh tokens en GNOME Keyring via crate `keyring`. |

### `db/` — Capa de persistencia SQLite
| Archivo        | Responsabilidad |
|----------------|----------------|
| `repository.rs` | `MetadataRepository`: pool SQLite (`sqlx`), modo WAL, `busy_timeout=60s`, `synchronous=NORMAL` via `SqliteConnectOptions`. Operaciones CRUD y bulk (transacciones agrupadas en bloques de 500) sobre tablas `inodes`, `dentry`, `attrs`, `sync_state`, `local_sync_dirs`, `local_sync_files`, `sync_meta`, `dir_counters`. `soft_delete_remote`: elimina lógicamente sin marcar `dirty` (eliminaciones ya ocurridas en Drive). `clear_stale_dirty_deletes`: limpia al inicio de sesión los `dirty=1 AND deleted_at IS NOT NULL` de sesiones anteriores. Ver ADR-009, ADR-011. |
| `schema.sql`   | DDL embebido via `include_str!`. |

### `fuse/` — Sistema de archivos FUSE
| Archivo         | Responsabilidad |
|-----------------|----------------|
| `filesystem.rs` | Implementación del trait `fuse3::raw::Filesystem`. Operaciones: `init`, `lookup`, `getattr`, `readdir`, `readdirplus`, `read`, `write`, `create`, `mkdir`, `unlink`, `rmdir`, `rename`, `setattr`, `open`, `release`. Descarga bajo demanda con caché en disco. |
| `attr.rs`       | Conversión de metadatos SQLite a `FileAttr` FUSE. |
| `shortcuts.rs`  | Generación de archivos `.desktop` para Google Docs/Sheets/Slides dentro de FUSE. |

### `gdrive/` — Cliente de Google Drive API
| Archivo     | Responsabilidad |
|-------------|----------------|
| `client.rs` | `DriveClient`: wrapper sobre `google-drive3::DriveHub`. Campo `http: reqwest::Client` reutilizado para todas las llamadas HTTP directas (evita handshakes TLS repetidos). `list_all_files` usa `pageSize=1000` y compresión gzip transparente. `fetch_files_page(page_token)` retorna una sola página `(Vec<File>, Option<String>)` para escaneo progresivo. Métodos: `list_files`, `list_all_files`, `fetch_files_page`, `get_file`, `download_file`, `upload_file` (Resumable Upload), `create_folder`, `delete_file`, `get_root_file_id`, `get_changes`. `ProgressReader` para reporting de progreso. Ver ADR-009, ADR-011. |
| `error.rs`  | `DriveError`: tipado de errores de la API (quota, auth, network). |

### `sync/` — Sincronización bidireccional
| Archivo        | Responsabilidad |
|----------------|----------------|
| `bootstrap.rs` | Inicialización del árbol de metadatos. `bootstrap_level1` (primer nivel, síncrono, ~1s). `bootstrap_remaining_bfs` (escaneo progresivo page-by-page: procesa cada página de ~1,000 archivos de forma inmediata, reporta progreso a GUI via `history.set_scanning_total`, emite `MirrorCommand::Refresh` al finalizar). Ver ADR-011. |
| `syncer.rs`    | `BackgroundSyncer`: polling periódico via `changes.list` con exponential backoff. Aplica cambios remotos a SQLite y notifica al MirrorManager. |
| `uploader.rs`  | `Uploader`: escanea archivos `dirty=1` y los sube con Resumable Upload. Soporta archivos de FUSE y de Local Sync. |

### `mirror/` — Arquitectura Espejo (Mirror)
| Archivo      | Responsabilidad |
|--------------|----------------|
| `manager.rs` | `MirrorManager`: mantiene `~/GoogleDrive/` como directorio espejo visible. Los archivos "Online Only" son symlinks a FUSE, los "Local & Online" son copias reales. Recibe `MirrorCommand` via channel (desde IPC/GUI). `Refresh` pausa el watcher durante bootstrap para evitar falsos dirty. Los handlers de rename filtran `.gdrive_tmp_ops` y symlinks como defensa en profundidad. |
| `watcher.rs` | `MirrorWatcher`: monitorea cambios del filesystem local con `notify` (debounced). Detecta creaciones, ediciones, renombramientos y eliminaciones en el espejo. |

### `ipc/` — Comunicación Inter-Procesos
| Archivo     | Responsabilidad |
|-------------|----------------|
| `mod.rs`    | Protocolo IPC: tipos `IpcRequest`, `IpcResponse`, `SyncStatus`, `FileAvailability`, `FileStatusData`. Serialización con `bincode`. Socket path: `/run/user/<uid>/gdrivexp.sock`. |
| `server.rs` | `IpcServer`: servidor Unix Domain Socket. Responde a consultas de estado, disponibilidad, y comandos de cambio de modo (Online Only / Local & Online). |

### `gui/` — Interfaz Gráfica
| Archivo        | Responsabilidad |
|----------------|----------------|
| `app_model.rs` | `AppModel`: componente Relm4 principal. Estado de la aplicación, gestión de vistas (Main/Activity), actualización reactiva de widgets GTK4/Libadwaita. Recibe mensajes `AppMsg` desde el backend. Campo `scanning_total: usize`: cuando es > 0, muestra el icono de sincronización activa y el conteo de archivos escaneados; regresa a estado normal al llegar a 0. |
| `history.rs`   | `ActionHistory`: registro thread-safe de acciones (descargas, subidas, errores) y transferencias activas. `SyncProgress` incluye `scanning_total: usize` para reportar el progreso del escaneo inicial. `set_scanning_total(n)` notifica cambios a la GUI. |
| `tray.rs`      | `TrayIcon`: icono en la bandeja del sistema via `ksni` (SNI/DBus). |

### `utils/` — Utilidades
| Archivo       | Responsabilidad |
|---------------|----------------|
| `mount.rs`    | `cleanup_if_needed`, `unmount_and_wait`: gestión segura de puntos de montaje FUSE huérfanos. |
| `hash.rs`     | Cálculo de hashes MD5 para verificación de integridad. |
| `cleanup.rs`  | Limpieza de caché y datos temporales. |
| `shutdown.rs` | Coordinación de cierre graceful: `SHUTDOWN_REQUESTED` (AtomicBool), `SHUTDOWN_NOTIFY` (Notify reactivo de Tokio), `request_shutdown()`, `wait_for_shutdown()` (async puro sin polling). |

## Crate `nautilus-ext`

Extensión nativa de Nautilus compilada como `cdylib` (shared library). Se carga en el proceso de Nautilus.

| Archivo          | Responsabilidad |
|------------------|----------------|
| `lib.rs`         | Puntos de entrada FFI: `nautilus_module_initialize`, `nautilus_module_shutdown`, `nautilus_module_list_types`. |
| `ffi.rs`         | Bindings raw a `libnautilus-extension-4` (GObject/GType). |
| `provider.rs`    | `GDriveXPProvider`: implementa `NautilusInfoProvider` para mostrar emblemas de sincronización. |
| `menu_provider.rs` | Implementa `NautilusMenuProvider` para menú contextual (Online Only / Local & Online). |
| `ipc_client.rs`  | Cliente IPC que conecta al daemon via Unix socket para consultar estado de archivos. |

## Flujo de Arranque

1. `main()` inicia logging e instancia `RelmApp`.
2. La GUI llama a `run_backend()` en un hilo separado.
3. `run_backend()` crea el runtime Tokio y ejecuta secuencialmente. Las fases que implican I/O de red o esperas potencialmente largas están envueltas en la macro `or_shutdown!`, que permite cancelar la inicialización si el usuario solicita cierre antes de que FUSE esté montado (ver Flujo de Cierre):
   - Carga de `Config` (con migración automática).
   - Autenticación OAuth2 (puede abrir navegador). **Cancelable via `or_shutdown!`**.
   - Inicialización de `MetadataRepository` (SQLite WAL).
   - **Resiliencia post-crash**: si `session_active` existe en `sync_meta`, se detecta cierre no limpio y se limpian `bootstrap_complete` + `changes_page_token` + `file_cache_chunks` (tabla completa) + directorio de caché físico. Esto garantiza que no quede estado obsoleto que provoque errores 416. Ver ADR-007.
   - Creación de `DriveClient`.
   - Obtención de Root ID. **Cancelable via `or_shutdown!`**.
   - Creación de `watch` channel para coordinación BFS → MirrorManager.
   - Instanciación de `GDriveFS` y `MirrorManager` (recibe `watch::Receiver`).
   - Bootstrap de metadatos: nivel 1 (síncrono, ~1s, solo si DB vacía) → señal inmediata a MirrorManager (`bfs_ready_tx.send(true)`) → escaneo progresivo. **Post-crash: escaneo síncrono** (await) antes de montar FUSE para actualizar todos los sizes y evitar errores 416. **Normal: escaneo en background** (tokio::spawn). El escaneo procesa página a página (~1,000 archivos/página) actualizando el espejo incrementalmente. Ver ADR-011.
   - `clear_stale_dirty_deletes()`: limpia entradas dirty-delete huérfanas de sesiones anteriores antes de lanzar el Uploader.
   - Creación de `BackgroundSyncer` (60s) y **sync inicial síncrono** (`sync_once()`) para detectar cambios recientes. **Cancelable via `or_shutdown!`**.
   - Spawn de `BackgroundSyncer` y `Uploader` (30s).
   - Limpieza de mount huérfano y montaje FUSE.
   - Spawn de `MirrorManager` (post-FUSE, espera señal `watch` antes de bootstrap). Ver ADR-007.
   - Spawn de `IpcServer`.
4. `tokio::select!` espera terminación de FUSE, o señal unificada de shutdown (GUI o Señales OS capturadas internamente).

## Flujo de Cierre (Shutdown)

### Cierre durante inicialización (pre-FUSE)

Si el usuario solicita cierre mientras el backend aún está en una fase de inicialización (autenticación, bootstrap, sync inicial), la macro `or_shutdown!` detecta la señal via `wait_for_shutdown()` dentro de un `tokio::select!` y ejecuta `process::exit(0)` inmediatamente. Esto es seguro porque no existen recursos montados ni subsistemas activos que requieran cleanup.

### Cierre normal (post-FUSE)

1. La GUI (`AppMsg::Quit`) llama `utils::shutdown::request_shutdown()` — solo señaliza, no ejecuta acciones.
2. `tokio::select!` en `run_backend()` detecta la señal via `wait_for_shutdown()`.
3. Se elimina `session_active` de `sync_meta` (marca cierre limpio). Ver ADR-007.
4. Syncer, Uploader y Progress Monitor detectan el flag de shutdown y terminan sus loops.
5. `MirrorCommand::Shutdown` detiene el `MirrorWatcher` (drop) y sale de `run_loop()`.
6. Gracia de 600ms para drenar el último batch del debouncer.
7. `hide_online_only_files()` oculta symlinks OnlineOnly escribiendo `.hidden` + manifiesto `.gdrivexp_hidden_manifest` en cada directorio afectado. Ningún subsistema observa estos archivos.
8. `unmount_and_wait()` desmonta FUSE limpiamente.
9. `process::exit(0)` cierra la aplicación.

**Crítico**: La GUI NO debe llamar `process::exit()` ni desmontar FUSE directamente. Hacerlo causa race conditions donde el proceso muere antes de completar `hide_online_only_files`. Ver ADR-006.

**Crítico**: Los archivos `.hidden` y `.gdrivexp_hidden_manifest` son artefactos internos del shutdown. Están filtrados en 5 capas defensivas (watcher events, process_local_change, escaneo recursivo, shutdown del watcher, y el Uploader) para que nunca se registren en la DB ni se sincronicen con Google Drive. Ver ADR-006 Rev 2.

## Estrategia de Datos

- **Metadatos primero**: se sincronizan metadatos al inicio, el contenido se descarga bajo demanda.
- **Caché en disco**: `~/.cache/fedoradrive/` almacena contenido descargado.
- **Write-back**: escrituras locales se marcan `dirty=1` y se suben en background.
- **Consistencia eventual**: el Syncer aplica cambios remotos periódicamente; el Mirror refleja el estado actualizado.

## Rutas del Sistema

| Ruta | Propósito |
|------|-----------|
| `~/GoogleDrive/` | Directorio espejo visible al usuario |
| `~/GoogleDrive/FUSE_Mount/` | Punto de montaje FUSE (oculto via `.hidden`) |
| `~/.config/fedoradrive/config.json` | Configuración |
| `~/.config/fedoradrive/metadata.db` | Base de datos SQLite |
| `~/.config/fedoradrive/tokens.json` | Tokens OAuth2 |
| `~/.cache/fedoradrive/` | Caché de contenido |
| `/run/user/<uid>/gdrivexp.sock` | Socket IPC |

## Stack Tecnológico

| Componente | Tecnología | Justificación |
|------------|-----------|---------------|
| Lenguaje | Rust (Edition 2024) | Seguridad de memoria sin GC |
| Async Runtime | Tokio | Ecosistema maduro, requerido por fuse3 |
| Filesystem | fuse3 (unprivileged) | API async, readdirplus, integración Tokio |
| Database | SQLite via sqlx | WAL mode, ACID, consultas async |
| API Client | google-drive3 + yup-oauth2 | Bindings oficiales |
| GUI | GTK4 + Libadwaita + Relm4 (`libadwaita` feature) | Nativo GNOME, patrón MVU. Feature requerido para `adw::init()` e integración con dock. Ver ADR-008 |
| Secrets | keyring (libsecret) | Integración con GNOME Keyring |
| IPC | Unix Domain Socket + bincode | Baja latencia, serialización binaria |
| File Watching | notify + debouncer | Multiplataforma, debounced events |
| Testing | rstest | Fixtures parametrizados y casos tabulados para tests unitarios |
| System Tray | ksni | Protocolo SNI/DBus |
| Desktop Integration | `.desktop` file + symlink en `~/.local/bin/` | GIO valida `Exec`; sin binario en PATH descarta el `.desktop`. Ver ADR-008 |
