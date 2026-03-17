use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error, warn};

use crate::db::MetadataRepository;
use crate::gui::history::{ActionHistory, ActionType, TransferOp};

const HIDDEN_MANIFEST: &str = ".gdrivexp_hidden_manifest";

/// Comandos para el MirrorManager (desde IPC o GUI)
#[derive(Debug)]
pub enum MirrorCommand {
    /// Convertir archivo real a Symlink (Liberar espacio)
    SetOnlineOnly { path: String },
    /// Descargar archivo real (Mantener local)
    SetLocalOnline { path: String },
    /// Reprocesar todo el directorio espejo
    #[allow(dead_code)]
    Refresh,
    /// Notificación de archivos eliminados en Google Drive
    RemoteDeleted { paths: Vec<String> },
    /// Detener watcher y salir del run_loop (previo a shutdown)
    Shutdown,
}

use crate::mirror::watcher::MirrorWatcher;
use notify_debouncer_full::DebouncedEvent;
use notify::{EventKind, event::{ModifyKind, RenameMode}};

#[derive(Clone)]
struct MirrorContext {
    db: Arc<MetadataRepository>,
    mirror_path: PathBuf,
    fuse_mount_path: PathBuf,
    history: ActionHistory,
}

/// Gestor principal de la arquitectura Espejo
/// Mantiene la sincronización entre el direcotrio visible (Mirror) y el montaje FUSE oculto.
pub struct MirrorManager {
    ctx: Arc<MirrorContext>,
    command_rx: mpsc::Receiver<MirrorCommand>,
    watcher_rx: mpsc::Receiver<Vec<DebouncedEvent>>,
    watcher_tx: mpsc::Sender<Vec<DebouncedEvent>>, // Needed to spawn watcher
    watcher: Option<MirrorWatcher>,
    bfs_ready_rx: tokio::sync::watch::Receiver<bool>,
}

impl MirrorManager {
    pub fn new(
        db: Arc<MetadataRepository>,
        mirror_path: PathBuf,
        fuse_mount_path: PathBuf,
        history: ActionHistory,
        bfs_ready_rx: tokio::sync::watch::Receiver<bool>,
    ) -> (Self, mpsc::Sender<MirrorCommand>) {
        let (tx, rx) = mpsc::channel(32);
        let (w_tx, w_rx) = mpsc::channel(100);

        let ctx = Arc::new(MirrorContext {
            db,
            mirror_path,
            fuse_mount_path,
            history,
        });

        let manager = Self {
            ctx,
            command_rx: rx,
            watcher_rx: w_rx,
            watcher_tx: w_tx,
            watcher: None,
            bfs_ready_rx,
        };

        (manager, tx)
    }

    /// Inicia el gestor en segundo plano
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Mitigación de Race Condition:
            // Esperar un momento a que FUSE esté totalmente listo y montado por el Kernel
            // aunque main.rs ya esperó al montaje, el sistema de archivos puede tardar ms en ser visible.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            
            info!("🪞 MirrorManager iniciado (Deferred & Async Bootstrap)");
            info!("   Mirror: {:?}", self.ctx.mirror_path);
            info!("   FUSE:   {:?}", self.ctx.fuse_mount_path);

            // Esperar a que BFS complete antes de iniciar el bootstrap del espejo.
            // En startup normal (bootstrap_complete ya existe), el canal ya tiene true
            // y esta espera retorna de inmediato sin latencia adicional.
            info!("⏳ Esperando a que BFS complete antes de iniciar bootstrap del espejo...");
            while !*self.bfs_ready_rx.borrow() {
                if self.bfs_ready_rx.changed().await.is_err() {
                    warn!("Canal BFS cerrado inesperadamente, procediendo con bootstrap.");
                    break;
                }
            }
            info!("✅ BFS listo. Procediendo con bootstrap del espejo.");

            // 1. Initial Scan / Bootstrap (SEQUENTIAL)
            // Ejecutamos bootstrap ANTES de iniciar el watcher para que las correcciones
            // (como reparar symlinks) no generen eventos de "Eliminación" que el watcher
            // malinterprete como acciones del usuario.
            info!("🔄 Ejecutando Bootstrap (Reparación de estado)...");
            if let Err(e) = Self::run_bootstrap(self.ctx.clone()).await {
                error!("Error durante bootstrap: {:?}", e);
            }
            info!("✅ Bootstrap completado. Iniciando vigilancia.");

            // 2. Iniciar Watcher
            let w_tx = self.watcher_tx.clone();
            let mirror_path = self.ctx.mirror_path.clone();
            
            info!("👷 Despachando inicialización del Watcher a thread pool dedicado (blocking)...");
            let watcher_result = tokio::task::spawn_blocking(move || {
                MirrorWatcher::new(&mirror_path, w_tx)
            }).await;

            match watcher_result {
                Ok(Ok(w)) => {
                    info!("👀 Watcher activado exitosamente");
                    self.watcher = Some(w);
                },
                Ok(Err(e)) => {
                    error!("❌ Error inicializando watcher (Lógica): {:?}", e);
                },
                Err(e) => {
                    error!("❌ Error fatal en task::spawn_blocking del watcher: {:?}", e);
                }
            }

            self.run_loop().await;
        })
    }

    /// Reconcilia el estado del sistema de archivos visible con la base de datos
    // Función estática asociada que corre independiente del estado mut del manager
    async fn run_bootstrap(ctx: Arc<MirrorContext>) -> Result<()> {
        info!("🔄 Iniciando bootstrap del espejo...");

        // Restaurar archivos OnlineOnly ocultados por un shutdown previo
        restore_hidden_online_only_files(&ctx.mirror_path).await;

        // Pequeña pausa adicional de seguridad
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // PASO 0: Crear todos los directorios activos, incluyendo los vacíos.
        // get_all_active_files() filtra is_dir=0, así que directorios sin archivos
        // nunca se crearían como padres implícitos. Lo hacemos aquí explícitamente.
        let dirs = ctx.db.get_all_active_dirs().await?;
        info!("📁 Se encontraron {} directorios activos en DB", dirs.len());
        for (_inode, relative_path) in dirs {
            let mirror_dir = ctx.mirror_path.join(&relative_path);
            if tokio::fs::symlink_metadata(&mirror_dir).await.is_err() {
                if let Err(e) = tokio::fs::create_dir_all(&mirror_dir).await {
                    warn!("No se pudo crear directorio {:?}: {:?}", mirror_dir, e);
                }
            }
        }

        let files = ctx.db.get_all_active_files().await?;
        let total = files.len();
        info!("📂 Se encontraron {} archivos activos en DB", total);

        let mut processed = 0u64;
        let mut repaired = 0u64;

        for (_inode, relative_path, availability) in files {
            processed += 1;
            if processed % 5000 == 0 || processed == total as u64 {
                info!("🔄 Bootstrap progreso: {}/{} archivos verificados ({} reparados)", processed, total, repaired);
            }

            let mirror_file = ctx.mirror_path.join(&relative_path);

            // Asegurar que el directorio padre existe (async, no sigue symlinks)
            if let Some(parent) = mirror_file.parent() {
                if tokio::fs::symlink_metadata(parent).await.is_err() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
            }

            // Una sola llamada async para obtener metadata del mirror file (lstat, NO sigue symlinks)
            let meta = tokio::fs::symlink_metadata(&mirror_file).await;
            let file_exists = meta.is_ok();
            let is_symlink = meta.as_ref().map(|m| m.is_symlink()).unwrap_or(false);
            // is_file() sobre symlink_metadata retorna true solo para archivos reales, no symlinks
            let is_real_file = meta.as_ref().map(|m| m.is_file()).unwrap_or(false);

            match availability.as_str() {
                "online_only" => {
                    let should_recreate = if is_symlink {
                        match tokio::fs::read_link(&mirror_file).await {
                            Ok(target) => {
                                // Si no empieza con el path de montaje actual, está roto/obsoleto
                                !target.starts_with(&ctx.fuse_mount_path)
                            },
                            Err(_) => true,
                        }
                    } else {
                        !file_exists
                    };

                    if should_recreate {
                        // bubble=false: no burbujear dir_counters individualmente; se reconstruyen al final.
                        Self::static_handle_set_online_only_opt(&ctx, &mirror_file.to_string_lossy(), false).await;
                        repaired += 1;
                    } else if is_real_file {
                        // Caso delicado: DB dice OnlineOnly, FS tiene archivo real (no symlink).
                        warn!("CONFLICTO: DB dice OnlineOnly pero existe archivo local: {:?}", relative_path);
                    }
                }
                "local_online" => {
                    // Queremos archivo real. Si es symlink o no existe, reparamos.
                    if !file_exists || is_symlink {
                        // bubble=false: no burbujear dir_counters individualmente; se reconstruyen al final.
                        Self::static_handle_set_local_online_opt(&ctx, &mirror_file.to_string_lossy(), false).await;
                        repaired += 1;
                    }
                }
                _ => {}
            }
        }
        info!("🔄 Bootstrap: {} archivos verificados, {} reparados", processed, repaired);
        // PASO 3: LIMPIEZA DE SYMLINKS OBSOLETOS EN ROOT
        // En versiones anteriores, los archivos compartidos (owned_by_me=0) se colocaban
        // en la raíz del espejo. Ahora se colocan bajo SHARED/. Este paso detecta y elimina
        // los symlinks "huérfanos" que ya no corresponden a ninguna ruta en la DB de root.
        info!("🧹 Inspeccionando symlinks obsoletos en el root del espejo...");

        // Construir un Set de todas las rutas relativas válidas en la raíz (sin prefijo SHARED/)
        let valid_root_names: std::collections::HashSet<String> = {
            let all_files = ctx.db.get_all_active_files().await.unwrap_or_default();
            let all_dirs = ctx.db.get_all_active_dirs().await.unwrap_or_default();
            let mut set: std::collections::HashSet<String> = all_files.into_iter()
                .filter_map(|(_, path, _)| {
                    // Solo nombres en root (sin separador '/')
                    if !path.contains('/') { Some(path) } else { None }
                })
                .collect();
            // Añadir también directorios de root válidos
            for (_, path) in all_dirs {
                if !path.contains('/') {
                    set.insert(path);
                }
            }
            // SHARED/ es siempre válido (directorio virtual)
            set.insert("SHARED".to_string());
            set
        };

        if let Ok(mut root_entries) = tokio::fs::read_dir(&ctx.mirror_path).await {
            let mut stale_count = 0;
            while let Ok(Some(entry)) = root_entries.next_entry().await {
                let path = entry.path();

                // Solo nos interesan los symlinks del root
                let meta = match tokio::fs::symlink_metadata(&path).await {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if !meta.is_symlink() { continue; }

                let name = path.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();

                // Ignorar entradas del sistema y temporales
                if name.starts_with('.') { continue; }

                if !valid_root_names.contains(&name) {
                    warn!("🧹 Eliminando symlink obsoleto del root: {}", name);
                    if let Err(e) = tokio::fs::remove_file(&path).await {
                        warn!("   No se pudo eliminar {:?}: {:?}", path, e);
                    } else {
                        stale_count += 1;
                    }
                }
            }
            if stale_count > 0 {
                info!("🧹 {} symlinks obsoletos eliminados del root.", stale_count);
            }
        }

        // PASO FINAL: Reconstruir dir_counters en una sola pasada CTE.
        // Durante el bootstrap, las llamadas a set_availability usaron bubble=false
        // para evitar N burbujeos individuales. Consolidamos todo aquí.
        if let Err(e) = ctx.db.rebuild_all_dir_counters().await {
            error!("Error reconstruyendo dir_counters post-bootstrap: {:?}", e);
        }

        info!("✅ Bootstrap completado");
        Ok(())
    }

    async fn run_loop(&mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.command_rx.recv() => {
                    tracing::info!("🪞 MirrorManager recibió comando: {:?}", cmd);
                    match cmd {
                        MirrorCommand::SetOnlineOnly { path } => {
                            Self::static_handle_set_online_only(&self.ctx, &path).await;
                        }
                        MirrorCommand::SetLocalOnline { path } => {
                            Self::static_handle_set_local_online(&self.ctx, &path).await;
                        }
                        MirrorCommand::Refresh => {
                             let ctx_refresh = self.ctx.clone();
                             tokio::spawn(async move {
                                 let _ = Self::run_bootstrap(ctx_refresh).await;
                             });
                        }
                        MirrorCommand::RemoteDeleted { paths } => {
                            for relative in paths {
                                let path_to_check = self.ctx.mirror_path.join(&relative);
                                if path_to_check.exists() || tokio::fs::symlink_metadata(&path_to_check).await.is_ok() {
                                    tracing::info!("🗑️ MirrorManager eliminando reflejo obsoleto de eliminación remota: {:?}", path_to_check);
                                    if let Ok(meta) = tokio::fs::symlink_metadata(&path_to_check).await {
                                        if meta.is_dir() {
                                            let _ = tokio::fs::remove_dir_all(&path_to_check).await;
                                        } else {
                                            let _ = tokio::fs::remove_file(&path_to_check).await;
                                        }
                                    }
                                }
                            }
                        }
                        MirrorCommand::Shutdown => {
                            tracing::info!("🛑 MirrorManager: Shutdown recibido, deteniendo watcher...");
                            // Dropear el watcher detiene la vigilancia del filesystem
                            self.watcher.take();
                            tracing::info!("🛑 MirrorManager: Watcher detenido. Saliendo de run_loop.");
                            return;
                        }
                    }
                }
                Some(events) = self.watcher_rx.recv() => {
                    self.handle_fs_events(events).await;
                }
                else => {
                    break;
                }
            }
        }
        tracing::warn!("🪞 MirrorManager run_loop terminó (channel cerrado)");
    }

    // Funciones estáticas que reciben contexto en lugar de &self
    
    async fn static_handle_set_online_only(ctx: &MirrorContext, path_str: &str) {
        Self::static_handle_set_online_only_opt(ctx, path_str, true).await;
    }

    /// `bubble`: si es true, propaga cambios de dir_counters (runtime normal).
    /// Si es false, omite el burbujeo (bootstrap masivo — se invoca rebuild al final).
    async fn static_handle_set_online_only_opt(ctx: &MirrorContext, path_str: &str, bubble: bool) {
        let path = PathBuf::from(path_str);
        tracing::info!("🪞 Procesando SetOnlineOnly para: {:?}", path);

        // 1. Validar que el path está dentro del mirror
        if !path.starts_with(&ctx.mirror_path) {
            warn!("Intento de modificar archivo fuera del mirror: {}", path_str);
            return;
        }

        match tokio::fs::symlink_metadata(&path).await {
            Ok(meta) => {
                if meta.is_dir() {
                    let name_display = path.file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| path_str.to_string());
                    ctx.history.log(ActionType::Sync, format!("Liberando espacio en carpeta: {}", name_display));

                    let mut stack = vec![path.clone()];
                    while let Some(current_dir) = stack.pop() {
                        if let Ok(mut entries) = tokio::fs::read_dir(&current_dir).await {
                            while let Ok(Some(entry)) = entries.next_entry().await {
                                let child_path = entry.path();
                                if let Ok(m) = entry.file_type().await {
                                    if m.is_dir() {
                                        stack.push(child_path);
                                    } else {
                                        Self::do_handle_set_online_only_opt(ctx, &child_path, false, bubble).await;
                                    }
                                }
                            }
                        }
                    }
                    return;
                }
            },
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    tracing::warn!("Archivo no encontrado en disco, intentando reparar symlink: {:?}", e);
                } else {
                    error!("Error leyendo metadata de archivo: {:?}", e);
                    return;
                }
            }
        }

        Self::do_handle_set_online_only_opt(ctx, &path, true, bubble).await;
    }

    async fn do_handle_set_online_only_opt(ctx: &MirrorContext, path: &PathBuf, log_history: bool, bubble: bool) {
        // 2. Calcular path relativo y path FUSE
        let relative = match path.strip_prefix(&ctx.mirror_path) {
            Ok(p) => p,
            Err(_) => return,
        };

        let fuse_path = ctx.fuse_mount_path.join(relative);

        // 3. Database is source of truth - No FUSE access to avoid deadlock

        // If inode exists in DB with valid gdrive_id, file WILL exist in FUSE when accessed
        // If inode exists in DB with valid gdrive_id, file WILL exist in FUSE when accessed
        if let Ok(Some(inode)) = ctx.db.resolve_relative_path_to_inode(relative.to_str().unwrap_or("")).await {
            // Verificar si el archivo es puramente local (aún no subido)
            let gdrive_id: Option<String> = sqlx::query_scalar("SELECT gdrive_id FROM inodes WHERE inode = ?")
                .bind(inode as i64)
                .fetch_optional(ctx.db.pool())
                .await
                .unwrap_or(None);

            if let Some(id) = gdrive_id {
                if id.starts_with("temp_") {
                    let file_name = path.file_name()
                        .map(|f| f.to_string_lossy())
                        .unwrap_or_else(|| "unknown".into());
                    warn!("Intento bloqueado de liberar espacio de archivo local no sincronizado: {:?}", path);
                    if log_history {
                        ctx.history.log(ActionType::Error, format!("No se puede liberar: {} (Pendiente a subir)", file_name));
                    }
                    return;
                }
            }
        }

        // 5. ATOMIC SYMLINK SWAP (EXTERNAL TEMP DIR STRATEGY)
        // Usamos un directorio temporal FUERA de la vista actual para evitar que Nautilus refresque
        // la lista de archivos mientras preparamos el reemplazo.
        let temp_dir_root = ctx.mirror_path.join(".gdrive_tmp_ops");
        if let Err(e) = tokio::fs::create_dir_all(&temp_dir_root).await {
             error!("No se pudo crear directorio temporal de operaciones: {:?}", e);
             return;
        }

        let file_name = path.file_name()
            .map(|f| f.to_string_lossy())
            .unwrap_or_else(|| "unknown".into());

        // Usar UUID o random, pero por simplicidad usaremos un prefijo único simple
        // para evitar colisiones si hay múltiples operaciones en el mismo archivo.
        let unique_name = format!("{}.{}.link.tmp", uuid::Uuid::new_v4(), file_name);
        let tmp_symlink_path = temp_dir_root.join(&unique_name);

        let fuse_path_clone = fuse_path.clone();
        let tmp_path_clone = tmp_symlink_path.clone();

        tracing::debug!("🪞 Creando symlink temporal en zona segura: {:?} -> {:?}", tmp_symlink_path, fuse_path);

        // Crear el symlink en ubicación temporal externa (blocking)
        let create_result = tokio::task::spawn_blocking(move || {
            std::os::unix::fs::symlink(&fuse_path_clone, &tmp_path_clone)
        }).await;

        match create_result {
            Ok(Ok(())) => {
                // 6. ACTUALIZAR DB ANTES DEL RENAME
                if let Ok(Some(inode)) = ctx.db.resolve_relative_path_to_inode(relative.to_str().unwrap_or("")).await {
                    if let Err(e) = ctx.db.set_availability(inode, "online_only", bubble).await {
                        warn!("Error actualizando disponibilidad en DB para {:?}: {:?}", relative, e);
                    }
                } else {
                    warn!("No se pudo resolver inode para actualizar DB: {:?}", relative);
                }

                tracing::debug!("🪞 Symlink creado. Ejecutando intercambio atómico trans-directorio...");
                
                // Renombrar el symlink temporal (externo) sobre el archivo original (ATOMIC Move)
                // Al venir desde fuera del directorio observado, Nautilus ve esto como una actualización
                // directa del nodo, sin ruido previo de creación.
                if let Err(e) = tokio::fs::rename(&tmp_symlink_path, &path).await {
                    error!("Error en intercambio atómico de symlink: {:?}", e);
                    let _ = tokio::fs::remove_file(&tmp_symlink_path).await;
                    return;
                }
                
                if log_history {
                    let name_display = path.file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| path.to_string_lossy().into_owned());
                    ctx.history.log(ActionType::Sync, format!("Solo online: {}", name_display));
                }
                info!("☁️ Espacio liberado (External Temp): {:?}", relative);

                // 7. FORCE NAUTILUS REFRESH
                // El rename atómico desde fuera a veces es tan limpio que Nautilus no refresca el emblema.
                // Disparamos un evento IN_ATTRIB extra para despertar la UI.
                let path_clone = path.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    
                    // CRITICAL FIX: Usar lutimes para tocar el SYMLINK mismo, no el target.
                    // set_permissions sigue symlinks, lo cual actualiza el archivo oculto en FUSE
                    // pero no el archivo visible en Mirror, por lo que Nautilus no se entera.
                    use std::os::unix::ffi::OsStrExt;
                    let c_path = std::ffi::CString::new(path_clone.as_os_str().as_bytes());
                    
                    if let Ok(c_p) = c_path {
                        unsafe {
                            // lutimes(path, NULL) actualiza atime/mtime a "ahora" SIN seguir symlinks.
                            // Esto genera IN_ATTRIB sobre el symlink visible.
                            if libc::lutimes(c_p.as_ptr(), std::ptr::null()) != 0 {
                                tracing::debug!("Error en lutimes (touch symlink) para refresh");
                            }
                        }
                    }
                });
            }
            Ok(Err(e)) => {
                error!("Error creando symlink temporal: {:?}", e);
            }
            Err(e) => {
                error!("Error en spawn_blocking (symlink): {:?}", e);
            }
        }
    }

    async fn static_handle_set_local_online(ctx: &MirrorContext, path_str: &str) {
        Self::static_handle_set_local_online_opt(ctx, path_str, true).await;
    }

    /// `bubble`: si es true, propaga cambios de dir_counters (runtime normal).
    /// Si es false, omite el burbujeo (bootstrap masivo — se invoca rebuild al final).
    async fn static_handle_set_local_online_opt(ctx: &MirrorContext, path_str: &str, bubble: bool) {
        let path = PathBuf::from(path_str);
        tracing::info!("🪞 Procesando SetLocalOnline para: {:?}", path);

        // 1. Validar path
        if !path.starts_with(&ctx.mirror_path) {
            tracing::warn!("Path fuera del mirror");
            return;
        }

        match tokio::fs::symlink_metadata(&path).await {
            Ok(meta) => {
                if meta.is_dir() {
                    let name_display = path.file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| path_str.to_string());
                    ctx.history.log(ActionType::Download, format!("Descargando carpeta: {}", name_display));

                    let mut stack = vec![path.clone()];
                    while let Some(current_dir) = stack.pop() {
                        if let Ok(mut entries) = tokio::fs::read_dir(&current_dir).await {
                            while let Ok(Some(entry)) = entries.next_entry().await {
                                let child_path = entry.path();
                                if let Ok(m) = entry.file_type().await {
                                    if m.is_dir() {
                                        stack.push(child_path);
                                    } else {
                                        Self::do_handle_set_local_online_opt(ctx, &child_path, false, bubble).await;
                                    }
                                }
                            }
                        }
                    }
                    return;
                }
            },
            Err(_) => {} // Proceed as normal if error, maybe file doesn't exist but is in DB
        }

        Self::do_handle_set_local_online_opt(ctx, &path, true, bubble).await;
    }

    async fn do_handle_set_local_online_opt(ctx: &MirrorContext, path: &PathBuf, log_history: bool, bubble: bool) {
        let relative = match path.strip_prefix(&ctx.mirror_path) {
           Ok(p) => p,
           Err(_) => return,
        };

        let fuse_path = ctx.fuse_mount_path.join(relative);
        tracing::debug!("🪞 Fuse path objetivo: {:?}", fuse_path);

        let meta = tokio::fs::symlink_metadata(&path).await;
        if let Ok(m) = meta {
            if !m.is_symlink() && m.is_file() {
                info!("El archivo ya es local y real: {:?}", relative);
                // Asegurar que DB esté sincronizada
                 if let Ok(Some(inode)) = ctx.db.resolve_relative_path_to_inode(relative.to_str().unwrap_or("")).await {
                    let _ = ctx.db.set_availability(inode, "local_online", bubble).await;
                }
                return;
            }
        }

        // 2. Database is source of truth - If DB has inode, we proceed
        // FUSE will serve the file on-demand when accessed

        if log_history {
            let name_display = path.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| path.to_string_lossy().into_owned());
            ctx.history.log(ActionType::Download, format!("Descargando: {}", name_display));
        }
        info!("📥 Iniciando descarga: {:?}", relative);

        // 3. Copiar contenido usando spawn_blocking (evitar bloqueo de runtime)
        // La lectura de FUSE es bloqueante, debe ejecutarse en thread separado
        // 3. Copiar contenido usando spawn_blocking (evitar bloqueo de runtime)
        // Usamos directorio externo para la descarga
                // 5. ATOMIC SYMLINK SWAP (EXTERNAL TEMP DIR STRATEGY)
        // Usamos un directorio temporal FUERA de la vista actual para evitar que Nautilus refresque
        // la lista de archivos mientras preparamos el reemplazo.
        let temp_dir_root = ctx.mirror_path.join(".gdrive_tmp_ops");
        if let Err(e) = tokio::fs::create_dir_all(&temp_dir_root).await {
             error!("No se pudo crear directorio temporal de operaciones: {:?}", e);
             return;
        }

        let file_name = path.file_name()
            .map(|f| f.to_string_lossy())
            .unwrap_or_else(|| "unknown".into());

        let unique_name = format!("{}.{}.tmp_download", uuid::Uuid::new_v4(), file_name);

        let tmp_path = temp_dir_root.join(&unique_name);

        let fuse_path_clone = fuse_path.clone();
        let tmp_path_copy = tmp_path.clone();

        let total_bytes = tokio::fs::metadata(&fuse_path).await.map(|m| m.len()).unwrap_or(0);
        let transfer_id = ctx.history.start_transfer(file_name.to_string(), TransferOp::Download, total_bytes);
        let history_clone = ctx.history.clone();

        tracing::debug!("🪞 Copiando {:?} -> {:?}", fuse_path, tmp_path);
        let copy_result = tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};
            let mut src = std::fs::File::open(&fuse_path_clone)?;
            let mut dst = std::fs::File::create(&tmp_path_copy)?;

            let mut buffer = vec![0u8; 2 * 1024 * 1024]; // 2MB buffer
            let mut copied = 0u64;

            loop {
                let n = src.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                dst.write_all(&buffer[..n])?;
                copied += n as u64;
                history_clone.update_transfer_progress(transfer_id, copied);
            }
            dst.sync_all()?;
            Ok::<u64, std::io::Error>(copied)
        }).await;

        ctx.history.complete_transfer(transfer_id);

        match copy_result {
            Ok(Ok(_)) => {
                tracing::debug!("🪞 Copia finalizada. Preparando intercambio...");
            }
            Ok(Err(e)) => {
                error!("Error descargando archivo desde FUSE: {:?}", e);
                let _ = tokio::fs::remove_file(&tmp_path).await;
                return;
            }
            Err(e) => {
                error!("Error en spawn_blocking: {:?}", e);
                let _ = tokio::fs::remove_file(&tmp_path).await;
                return;
            }
        }

        // 4. ACTUALIZAR DB ANTES DEL SWAP FINAL
        if let Ok(Some(inode)) = ctx.db.resolve_relative_path_to_inode(relative.to_str().unwrap_or("")).await {
            if let Err(e) = ctx.db.set_availability(inode, "local_online", bubble).await {
                warn!("Error actualizando disponibilidad en DB para {:?}: {:?}", relative, e);
            }
        }

        // 5. Mover TMP a Real (Atomic Replace)
        if let Err(e) = tokio::fs::rename(&tmp_path, &path).await {
              error!("Error moviendo archivo descargado a destino final: {:?}", e);
              let _ = tokio::fs::remove_file(&tmp_path).await;
        } else {
              if log_history {
                  let name_display = path.file_name()
                      .map(|n| n.to_string_lossy().to_string())
                      .unwrap_or_else(|| path.to_string_lossy().into_owned());
                  ctx.history.log(ActionType::Download, format!("Descargado: {}", name_display));
              }
              info!("✅ Archivo descargado exitosamente (External Temp): {:?}", relative);

              // 6. FORCE NAUTILUS REFRESH
              let path_clone = path.clone();
              tokio::spawn(async move {
                  tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                  if let Ok(metadata) = tokio::fs::metadata(&path_clone).await {
                        let perms = metadata.permissions();
                        let _ = tokio::fs::set_permissions(&path_clone, perms).await;
                  }
              });
        }
    }

    /// Procesa eventos del sistema de archivos (Watcher)
    async fn handle_fs_events(&self, events: Vec<DebouncedEvent>) {
        for debounced_event in events {
            let event = debounced_event.event;
            let paths = event.paths;
            
            // 1. Manejo Inteligente de Renombrados/Movimientos
            match event.kind {
                // Caso A: Renombrado completo (Source + Dest) detectado por Notify
                EventKind::Modify(ModifyKind::Name(RenameMode::Both)) if paths.len() == 2 => {
                    self.handle_local_rename(&paths[0], &paths[1]).await;
                    continue;
                }
                // Caso B: "Rename From" (Mover FUERA del espejo o a la papelera)
                EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                    for path in paths {
                        if let Ok(relative) = path.strip_prefix(&self.ctx.mirror_path) {
                            self.handle_local_delete(&relative.to_string_lossy()).await;
                        }
                    }
                    continue;
                }
                // Caso C: "Rename To" (Mover DESDE FUERA al espejo)
                EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                    for path in paths {
                        if let Ok(relative) = path.strip_prefix(&self.ctx.mirror_path) {
                            if let Ok(meta) = tokio::fs::symlink_metadata(&path).await {
                                self.handle_local_change(&path, &relative.to_string_lossy(), meta.is_dir()).await;
                            }
                        }
                    }
                    continue;
                }
                _ => {}
            }

            // 2. Procesar otros eventos (Create, Modify, Remove)
            for path in paths {
                // 1. Filtrar eventos fuera de interés
                // Ignorar .gdrive_tmp_ops y el punto de montaje oculto
                let path_str = path.to_string_lossy();
                if path_str.contains(".gdrive_tmp_ops") || path_str.contains(".cloud_mount") {
                    continue;
                }
                
                // Ignorar archivos parciales o temporales comunes
                if let Some(ext) = path.extension() {
                    let ext_str = ext.to_string_lossy();
                    if ext_str == "part" || ext_str == "tmp" || ext_str == "crdownload" {
                        continue;
                    }
                }

                // Ignorar archivos de control de G-DriveXP (.hidden, manifiesto)
                // Estos son artefactos internos del shutdown y nunca deben sincronizarse
                if let Some(file_name) = path.file_name() {
                    let name = file_name.to_string_lossy();
                    if name == ".hidden" || name == HIDDEN_MANIFEST {
                        continue;
                    }
                }
                
                // Calcular ruta relativa
                let relative = match path.strip_prefix(&self.ctx.mirror_path) {
                    Ok(p) => p,
                    Err(_) => continue, // No debería pasar
                };
                
                let relative_str = relative.to_string_lossy();
                if relative_str.is_empty() { continue; } // Root

                // 2. Determinar tipo de evento
                match event.kind {
                    EventKind::Create(_) | EventKind::Modify(_) => {
                         // Check existence and type using tokio fs
                        match tokio::fs::symlink_metadata(&path).await {
                            Ok(meta) => {
                                if meta.is_symlink() {
                                    // Es un symlink (OnlineOnly), ignorar cambios
                                    continue;
                                }
                                // Es archivo real o directorio -> Procesar como Modificación/Creación
                                self.handle_local_change(&path, &relative_str, meta.is_dir()).await;
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                // Podría ser un delete que llegó como modify? Raro, pero posible.
                                // Ignoramos si no existe, el evento Remove lo capturará si aplica.
                            }
                            Err(e) => {
                                error!("Error leyendo metadata para evento FS: {:?}", e);
                            }
                        }
                    }
                    EventKind::Remove(_) => {
                        self.handle_local_delete(&relative_str).await;
                    }
                    _ => {} 
                }
            }
        }
    }

    async fn handle_local_rename(&self, old_path: &PathBuf, new_path: &PathBuf) {
        let old_name = old_path.file_name().map(|n| n.to_string_lossy()).unwrap_or_default();
        self.ctx.history.log(ActionType::Sync, format!("Moviendo: {}", old_name));
        tracing::info!("🔄 DETECTADO RENOMBRADO INTELIGENTE: {:?} -> {:?}", old_path, new_path);
        
        // 1. Calcular relativas
        let old_relative = match old_path.strip_prefix(&self.ctx.mirror_path) {
            Ok(p) => p.to_string_lossy(),
            Err(_) => return,
        };
        let new_relative = match new_path.strip_prefix(&self.ctx.mirror_path) {
            Ok(p) => p.to_string_lossy(),
            Err(_) => return,
        };

        let db = &self.ctx.db;

        // 2. Resolver Inode Origen (que ya no existe en disco en old_path, pero sí en DB)
        let inode = match db.resolve_relative_path_to_inode(&old_relative).await {
            Ok(Some(i)) => i,
            Ok(None) => {
                warn!("Origen de renombrado no encontrado en DB: {}", old_relative);
                // Fallback: tratar como Create en destino
                if let Ok(meta) = tokio::fs::metadata(new_path).await {
                     self.handle_local_change(new_path, &new_relative, meta.is_dir()).await;
                }
                return;
            }
            Err(e) => {
                error!("Error resolviendo origen rename: {:?}", e);
                return;
            }
        };

        // 2.5 VERIFICACIÓN DE PERMISOS (Blocking at Source - Mirror)
        // Si no tenemos permiso para moverlo en Drive, revertimos el movimiento físico de inmediato
        if let Ok(attrs) = db.get_attrs(inode).await {
            if !attrs.can_move {
                warn!("⛔ Bloqueando movimiento de archivo compartido (ReadOnly). Revirtiendo físicamente: {:?} -> {:?}", new_path, old_path);
                self.ctx.history.log(ActionType::Error, format!("Movimiento bloqueado: {}", old_name));

                // Intentar moverlo de vuelta físicamente
                if let Err(e) = tokio::fs::rename(new_path, old_path).await {
                    error!("Fallo crítico al intentar revertir movimiento físico: {:?}", e);
                }

                return;
            }
        }

        // 3. Resolver Nuevo Padre
        let new_parent_path_buf = new_path.parent().unwrap_or(std::path::Path::new(""));
        let new_parent_relative = match new_parent_path_buf.strip_prefix(&self.ctx.mirror_path) {
             Ok(p) => p.to_string_lossy(),
             Err(_) => "".into(),
        };

        let new_parent_inode = if new_parent_relative.is_empty() {
            1
        } else {
            match db.resolve_relative_path_to_inode(&new_parent_relative).await {
                Ok(Some(i)) => i,
                _ => {
                    warn!("Padre destino no encontrado: {}", new_parent_relative);
                    return;
                }
            }
        };

        // 4. Obtener Nuevo Nombre
        let new_name = match new_path.file_name() {
             Some(n) => n.to_string_lossy(),
             None => return,
        };

        // 5. ACTUALIZACIÓN ATÓMICA EN DB CON GESTIÓN DE CONFLICTOS
        // Si el destino ya existe (sobreescritura), debemos eliminar el dentry antiguo
        // del destino para evitar el error UNIQUE constraint failed.
        
        info!("📝 Preparando Move en DB: inode={} -> new_parent={}, new_name={}", inode, new_parent_inode, new_name);

        // A. Verificar si el destino ya existe en la DB
        if let Ok(Some(existing_dest_inode)) = db.lookup(new_parent_inode, &new_name).await {
            warn!("⚠️ Conflicto detectado en Rename. El destino '{}' ya existe (inode={}). Eliminando anterior...", new_name, existing_dest_inode);
            
            // Resolvemos gdrive_id para aplicar soft_delete si es posible
            let existing_gdrive_id: Option<String> = sqlx::query_scalar("SELECT gdrive_id FROM inodes WHERE inode = ?")
                .bind(existing_dest_inode as i64)
                .fetch_optional(db.pool())
                .await
                .unwrap_or(None);

            if let Some(gid) = existing_gdrive_id {
                let _ = db.soft_delete_by_gdrive_id(&gid).await;
            } else {
                // Si no tiene gdrive_id, es un dentry local puro, lo borramos de dentry
                let _ = sqlx::query("DELETE FROM dentry WHERE child_inode = ?")
                    .bind(existing_dest_inode as i64)
                    .execute(db.pool())
                    .await;
            }
        }

        let update_sql = "UPDATE dentry SET parent_inode = ?, name = ? WHERE child_inode = ?";
        if let Err(e) = sqlx::query(update_sql)
            .bind(new_parent_inode as i64)
            .bind(new_name.to_string())
            .bind(inode as i64)
            .execute(db.pool())
            .await 
        {
             error!("Error crítico actualizando dentry en Rename: {:?}", e);
             return;
        }

        // 6. Marcar DIRTY y burbujear estado a ancestros
        if let Err(e) = db.set_dirty_and_bubble(inode).await {
             error!("Error marcando dirty tras Rename: {:?}", e);
        }

        // 7. Reparar target de symlink si el archivo movido es online_only.
        // El kernel mueve el *archivo* symlink correctamente, pero su contenido (el target path)
        // sigue apuntando a la ruta FUSE antigua. Como la DB acaba de actualizarse, FUSE ahora
        // expone el archivo en la nueva ruta: necesitamos recrear el symlink con el target correcto.
        if let Ok(meta) = tokio::fs::symlink_metadata(new_path).await {
            if meta.is_symlink() {
                info!("🔗 Symlink detectado tras movimiento, reparando target: {:?}", new_path);
                let new_fuse_target = self.ctx.fuse_mount_path.join(
                    new_path.strip_prefix(&self.ctx.mirror_path).unwrap_or(new_path)
                );

                // Misma estrategia atómica que static_handle_set_online_only:
                // creamos en temp dir externo al directorio observado y luego renombramos.
                let temp_dir_root = self.ctx.mirror_path.join(".gdrive_tmp_ops");
                if let Err(e) = tokio::fs::create_dir_all(&temp_dir_root).await {
                    error!("No se pudo crear directorio temporal para reparar symlink: {:?}", e);
                } else {
                    let file_name = new_path.file_name()
                        .map(|f| f.to_string_lossy())
                        .unwrap_or_else(|| "unknown".into());
                    let unique_name = format!("{}.{}.link.tmp", uuid::Uuid::new_v4(), file_name);
                    let tmp_symlink_path = temp_dir_root.join(&unique_name);

                    let fuse_clone = new_fuse_target.clone();
                    let tmp_clone = tmp_symlink_path.clone();
                    let new_path_clone = new_path.clone();

                    let create_result = tokio::task::spawn_blocking(move || {
                        std::os::unix::fs::symlink(&fuse_clone, &tmp_clone)
                    }).await;

                    match create_result {
                        Ok(Ok(())) => {
                            if let Err(e) = tokio::fs::rename(&tmp_symlink_path, &new_path_clone).await {
                                error!("Error al intercambiar symlink reparado: {:?}", e);
                                let _ = tokio::fs::remove_file(&tmp_symlink_path).await;
                            } else {
                                info!("✅ Symlink reparado: {:?} → {:?}", new_path_clone, new_fuse_target);
                            }
                        }
                        Ok(Err(e)) => error!("Error creando symlink temporal para reparación: {:?}", e),
                        Err(e) => error!("Error en spawn_blocking (symlink repair): {:?}", e),
                    }
                }
            }
        }

        info!("✅ Renombrado local procesado exitosamente (pendiente confirmación de Drive).");
    }

    async fn handle_local_change(&self, path: &PathBuf, relative_path: &str, is_dir: bool) {
        // 1. Procesar el nodo principal
        let is_new = self.process_local_change(path, relative_path, is_dir).await;
        
        // 2. Si es un directorio nuevo, iniciar escaneo recursivo (iterativo con stack)
        // Esto evita "recursion in async fn" y asegura captura de contenido inicial.
        if is_new && is_dir {
            info!("magnifying_glass_tilted_left Ejecutando Escaneo de Seguridad Iterativo en: {}", relative_path);
            
            // Stack de directorios a explorar: (path_absoluto, path_relativo)
            let mut stack = vec![(path.clone(), relative_path.to_string())];
            
            // Límite de seguridad para evitar loops de symlinks o profundidad excesiva
            let mut depth_guard = 0;
            const MAX_SCAN_DEPTH: usize = 5000; 

            while let Some((current_path, current_rel)) = stack.pop() {
                depth_guard += 1;
                if depth_guard > MAX_SCAN_DEPTH {
                    warn!("Abortando escaneo de seguridad por exceso de elementos/profundidad");
                    break;
                }

                let mut read_dir = match tokio::fs::read_dir(&current_path).await {
                    Ok(rd) => rd,
                    Err(e) => {
                        // Puede pasar si se borró mientras escaneábamos
                        tracing::debug!("No se pudo leer dir en escaneo: {:?}", e);
                        continue;
                    }
                };

                while let Ok(Some(entry)) = read_dir.next_entry().await {
                    let child_path = entry.path();
                    let child_name = entry.file_name().to_string_lossy().to_string();
                    
                    // Ignorar archivos internos
                     if child_name.starts_with(".gdrive") || child_name.starts_with(".cloud")
                        || child_name == ".hidden" || child_name == HIDDEN_MANIFEST {
                        continue;
                    }

                    let child_relative = if current_rel.is_empty() {
                         child_name.clone()
                    } else {
                         format!("{}/{}", current_rel, child_name)
                    };
                    
                    if let Ok(child_meta) = entry.metadata().await {
                        if !child_meta.is_symlink() {
                            let child_is_dir = child_meta.is_dir();
                            
                            // Procesar hijo
                            info!("   ↪️ Detectado hijo en escaneo: {}", child_relative);
                            let _ = self.process_local_change(&child_path, &child_relative, child_is_dir).await;
                            
                            // Si es directorio, añadir al stack para explorar SUS hijos
                            if child_is_dir {
                                stack.push((child_path, child_relative));
                            }
                        }
                    }
                }
            }
            info!("✅ Escaneo de seguridad completado para cluster de: {}", relative_path);
        }
    }

    /// Lógica core de registro de cambios. Retorna true si el archivo es NUEVO.
    async fn process_local_change(&self, path: &PathBuf, relative_path: &str, is_dir: bool) -> bool {
        // Guard: nunca registrar archivos de control interno en la DB
        if let Some(file_name) = path.file_name() {
            let name = file_name.to_string_lossy();
            if name == ".hidden" || name == HIDDEN_MANIFEST {
                tracing::debug!("⏭️ Ignorando archivo de control interno: {}", relative_path);
                return false;
            }
        }

        tracing::debug!("📝 Cambio local detectado: {} (dir={})", relative_path, is_dir);

        let db = &self.ctx.db;
        
        // 1. Resolver padre
        let parent_path = match PathBuf::from(relative_path).parent() {
            Some(p) => p.to_string_lossy().to_string(),
            None => "".to_string(),
        };
        
        let parent_inode = if parent_path.is_empty() {
            1 // Root
        } else {
            match db.resolve_relative_path_to_inode(&parent_path).await {
                Ok(Some(i)) => i,
                Ok(None) => {
                    warn!("Padre no encontrado en DB para cambio local: {}", relative_path);
                    return false;
                }
                Err(e) => {
                    error!("Error resolviendo padre: {:?}", e);
                    return false;
                }
            }
        };

        // 2. Obtener nombre
        let name = match PathBuf::from(relative_path).file_name() {
            Some(n) => n.to_string_lossy().to_string(),
            None => return false,
        };

        // 3. Verificar si ya existe (Update vs Create)
        let existing_inode = match db.lookup(parent_inode, &name).await {
            Ok(i) => i,
            Err(_) => None,
        };

        let is_new = existing_inode.is_none();
        let inode = if let Some(i) = existing_inode {
            // UPDATE
            i
        } else {
            // CREATE - Generar ID temporal
            let temp_id = format!("temp_{}", uuid::Uuid::new_v4());
            match db.get_or_create_inode(&temp_id).await {
                Ok(i) => i,
                Err(e) => {
                    error!("Error creando inode temporal: {:?}", e);
                    return false;
                }
            }
        };

        // 4. Actualizar Metadatos (Size, Mtime)
        // Leemos del disco real
        if let Ok(meta) = tokio::fs::metadata(path).await {
            use std::os::unix::fs::MetadataExt;
            let size = meta.len() as i64;
            let mtime = meta.mtime();
            let mode = meta.mode();
            
            // Detectar MIME type básico
            let mime = mime_guess::from_path(path).first().map(|m| m.essence_str().to_string());

            if let Err(e) = db.upsert_file_metadata(inode, size, mtime, mode, is_dir, mime.as_deref(), true, false, true).await {
                error!("Error actualizando metadata en DB: {:?}", e);
            }
        }

        // 5. Vincular al directorio (Dentry)
        if let Err(e) = db.upsert_dentry(parent_inode, inode, &name).await {
             error!("Error actualizando dentry: {:?}", e);
        }

        // 6. Marcar DIRTY, LocalOnline y burbujear estado
        // Primero asegurar availability='local_online'
        // Primero asegurar availability='local_online' y burbujear si aplica
        if let Err(e) = db.set_availability(inode, "local_online", true).await {
            error!("Error asegurando availability='local_online': {:?}", e);
        }
        // Luego set_dirty_and_bubble (detecta estado previo automáticamente)
        if let Err(e) = db.set_dirty_and_bubble(inode).await {
             error!("Error marcando dirty: {:?}", e);
        }
        
        let name_display = PathBuf::from(relative_path).file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| relative_path.to_string());
        if is_new {
            self.ctx.history.log(ActionType::Create, format!("Local Creado: {}", name_display));
        } else {
            self.ctx.history.log(ActionType::Upload, format!("Modificado: {}", name_display));
        }
        info!("✅ Cambio local registrado: {} (inode={})", relative_path, inode);
        
        is_new
    }
    
    async fn handle_local_delete(&self, relative_path: &str) {
        tracing::debug!("🗑️ Eliminación local detectada: {}", relative_path);
        
        // DOUBLE CHECK: El evento puede ser falso positivo o redundante (por eliminación recursiva previa)
        let check_path = self.ctx.mirror_path.join(relative_path);
        if check_path.exists() || tokio::fs::symlink_metadata(&check_path).await.is_ok() {
            tracing::warn!("⚠️ FALSO NEGATIVO: Evento Remove para '{}' pero el archivo existe. Ignorando.", relative_path);
            return;
        }

        let db = &self.ctx.db;
        
        // 1. Resolver inode
        if let Ok(Some(inode)) = db.resolve_relative_path_to_inode(relative_path).await {
            // 2. OPTIMIZACIÓN CRÍTICA: Verificar si ya está marcado como eliminado
            // Si el padre ya fue eliminado recursivamente, este inode ya no tendrá dentry
            // y no necesitamos procesarlo de nuevo.
            if let Ok(Some(deleted)) = sqlx::query_scalar::<_, bool>(
                "SELECT deleted_at IS NOT NULL FROM sync_state WHERE inode = ?"
            ).bind(inode as i64).fetch_optional(db.pool()).await {
                if deleted {
                    tracing::debug!("   Inodo {} ya está marcado como eliminado (posiblemente por cascada).", inode);
                    return;
                }
            } else {
                // If sync_state entry doesn't exist, it's not deleted, proceed.
                // Or if there was an error, log it and proceed cautiously.
                // For now, if fetch_optional returns None, it means no entry, so not deleted.
            }

            // 3. Obtener gdrive_id para soft delete
            let gdrive_id: Option<String> = sqlx::query_scalar("SELECT gdrive_id FROM inodes WHERE inode = ?")
                .bind(inode as i64)
                .fetch_optional(db.pool())
                .await
                .unwrap_or(None);
            
            if let Some(gid) = gdrive_id {
                // Ahora es RECURSIVO en la base de datos
                if let Err(e) = db.soft_delete_by_gdrive_id(&gid).await {
                    error!("Error realizando soft delete en DB: {:?}", e);
                } else {
                    let name_display = PathBuf::from(relative_path).file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| relative_path.to_string());
                    self.ctx.history.log(ActionType::Delete, format!("Eliminado: {}", name_display));
                    info!("✅ Eliminación registrada (Cascada): {} (id={})", relative_path, gid);
                }
            } else {
                 // Archivo sin gdrive_id (temporal local), hard delete
                 info!("Eliminando archivo temporal local: {}", relative_path);
                 // TODO: Hard delete inode implementado en repo?
                 // No expuesto publicamente soft_delete_inode, pero podemos marcar en sync_state manual
                 // O usar soft_delete con un ID falso? No.
                 // Mejor ignorar o limpiar.
            }
        } else {
            // Ya no existe en DB? Entonces no importa.
        }
    }
}

/// Oculta archivos OnlineOnly en Nautilus escribiendo sus nombres en archivos `.hidden` por directorio.
/// Debe llamarse ANTES de desmontar FUSE para que la secuencia sea:
/// escribir .hidden → desmontar FUSE → symlinks rotos pero ocultos en Nautilus.
pub async fn hide_online_only_files(db: &MetadataRepository, mirror_path: &Path) -> Result<()> {
    use std::collections::HashMap;

    let files = db.get_all_active_files().await?;

    // Agrupar nombres de archivos online_only por directorio padre
    let mut by_dir: HashMap<String, Vec<String>> = HashMap::new();

    for (_inode, relative_path, availability) in files {
        if availability != "online_only" {
            continue;
        }
        let p = Path::new(&relative_path);
        let dir = p.parent().map(|d| d.to_string_lossy().to_string()).unwrap_or_default();
        let name = match p.file_name() {
            Some(n) => n.to_string_lossy().to_string(),
            None => continue,
        };
        by_dir.entry(dir).or_default().push(name);
    }

    if by_dir.is_empty() {
        info!("🙈 No hay archivos OnlineOnly que ocultar");
        return Ok(());
    }

    let mut manifest_lines = Vec::new();

    for (dir, names) in &by_dir {
        let hidden_path = if dir.is_empty() {
            mirror_path.join(".hidden")
        } else {
            mirror_path.join(dir).join(".hidden")
        };

        let existing = tokio::fs::read_to_string(&hidden_path).await.unwrap_or_default();
        let existing_set: std::collections::HashSet<&str> = existing.lines().collect();

        let mut content = existing.clone();
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }

        for name in names {
            if !existing_set.contains(name.as_str()) {
                content.push_str(name);
                content.push('\n');
            }
            manifest_lines.push(format!("{}\t{}", dir, name));
        }

        if let Err(e) = tokio::fs::write(&hidden_path, &content).await {
            warn!("Error escribiendo .hidden en {:?}: {:?}", hidden_path, e);
        }
    }

    // Escribir manifiesto para que el próximo arranque pueda revertir
    let manifest_path = mirror_path.join(HIDDEN_MANIFEST);
    let manifest_content = manifest_lines.join("\n");
    if let Err(e) = tokio::fs::write(&manifest_path, &manifest_content).await {
        warn!("Error escribiendo manifiesto: {:?}", e);
    }

    info!("🙈 {} archivos OnlineOnly ocultados en {} directorios", manifest_lines.len(), by_dir.len());
    Ok(())
}

/// Restaura archivos OnlineOnly previamente ocultados, limpiando las entradas de `.hidden`
/// que fueron agregadas por `hide_online_only_files`.
/// Debe llamarse al inicio, DESPUÉS de montar FUSE (durante bootstrap del MirrorManager).
pub async fn restore_hidden_online_only_files(mirror_path: &Path) {
    use std::collections::{HashMap, HashSet};

    let manifest_path = mirror_path.join(HIDDEN_MANIFEST);

    let manifest = match tokio::fs::read_to_string(&manifest_path).await {
        Ok(content) => content,
        Err(_) => return, // No hay manifiesto → nada que limpiar
    };

    let mut by_dir: HashMap<String, HashSet<String>> = HashMap::new();
    for line in manifest.lines() {
        if let Some((dir, name)) = line.split_once('\t') {
            by_dir.entry(dir.to_string()).or_default().insert(name.to_string());
        }
    }

    for (dir, names_to_remove) in &by_dir {
        let hidden_path = if dir.is_empty() {
            mirror_path.join(".hidden")
        } else {
            mirror_path.join(dir).join(".hidden")
        };

        let existing = match tokio::fs::read_to_string(&hidden_path).await {
            Ok(c) => c,
            Err(_) => continue,
        };

        let cleaned: Vec<&str> = existing
            .lines()
            .filter(|line| !line.is_empty() && !names_to_remove.contains(*line))
            .collect();

        if cleaned.is_empty() {
            if dir.is_empty() {
                // El root .hidden puede tener FUSE_Mount; si quedó vacío, dejarlo vacío
                // config.ensure_directories() lo recreará con FUSE_Mount en el siguiente arranque
                let _ = tokio::fs::write(&hidden_path, "").await;
            } else {
                let _ = tokio::fs::remove_file(&hidden_path).await;
            }
        } else {
            let new_content = format!("{}\n", cleaned.join("\n"));
            let _ = tokio::fs::write(&hidden_path, &new_content).await;
        }
    }

    let _ = tokio::fs::remove_file(&manifest_path).await;
    info!("👁️ Archivos OnlineOnly restaurados ({} entradas .hidden limpiadas)",
        by_dir.values().map(|s| s.len()).sum::<usize>());
}
