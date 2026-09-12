use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tracing::{info, error, warn};

use crate::db::MetadataRepository;
use crate::gui::history::{ActionHistory, ActionType, TransferOp};

/// Salud del watcher del espejo, legible sin canal: la GUI la sondea cada
/// 2 s (RefreshActivity) y muestra banner + botón Reiniciar mientras siga
/// degradada. También la pone el supervisor al rendirse con el mirror
/// (GaveUp): cubre muerte de tarea Y ceguera con tarea viva.
pub static MIRROR_DEGRADED: AtomicBool = AtomicBool::new(false);

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
    /// Notificación de archivos restaurados desde la papelera de Google Drive
    RemoteRestored { paths: Vec<String> },
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
            if let Err(e) = Self::run_bootstrap(self.ctx.clone(), true).await {
                error!("Error durante bootstrap: {:?}", e);
            }
            info!("✅ Bootstrap completado. Iniciando vigilancia.");

            // 2. Iniciar Watcher (con reintento periódico si falla: jamás ciego en silencio)
            info!("👷 Despachando inicialización del Watcher a thread pool dedicado (blocking)...");
            self.ensure_watcher("arranque").await;

            self.run_loop().await;
        })
    }

    /// Marca/limpia el degradado con una sola entrada en el historial por
    /// transición (sin spam cada ciclo).
    fn set_degraded(&self, degraded: bool, why: &str) {
        if MIRROR_DEGRADED.swap(degraded, Ordering::SeqCst) != degraded {
            if degraded {
                error!("⛔ Espejo degradado ({}): sin vigilancia local hasta recuperarlo", why);
                self.ctx.history.log(
                    ActionType::Error,
                    format!("Espejo degradado ({}): usa Reiniciar si persiste", why),
                );
            } else {
                info!("👀 Vigilancia del espejo recuperada ({})", why);
                self.ctx.history.log(
                    ActionType::Sync,
                    format!("Vigilancia del espejo recuperada ({})", why),
                );
            }
        }
    }

    /// Arranca el watcher en el thread pool dedicado (el constructor es
    /// bloqueante). Helper único para los 4 sitios que lo (re)crean.
    async fn start_watcher(
        mirror_path: &PathBuf,
        w_tx: mpsc::Sender<Vec<DebouncedEvent>>,
    ) -> Result<MirrorWatcher> {
        let path = mirror_path.clone();
        tokio::task::spawn_blocking(move || MirrorWatcher::new(&path, w_tx))
            .await
            .context("task de watcher interrumpida")?
    }

    /// Garantiza watcher activo: un intento; si falla, marca degradado (la
    /// GUI lo muestra con botón Reiniciar) y lo deja para el tick periódico
    /// del run_loop — jamás se queda ciego en silencio.
    async fn ensure_watcher(&mut self, why: &str) -> bool {
        match Self::start_watcher(&self.ctx.mirror_path, self.watcher_tx.clone()).await {
            Ok(w) => {
                self.watcher = Some(w);
                self.set_degraded(false, why);
                info!("👀 Watcher activo ({})", why);
                true
            }
            Err(e) => {
                error!("❌ Watcher no disponible ({}): {:?}", why, e);
                self.set_degraded(true, why);
                false
            }
        }
    }

    /// Reconcilia el estado del sistema de archivos visible con la base de datos
    // Función estática asociada que corre independiente del estado mut del manager
    async fn run_bootstrap(ctx: Arc<MirrorContext>, skip_orphan_cleanup: bool) -> Result<()> {
        info!("🔄 Iniciando bootstrap del espejo...");

        restore_hidden_online_only_files(&ctx.mirror_path).await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // PASO 0: Crear directorios activos en batch (blocking)
        let dirs = ctx.db.get_all_active_dirs().await?;
        info!("📁 Se encontraron {} directorios activos en DB", dirs.len());
        let mirror_path_clone = ctx.mirror_path.clone();
        let dirs_for_blocking: Vec<String> = dirs.iter().map(|(_, p)| p.clone()).collect();
        tokio::task::spawn_blocking(move || {
            for relative_path in &dirs_for_blocking {
                let mirror_dir = mirror_path_clone.join(relative_path);
                if !mirror_dir.exists() {
                    let _ = std::fs::create_dir_all(&mirror_dir);
                }
            }
        }).await?;

        // PASO 1: Clasificar archivos en fast path vs slow path
        let files = ctx.db.get_all_active_files().await?;
        let total = files.len();
        info!("📂 Se encontraron {} archivos activos en DB", total);

        let mut symlinks_to_create: Vec<(PathBuf, PathBuf)> = Vec::new();
        let mut repaired = 0u64;

        for (_inode, relative_path, availability) in &files {
            let mirror_file = ctx.mirror_path.join(relative_path);
            let meta = tokio::fs::symlink_metadata(&mirror_file).await;
            let file_exists = meta.is_ok();
            let is_symlink = meta.as_ref().map(|m| m.is_symlink()).unwrap_or(false);
            let is_real_file = meta.as_ref().map(|m| m.is_file()).unwrap_or(false);

            match availability.as_str() {
                "online_only" => {
                    if !file_exists {
                        // Fast path: solo necesitamos crear el symlink
                        let fuse_path = ctx.fuse_mount_path.join(relative_path);
                        symlinks_to_create.push((fuse_path, mirror_file));
                    } else if is_symlink {
                        match tokio::fs::read_link(&mirror_file).await {
                            Ok(target) if !target.starts_with(&ctx.fuse_mount_path) => {
                                // Symlink roto: reparar via slow path
                                Self::static_handle_set_online_only_opt(&ctx, &mirror_file.to_string_lossy(), false).await;
                                repaired += 1;
                            }
                            _ => {}
                        }
                    } else if is_real_file {
                        warn!("CONFLICTO: DB dice OnlineOnly pero existe archivo local: {:?}", relative_path);
                    }
                }
                "local_online" => {
                    if !file_exists || is_symlink {
                        let mut moved_locally = false;
                        if relative_path.starts_with("SHARED/") {
                            let legacy_path = relative_path.strip_prefix("SHARED/").unwrap_or("");
                            let legacy_mirror_file = ctx.mirror_path.join(legacy_path);
                            if let Ok(meta_old) = tokio::fs::metadata(&legacy_mirror_file).await {
                                if meta_old.is_file() {
                                    info!("🚚 Detectado cambio de ruta por propiedad: {:?} -> {:?}", legacy_path, relative_path);
                                    if let Err(e) = tokio::fs::rename(&legacy_mirror_file, &mirror_file).await {
                                        warn!("   Falló el renombrado local: {:?}", e);
                                    } else {
                                        moved_locally = true;
                                    }
                                }
                            }
                        }
                        if !moved_locally {
                            Self::static_handle_set_local_online_opt(&ctx, &mirror_file.to_string_lossy(), false).await;
                            repaired += 1;
                        }
                    }
                }
                _ => {}
            }
        }

        // PASO 2: Crear symlinks del fast path en batch (un solo spawn_blocking)
        if !symlinks_to_create.is_empty() {
            let count = symlinks_to_create.len();
            info!("🔗 Creando {} symlinks...", count);
            let created = tokio::task::spawn_blocking(move || {
                let mut ok = 0u64;
                for (target, link) in &symlinks_to_create {
                    if let Some(parent) = link.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    match std::os::unix::fs::symlink(target, link) {
                        Ok(()) => ok += 1,
                        Err(e) => tracing::warn!("Error creando symlink {:?}: {:?}", link, e),
                    }
                }
                ok
            }).await.unwrap_or(0);
            repaired += created;
            info!("🔗 {} symlinks creados", created);
        }

        info!("🔄 Bootstrap: {} archivos verificados, {} creados/reparados", total, repaired);

        // PASO 3: Limpieza de huérfanos (solo si no hay escaneo en curso)
        // Si skip_orphan_cleanup es true, no eliminar archivos del espejo
        // ya que la DB puede estar incompleta (escaneo en progreso).
        if skip_orphan_cleanup {
            info!("⏭️ Omitiendo limpieza de huérfanos (escaneo pendiente)");
        } else {
            info!("🧹 Iniciando limpieza recursiva de archivos huérfanos en el espejo...");
            let mut valid_paths: std::collections::HashSet<PathBuf> = {
                let mut paths = std::collections::HashSet::new();
                for (_, path, _) in &files {
                    paths.insert(PathBuf::from(path));
                }
                for (_, path) in &dirs {
                    paths.insert(PathBuf::from(path));
                }
                paths.insert(PathBuf::from("SHARED"));
                paths
            };

            let (deleted_count, ingested_count) = Self::cleanup_orphans(&ctx, &mut valid_paths).await?;
            if deleted_count > 0 || ingested_count > 0 {
                info!("🧹 Limpieza completada: {} symlinks obsoletos eliminados, {} locales recuperados.", deleted_count, ingested_count);
            }
        }

        // PASO FINAL: Reconstruir dir_counters
        if let Err(e) = ctx.db.rebuild_all_dir_counters().await {
            error!("Error reconstruyendo dir_counters post-bootstrap: {:?}", e);
        }

        info!("✅ Bootstrap completado");
        Ok(())
    }

    /// Reconcilia el espejo en disco con `valid_paths` (snapshot de la DB).
    ///
    /// Retorna `(symlinks_eliminados, locales_ingestados)`.
    ///
    /// INVARIANTE DE SEGURIDAD (P0, ADR-014): solo se eliminan symlinks
    /// obsoletos —artefactos OnlineOnly cuyo contenido vive en Drive y son
    /// recreables—. Los archivos/directorios REALES no registrados (p. ej.
    /// creados mientras el watcher estaba pausado durante un Refresh, o con
    /// eventos aún drenados sin procesar) se INGESTAN en la DB como
    /// `dirty`/`local_online`, exactamente como habría hecho el watcher vía
    /// `process_local_change`. Nunca se borra contenido real del usuario.
    async fn cleanup_orphans(
        ctx: &Arc<MirrorContext>,
        valid_paths: &mut std::collections::HashSet<PathBuf>,
    ) -> Result<(u64, u64)> {
        // Directorios que NUNCA deben recorrerse ni eliminarse
        let mut skip_dirs: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
        skip_dirs.insert(ctx.fuse_mount_path.clone());

        let mut entries_to_check = vec![ctx.mirror_path.clone()];
        let mut deleted_count = 0u64;
        let mut ingested_count = 0u64;

        while let Some(current_dir) = entries_to_check.pop() {
            if let Ok(mut entries) = tokio::fs::read_dir(&current_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let full_path = entry.path();

                    // NUNCA tocar el punto de montaje FUSE ni su contenido
                    if skip_dirs.contains(&full_path) {
                        continue;
                    }

                    let rel_path = match full_path.strip_prefix(&ctx.mirror_path) {
                        Ok(p) => p.to_path_buf(),
                        Err(_) => continue,
                    };

                    let name = rel_path.file_name().map(|n| n.to_string_lossy()).unwrap_or_default();
                    if name.starts_with('.') { continue; }

                    if valid_paths.contains(&rel_path) {
                        if let Ok(m) = entry.file_type().await {
                            if m.is_dir() {
                                entries_to_check.push(full_path);
                            }
                        }
                        continue;
                    }

                    // Entrada desconocida para la DB: clasificar SIN seguir symlinks.
                    let ft = match entry.file_type().await {
                        Ok(f) => f,
                        Err(_) => continue, // desapareció durante el escaneo: ignorar
                    };
                    if ft.is_symlink() {
                        warn!("🧹 Eliminando symlink huérfano en espejo: {:?}", rel_path);
                        let _ = tokio::fs::remove_file(&full_path).await;
                        deleted_count += 1;
                    } else if ft.is_dir() {
                        // Directorio real nuevo: ingestar y descender para ingestar su contenido.
                        match Self::static_ingest_unknown(ctx, &full_path, &rel_path, true).await {
                            Ok(true) => {
                                valid_paths.insert(rel_path);
                                ingested_count += 1;
                                entries_to_check.push(full_path);
                            }
                            Ok(false) => {} // artefacto interno: se deja intacto
                            Err(e) => warn!("⚠️ No se pudo ingestar directorio {:?}: {:?}", rel_path, e),
                        }
                    } else {
                        // Archivo real nuevo: ingestar en lugar de borrar (P0).
                        match Self::static_ingest_unknown(ctx, &full_path, &rel_path, false).await {
                            Ok(true) => {
                                valid_paths.insert(rel_path);
                                ingested_count += 1;
                            }
                            Ok(false) => {} // artefacto interno/temporal: se deja intacto
                            Err(e) => warn!("⚠️ No se pudo ingestar archivo {:?}: {:?}", rel_path, e),
                        }
                    }
                }
            }
        }
        Ok((deleted_count, ingested_count))
    }

    /// Registra en la DB una entrada real del espejo que la DB desconoce,
    /// replicando lo que el watcher habría hecho vía `process_local_change`
    /// (inode temporal + metadata + dentry + `local_online` + dirty).
    ///
    /// Retorna `Ok(true)` si se ingestó, `Ok(false)` si es un artefacto
    /// interno/temporal que debe dejarse intacto (nunca borrarse aquí).
    /// Ante cualquier error, el llamador debe CONSERVAR el archivo en disco.
    async fn static_ingest_unknown(
        ctx: &Arc<MirrorContext>,
        abs_path: &Path,
        rel_path: &Path,
        is_dir: bool,
    ) -> Result<bool> {
        // Filtros de artefactos internos y temporales (igual que handle_fs_events):
        // se dejan intactos, su dueño los gestiona.
        if let Some(file_name) = rel_path.file_name() {
            let name = file_name.to_string_lossy();
            if name == ".hidden"
                || name == HIDDEN_MANIFEST
                || name.starts_with(".gdrive")
                || name.starts_with(".cloud")
            {
                return Ok(false);
            }
            if !is_dir {
                if let Some(ext) = rel_path.extension() {
                    let ext_str = ext.to_string_lossy();
                    if ext_str == "part" || ext_str == "tmp" || ext_str == "crdownload" {
                        return Ok(false);
                    }
                }
            }
        }

        let db = &ctx.db;
        let parent_inode = Self::static_ensure_parent_chain(ctx, rel_path).await?;
        let name = match rel_path.file_name() {
            Some(n) => n.to_string_lossy().to_string(),
            None => return Ok(false),
        };

        // Carreras: alguien (watcher recreado, otro bootstrap) ya lo registró.
        if db.lookup(parent_inode, &name).await?.is_some() {
            return Ok(true);
        }

        let meta = tokio::fs::metadata(abs_path).await;
        let (size, mtime, mode) = match meta {
            Ok(m) => {
                use std::os::unix::fs::MetadataExt;
                (m.len() as i64, m.mtime(), m.mode())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(true), // se borró solo: nada que hacer
            Err(e) => return Err(e.into()),
        };
        let mime = (!is_dir)
            .then(|| mime_guess::from_path(abs_path).first().map(|m| m.essence_str().to_string()))
            .flatten();

        let temp_id = format!("temp_{}", uuid::Uuid::new_v4());
        let inode = db.get_or_create_inode(&temp_id).await?;
        db.upsert_file_metadata(inode, size, mtime, mode, is_dir, mime.as_deref(), true, false, true)
            .await?;
        db.upsert_dentry(parent_inode, inode, &name).await?;
        // bubble=false: el bootstrap reconstruye dir_counters al final (PASO FINAL).
        db.set_availability(inode, "local_online", false).await?;
        db.set_dirty_and_bubble(inode).await?;

        // Si otro escritor ganó el nombre entretanto, nuestro temp quedaría
        // huérfano (file_N): darlo de baja. La ruta sigue siendo válida vía el ganador.
        if let Err(e) = Self::drop_losing_temp(&ctx.db, parent_inode, &name, inode).await {
            warn!("Error autolimpiando temporal perdedor: {:?}", e);
        }

        let name_display = name.clone();
        ctx.history.log(
            ActionType::Create,
            format!("Local Recuperado (Refresh): {}", name_display),
        );
        info!("✅ Huérfano real ingestado en DB en lugar de borrarse: {} (inode={})", rel_path.display(), inode);
        Ok(true)
    }

    /// Asegura que toda la cadena de directorios padres de `rel_path` exista
    /// en la DB, creando los inodes intermedios faltantes como `local_online`.
    /// Retorna el inode del padre directo. Falla solo ante error de DB.
    async fn static_ensure_parent_chain(ctx: &Arc<MirrorContext>, rel_path: &Path) -> Result<u64> {
        let db = &ctx.db;
        let mut parent_inode = 1u64; // raíz
        let mut acc = PathBuf::new();
        let parent_rel = match rel_path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => return Ok(parent_inode),
        };
        for comp in parent_rel.components() {
            acc.push(comp);
            let name = comp.as_os_str().to_string_lossy().to_string();
            let next = match db.lookup(parent_inode, &name).await? {
                Some(i) => i,
                None => {
                    let temp_id = format!("temp_{}", uuid::Uuid::new_v4());
                    let inode = db.get_or_create_inode(&temp_id).await?;
                    let abs = ctx.mirror_path.join(&acc);
                    let (size, mtime, mode) = tokio::fs::metadata(&abs)
                        .await
                        .map(|m| {
                            use std::os::unix::fs::MetadataExt;
                            (m.len() as i64, m.mtime(), m.mode())
                        })
                        .unwrap_or((0, 0, 0o755));
                    db.upsert_file_metadata(inode, size, mtime, mode, true, None, true, false, true)
                        .await?;
                    db.upsert_dentry(parent_inode, inode, &name).await?;
                    db.set_availability(inode, "local_online", false).await?;
                    db.set_dirty_and_bubble(inode).await?;
                    inode
                }
            };
            parent_inode = next;
        }
        Ok(parent_inode)
    }

    async fn run_loop(&mut self) {
        // Auto-recuperación del watcher: si una (re)creación falló, se
        // reintenta cada minuto sin bloquear los comandos entre tanto.
        let mut health_tick = tokio::time::interval(std::time::Duration::from_secs(60));
        health_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = health_tick.tick() => {
                    if MIRROR_DEGRADED.load(Ordering::SeqCst) {
                        self.ensure_watcher("tick").await;
                    }
                }
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
                            // Pausar watcher para evitar que el bootstrap genere falsos dirty
                            self.watcher.take();
                            while self.watcher_rx.try_recv().is_ok() {}

                            if let Err(e) = Self::run_bootstrap(self.ctx.clone(), false).await {
                                error!("Error durante bootstrap (Refresh): {:?}", e);
                            }

                            // Recrear watcher (con reintento periódico si falla)
                            self.ensure_watcher("Refresh").await;
                        }
                        MirrorCommand::RemoteDeleted { paths } => {
                            // Pausar watcher para que las eliminaciones del mirror
                            // no se detecten como eliminaciones del usuario
                            self.watcher.take();
                            while self.watcher_rx.try_recv().is_ok() {}

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

                            // Recrear watcher (con reintento periódico si falla)
                            self.ensure_watcher("RemoteDeleted").await;
                        }
                        MirrorCommand::RemoteRestored { paths } => {
                            self.watcher.take();
                            while self.watcher_rx.try_recv().is_ok() {}

                            for relative in paths {
                                let mirror_file = self.ctx.mirror_path.join(&relative);
                                if tokio::fs::symlink_metadata(&mirror_file).await.is_ok() {
                                    continue;
                                }

                                if let Some(parent) = mirror_file.parent() {
                                    let _ = tokio::fs::create_dir_all(parent).await;
                                }

                                if let Ok(Some(inode)) = self.ctx.db.resolve_relative_path_to_inode(&relative).await {
                                    let availability = self.ctx.db.get_availability(inode).await.unwrap_or_default();
                                    match availability.as_str() {
                                        "online_only" => {
                                            let fuse_path = self.ctx.fuse_mount_path.join(&relative);
                                            match tokio::fs::symlink(&fuse_path, &mirror_file).await {
                                                Ok(()) => info!("🔗 Restaurado (symlink): {}", relative),
                                                Err(e) => warn!("Error creando symlink para restaurado: {:?}", e),
                                            }
                                        }
                                        "local_online" => {
                                            Self::static_handle_set_local_online_opt(&self.ctx, &mirror_file.to_string_lossy(), true).await;
                                            info!("📥 Restaurado (local): {}", relative);
                                        }
                                        _ => {}
                                    }
                                }
                            }

                            // Recrear watcher (con reintento periódico si falla)
                            self.ensure_watcher("RemoteRestored").await;
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
            Ok(Ok(copied)) => {
                if copied == 0 {
                    warn!("🛡️ ABORTADO: copia desde FUSE resultó en 0 bytes para {:?}. No se reemplazará el symlink.", relative);
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    return;
                }
                tracing::debug!("🪞 Copia finalizada ({} bytes). Preparando intercambio...", copied);
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
                    let src_str = paths[0].to_string_lossy();
                    let dst_str = paths[1].to_string_lossy();
                    if src_str.contains(".gdrive_tmp_ops") || dst_str.contains(".gdrive_tmp_ops") {
                        continue;
                    }
                    self.handle_local_rename(&paths[0], &paths[1]).await;
                    continue;
                }
                // Caso B: "Rename From" (Mover FUERA del espejo o a la papelera)
                EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                    for path in paths {
                        if path.to_string_lossy().contains(".gdrive_tmp_ops") { continue; }
                        if let Ok(relative) = path.strip_prefix(&self.ctx.mirror_path) {
                            self.handle_local_delete(&relative.to_string_lossy()).await;
                        }
                    }
                    continue;
                }
                // Caso C: "Rename To" (Mover DESDE FUERA al espejo)
                EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                    for path in paths {
                        if path.to_string_lossy().contains(".gdrive_tmp_ops") { continue; }
                        if let Ok(relative) = path.strip_prefix(&self.ctx.mirror_path) {
                            if let Ok(meta) = tokio::fs::symlink_metadata(&path).await {
                                if meta.is_symlink() { continue; }
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
                // Fallback: tratar como Create en destino (solo si no es symlink)
                if let Ok(meta) = tokio::fs::symlink_metadata(new_path).await {
                    if !meta.is_symlink() {
                        self.handle_local_change(new_path, &new_relative, meta.is_dir()).await;
                    }
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

        // Si lo creamos nosotros y otro escritor ganó el nombre entretanto,
        // nuestro temp quedaría huérfano (file_N): darlo de baja.
        if is_new {
            if let Err(e) = Self::drop_losing_temp(db, parent_inode, &name, inode).await {
                error!("Error autolimpiando temporal perdedor: {:?}", e);
            }
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
    
    /// Si `my_inode` (recién creado por esta llamada) perdió la carrera por
    /// `(parent, name)` frente a otro inodo, lo da de baja para no acumular
    /// huérfanos que luego subirían como `file_N` a la raíz de Drive.
    /// JAMÁS toca inodos reales (solo `temp_*`) ni al ganador.
    /// Retorna true si limpió.
    async fn drop_losing_temp(
        db: &MetadataRepository,
        parent_inode: u64,
        name: &str,
        my_inode: u64,
    ) -> Result<bool> {
        match db.lookup(parent_inode, name).await? {
            Some(winner) if winner == my_inode => Ok(false),
            Some(_) => {
                let my_gid: Option<String> =
                    sqlx::query_scalar("SELECT gdrive_id FROM inodes WHERE inode = ?")
                        .bind(my_inode as i64)
                        .fetch_optional(db.pool())
                        .await?;
                match my_gid {
                    Some(gid) if gid.starts_with("temp_") => {
                        warn!(
                            "🧹 Temporal {} perdió '{}': dando de baja para evitar huérfano file_N",
                            my_inode, name
                        );
                        db.hard_delete_by_gdrive_id(&gid).await?;
                        Ok(true)
                    }
                    _ => Ok(false),
                }
            }
            // Sin ganador (borrado concurrente): no tocar, otros flujos lo gestionan.
            None => Ok(false),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::MetadataRepository;
    use std::collections::HashSet;

    /// Monta un espejo temporal + DB fresca y devuelve el snapshot de DB
    /// tal como lo vería un Refresh: SIN el archivo recién creado.
    ///
    /// Modela la carrera exacta del P0: el snapshot (`valid_paths`) se toma
    /// en PASO 1 del bootstrap, el archivo se crea después (watcher pausado
    /// y eventos drenados en `MirrorCommand::Refresh`), y la limpieza de
    /// PASO 3 lo encuentra "huérfano".
    async fn setup_race() -> (Arc<MirrorContext>, tempfile::TempDir, HashSet<PathBuf>) {
        let tmp = tempfile::tempdir().unwrap();
        let mirror = tmp.path().join("mirror");
        let fuse = tmp.path().join("fuse");
        tokio::fs::create_dir_all(&mirror).await.unwrap();
        tokio::fs::create_dir_all(&fuse).await.unwrap();
        let db = Arc::new(
            MetadataRepository::new(&tmp.path().join("meta.sqlite"))
                .await
                .unwrap(),
        );
        // Invariante del sistema: inode raíz = 1 (primera inserción en DB fresca).
        let root = db.get_or_create_inode("root").await.unwrap();
        assert_eq!(root, 1);
        let ctx = Arc::new(MirrorContext {
            db,
            mirror_path: mirror,
            fuse_mount_path: fuse,
            history: ActionHistory::new(),
        });
        // Snapshot de una DB que aún no conoce el archivo nuevo.
        let mut valid = HashSet::new();
        valid.insert(PathBuf::from("SHARED"));
        (ctx, tmp, valid)
    }

    #[tokio::test]
    async fn orphan_cleanup_preserves_new_unregistered_file() {
        let (ctx, _tmp, mut valid) = setup_race().await;
        // El usuario crea el archivo DESPUÉS del snapshot de DB.
        tokio::fs::write(ctx.mirror_path.join("nota_urgente.txt"), "datos valiosos")
            .await
            .unwrap();
        let (deleted, _) = MirrorManager::cleanup_orphans(&ctx, &mut valid).await.unwrap();
        let body = tokio::fs::read(ctx.mirror_path.join("nota_urgente.txt"))
            .await
            .expect("P0: el Refresh borró un archivo recién creado no registrado en DB");
        assert_eq!(body, b"datos valiosos");
        assert_eq!(deleted, 0, "un archivo real nunca debe contarse como eliminado");
    }

    #[tokio::test]
    async fn orphan_cleanup_preserves_new_unregistered_tree() {
        let (ctx, _tmp, mut valid) = setup_race().await;
        tokio::fs::create_dir_all(ctx.mirror_path.join("proyecto_nuevo"))
            .await
            .unwrap();
        tokio::fs::write(ctx.mirror_path.join("proyecto_nuevo/notas.txt"), "x")
            .await
            .unwrap();
        MirrorManager::cleanup_orphans(&ctx, &mut valid).await.unwrap();
        assert!(
            ctx.mirror_path.join("proyecto_nuevo/notas.txt").is_file(),
            "P0: el Refresh borró con remove_dir_all un árbol recién creado"
        );
    }

    #[tokio::test]
    async fn orphan_cleanup_registers_unknown_file_instead_of_deleting() {
        let (ctx, _tmp, mut valid) = setup_race().await;
        tokio::fs::write(ctx.mirror_path.join("nuevo.txt"), "hola")
            .await
            .unwrap();
        MirrorManager::cleanup_orphans(&ctx, &mut valid).await.unwrap();
        let inode = ctx
            .db
            .resolve_relative_path_to_inode("nuevo.txt")
            .await
            .unwrap();
        assert!(
            inode.is_some(),
            "el archivo desconocido debe ingestarse en la DB (como haría el watcher), no borrarse"
        );
        assert!(
            ctx.db.is_dirty(inode.unwrap()).await.unwrap(),
            "el archivo ingestado debe quedar dirty para subir a Drive"
        );
    }

    #[tokio::test]
    #[ignore]
    /// E2E a escala real, SOLO sobre copias (nunca datos de producción).
    ///
    /// Uso:
    ///   MIRROR_E2E_DB=/tmp/gdrivexp-e2e/metadata.db \
    ///   MIRROR_E2E_MIRROR=/tmp/gdrivexp-e2e/mirror \
    ///   cargo test --bin g-drive-xp e2e_refresh -- --ignored --nocapture
    ///
    /// Maneja el MirrorManager real: spawn → bootstrap inicial → Refresh real
    /// (watcher pausado + eventos drenados) mientras se crean 50 archivos de
    /// carrera + 1 árbol anidado. Verifica que TODOS sobreviven con contenido
    /// intacto y quedan registrados dirty en la DB.
    async fn e2e_refresh_preserves_race_files_at_real_scale() {
        use tokio::time::{sleep, timeout, Duration};

        let db_path = std::env::var("MIRROR_E2E_DB").expect("MIRROR_E2E_DB no definido");
        let mirror_path = PathBuf::from(std::env::var("MIRROR_E2E_MIRROR").expect("MIRROR_E2E_MIRROR no definido"));
        assert!(mirror_path.is_dir(), "el espejo E2E debe ser una copia, no el real");
        // Salvaguarda: jamás correr contra el espejo de producción.
        assert!(
            !mirror_path.starts_with("/home/alcss/GoogleDrive") || mirror_path.starts_with("/tmp/"),
            "protección: MIRROR_E2E_MIRROR apunta a datos reales"
        );

        let db = Arc::new(
            MetadataRepository::new(Path::new(&db_path)).await.unwrap(),
        );
        let fuse = mirror_path.join("FUSE_Mount");
        let (_tx_ready, rx_ready) = tokio::sync::watch::channel(true);
        let (manager, cmd_tx) = MirrorManager::new(
            db.clone(),
            mirror_path.clone(),
            fuse,
            ActionHistory::new(),
            rx_ready,
        );
        let handle = manager.spawn();

        // 1. Esperar a que el watcher esté activo: reescribir el centinela en
        //    bucle. Si el watcher aún no existe, el write no genera evento;
        //    en cuanto está vivo, la siguiente escritura lo registra en DB.
        tokio::fs::write(mirror_path.join("E2E_SENTINEL.txt"), "sentinel").await.unwrap();
        timeout(Duration::from_secs(900), async {
            let mut ticks = 0u32;
            loop {
                tokio::fs::write(mirror_path.join("E2E_SENTINEL.txt"), "sentinel").await.unwrap();
                sleep(Duration::from_secs(5)).await;
                if db.resolve_relative_path_to_inode("E2E_SENTINEL.txt").await.unwrap().is_some() {
                    break;
                }
                ticks += 1;
                if ticks % 6 == 0 {
                    eprintln!("e2e: esperando watcher... {}s", ticks * 5);
                }
            }
        })
        .await
        .expect("el watcher nunca registró el centinela (startup incompleto)");
        // Limpiar centinela sin pasar por el watcher de borrado: lo borramos y
        // aceptamos que quede un dirty residual en la copia (irrelevante).
        let _ = tokio::fs::remove_file(mirror_path.join("E2E_SENTINEL.txt")).await;

        // 2. Disparar Refresh real e inyectar archivos de carrera DURANTE el bootstrap.
        cmd_tx.send(MirrorCommand::Refresh).await.unwrap();
        const N: usize = 50;
        for i in 0..N {
            let name = format!("E2E_RACE_{:02}.txt", i);
            let body = format!("contenido-valioso-{}", i);
            tokio::fs::write(mirror_path.join(&name), &body).await.unwrap();
        }
        tokio::fs::create_dir_all(mirror_path.join("E2E_RACE_DIR")).await.unwrap();
        tokio::fs::write(mirror_path.join("E2E_RACE_DIR/hijo.txt"), "hijo-valioso").await.unwrap();

        // 3. Esperar a que los 50 + hijo estén registrados en DB (ingesta del
        //    cleanup o watcher reactivado) y verificar contenido intacto.
        timeout(Duration::from_secs(1200), async {
            let mut ticks = 0u32;
            loop {
                let mut done = 0;
                for i in 0..N {
                    let name = format!("E2E_RACE_{:02}.txt", i);
                    if db.resolve_relative_path_to_inode(&name).await.unwrap().is_some() {
                        done += 1;
                    }
                }
                if done == N
                    && db.resolve_relative_path_to_inode("E2E_RACE_DIR/hijo.txt").await.unwrap().is_some()
                {
                    break;
                }
                ticks += 1;
                if ticks % 15 == 0 {
                    eprintln!("e2e: esperando ingesta... {}/{} ({}s)", done, N, ticks * 2);
                }
                sleep(Duration::from_secs(2)).await;
            }
        })
        .await
        .expect("los archivos de carrera nunca llegaron a la DB");

        for i in 0..N {
            let name = format!("E2E_RACE_{:02}.txt", i);
            let body = tokio::fs::read(mirror_path.join(&name))
                .await
                .unwrap_or_else(|_| panic!("P0 E2E: {} fue borrado por el Refresh", name));
            assert_eq!(body, format!("contenido-valioso-{}", i).into_bytes(), "contenido corrupto en {}", name);
        }
        assert_eq!(
            tokio::fs::read(mirror_path.join("E2E_RACE_DIR/hijo.txt")).await.unwrap(),
            b"hijo-valioso",
        );

        // 4. Apagado ordenado del manager.
        let _ = cmd_tx.send(MirrorCommand::Shutdown).await;
        timeout(Duration::from_secs(60), handle).await.expect("shutdown colgado").unwrap();
    }

    #[tokio::test]
    async fn orphan_cleanup_still_removes_stale_symlink() {
        let (ctx, _tmp, mut valid) = setup_race().await;
        // Symlink OnlineOnly cuyo origen ya no existe en DB: artefacto propio, seguro de borrar.
        std::os::unix::fs::symlink(
            ctx.fuse_mount_path.join("borrado_remoto.txt"),
            ctx.mirror_path.join("borrado_remoto.txt"),
        )
        .unwrap();
        let (deleted, _) = MirrorManager::cleanup_orphans(&ctx, &mut valid).await.unwrap();
        assert_eq!(deleted, 1, "los symlinks obsoletos sí deben limpiarse");
        assert!(
            tokio::fs::symlink_metadata(ctx.mirror_path.join("borrado_remoto.txt"))
                .await
                .is_err(),
            "el symlink obsoleto debe desaparecer del espejo"
        );
    }

    /// ensure_watcher degrada visible (flag + historial) cuando el watcher
    /// no arranca y se recupera solo con un path válido. Único test que
    /// toca el flag global MIRROR_DEGRADED: lo aísla y lo deja limpio.
    #[tokio::test]
    async fn ensure_watcher_degrada_y_recupera() {
        MIRROR_DEGRADED.store(false, Ordering::SeqCst);

        let tmp = tempfile::tempdir().unwrap();
        let db = Arc::new(
            MetadataRepository::new(&tmp.path().join("meta.sqlite"))
                .await
                .unwrap(),
        );

        // 1. Path inexistente → inotify falla: degrada y reporta false.
        let (_tx1, rx1) = tokio::sync::watch::channel(false);
        let (mut mgr, _cmd1) = MirrorManager::new(
            db.clone(),
            tmp.path().join("noexiste"),
            tmp.path().join("fuse"),
            ActionHistory::new(),
            rx1,
        );
        assert!(!mgr.ensure_watcher("sonda-fallo").await);
        assert!(MIRROR_DEGRADED.load(Ordering::SeqCst));

        // 2. Path válido → se recupera, limpia el flag y reporta true.
        let ok_dir = tmp.path().join("mirror-ok");
        std::fs::create_dir_all(&ok_dir).unwrap();
        let (_tx2, rx2) = tokio::sync::watch::channel(false);
        let (mut mgr2, _cmd2) = MirrorManager::new(
            db,
            ok_dir,
            tmp.path().join("fuse"),
            ActionHistory::new(),
            rx2,
        );
        assert!(mgr2.ensure_watcher("sonda-ok").await);
        assert!(!MIRROR_DEGRADED.load(Ordering::SeqCst));

        MIRROR_DEGRADED.store(false, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn perdedor_temporal_se_autolimpia() {
        // Dos inodos temporales para (1,"dup"): el ganador conserva el nombre,
        // el perdedor debe darse de baja en lugar de quedar huérfano (file_N).
        let (ctx, _tmp, _valid) = setup_race().await;
        let a = ctx.db.get_or_create_inode("temp_dup_a").await.unwrap();
        let b = ctx.db.get_or_create_inode("temp_dup_b").await.unwrap();
        ctx.db.upsert_dentry(1, a, "dup").await.unwrap();
        assert!(MirrorManager::drop_losing_temp(&ctx.db, 1, "dup", b).await.unwrap());
        assert_eq!(ctx.db.lookup(1, "dup").await.unwrap(), Some(a));
        assert!(ctx.db.get_inode_by_gdrive_id("temp_dup_b").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ganador_no_se_toca() {
        let (ctx, _tmp, _valid) = setup_race().await;
        let a = ctx.db.get_or_create_inode("temp_dup_a").await.unwrap();
        ctx.db.upsert_dentry(1, a, "dup").await.unwrap();
        assert!(!MirrorManager::drop_losing_temp(&ctx.db, 1, "dup", a).await.unwrap());
        assert_eq!(ctx.db.lookup(1, "dup").await.unwrap(), Some(a));
    }

    #[tokio::test]
    async fn inodo_real_nunca_se_autolimpia() {
        // Aunque un inodo REAL pierda el nombre frente a otro, jamás se borra solo.
        let (ctx, _tmp, _valid) = setup_race().await;
        let real = ctx.db.get_or_create_inode("real123").await.unwrap();
        let t = ctx.db.get_or_create_inode("temp_dup_t").await.unwrap();
        ctx.db.upsert_dentry(1, t, "dup").await.unwrap();
        assert!(!MirrorManager::drop_losing_temp(&ctx.db, 1, "dup", real).await.unwrap());
        assert!(ctx.db.get_inode_by_gdrive_id("real123").await.unwrap().is_some());
    }
}
