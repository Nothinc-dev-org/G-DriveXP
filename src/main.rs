mod auth;
mod config;
mod db;
mod fuse;
mod gdrive;
mod sync;
mod gui;
mod ipc;
mod utils;
mod mirror;

use anyhow::{Context, Result};
use fuse3::MountOptions;
use fuse3::raw::Session;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use relm4::{RelmApp, ComponentSender};

use config::Config;
use fuse::GDriveFS;

/// Flag global: cuando Hard Reset está en curso, main.rs NO debe hacer process::exit.
pub static HARD_RESET_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

fn main() -> Result<()> {
    // Inicializar sistema de logging
    init_logging()?;
    
    tracing::info!("🚀 Iniciando FedoraDrive-rs v{}", env!("CARGO_PKG_VERSION"));
    
    // El registro de manejadores de señales se delega al runtime asíncrono
    // dentro de la función de backend para operar mediante primitivas exclusivas de Tokio.

    // Iniciar la aplicación Relm4
    tracing::info!("🖥️ Iniciando interfaz gráfica...");
    let app = RelmApp::new("org.gnome.FedoraDrive");
    app.run::<gui::app_model::AppModel>(());

    Ok(())
}

/// Ejecuta un future cancelable por shutdown.
/// Uso exclusivo durante inicialización, donde no hay recursos que limpiar.
macro_rules! or_shutdown {
    ($future:expr) => {
        tokio::select! {
            biased;
            result = $future => result,
            _ = crate::utils::shutdown::wait_for_shutdown() => {
                tracing::info!("🛑 Shutdown durante inicialización, saliendo.");
                std::process::exit(0);
            }
        }
    };
}

/// Ejecuta toda la lógica de backend (asíncrona)
pub fn run_backend(
    ui_sender: ComponentSender<gui::app_model::AppModel>,
    history: gui::history::ActionHistory,
    sync_paused: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Inicializando backend...".to_string()));
    // Crear runtime de Tokio
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Error al construir Tokio Runtime")?;

    rt.block_on(async {
        // --- Escucha reactiva de señales del OS (SIGTERM/SIGINT) ---
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {},
                        _ = sigterm.recv() => {},
                    }
                    tracing::info!("🛑 Señal OS recibida (SIGTERM/SIGINT) - Despertando a Tokio...");
                    crate::utils::shutdown::request_shutdown();
                } else {
                    // Fallback a ctrl_c si falla SIGTERM
                    let _ = tokio::signal::ctrl_c().await;
                    crate::utils::shutdown::request_shutdown();
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
                crate::utils::shutdown::request_shutdown();
            }
        });

        // Cargar o crear configuración
        let config = Config::load().unwrap_or_else(|_| {
            tracing::warn!("No se pudo cargar configuración, usando valores predeterminados");
            Config::default().expect("Error al crear configuración predeterminada")
        });
        
        // Crear directorios necesarios
        config
            .ensure_directories()
            .context("Error al crear directorios de configuración")?;
        
        // Guardar configuración
        config.save().context("Error al guardar configuración")?;
        
        // Mostrar ambas rutas para depuración
        tracing::info!("Ruta Espejo (Visible): {:?}", config.mirror_path);
        tracing::info!("Punto de Montaje FUSE (Oculto): {:?}", config.fuse_mount_path);
        tracing::info!("Directorio de caché: {:?}", config.cache_dir);
        tracing::info!("Base de datos: {:?}", config.db_path);
        
        // Fase 1: Autenticación OAuth2
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Verificando autenticación...".to_string()));
        
        // Buscar archivo de credenciales
        let cred_path = "credentials.json";
        if !std::path::Path::new(cred_path).exists() {
            tracing::error!("No se encontró el archivo '{}'. Por favor siga las instrucciones de instalación.", cred_path);
            ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Error: credentials.json no encontrado".to_string()));
            anyhow::bail!("Archivo de credenciales no encontrado");
        }

        let oauth_manager = auth::OAuth2Manager::new_from_file(cred_path)
            .await
            .context("Error al inicializar gestor OAuth2")?;

        tracing::info!("Verificando estado de autenticación (esto puede abrir su navegador)...");
        or_shutdown!(oauth_manager.authenticate(Some(ui_sender.clone())))
            .context("Fallo crítico en autenticación")?;
            
        tracing::info!("✅ Autenticación correcta");
        ui_sender.input(gui::app_model::AppMsg::SetConnected(true));
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Autenticación correcta".to_string()));
        
        // Inicializar base de datos SQLite
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Cargando base de datos...".to_string()));
        let db = Arc::new(db::MetadataRepository::new(&config.db_path).await?);

        // --- Resiliencia post-crash: detectar cierre no limpio ---
        // Usamos un marcador físico en el espejo para mayor robustez.
        let shutdown_marker = config.mirror_path.join(".gdrivexp_clean_shutdown");
        let is_clean_shutdown = shutdown_marker.exists();
        let is_crash_recovery = !is_clean_shutdown && db.get_sync_meta("bootstrap_complete").await?.is_some();

        if is_crash_recovery {
            tracing::warn!("⚠️ Detectado cierre no limpio (crash/power loss). Iniciando recuperación gradual...");
            // No borramos bootstrap_complete inmediatamente para permitir que el MirrorManager
            // siga viendo el árbol mientras el Syncer/BFS actualiza metadatos.

            let chunks_cleared = db.clear_all_chunks().await.unwrap_or(0);
            if chunks_cleared > 0 {
                tracing::info!("🧹 {} registros de caché invalidados (post-crash cleanup)", chunks_cleared);
            }

            // Purgar caché física para mantener consistencia con la DB.
            // Sin esto, los archivos físicos huérfanos disparan "zombie cache" en cada sesión futura.
            if config.cache_dir.exists() {
                let _ = std::fs::remove_dir_all(&config.cache_dir);
                let _ = std::fs::create_dir_all(&config.cache_dir);
                tracing::info!("🧹 Caché física purgada (post-crash cleanup)");
            }
        }

        // Borrar marcador para la sesión actual (si existe)
        if is_clean_shutdown {
            let _ = std::fs::remove_file(&shutdown_marker);
        }


        // Enviar DB a la GUI para que pueda gestionar directorios locales
        ui_sender.input(gui::app_model::AppMsg::SetDatabase(db.clone()));

        // Inicializar cliente de Google Drive
        let authenticator = oauth_manager.get_authenticator(None).await?;
        let drive_client = Arc::new(gdrive::client::DriveClient::new(authenticator));

        // Obtener Root ID para optimizaciones del Uploader
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Obteniendo ID de carpeta raíz...".to_string()));
        let root_id = or_shutdown!(drive_client.get_root_file_id())
            .context("Error crítico obteniendo Root ID de Google Drive")?;

        // Inicializar sistema de archivos
        let fs = GDriveFS::new(
            db.clone(),
            drive_client.clone(),
            &config.cache_dir,
            Arc::new(history.clone()),
        );

        // Canal de coordinación: BFS bootstrap → MirrorManager
        let (bfs_ready_tx, bfs_ready_rx) = tokio::sync::watch::channel(false);

        // Fase 2.15: Instanciar MirrorManager tempranamente para compartir su sender
        let (mirror_manager, mirror_sender) = mirror::MirrorManager::new(
            db.clone(),
            config.mirror_path.clone(),
            config.fuse_mount_path.clone(),
            history.clone(),
            bfs_ready_rx,
        );

        // Fase 2.1: Bootstrap inicial + Escaneo progresivo
        let bootstrap_done = db.get_sync_meta("bootstrap_complete").await?;

        // Primera vez con DB vacía: nivel 1 rápido para mostrar root de inmediato
        if bootstrap_done.is_none() && db.is_empty().await? {
            ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Cargando estructura inicial...".to_string()));
            or_shutdown!(sync::bootstrap::bootstrap_level1(&db, &drive_client, &root_id))?;
            let _ = db.set_sync_meta("repair_ownership_done_v2", "true").await;
        }

        // Señalar a MirrorManager que puede arrancar con los datos actuales
        let _ = bfs_ready_tx.send(true);

        // Escaneo progresivo: SIEMPRE se ejecuta al iniciar/reanudar sesión
        if is_crash_recovery {
            // Post-crash: escaneo SÍNCRONO antes de montar FUSE (evita 416 por sizes desactualizados)
            ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Recuperando metadatos...".to_string()));
            tracing::info!("Escaneo síncrono post-crash...");
            if let Err(e) = or_shutdown!(sync::bootstrap::bootstrap_remaining_bfs(
                &db, &drive_client, &root_id, &history, &mirror_sender
            )) {
                tracing::error!("Error en escaneo post-crash: {:?}", e);
            }
            if bootstrap_done.is_none() {
                let _ = db.set_sync_meta("bootstrap_complete", "true").await;
            }
        } else {
            // Normal: escaneo en background (no bloquea arranque)
            let db_bg = db.clone();
            let client_bg = drive_client.clone();
            let root_id_bg = root_id.clone();
            let mirror_tx_bg = mirror_sender.clone();
            let history_bg = history.clone();
            let needs_bootstrap_mark = bootstrap_done.is_none();
            let ui_bg = ui_sender.clone();
            ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Escaneando...".to_string()));
            tokio::spawn(async move {
                if let Err(e) = sync::bootstrap::bootstrap_remaining_bfs(
                    &db_bg, &client_bg, &root_id_bg, &history_bg, &mirror_tx_bg
                ).await {
                    tracing::error!("Error en escaneo background: {:?}", e);
                } else if needs_bootstrap_mark {
                    let _ = db_bg.set_sync_meta("bootstrap_complete", "true").await;
                }
                ui_bg.input(gui::app_model::AppMsg::UpdateStatus(
                    "Sistema de archivos montado y activo".to_string()
                ));
            });
        }

        // Fase 2.2: Background Syncer (sincronización continua)
        tracing::info!("Iniciando sincronizador en background...");
        let syncer = sync::syncer::BackgroundSyncer::new(
            db.clone(),
            drive_client.clone(),
            60, // Intervalo base: 60 segundos
            history.clone(),
            sync_paused.clone(),
            mirror_sender.clone(),
        );

        // Sync inicial ANTES de montar FUSE: actualizar metadatos (sizes) para evitar
        // 416 Range Not Satisfiable masivos cuando GNOME escanea el montaje.
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Sincronizando cambios recientes...".to_string()));
        match or_shutdown!(syncer.sync_once()) {
            Ok(n) if n > 0 => tracing::info!("✅ Sync inicial pre-FUSE: {} cambios aplicados", n),
            Ok(_) => tracing::info!("✅ Sync inicial pre-FUSE: sin cambios pendientes"),
            Err(e) => tracing::warn!("⚠️ Sync inicial pre-FUSE falló (no bloqueante): {:?}", e),
        }

        let _syncer_handle = syncer.spawn();
        
        // Limpiar dirty-deletes stale de sesiones anteriores
        // (previene que el uploader envíe a papelera archivos que no borró el usuario)
        let _ = db.clear_stale_dirty_deletes().await;

        // Fase 2.3: Uploader (subida de archivos dirty)
        tracing::info!("Iniciando uploader en background...");
        let uploader = sync::uploader::Uploader::new(
            db.clone(),
            drive_client.clone(),
            30, // Intervalo: 30 segundos
            &config.cache_dir,
            &config.mirror_path,
            history.clone(),
            root_id.clone(),
        );
        let _uploader_handle = uploader.spawn();
        
        // Fase 2.3.5: Progress Monitor (Monitor de Operaciones Pendientes)
        let db_monitor = db.clone();
        let history_monitor = history.clone();
        tokio::spawn(async move {
            tracing::info!("🔍 Iniciando monitor de progreso DB...");
            loop {
                if utils::shutdown::is_shutdown_requested() {
                    tracing::info!("🛑 Progress Monitor: Shutdown detectado, deteniendo.");
                    break;
                }

                let dirty_fuse = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sync_state WHERE dirty = 1")
                    .fetch_one(db_monitor.pool())
                    .await
                    .unwrap_or(0);

                let dirty_local = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM local_sync_files WHERE dirty = 1")
                    .fetch_one(db_monitor.pool())
                    .await
                    .unwrap_or(0);

                history_monitor.set_pending_uploads((dirty_fuse + dirty_local) as usize);

                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        });

        // Fase 2.4: MirrorManager (Nuevo Sistema Híbrido)
        // Reemplaza a LocalSyncManager
        // Fase 2.4: MirrorManager & IPC DEFERRED
        // Se inician DESPUÉS de montar FUSE para evitar Deadlocks por race condition
        // (MirrorManager intenta acceder a FUSE antes de que esté listo)
        
        // CRITICAL: Limpiar punto de montaje huérfano antes de intentar montar
        utils::mount::cleanup_if_needed(&config.fuse_mount_path)
            .context("Error al limpiar punto de montaje huérfano")?;
        
        // Informar a la GUI de las rutas (Mirror y FUSE)
        ui_sender.input(gui::app_model::AppMsg::SetPaths {
            mirror: config.mirror_path.clone(),
            fuse: config.fuse_mount_path.clone(),
        });
        
        // Configurar opciones de montaje
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        
        let mut mount_options = MountOptions::default();
        mount_options
            .uid(uid)
            .gid(gid)
            .fs_name("fedoradrive")
            .allow_other(true)
            .custom_options("default_permissions") // Apply permissions locally
            .custom_options("exec") // CRÍTICO: Permitir ejecución de binarios y .desktop
            .custom_options("max_read=1048576"); // Rendimiento: Kernel debe solicitar hasta 1MB por read()
            
        tracing::info!("Montando sistema de archivos en {:?}...", config.fuse_mount_path);
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus(format!("Montando en {:?}...", config.mirror_path)));
        
        let mut handle = Session::new(mount_options)
            .mount_with_unprivileged(fs, &config.fuse_mount_path)
            .await
            .context("Error al montar sistema de archivos FUSE")?;
        
        // Fase 2.4: MirrorManager (Nuevo Sistema Híbrido)
        // Reemplaza a LocalSyncManager
        // SE INICIA AHORA, con FUSE ya montado.
        tracing::info!("Iniciando MirrorManager (Arquitectura Espejo)...");

        // SINCRONIZAR propiedad ANTES del bootstrap del espejo para evitar race condition:
        let db_mirror = db.clone();
        let client_mirror = drive_client.clone();
        tokio::spawn(async move {
            if let Ok(None) = db_mirror.get_sync_meta("repair_ownership_done_v2").await {
                tracing::info!("⚙️ Verificando consistencia de propiedad para limpieza de redundancias...");
                if let Err(e) = sync::bootstrap::repair_ownership_metadata(&db_mirror, &client_mirror).await {
                    tracing::error!("❌ Error reparando propiedad: {:?}", e);
                } else {
                    let _ = db_mirror.set_sync_meta("repair_ownership_done_v2", "true").await;
                    tracing::info!("✅ Reparación de propiedad v2 completada");
                }
            }

            let _mirror_handle = mirror_manager.spawn();
        });
        
        // Fase 2.5: Servidor IPC para extensiones externas (Nautilus)
        tracing::info!("Iniciando servidor IPC...");
        let socket_path = ipc::get_socket_path();
        let ipc_server = ipc::server::IpcServer::new(
            socket_path,
            db.clone(),
            config.mirror_path.clone(), // IPC usa rutas visibles del usuario
            config.cache_dir.clone(),
        )
        .with_mirror_manager(mirror_sender.clone());
        let _ipc_handle = ipc_server.spawn();
        
        tracing::info!("✅ Sistema de archivos montado exitosamente");
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Sistema de archivos montado y activo".to_string()));

        // TODO: Actualizar GUI para usar MirrorManager Sender
        // ui_sender.input(gui::app_model::AppMsg::SetLocalSyncSender(local_sync_sender));

        
        // Esperar a que termine la sesión recursiva, o se notifique un shutdown coordinado
        // (el cual unifica cierres provenientes vía GUI o del Systema Operativo vía Señal)
        tokio::select! {
            res = &mut handle => {
                if let Err(e) = res {
                    tracing::error!("Error en la sesión FUSE: {:?}", e);
                }
            }
            _ = utils::shutdown::wait_for_shutdown() => {
                tracing::info!("🛑 Desmontaje coordinado activado...");
                ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Cerrando subsistemas...".to_string()));
            }
        }
        
        // Marcar cierre limpio antes de cualquier ruta de salida
        let _ = db.delete_sync_meta("session_active").await;

        // Si un Hard Reset está en curso, dejar que su hilo maneje el cierre.
        // Este hilo simplemente se duerme para no competir con process::exit.
        if HARD_RESET_IN_PROGRESS.load(Ordering::SeqCst) {
            tracing::info!("Hard Reset en curso, cediendo control al hilo de limpieza...");
            loop { std::thread::sleep(std::time::Duration::from_secs(60)); }
        }

        tracing::info!("🛑 Desmontando sistema de archivos y cerrando...");
        ui_sender.input(gui::app_model::AppMsg::UpdateStatus("Desmontando...".to_string()));

        // Detener el MirrorWatcher ANTES de escribir .hidden para evitar que
        // el watcher detecte los archivos y los registre como cambios del usuario.
        let _ = mirror_sender.send(mirror::MirrorCommand::Shutdown).await;
        // Dar tiempo para que el watcher se detenga y se drene el último batch debounced
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        // Ocultar archivos OnlineOnly ANTES de desmontar FUSE
        // para que Nautilus no muestre symlinks rotos con opciones destructivas
        if let Err(e) = mirror::hide_online_only_files(&db, &config.mirror_path).await {
            tracing::error!("Error ocultando archivos OnlineOnly: {:?}", e);
        }

        // El drop de 'handle' debería intentar desmontar, pero lo forzamos por seguridad
        let _ = utils::mount::unmount_and_wait(&config.fuse_mount_path);

        // Crear marcador de cierre limpio FÍSICO tras desmontaje exitoso
        tracing::info!("💾 Escribiendo marcador de cierre limpio...");
        if let Err(e) = std::fs::File::create(&shutdown_marker) {
            tracing::error!("No se pudo crear marcador de cierre limpio: {:?}", e);
        }

        // Forzar salida del proceso (GTK no responde a señales del backend)
        tracing::info!("👋 Cerrando aplicación...");
        std::process::exit(0);
    })
}

/// Inicializa el sistema de logging con tracing
fn init_logging() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "g_drive_xp=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    Ok(())
}
