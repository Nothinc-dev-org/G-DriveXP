use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error, warn};

use crate::db::MetadataRepository;

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
}

#[derive(Clone)]
struct MirrorContext {
    db: Arc<MetadataRepository>,
    mirror_path: PathBuf,
    fuse_mount_path: PathBuf,
}

/// Gestor principal de la arquitectura Espejo
/// Mantiene la sincronización entre el direcotrio visible (Mirror) y el montaje FUSE oculto.
pub struct MirrorManager {
    ctx: Arc<MirrorContext>,
    command_rx: mpsc::Receiver<MirrorCommand>,
}

impl MirrorManager {
    pub fn new(
        db: Arc<MetadataRepository>,
        mirror_path: PathBuf,
        fuse_mount_path: PathBuf,
    ) -> (Self, mpsc::Sender<MirrorCommand>) {
        let (tx, rx) = mpsc::channel(32);
        
        let ctx = Arc::new(MirrorContext {
            db,
            mirror_path,
            fuse_mount_path,
        });
        
        let manager = Self {
            ctx,
            command_rx: rx,
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

            // Initial Scan / Bootstrap DESACOPLADO
            // Ejecutamos bootstrap en su propia task para no bloquear el procesamiento de mensajes IPC
            // y permitir que la GUI/Nautilus interactúen inmediatamente.
            let ctx_bootstrap = self.ctx.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::run_bootstrap(ctx_bootstrap).await {
                    error!("Error durante bootstrap: {:?}", e);
                }
            });

            self.run_loop().await;
        })
    }

    /// Reconcilia el estado del sistema de archivos visible con la base de datos
    // Función estática asociada que corre independiente del estado mut del manager
    async fn run_bootstrap(ctx: Arc<MirrorContext>) -> Result<()> {
        info!("🔄 Iniciando bootstrap del espejo...");
        
        // Pequeña pausa adicional de seguridad
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        
        let files = ctx.db.get_all_active_files().await?;
        info!("📂 Se encontraron {} archivos activos en DB", files.len());
        
        for (_inode, relative_path, availability) in files {
            let mirror_file = ctx.mirror_path.join(&relative_path);
            
            // Asegurar que el directorio padre existe
            if let Some(parent) = mirror_file.parent() {
                if !parent.exists() {
                   let _ = tokio::fs::create_dir_all(parent).await;
                }
            }
            
            match availability.as_str() {
                "online_only" => {
                    // Si no existe, o es un archivo regular, forzamos symlink
                    if !mirror_file.exists() && !mirror_file.is_symlink() {
                        Self::static_handle_set_online_only(&ctx, &mirror_file.to_string_lossy()).await;
                    } else if mirror_file.is_file() && !mirror_file.is_symlink() {
                        // Caso delicado: DB dice OnlineOnly, FS tiene archivo real.
                        warn!("CONFLICTO: DB dice OnlineOnly pero existe archivo local: {:?}", relative_path);
                    }
                }
                "local_online" => {
                    // Queremos archivo real. Si es symlink o no existe, descargamos.
                    let is_symlink = tokio::fs::symlink_metadata(&mirror_file).await
                        .map(|m| m.is_symlink())
                        .unwrap_or(false);
                        
                    if !mirror_file.exists() || is_symlink {
                        Self::static_handle_set_local_online(&ctx, &mirror_file.to_string_lossy()).await;
                    }
                }
                _ => {}
            }
        }
        
        info!("✅ Bootstrap completado");
        Ok(())
    }

    async fn run_loop(&mut self) {
        while let Some(cmd) = self.command_rx.recv().await {
            tracing::info!("🪞 MirrorManager recibió comando: {:?}", cmd);
            match cmd {
                MirrorCommand::SetOnlineOnly { path } => {
                    Self::static_handle_set_online_only(&self.ctx, &path).await;
                }
                MirrorCommand::SetLocalOnline { path } => {
                    Self::static_handle_set_local_online(&self.ctx, &path).await;
                }
                MirrorCommand::Refresh => {
                    // Trigger manual bootstrap?
                    // Requeriría spawnear de nuevo la task de bootstrap
                     let ctx_refresh = self.ctx.clone();
                     tokio::spawn(async move {
                         let _ = Self::run_bootstrap(ctx_refresh).await;
                     });
                }
            }
        }
        tracing::warn!("🪞 MirrorManager run_loop terminó (channel cerrado)");
    }

    // Funciones estáticas que reciben contexto en lugar de &self
    
    async fn static_handle_set_online_only(ctx: &MirrorContext, path_str: &str) {
        let path = PathBuf::from(path_str);
        tracing::info!("🪞 Procesando SetOnlineOnly para: {:?}", path);
        
        // 1. Validar que el path está dentro del mirror
        if !path.starts_with(&ctx.mirror_path) {
            warn!("Intento de modificar archivo fuera del mirror: {}", path_str);
            return;
        }
        
        // 2. Calcular path relativo y path FUSE
        let relative = match path.strip_prefix(&ctx.mirror_path) {
            Ok(p) => p,
            Err(_) => return,
        };
        
        let fuse_path = ctx.fuse_mount_path.join(relative);
        
        // 3. Database is source of truth - No FUSE access to avoid deadlock

        // If inode exists in DB with valid gdrive_id, file WILL exist in FUSE when accessed

        // 4. Validar tipo de archivo de forma ASYNC y SIN seguir symlinks (evitar FUSE deadlock)
        tracing::debug!("🪞 Verificando tipo de archivo para: {:?}", path);
        match tokio::fs::symlink_metadata(&path).await {
            Ok(meta) => {
                if meta.is_dir() {
                    warn!("La liberación de espacio en directorios completos no es atómica aún");
                    return;
                }
            },
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    // Si no existe, podría ser que ya se borró o nunca existió.
                    // Si la DB dice que existe, procedemos a crear el symlink de todos modos
                    // para reparar el estado.
                    tracing::warn!("Archivo no encontrado en disco, intentando reparar symlink: {:?}", e);
                } else {
                    error!("Error leyendo metadata de archivo: {:?}", e);
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
        
        tracing::info!("🪞 Creando symlink temporal en zona segura: {:?} -> {:?}", tmp_symlink_path, fuse_path);
        
        // Crear el symlink en ubicación temporal externa (blocking)
        let create_result = tokio::task::spawn_blocking(move || {
            std::os::unix::fs::symlink(&fuse_path_clone, &tmp_path_clone)
        }).await;
        
        match create_result {
            Ok(Ok(())) => {
                // 6. ACTUALIZAR DB ANTES DEL RENAME
                if let Ok(Some(inode)) = ctx.db.resolve_relative_path_to_inode(relative.to_str().unwrap_or("")).await {
                    if let Err(e) = ctx.db.set_availability(inode, "online_only").await {
                        warn!("Error actualizando disponibilidad en DB para {:?}: {:?}", relative, e);
                    }
                } else {
                    warn!("No se pudo resolver inode para actualizar DB: {:?}", relative);
                }

                tracing::info!("🪞 Symlink creado. Ejecutando intercambio atómico trans-directorio...");
                
                // Renombrar el symlink temporal (externo) sobre el archivo original (ATOMIC Move)
                // Al venir desde fuera del directorio observado, Nautilus ve esto como una actualización
                // directa del nodo, sin ruido previo de creación.
                if let Err(e) = tokio::fs::rename(&tmp_symlink_path, &path).await {
                    error!("Error en intercambio atómico de symlink: {:?}", e);
                    let _ = tokio::fs::remove_file(&tmp_symlink_path).await;
                    return;
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
        let path = PathBuf::from(path_str);
        tracing::info!("🪞 Procesando SetLocalOnline para: {:?}", path);
        
        // 1. Validar path
        if !path.starts_with(&ctx.mirror_path) {
            tracing::warn!("Path fuera del mirror");
            return;
        }

        let relative = match path.strip_prefix(&ctx.mirror_path) {
           Ok(p) => p,
           Err(_) => return,
        };
        
        let fuse_path = ctx.fuse_mount_path.join(relative);
        tracing::info!("🪞 Fuse path objetivo: {:?}", fuse_path);
        
        // 2. Verificar si ya es archivo real (y no symlink)
        let meta = tokio::fs::symlink_metadata(&path).await;
        if let Ok(m) = meta {
            if !m.is_symlink() && m.is_file() {
                info!("El archivo ya es local y real: {:?}", relative);
                // Asegurar que DB esté sincronizada
                 if let Ok(Some(inode)) = ctx.db.resolve_relative_path_to_inode(relative.to_str().unwrap_or("")).await {
                    let _ = ctx.db.set_availability(inode, "local_online").await;
                }
                return;
            }
        }
        
        // 2. Database is source of truth - If DB has inode, we proceed
        // FUSE will serve the file on-demand when accessed

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
        
        let fuse_path_copy = fuse_path.clone();
        let tmp_path_copy = tmp_path.clone();
        
        tracing::info!("🪞 Copiando {:?} -> {:?}", fuse_path, tmp_path);
        let copy_result = tokio::task::spawn_blocking(move || {
            std::fs::copy(&fuse_path_copy, &tmp_path_copy)
        }).await;
        
        match copy_result {
            Ok(Ok(_)) => {
                tracing::info!("🪞 Copia finalizada. Preparando intercambio...");
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
            if let Err(e) = ctx.db.set_availability(inode, "local_online").await {
                warn!("Error actualizando disponibilidad en DB para {:?}: {:?}", relative, e);
            }
        }

        // 5. Mover TMP a Real (Atomic Replace)
        if let Err(e) = tokio::fs::rename(&tmp_path, &path).await {
              error!("Error moviendo archivo descargado a destino final: {:?}", e);
              let _ = tokio::fs::remove_file(&tmp_path).await;
        } else {
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
}
