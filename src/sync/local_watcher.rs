//! LocalWatcher: Monitor de cambios en carpetas locales sincronizadas
//!
//! Usa el crate `notify` para detectar cambios de sistema de archivos
//! y registrarlos en la base de datos para sincronización posterior.

use anyhow::{Context, Result};
use notify::{RecommendedWatcher, RecursiveMode, Watcher, Config, Event, EventKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::db::MetadataRepository;

/// Período de debounce para eventos de archivo (ms)
const DEBOUNCE_MS: u64 = 500;

pub struct LocalWatcher {
    db: Arc<MetadataRepository>,
    watched_dirs: Vec<(i64, PathBuf)>,  // (sync_dir_id, path)
    event_rx: mpsc::Receiver<Result<Event, notify::Error>>,
}

impl LocalWatcher {
    pub fn new(db: Arc<MetadataRepository>) -> Result<(Self, RecommendedWatcher)> {
        let (tx, rx) = mpsc::channel(100);
        
        let config = Config::default()
            .with_poll_interval(Duration::from_secs(2));
        
        let watcher = RecommendedWatcher::new(
            move |res: Result<Event, notify::Error>| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = tx.send(res).await {
                        error!("Error enviando evento de watcher: {:?}", e);
                    }
                });
            },
            config,
        )?;
        
        Ok((
            Self {
                db,
                watched_dirs: Vec::new(),
                event_rx: rx,
            },
            watcher,
        ))
    }
    
    /// Registra un directorio para observar
    pub fn add_watch(&mut self, sync_dir_id: i64, path: PathBuf) {
        self.watched_dirs.push((sync_dir_id, path));
    }
    
    /// Inicia el loop de procesamiento de eventos
    pub async fn run(mut self) {
        info!("👁️ LocalWatcher iniciado");
        
        while let Some(res) = self.event_rx.recv().await {
            match res {
                Ok(event) => {
                    if let Err(e) = self.handle_event(event).await {
                        warn!("Error procesando evento: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Error de notify: {:?}", e);
                }
            }
        }
        
        info!("LocalWatcher terminado");
    }
    
    /// Procesa un evento de filesystem
    async fn handle_event(&self, event: Event) -> Result<()> {
        // Filtrar eventos irrelevantes
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                for path in &event.paths {
                    // Ignorar archivos temporales y ocultos
                    if let Some(name) = path.file_name() {
                        let name_str = name.to_string_lossy();
                        if name_str.starts_with('.') || name_str.ends_with('~') {
                            continue;
                        }
                    }
                    
                    self.mark_file_dirty(path).await?;
                }
            }
            EventKind::Remove(_) => {
                for path in &event.paths {
                    self.mark_file_deleted(path).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    
    async fn mark_file_dirty(&self, path: &Path) -> Result<()> {
        // Encontrar el sync_dir al que pertenece
        if let Some((sync_dir_id, relative_path)) = self.resolve_path(path) {
            // Verificar si el path existe (puede haber sido eliminado)
            if !path.exists() {
                return Ok(());
            }
            
            // Calcular MD5 y metadatos
            let metadata = tokio::fs::metadata(path).await?;
            let is_dir = metadata.is_dir();
            let size = metadata.len() as i64;
            let mtime = metadata.modified()?.duration_since(std::time::UNIX_EPOCH)?.as_secs() as i64;
            
            let md5 = if is_dir {
                None
            } else {
                Some(compute_file_md5(path).await?)
            };
            
            // Upsert en local_sync_files
            self.db.upsert_local_sync_file(
                sync_dir_id,
                &relative_path,
                is_dir,
                "local_online",
                Some(mtime),
                Some(size),
                md5.as_deref(),
            ).await?;
            
            debug!("📝 Marcado dirty: {}", relative_path);
        }
        Ok(())
    }
    
    async fn mark_file_deleted(&self, path: &Path) -> Result<()> {
        if let Some((sync_dir_id, relative_path)) = self.resolve_path(path) {
            // Eliminar de la base de datos
            // TODO: Implementar eliminación en local_sync_files
            debug!("🗑️ Archivo eliminado: {}", relative_path);
        }
        Ok(())
    }
    
    fn resolve_path(&self, full_path: &Path) -> Option<(i64, String)> {
        for (sync_dir_id, base_path) in &self.watched_dirs {
            if let Ok(relative) = full_path.strip_prefix(base_path) {
                return Some((*sync_dir_id, relative.to_string_lossy().into_owned()));
            }
        }
        None
    }
}

async fn compute_file_md5(path: &Path) -> Result<String> {
    use md5::{Md5, Digest};
    let data = tokio::fs::read(path).await?;
    let hash = Md5::digest(&data);
    Ok(format!("{:x}", hash))
}
