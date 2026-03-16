use anyhow::{Context, Result};
use notify_debouncer_full::{new_debouncer, DebouncedEvent, Debouncer, FileIdMap};
use notify::{RecursiveMode, Watcher};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

/// Estructura que mantiene vivo el watcher
pub struct MirrorWatcher {
    // Mantener el debouncer vivo es suficiente para asegurar vigilancia
    _debouncer: Debouncer<notify::RecommendedWatcher, FileIdMap>,
}

impl MirrorWatcher {
    /// Inicia el watcher en el path especificado
    /// Los eventos se envían por el canal provider
    pub fn new(
        path: impl AsRef<Path>,
        event_tx: mpsc::Sender<Vec<DebouncedEvent>>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        let mut debouncer = new_debouncer(
            Duration::from_millis(500),
            None, // tick_rate default
            move |res: std::result::Result<Vec<DebouncedEvent>, _>| {
                match res {
                    Ok(events) => {
                        // Enviar eventos al canal (non-blocking)
                        let _ = event_tx.blocking_send(events);
                    }
                    Err(e) => {
                        error!("Error en watcher de sistema de archivos (Full): {:?}", e);
                    }
                }
            },
        ).context("Error creando debouncer full")?;

        // Iniciar vigilancia recursiva
        info!("👀 Iniciando vigilancia recursiva (Full+Rename) en: {:?}", path);
        debouncer
            .watcher()
            .watch(&path, RecursiveMode::Recursive)
            .context("Error iniciando vigilancia de directorio")?;

        info!("👀 MirrorWatcher iniciado en: {:?}", path);

        Ok(Self {
            _debouncer: debouncer,
        })
    }
}
