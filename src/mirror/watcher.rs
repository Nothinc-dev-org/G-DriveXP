use anyhow::{Context, Result};
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebouncedEvent, Debouncer};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

/// Estructura que mantiene vivo el watcher
pub struct MirrorWatcher {
    // Mantener el debouncer vivo es suficiente para que el watcher siga corriendo
    _debouncer: Debouncer<notify::RecommendedWatcher>,
}

impl MirrorWatcher {
    /// Inicia el watcher en el path especificado
    /// Los eventos se envían por el canal provider
    pub fn new(
        path: impl AsRef<Path>,
        event_tx: mpsc::Sender<Vec<DebouncedEvent>>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Configurar debouncer
        // 500ms de debounce para agrupar escrituras secuenciales
        let mut debouncer = new_debouncer(
            Duration::from_millis(500),
            move |res: std::result::Result<Vec<DebouncedEvent>, _>| {
                match res {
                    Ok(events) => {
                        // Enviar eventos al canal (non-blocking)
                        let _ = event_tx.blocking_send(events);
                    }
                    Err(e) => {
                        error!("Error en watcher de sistema de archivos: {:?}", e);
                    }
                }
            },
        ).context("Error creando debouncer")?;

        // Iniciar vigilancia recursiva
        info!("👀 Iniciando vigilancia recursiva en: {:?}", path);
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
