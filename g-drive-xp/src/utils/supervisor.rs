//! Supervisión con reinicio para los servicios background de larga vida
//! (syncer, uploader, mirror, IPC, monitor). Sin esto, un panic en una de
//! esas tareas era muerte silenciosa del subsistema: el JoinHandle se
//! descartaba (`let _handle`) y nadie lo notaba ni lo relanzaba.
//!
//! Diseño: cada servicio se registra con una factoría (`FnMut() -> JoinHandle`).
//! `supervise_once` cosecha tareas terminadas y las relanza (acotado); el loop
//! llamador duerme entre pasadas y respeta el shutdown global. Los eventos se
//! emiten por callback para no acoplar utils con GUI/tracing.

use std::time::Duration;
use tokio::task::JoinHandle;

/// Eventos del supervisor; el llamador decide el sink (tracing + historial + UI).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceEvent {
    /// La tarea terminó: panic=true si fue panic, false si retornó limpio
    /// antes del shutdown (también inesperado en un loop infinito).
    Died { service: &'static str, panic: bool },
    /// Se relanzó la tarea (attempt = nº de fallo consecutivo).
    Restarted { service: &'static str, attempt: u32 },
    /// Se superó max_restarts: NO se relanza (requiere intervención manual).
    GaveUp { service: &'static str },
}

pub const DEFAULT_MAX_RESTARTS: u32 = 5;
pub const DEFAULT_HEALTHY_WINDOW: Duration = Duration::from_secs(600);
pub const TICK: Duration = Duration::from_secs(5);

pub struct SupervisedService {
    pub name: &'static str,
    factory: Box<dyn FnMut() -> JoinHandle<()> + Send>,
    handle: Option<JoinHandle<()>>,
    failures: u32,
    last_start: tokio::time::Instant,
    pub max_restarts: u32,
    pub healthy_window: Duration,
}

impl SupervisedService {
    pub fn new(
        name: &'static str,
        mut factory: impl FnMut() -> JoinHandle<()> + Send + 'static,
    ) -> Self {
        let handle = Some(factory());
        Self {
            name,
            factory: Box::new(factory),
            handle,
            failures: 0,
            last_start: tokio::time::Instant::now(),
            max_restarts: DEFAULT_MAX_RESTARTS,
            healthy_window: DEFAULT_HEALTHY_WINDOW,
        }
    }

    fn respawn(&mut self) {
        self.handle = Some((self.factory)());
        self.last_start = tokio::time::Instant::now();
    }
}

/// Una pasada de supervisión: cosecha tareas terminadas y las relanza.
/// No duerme ni consulta shutdown: el loop llamador decide eso.
pub async fn supervise_once(
    services: &mut [SupervisedService],
    emit: &(dyn Fn(ServiceEvent) + Send + Sync),
) {
    for svc in services.iter_mut() {
        let finished = svc
            .handle
            .as_ref()
            .map(JoinHandle::is_finished)
            .unwrap_or(true);
        if !finished {
            continue;
        }
        let outcome = match svc.handle.take() {
            Some(h) => h.await,
            None => continue, // ya en GaveUp: sin handle, sin eventos
        };
        match &outcome {
            Ok(()) => emit(ServiceEvent::Died {
                service: svc.name,
                panic: false,
            }),
            Err(e) => emit(ServiceEvent::Died {
                service: svc.name,
                panic: e.is_panic(),
            }),
        }
        if svc.last_start.elapsed() > svc.healthy_window {
            svc.failures = 0; // racha sana larga: perdonar historial
        }
        svc.failures += 1;
        if svc.failures > svc.max_restarts {
            emit(ServiceEvent::GaveUp { service: svc.name });
            continue;
        }
        svc.respawn();
        emit(ServiceEvent::Restarted {
            service: svc.name,
            attempt: svc.failures,
        });
    }
}

/// Loop del supervisor: tick cada TICK, sale ante shutdown global.
pub async fn supervise_loop(
    mut services: Vec<SupervisedService>,
    emit: impl Fn(ServiceEvent) + Send + Sync + 'static,
) {
    loop {
        tokio::time::sleep(TICK).await;
        if super::shutdown::is_shutdown_requested() {
            break;
        }
        supervise_once(&mut services, &emit).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn events() -> (Arc<Mutex<Vec<ServiceEvent>>>, impl Fn(ServiceEvent) + Send + Sync) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();
        (log, move |ev| log2.lock().unwrap().push(ev))
    }

    fn panicker() -> SupervisedService {
        SupervisedService::new("boom", || {
            tokio::spawn(async {
                panic!("fallo inyectado");
            })
        })
    }

    /// Un panic relanza la tarea (Died{panic:true} + Restarted).
    #[tokio::test]
    async fn panic_relanza() {
        let (log, emit) = events();
        let mut svc = panicker();
        svc.healthy_window = Duration::from_secs(3600); // sin perdón en el test
        let mut services = vec![svc];
        tokio::time::sleep(Duration::from_millis(100)).await; // dejar que paniquee
        supervise_once(&mut services, &emit).await;
        let ev = log.lock().unwrap().clone();
        assert!(ev.contains(&ServiceEvent::Died {
            service: "boom",
            panic: true
        }));
        assert!(ev.contains(&ServiceEvent::Restarted {
            service: "boom",
            attempt: 1
        }));
        assert!(services[0].handle.is_some());
    }

    /// Tras max_restarts se rinde: GaveUp, sin handle, sin más eventos.
    #[tokio::test]
    async fn agotado_se_rinde() {
        let (log, emit) = events();
        let mut svc = panicker();
        svc.max_restarts = 1;
        svc.healthy_window = Duration::from_secs(3600);
        let mut services = vec![svc];
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            supervise_once(&mut services, &emit).await;
        }
        let ev = log.lock().unwrap().clone();
        assert!(ev.contains(&ServiceEvent::GaveUp { service: "boom" }));
        assert!(services[0].handle.is_none());
        let n = ev.len();
        supervise_once(&mut services, &emit).await;
        assert_eq!(log.lock().unwrap().len(), n); // sin handle: silencio total
    }

    /// Una salida limpia inesperada también relanza (los loops no retornan).
    #[tokio::test]
    async fn salida_limpia_relanza() {
        let (log, emit) = events();
        let mut services = vec![SupervisedService::new("ok", || tokio::spawn(async {}))];
        tokio::time::sleep(Duration::from_millis(100)).await;
        supervise_once(&mut services, &emit).await;
        let ev = log.lock().unwrap().clone();
        assert!(ev.contains(&ServiceEvent::Died {
            service: "ok",
            panic: false
        }));
        assert!(ev.contains(&ServiceEvent::Restarted {
            service: "ok",
            attempt: 1
        }));
    }

    /// Racha sana larga perdona el historial (no acumula hasta GaveUp).
    #[tokio::test]
    async fn racha_sana_perdona() {
        let (log, emit) = events();
        let mut svc = panicker();
        svc.max_restarts = 1;
        svc.healthy_window = Duration::from_millis(0); // todo es "racha sana"
        let mut services = vec![svc];
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            supervise_once(&mut services, &emit).await;
        }
        let ev = log.lock().unwrap().clone();
        assert!(!ev.contains(&ServiceEvent::GaveUp { service: "boom" }));
        assert!(services[0].handle.is_some());
    }
}
