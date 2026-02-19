//! Historial de acciones recientes para mostrar en el icono de bandeja
//!
//! Almacena las últimas N acciones de sincronización, creación, eliminación, etc.
//! También rastrea transfers activos (uploads/downloads) con progreso.

use std::collections::{VecDeque, HashMap};
use std::sync::{Arc, RwLock, mpsc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

/// Número máximo de entradas en el historial
const MAX_HISTORY_ENTRIES: usize = 50;

/// Contador global para IDs únicos de transfers
static NEXT_TRANSFER_ID: AtomicU64 = AtomicU64::new(1);

/// Tipo de acción registrada
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionType {
    Sync,
    Upload,
    Download,
    Create,
    Delete,
    Conflict,
    Error,
}

impl ActionType {
    /// Retorna un emoji representativo de la acción
    pub fn emoji(&self) -> &'static str {
        match self {
            ActionType::Sync => "🔄",
            ActionType::Upload => "📤",
            ActionType::Download => "📥",
            ActionType::Create => "✨",
            ActionType::Delete => "🗑️",
            ActionType::Conflict => "⚠️",
            ActionType::Error => "❌",
        }
    }
}

/// Tipo de operación de transferencia
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferOp {
    Upload,
    Download,
}

impl TransferOp {
    pub fn emoji(&self) -> &'static str {
        match self {
            TransferOp::Upload => "📤",
            TransferOp::Download => "📥",
        }
    }
}

/// Transfer activo con progreso
#[derive(Debug, Clone)]
pub struct ActiveTransfer {
    pub id: u64,
    pub file_name: String,
    pub operation: TransferOp,
    pub bytes_transferred: u64,
    pub total_bytes: u64,
}

impl ActiveTransfer {
    pub fn progress_fraction(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        (self.bytes_transferred as f64) / (self.total_bytes as f64)
    }
}

/// Estado de progreso de sincronización (cambios detectados vs aplicados)
#[derive(Debug, Clone, Default)]
pub struct SyncProgress {
    pub changes_detected: usize,
    pub changes_applied: usize,
}

impl SyncProgress {
    pub fn is_synced(&self) -> bool {
        self.changes_detected == 0 || self.changes_detected == self.changes_applied
    }
}

/// Entrada individual del historial
#[derive(Debug, Clone)]
pub struct ActionEntry {
    pub timestamp: SystemTime,
    pub action_type: ActionType,
    pub description: String,
}

impl ActionEntry {
    pub fn new(action_type: ActionType, description: impl Into<String>) -> Self {
        Self {
            timestamp: SystemTime::now(),
            action_type,
            description: description.into(),
        }
    }

    /// Formatea la entrada para mostrar en el menú del tray
    pub fn format_for_menu(&self) -> String {
        let elapsed = self.timestamp.elapsed().unwrap_or_default();
        let time_str = if elapsed.as_secs() < 60 {
            "ahora".to_string()
        } else if elapsed.as_secs() < 3600 {
            format!("{}m", elapsed.as_secs() / 60)
        } else {
            format!("{}h", elapsed.as_secs() / 3600)
        };

        format!(
            "{} {} ({})",
            self.action_type.emoji(),
            self.description,
            time_str
        )
    }
}

/// Historial de acciones thread-safe
#[derive(Clone)]
pub struct ActionHistory {
    entries: Arc<RwLock<VecDeque<ActionEntry>>>,
    active_transfers: Arc<RwLock<HashMap<u64, ActiveTransfer>>>,
    sync_progress: Arc<RwLock<SyncProgress>>,
    notify: Arc<RwLock<Option<mpsc::Sender<()>>>>,
}

impl Default for ActionHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl ActionHistory {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_HISTORY_ENTRIES))),
            active_transfers: Arc::new(RwLock::new(HashMap::new())),
            sync_progress: Arc::new(RwLock::new(SyncProgress::default())),
            notify: Arc::new(RwLock::new(None)),
        }
    }

    /// Registra un notificador que se dispara cada vez que se añade una entrada
    pub fn set_notifier(&self, tx: mpsc::Sender<()>) {
        if let Ok(mut notify) = self.notify.write() {
            *notify = Some(tx);
        }
    }

    fn notify_change(&self) {
        if let Ok(notify) = self.notify.read() {
            if let Some(tx) = notify.as_ref() {
                let _ = tx.send(());
            }
        }
    }

    /// Añade una nueva entrada al historial
    pub fn push(&self, entry: ActionEntry) {
        if let Ok(mut entries) = self.entries.write() {
            if entries.len() >= MAX_HISTORY_ENTRIES {
                entries.pop_back();
            }
            entries.push_front(entry);
        }
        self.notify_change();
    }

    /// Añade una entrada de forma conveniente
    pub fn log(&self, action_type: ActionType, description: impl Into<String>) {
        self.push(ActionEntry::new(action_type, description));
    }

    /// Obtiene las N entradas más recientes
    pub fn recent(&self, count: usize) -> Vec<ActionEntry> {
        if let Ok(entries) = self.entries.read() {
            entries.iter().take(count).cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// Obtiene todas las entradas
    pub fn all(&self) -> Vec<ActionEntry> {
        if let Ok(entries) = self.entries.read() {
            entries.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }

    // --- Transfer activo API ---

    /// Inicia un nuevo transfer activo. Retorna el ID asignado.
    pub fn start_transfer(&self, file_name: impl Into<String>, operation: TransferOp, total_bytes: u64) -> u64 {
        let id = NEXT_TRANSFER_ID.fetch_add(1, Ordering::Relaxed);
        let transfer = ActiveTransfer {
            id,
            file_name: file_name.into(),
            operation,
            bytes_transferred: 0,
            total_bytes,
        };
        if let Ok(mut transfers) = self.active_transfers.write() {
            transfers.insert(id, transfer);
        }
        self.notify_change();
        id
    }

    /// Actualiza el progreso de un transfer activo
    pub fn update_transfer_progress(&self, id: u64, bytes_transferred: u64) {
        if let Ok(mut transfers) = self.active_transfers.write() {
            if let Some(transfer) = transfers.get_mut(&id) {
                transfer.bytes_transferred = bytes_transferred;
            }
        }
    }

    /// Completa y remueve un transfer activo
    pub fn complete_transfer(&self, id: u64) {
        if let Ok(mut transfers) = self.active_transfers.write() {
            transfers.remove(&id);
        }
        self.notify_change();
    }

    /// Obtiene una copia de todos los transfers activos
    pub fn active_transfers(&self) -> Vec<ActiveTransfer> {
        if let Ok(transfers) = self.active_transfers.read() {
            transfers.values().cloned().collect()
        } else {
            Vec::new()
        }
    }

    // --- Sync progress API ---

    /// Establece el progreso de sincronización (cambios detectados y aplicados)
    pub fn set_sync_progress(&self, detected: usize, applied: usize) {
        if let Ok(mut progress) = self.sync_progress.write() {
            progress.changes_detected = detected;
            progress.changes_applied = applied;
        }
    }

    /// Incrementa en 1 el contador de cambios aplicados
    pub fn increment_applied(&self) {
        if let Ok(mut progress) = self.sync_progress.write() {
            progress.changes_applied += 1;
        }
    }

    /// Marca todo como sincronizado (resetea contadores)
    pub fn mark_all_synced(&self) {
        if let Ok(mut progress) = self.sync_progress.write() {
            progress.changes_detected = 0;
            progress.changes_applied = 0;
        }
    }

    /// Obtiene el estado actual de progreso de sincronización
    pub fn get_sync_progress(&self) -> SyncProgress {
        if let Ok(progress) = self.sync_progress.read() {
            progress.clone()
        } else {
            SyncProgress::default()
        }
    }
}
