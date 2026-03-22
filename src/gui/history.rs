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
    Stream,
}

impl ActionType {
    /// Retorna un emoji representativo de la acción
    pub fn emoji(&self) -> &'static str {
        match self {
            ActionType::Sync => "🔄",
            ActionType::Upload => "📤",
            ActionType::Download => "📥",
            ActionType::Create => "📄",
            ActionType::Delete => "🗑️",
            ActionType::Conflict => "⚠️",
            ActionType::Error => "❌",
            ActionType::Stream => "🎬",
        }
    }
}

/// Tipo de operación de transferencia
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferOp {
    Upload,
    Download,
    Stream,
}

impl TransferOp {
    pub fn emoji(&self) -> &'static str {
        match self {
            TransferOp::Upload => "📤",
            TransferOp::Download => "📥",
            TransferOp::Stream => "🎬",
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
    pub speed_bps: u64,
    pub last_update: Option<(SystemTime, u64)>,
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
    pub pending_uploads: usize,
    /// Total de archivos escaneados (0 = no hay escaneo en curso)
    pub scanning_total: usize,
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
            speed_bps: 0,
            last_update: Some((SystemTime::now(), 0)),
        };
        if let Ok(mut transfers) = self.active_transfers.write() {
            transfers.insert(id, transfer);
        }
        self.notify_change();
        id
    }

    /// Actualiza el progreso de un transfer activo
    pub fn update_transfer_progress(&self, id: u64, bytes_transferred: u64) {
        let changed = if let Ok(mut transfers) = self.active_transfers.write() {
            if let Some(transfer) = transfers.get_mut(&id) {
                // Calcular velocidad si ha pasado suficiente tiempo (ej. 500ms)
                let now = SystemTime::now();
                let mut speed_updated = false;
                
                if let Some((last_time, last_bytes)) = transfer.last_update {
                    if let Ok(elapsed) = now.duration_since(last_time) {
                        if elapsed.as_millis() >= 500 {
                            let bytes_delta = bytes_transferred.saturating_sub(last_bytes);
                            let current_speed = (bytes_delta as f64 / elapsed.as_secs_f64()) as u64;
                            
                            // Suavizado simple (EMA)
                            if transfer.speed_bps == 0 {
                                transfer.speed_bps = current_speed;
                            } else {
                                transfer.speed_bps = (transfer.speed_bps as f64 * 0.7 + current_speed as f64 * 0.3) as u64;
                            }
                            
                            transfer.last_update = Some((now, bytes_transferred));
                            speed_updated = true;
                        }
                    }
                } else {
                    transfer.last_update = Some((now, bytes_transferred));
                }

                if transfer.bytes_transferred != bytes_transferred || speed_updated {
                    transfer.bytes_transferred = bytes_transferred;
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if changed {
            self.notify_change();
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
        let changed = if let Ok(mut progress) = self.sync_progress.write() {
            let changed = progress.changes_detected != detected || progress.changes_applied != applied;
            progress.changes_detected = detected;
            progress.changes_applied = applied;
            changed
        } else {
            false
        };

        if changed {
            self.notify_change();
        }
    }

    /// Incrementa en 1 el contador de cambios aplicados
    pub fn increment_applied(&self) {
        if let Ok(mut progress) = self.sync_progress.write() {
            progress.changes_applied += 1;
        }
        self.notify_change();
    }

    /// Marca todo como sincronizado (resetea contadores)
    pub fn mark_all_synced(&self) {
        let changed = if let Ok(mut progress) = self.sync_progress.write() {
            let changed = progress.changes_detected != 0 || progress.changes_applied != 0;
            progress.changes_detected = 0;
            progress.changes_applied = 0;
            changed
        } else {
            false
        };

        if changed {
            self.notify_change();
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

    /// Actualiza el conteo de subidas pendientes localmente
    pub fn set_pending_uploads(&self, count: usize) {
        let changed = if let Ok(mut progress) = self.sync_progress.write() {
            let changed = progress.pending_uploads != count;
            progress.pending_uploads = count;
            changed
        } else {
            false
        };

        // Si el conteo cambió, notificar al tray (hacerlo fuera del lock)
        if changed {
            self.notify_change();
        }
    }

    /// Actualiza el total de archivos escaneados (0 = escaneo finalizado)
    pub fn set_scanning_total(&self, count: usize) {
        let changed = if let Ok(mut progress) = self.sync_progress.write() {
            let changed = progress.scanning_total != count;
            progress.scanning_total = count;
            changed
        } else {
            false
        };

        if changed {
            self.notify_change();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[fixture]
    fn history() -> ActionHistory {
        ActionHistory::new()
    }

    // --- ActionType ---

    #[rstest]
    #[case::sync(ActionType::Sync, "🔄")]
    #[case::upload(ActionType::Upload, "📤")]
    #[case::download(ActionType::Download, "📥")]
    #[case::create(ActionType::Create, "📄")]
    #[case::delete(ActionType::Delete, "🗑️")]
    #[case::conflict(ActionType::Conflict, "⚠️")]
    #[case::error(ActionType::Error, "❌")]
    #[case::stream(ActionType::Stream, "🎬")]
    fn test_action_type_emoji(#[case] action: ActionType, #[case] expected: &str) {
        assert_eq!(action.emoji(), expected);
    }

    // --- TransferOp ---

    #[rstest]
    #[case::upload(TransferOp::Upload, "📤")]
    #[case::download(TransferOp::Download, "📥")]
    #[case::stream(TransferOp::Stream, "🎬")]
    fn test_transfer_op_emoji(#[case] op: TransferOp, #[case] expected: &str) {
        assert_eq!(op.emoji(), expected);
    }

    // --- ActiveTransfer progress ---

    #[rstest]
    #[case::zero_total(0, 0, 0.0)]
    #[case::half(500, 1000, 0.5)]
    #[case::complete(1000, 1000, 1.0)]
    #[case::quarter(250, 1000, 0.25)]
    fn test_progress_fraction(
        #[case] transferred: u64,
        #[case] total: u64,
        #[case] expected: f64,
    ) {
        let transfer = ActiveTransfer {
            id: 1,
            file_name: "test.txt".into(),
            operation: TransferOp::Upload,
            bytes_transferred: transferred,
            total_bytes: total,
            speed_bps: 0,
            last_update: None,
        };
        let diff = (transfer.progress_fraction() - expected).abs();
        assert!(diff < 0.001, "Expected {}, got {}", expected, transfer.progress_fraction());
    }

    // --- SyncProgress ---

    #[rstest]
    fn test_sync_progress_default_is_synced() {
        let p = SyncProgress::default();
        assert!(p.is_synced());
    }

    #[rstest]
    #[case::no_changes(0, 0, true)]
    #[case::all_applied(10, 10, true)]
    #[case::partial(10, 5, false)]
    #[case::none_applied(10, 0, false)]
    fn test_sync_progress_is_synced(
        #[case] detected: usize,
        #[case] applied: usize,
        #[case] expected: bool,
    ) {
        let p = SyncProgress { changes_detected: detected, changes_applied: applied, pending_uploads: 0, scanning_total: 0 };
        assert_eq!(p.is_synced(), expected);
    }

    // --- ActionEntry ---

    #[rstest]
    fn test_action_entry_format_for_menu() {
        let entry = ActionEntry::new(ActionType::Upload, "archivo.txt subido");
        let formatted = entry.format_for_menu();
        assert!(formatted.contains("📤"));
        assert!(formatted.contains("archivo.txt subido"));
        assert!(formatted.contains("ahora"));
    }

    // --- ActionHistory: push y recent ---

    #[rstest]
    fn test_push_and_recent(history: ActionHistory) {
        history.log(ActionType::Sync, "sync 1");
        history.log(ActionType::Upload, "upload 1");
        history.log(ActionType::Download, "download 1");

        let recent = history.recent(2);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].description, "download 1"); // Más reciente primero
        assert_eq!(recent[1].description, "upload 1");
    }

    #[rstest]
    fn test_history_max_capacity(history: ActionHistory) {
        for i in 0..60 {
            history.log(ActionType::Sync, format!("entry {}", i));
        }

        let all = history.all();
        assert_eq!(all.len(), MAX_HISTORY_ENTRIES);
        assert_eq!(all[0].description, "entry 59"); // El más reciente
    }

    // --- Transfers ---

    #[rstest]
    fn test_start_and_complete_transfer(history: ActionHistory) {
        let id = history.start_transfer("big_file.zip", TransferOp::Upload, 1_000_000);
        assert!(id > 0);

        let active = history.active_transfers();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].file_name, "big_file.zip");
        assert_eq!(active[0].total_bytes, 1_000_000);

        history.complete_transfer(id);
        assert!(history.active_transfers().is_empty());
    }

    #[rstest]
    fn test_update_transfer_progress(history: ActionHistory) {
        let id = history.start_transfer("file.dat", TransferOp::Download, 1000);
        history.update_transfer_progress(id, 500);

        let active = history.active_transfers();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].bytes_transferred, 500);
    }

    // --- Sync Progress API ---

    #[rstest]
    fn test_set_sync_progress(history: ActionHistory) {
        history.set_sync_progress(10, 3);
        let p = history.get_sync_progress();
        assert_eq!(p.changes_detected, 10);
        assert_eq!(p.changes_applied, 3);
        assert!(!p.is_synced());
    }

    #[rstest]
    fn test_increment_applied(history: ActionHistory) {
        history.set_sync_progress(5, 2);
        history.increment_applied();
        let p = history.get_sync_progress();
        assert_eq!(p.changes_applied, 3);
    }

    #[rstest]
    fn test_mark_all_synced(history: ActionHistory) {
        history.set_sync_progress(10, 7);
        history.mark_all_synced();
        let p = history.get_sync_progress();
        assert!(p.is_synced());
        assert_eq!(p.changes_detected, 0);
        assert_eq!(p.changes_applied, 0);
    }

    #[rstest]
    fn test_set_pending_uploads(history: ActionHistory) {
        history.set_pending_uploads(5);
        let p = history.get_sync_progress();
        assert_eq!(p.pending_uploads, 5);
    }

    // --- Notifier ---

    #[rstest]
    fn test_notifier_fires_on_push(history: ActionHistory) {
        let (tx, rx) = std::sync::mpsc::channel();
        history.set_notifier(tx);

        history.log(ActionType::Sync, "test");

        // Debe recibir notificación
        let result = rx.recv_timeout(std::time::Duration::from_millis(100));
        assert!(result.is_ok(), "Notifier should fire on push");
    }

    #[rstest]
    fn test_notifier_fires_on_transfer(history: ActionHistory) {
        let (tx, rx) = std::sync::mpsc::channel();
        history.set_notifier(tx);

        history.start_transfer("f.txt", TransferOp::Upload, 100);

        let result = rx.recv_timeout(std::time::Duration::from_millis(100));
        assert!(result.is_ok(), "Notifier should fire on start_transfer");
    }

    // --- Clone / thread safety ---

    #[rstest]
    fn test_clone_shares_state(history: ActionHistory) {
        let clone = history.clone();
        history.log(ActionType::Create, "from original");

        let recent = clone.recent(1);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].description, "from original");
    }
}
