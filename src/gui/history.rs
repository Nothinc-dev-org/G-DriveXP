//! Historial de acciones recientes para mostrar en el icono de bandeja
//!
//! Almacena las últimas N acciones de sincronización, creación, eliminación, etc.

use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

/// Número máximo de entradas en el historial
const MAX_HISTORY_ENTRIES: usize = 50;

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
}
