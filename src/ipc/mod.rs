use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcMessage {
    Hello,
    GetStatus,
    StatusResponse { connected: bool, syncing_files: usize },
}
