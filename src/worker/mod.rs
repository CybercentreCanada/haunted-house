pub mod interface;
mod manager;

pub use manager::{worker_manager, WorkerData};
use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize)]
pub struct StatusReport {
    pub active_yara: Vec<i64>,
}
