use serde::{Serialize, Deserialize};

use crate::types::Sha256;

pub mod interface;
mod database;
mod database_sqlite;
mod manager;

#[derive(Serialize, Deserialize)]
pub struct YaraTask {
    pub id: i64,
    pub search: String,
    pub yara_rule: String,
    pub hashes: Vec<Sha256>,
}