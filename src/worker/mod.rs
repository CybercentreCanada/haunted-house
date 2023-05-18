use serde::{Serialize, Deserialize};

use crate::types::Sha256;

pub mod interface;
mod database;
mod database_sqlite;
mod encoding;
mod manager;
mod filter;
mod filter_worker;

#[derive(Serialize, Deserialize)]
pub struct YaraTask {
    pub id: i64,
    pub yara_rule: String,
    pub hashes: Vec<Sha256>,
}