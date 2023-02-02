
use serde::{Serialize, Deserialize};
use crate::size_type::{deserialize_size, serialize_size};


#[derive(Debug, Serialize, Deserialize)]
pub struct StaticKey {
    pub key: String,
    pub roles: Vec<crate::auth::Role>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Authentication {
    pub static_keys: Vec<StaticKey>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Database {
    SQLite {
        path: String,
    },
    SQLiteTemp,
}


#[derive(Debug, Serialize, Deserialize)]
pub enum CacheConfig {
    TempDir{
        #[serde(deserialize_with="deserialize_size", serialize_with="serialize_size")]
        size: u64
    },
    Directory {
        path: String,
        #[serde(deserialize_with="deserialize_size", serialize_with="serialize_size")]
        size: u64
    },
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub authentication: Authentication,
    pub database: Database,
    pub core: crate::core::CoreConfig,
    pub cache: CacheConfig,
    pub files: crate::storage::BlobStorageConfig,
    pub blobs: crate::storage::BlobStorageConfig,
    pub bind_address: Option<String>,
}
