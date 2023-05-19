
use std::path::PathBuf;

use serde::{Serialize, Deserialize};
use crate::broker::auth::Role;
use crate::size_type::{deserialize_size, serialize_size};


#[derive(Debug, Serialize, Deserialize)]
pub struct StaticKey {
    pub key: String,
    pub roles: Vec<Role>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Authentication {
    pub static_keys: Vec<StaticKey>,
}

impl Default for Authentication {
    fn default() -> Self {
        Self {
            static_keys: vec![StaticKey {
                key: hex::encode(uuid::Uuid::new_v4().as_bytes()),
                roles: vec![Role::Search]
            }, StaticKey {
                key: hex::encode(uuid::Uuid::new_v4().as_bytes()),
                roles: vec![Role::Ingest]
            }]
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Database {
    SQLite {
        path: String,
    },
    SQLiteTemp,
}

impl Default for Database {
    fn default() -> Self {
        Database::SQLite { path: "/database/path".to_owned() }
    }
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

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig::Directory {
            path: "/cache/path".to_owned(),
            size: 100 << 30
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TLSConfig {
    pub key_pem: String,
    pub certificate_pem: String
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerTLSConfig {
    AllowAll,
    Certificate(String)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub authentication: Authentication,
    pub database: Database,
    pub core: crate::broker::CoreConfig,
    pub cache: CacheConfig,
    // pub files: crate::storage::BlobStorageConfig,
    // pub blobs: crate::storage::BlobStorageConfig,

    pub bind_address: Option<String>,
    pub tls: Option<TLSConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            authentication: Default::default(),
            database: Default::default(),
            core: crate::broker::CoreConfig {
                workers: Default::default(),
                per_filter_pending_limit: 1000,
                per_worker_group_duplication: 2,
                search_hit_limit: 50000,
                yara_jobs_per_worker: 2,
                yara_batch_size: 100,
            },
            cache: Default::default(),
            // files: Default::default(),
            // blobs: Default::default(),
            bind_address: Some("localhost:4443".to_owned()),
            tls: None
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub file_cache: CacheConfig,
    pub files: crate::storage::BlobStorageConfig,
    pub settings: crate::worker::WorkerConfig,
    pub bind_address: Option<String>,
    pub tls: Option<TLSConfig>,
}


impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            file_cache: Default::default(),
            files: Default::default(),
            settings: crate::worker::WorkerConfig {
                filter_item_limit: 50_000_000,
                data_path: PathBuf::from("/data/"),
                data_limit: 1 << 40,
                data_reserve: 5 << 30,
                initial_segment_size: 128,
                extended_segment_size: 2048,
                ingest_batch_size: 100,
            },
            bind_address: Some("localhost:4444".to_owned()),
            tls: None,
        }
    }
}