
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

impl Default for Authentication {
    fn default() -> Self {
        Self {
            static_keys: vec![StaticKey {
                key: hex::encode(uuid::Uuid::new_v4().as_bytes()),
                roles: vec![crate::auth::Role::Worker]
            }, StaticKey {
                key: hex::encode(uuid::Uuid::new_v4().as_bytes()),
                roles: vec![crate::auth::Role::Search]
            }, StaticKey {
                key: hex::encode(uuid::Uuid::new_v4().as_bytes()),
                roles: vec![crate::auth::Role::Ingest]
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
    pub core: crate::core::CoreConfig,
    pub cache: CacheConfig,
    pub files: crate::storage::BlobStorageConfig,
    pub blobs: crate::storage::BlobStorageConfig,

    pub bind_address: Option<String>,
    pub tls: Option<TLSConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            authentication: Default::default(),
            database: Default::default(),
            core: Default::default(),
            cache: Default::default(),
            files: Default::default(),
            blobs: Default::default(),
            bind_address: Some("localhost:4443".to_owned()),
            tls: None
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub api_token: String,
    // pub authentication: Authentication,
    // pub database: Database,
    // pub core: crate::core::CoreConfig,
    pub file_cache: CacheConfig,
    pub blob_cache: CacheConfig,
    pub files: crate::storage::BlobStorageConfig,
    pub blobs: crate::storage::BlobStorageConfig,

    pub bind_address: Option<String>,
    pub tls: Option<TLSConfig>,

    pub server_address: String,
    pub server_tls: WorkerTLSConfig
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            api_token: "<API token with worker role>".to_owned(),
            file_cache: Default::default(),
            blob_cache: Default::default(),
            files: Default::default(),
            blobs: Default::default(),
            bind_address: Some("localhost:4444".to_owned()),
            tls: None,
            server_address: "localhost:4443".to_owned(),
            server_tls: WorkerTLSConfig::Certificate("Server TLS cert".to_owned())
        }
    }
}