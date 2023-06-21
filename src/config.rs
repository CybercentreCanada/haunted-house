//! Layout and parsing tools for server configuration
use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::Result;

use serde::{Serialize, Deserialize};
use crate::broker::auth::Role;
use crate::types::{WorkerID, serialize_size, deserialize_size};


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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerAddress(String);

impl From<&str> for WorkerAddress {
    fn from(value: &str) -> Self {
        WorkerAddress(value.to_owned())
    }
}

impl WorkerAddress {
    pub fn http(&self, path: &str) -> anyhow::Result<reqwest::Url> {
        Ok(reqwest::Url::parse(&format!("https://{}", self.0))?.join(path)?)
    }
    pub fn websocket(&self, path: &str) -> anyhow::Result<reqwest::Url> {
        Ok(reqwest::Url::parse(&format!("wss://{}", self.0))?.join(path)?)
    }
}

enum IngestSource {
    Elasticsearch {
        url: String,
        username: String,
        password: String,
        index: String,
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct CoreConfig {
    pub workers: HashMap<WorkerID, WorkerAddress>,
    pub worker_certificate: WorkerTLSConfig,
    #[serde(default="default_per_filter_pending_limit")]
    pub per_filter_pending_limit: u64,
    #[serde(default="default_per_worker_group_duplication")]
    pub per_worker_group_duplication: u32,
    #[serde(default="default_search_hit_limit")]
    pub search_hit_limit: usize,
    #[serde(default="default_yara_jobs_per_worker")]
    pub yara_jobs_per_worker: usize,
    #[serde(default="default_yara_batch_size")]
    pub yara_batch_size: u32,
    #[serde(default="default_filter_item_limit")]
    pub filter_item_limit: u64,
}

fn default_per_filter_pending_limit() -> u64 { 1000 }
fn default_per_worker_group_duplication() -> u32 { 2 }
fn default_search_hit_limit() -> usize { 50000 }
fn default_yara_jobs_per_worker() -> usize { 2 }
fn default_yara_batch_size() -> u32 { 100 }
fn default_filter_item_limit() -> u64 { 50_000_000 }

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub authentication: Authentication,
    pub database: Database,
    pub core: CoreConfig,
    // pub cache: CacheConfig,
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
            core: CoreConfig {
                workers: [
                    (WorkerID::from("worker-1".to_owned()), WorkerAddress::from("worker-0:4000"))
                ].into(),
                worker_certificate: WorkerTLSConfig::AllowAll,
                filter_item_limit: default_filter_item_limit(),
                per_filter_pending_limit: default_per_filter_pending_limit(),
                per_worker_group_duplication: default_per_worker_group_duplication(),
                search_hit_limit: default_search_hit_limit(),
                yara_jobs_per_worker: default_yara_jobs_per_worker(),
                yara_batch_size: default_yara_batch_size(),
            },
            // cache: Default::default(),
            // files: Default::default(),
            // blobs: Default::default(),
            bind_address: Some("localhost:4443".to_owned()),
            tls: None
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerSettings {
    #[serde(default="default_data_path")]
    pub data_path: PathBuf,
    #[serde(default="default_data_limit")]
    pub data_limit: u64,
    #[serde(default="default_data_reserve")]
    pub data_reserve: u64,
    #[serde(default="default_initial_segment_size")]
    pub initial_segment_size: u32,
    #[serde(default="default_extended_segment_size")]
    pub extended_segment_size: u32,
    #[serde(default="default_ingest_batch_size")]
    pub ingest_batch_size: u32,
    #[serde(default="default_parallel_file_downloads")]
    pub parallel_file_downloads: usize
}

const TRIGRAM_DIRECTORY: &str = "trigram-cache";
const FILTER_DIRETORY: &str = "filters";
const DATABASE_DIRETORY: &str = "sql-data";

impl WorkerSettings {
    pub fn init_directories(&self) -> Result<()> {
        std::fs::create_dir_all(self.get_trigram_cache_directory())?;
        std::fs::create_dir_all(self.get_filter_directory())?;
        std::fs::create_dir_all(self.get_database_directory())?;
        return Ok(())
    }

    pub fn get_trigram_cache_directory(&self) -> PathBuf {
        self.data_path.join(TRIGRAM_DIRECTORY)
    }

    pub fn get_filter_directory(&self) -> PathBuf {
        self.data_path.join(FILTER_DIRETORY)
    }

    pub fn get_database_directory(&self) -> PathBuf {
        self.data_path.join(DATABASE_DIRETORY)
    }
}


fn default_data_path() -> PathBuf { PathBuf::from("/data/") }
fn default_data_limit() -> u64 { 1 << 40 }
fn default_data_reserve() -> u64 { 5 << 30 }
fn default_initial_segment_size() -> u32 { 128 }
fn default_extended_segment_size() -> u32 { 2048 }
fn default_ingest_batch_size() -> u32 { 100 }
fn default_parallel_file_downloads() -> usize { 100 }


#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub file_cache: CacheConfig,
    pub files: crate::storage::BlobStorageConfig,
    pub settings: WorkerSettings,
    pub bind_address: Option<String>,
    pub tls: Option<TLSConfig>,
}


impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            file_cache: Default::default(),
            files: Default::default(),
            settings: WorkerSettings {
                data_path: default_data_path(),
                data_limit: default_data_limit(),
                data_reserve: default_data_reserve(),
                initial_segment_size: default_initial_segment_size(),
                extended_segment_size: default_extended_segment_size(),
                ingest_batch_size: default_ingest_batch_size(),
                parallel_file_downloads: default_parallel_file_downloads(),
            },
            bind_address: Some("localhost:4444".to_owned()),
            tls: None,
        }
    }
}


pub fn apply_env(data: &str) -> Result<String> {
    apply_variables(data, &std::env::vars().collect())
}

const REGEX: &str = r#"(^|.)\$\{([0-9[:alpha:]]+)(?::-?((?:\\\}|[^}])*))?\}"#;

pub fn apply_variables(data: &str, vars: &HashMap<String, String>) -> Result<String> {
    let parser = regex::Regex::new(REGEX)?;
    let mut input = data;
    let mut output: String = "".to_owned();

    while let Some(capture) = parser.captures(input) {
        // Include the input before the match
        let full_match = capture.get(0).unwrap();
        let start = full_match.start();
        output += &input[0..start];

        // Advance window
        let end = full_match.end();
        input = &input[end..];

        let prefix = capture.get(1).unwrap().as_str();

        // Handle the case where the substition is prefixed with a backslash
        if prefix == "\\" {
            output += &full_match.as_str()[1..];
            continue
        } else {
            output += prefix;
        }

        let name = capture.get(2).unwrap().as_str();
        let default = capture.get(3).map(|val| val.as_str());

        // Get the variable form the map
        if let Some(value) = vars.get(name) {
            output += value;
        } else if let Some(value) = default {
            output += &value.replace("\\}", "}");
        } else {
            return Err(anyhow::anyhow!("Unknown environment variable with no default: {name}"));
        }
    }

    output += input;
    return Ok(output)
}


#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::apply_variables;


    #[test]
    fn env_application() {
        // Simple substitution0
        let values = HashMap::from([("Abc".to_owned(), "cats".to_owned())]);
        assert_eq!(apply_variables("abc123", &values).unwrap(), "abc123".to_owned());
        assert_eq!(apply_variables("abc${Abc}123", &values).unwrap(), "abccats123".to_owned());
        assert_eq!(apply_variables("${Abc}", &values).unwrap(), "cats".to_owned());
        assert_eq!(apply_variables("${Abc}123", &values).unwrap(), "cats123".to_owned());
        assert_eq!(apply_variables("abc${Abc}", &values).unwrap(), "abccats".to_owned());
        assert_eq!(apply_variables("\\${Abc}123", &values).unwrap(), "${Abc}123".to_owned());
        assert_eq!(apply_variables("abc\\${Abc}123", &values).unwrap(), "abc${Abc}123".to_owned());

        // substitution with default
        assert_eq!(apply_variables("abc${Abc:-dogs}123", &values).unwrap(), "abccats123".to_owned());
        assert_eq!(apply_variables("abc${xyz:-dogs}123", &values).unwrap(), "abcdogs123".to_owned());
        assert_eq!(apply_variables("abc${Abc:dogs}123", &values).unwrap(), "abccats123".to_owned());
        assert_eq!(apply_variables("abc${xyz:dogs}123", &values).unwrap(), "abcdogs123".to_owned());

        // empty default
        assert_eq!(apply_variables("abc${Abc:}123", &values).unwrap(), "abccats123".to_owned());
        assert_eq!(apply_variables("abc${xyz:}123", &values).unwrap(), "abc123".to_owned());

        // Error for missing variables
        assert!(apply_variables("abc${xyz}123", &values).is_err());

        // Handle complex value string
        assert_eq!(apply_variables(r#"abc${xyz:{\/$$_0[\}]-+}123"#, &values).unwrap(), "abc{\\/$$_0[}]-+123".to_owned());
    }
}