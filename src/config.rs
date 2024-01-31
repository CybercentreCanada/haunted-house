//! Layout and parsing tools for server configuration
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::Result;

use assemblyline_markings::classification::ClassificationParser;
use serde::{Serialize, Deserialize};
use crate::broker::auth::Role;
use crate::types::{WorkerID, serialize_size, deserialize_size};

/// Static assignment of an api key to a set of roles
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StaticKey {
    /// API key to be assigned
    pub key: String,
    /// Set of roles assigned to key
    pub roles: Vec<Role>
}

/// Configuration for authentication to the server's api
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Authentication {
    /// Access granted via statically defined api keys
    pub static_keys: Vec<StaticKey>,
}

impl Default for Authentication {
    fn default() -> Self {
        Self {
            static_keys: vec![StaticKey {
                key: hex::encode(uuid::Uuid::new_v4().as_bytes()),
                roles: vec![Role::Search]
            }]
        }
    }
}

/// Configuration of the server's database
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Database {
    /// Server will use an sqlite database loaded on a fixed path
    SQLite {
        /// directory where sqlite database can be written
        path: String,
    },
    /// Server will use an squlite database loaded into a temporary directory
    SQLiteTemp,
}

impl Default for Database {
    fn default() -> Self {
        Database::SQLite { path: "/database/path".to_owned() }
    }
}

/// Configure directory to use as local cache for blob storage
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CacheConfig {
    /// Use a system defined temporary directory
    TempDir{
        /// Number of bytes to let the cache occupy
        #[serde(deserialize_with="deserialize_size", serialize_with="serialize_size")]
        size: u64
    },
    /// Use a fixed directory for the blob cache
    Directory {
        /// Path to directory for blob cache
        path: String,
        /// Number of bytes to let the cache occupy
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

/// Information server can use to configure its tls binding
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TLSConfig {
    /// Private key for the server certificate
    pub key_pem: String,
    /// TLS certificate for the server to use
    pub certificate_pem: String
}

/// Authentication information for broker's http client when connecting to workers
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum WorkerTLSConfig {
    /// Accept whatever self signed certificate the worker presents
    AllowAll,
    /// Only allow connections to workers presenting certificates signed by this CA
    Certificate(String)
}

/// hostname which can be used to locate a worker node
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerAddress(String);

impl From<&str> for WorkerAddress {
    fn from(value: &str) -> Self {
        WorkerAddress(value.to_owned())
    }
}

impl WorkerAddress {
    /// Build a url to access the worker via http
    pub fn http(&self, path: &str) -> anyhow::Result<reqwest::Url> {
        Ok(reqwest::Url::parse(&format!("https://{}", self.0))?.join(path)?)
    }
    /// Build a url to access the worker via websocket
    pub fn websocket(&self, path: &str) -> anyhow::Result<reqwest::Url> {
        Ok(reqwest::Url::parse(&format!("wss://{}", self.0))?.join(path)?)
    }
}

/// Path to use to extract data from a json document
#[derive(Debug, Serialize, Deserialize)]
pub struct FieldExtractor(Vec<String>);

/// Pull file information from an assemblyline server
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct Datastore {
    /// Url to connect to elasticsearch server
    pub url: String,
    /// Seconds between polling calls to fetch more file data
    pub poll_interval: f64,
    /// Maximum number of pending ingestion tasks
    pub concurrent_tasks: usize,
    /// How many items to get from assemblyline interface with each call
    pub batch_size: usize,

    pub ca_cert: Option<String>,

    pub connect_unsafe: bool,
    pub archive_access: bool,
}

impl Default for Datastore {
    fn default() -> Self {
        Self { 
            url: Default::default(), 
            poll_interval: 5.0, 
            concurrent_tasks: 30000, 
            batch_size: 2000, 
            ca_cert: None, 
            connect_unsafe: false, 
            archive_access: true,
        }
    }
}

/// default value for per_filter_pending_limit
fn default_per_filter_pending_limit() -> u64 { 1000 }
/// default value for per_worker_group_duplication
fn default_per_worker_group_duplication() -> u32 { 2 }
/// default value for search_hit_limit
fn default_search_hit_limit() -> usize { 50000 }
/// default value for yara_jobs_per_worker
fn default_yara_jobs_per_worker() -> usize { 2 }
/// default value for yara_batch_size
fn default_yara_batch_size() -> u32 { 100 }
/// default value for filter_item_limit
fn default_filter_item_limit() -> u64 { 50_000_000 }

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ClassificationConfig {
    pub classification_path: Option<PathBuf>,
    pub classification: Option<String>,
}

impl ClassificationConfig {
    pub fn load(&self) -> Result<String> {
        if let Some(classification) = &self.classification {
            Ok(classification.clone())
        } else if let Some(path) = &self.classification_path {
            Ok(std::fs::read_to_string(path)?)
        } else {
            Err(crate::error::ErrorKinds::ClassificationConfigurationError("Config missing".to_owned()).into())
        }
    }

    pub fn init(&self) -> Result<Arc<ClassificationParser>> {
        let definition = self.load()?;
        let classification: assemblyline_markings::config::ClassificationConfig = serde_json::from_str(&definition)?;
        let access_engine = Arc::new(ClassificationParser::new(classification)?);
        assemblyline_markings::set_default(access_engine.clone());
        Ok(access_engine)
    }
}


/// Root configuration schema for broker server
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BrokerSettings {
    /// Configure API tokens for accessing the system
    pub authentication: Authentication,
    /// A location that persistant files can be saved
    pub local_storage: PathBuf,
    /// Which address should the server present on
    pub bind_address: Option<String>,
    /// TLS settings for the outward facing API
    pub tls: Option<TLSConfig>,

    pub datastore: Datastore,
    #[serde(flatten)]
    pub classification: ClassificationConfig,

    /// List of workers controlled by the broker server
    pub workers: HashMap<WorkerID, WorkerAddress>,
    /// Certificate used to secure connection with broker server
    pub worker_certificate: WorkerTLSConfig,
    /// Maximum number of files that may be pending per filter file
    #[serde(default="default_per_filter_pending_limit")]
    pub per_filter_pending_limit: u64,
    /// Maximum number of filter files belonging to the same expiry
    /// group that may be co-located on a single worker
    #[serde(default="default_per_worker_group_duplication")]
    pub per_worker_group_duplication: u32,
    /// Maximum number of files that may be counted as hits for a single search
    /// before the result set is truncated
    #[serde(default="default_search_hit_limit")]
    pub search_hit_limit: usize,
    /// Maximum number of concurrent yara jobs assigned to a worker node
    #[serde(default="default_yara_jobs_per_worker")]
    pub yara_jobs_per_worker: usize,
    /// Maximum number of files per yara job
    #[serde(default="default_yara_batch_size")]
    pub yara_batch_size: u32,
    /// Maximum number of files in a single filter file
    #[serde(default="default_filter_item_limit")]
    pub filter_item_limit: u64,
}

impl Default for BrokerSettings {
    fn default() -> Self {
        Self {
            authentication: Default::default(),
            local_storage: PathBuf::from("/data/"),
            workers: [
                (WorkerID::from("worker-1".to_owned()), WorkerAddress::from("worker-0:4000"))
            ].into(),
            datastore: Datastore::default(),
            classification: Default::default(),
            worker_certificate: WorkerTLSConfig::AllowAll,
            filter_item_limit: default_filter_item_limit(),
            per_filter_pending_limit: default_per_filter_pending_limit(),
            per_worker_group_duplication: default_per_worker_group_duplication(),
            search_hit_limit: default_search_hit_limit(),
            yara_jobs_per_worker: default_yara_jobs_per_worker(),
            yara_batch_size: default_yara_batch_size(),
            bind_address: Some("localhost:4443".to_owned()),
            tls: None
        }
    }
}

impl BrokerSettings {
    pub fn runtime_config_file(&self) -> PathBuf {
        const CONFIG_NAME: &str = "runtime_config.txt";
        self.local_storage.join(CONFIG_NAME)
    }
}

/// The sub directory where trigram data is cached
const TRIGRAM_DIRECTORY: &str = "trigram-cache";
/// The sub directory where filter data is stored
const FILTER_DIRETORY: &str = "filters";
/// The sub directory where sql database are stored
const DATABASE_DIRETORY: &str = "sql-data";

impl WorkerSettings {
    /// Make sure all the data directories exist
    pub fn init_directories(&self) -> Result<()> {
        std::fs::create_dir_all(self.get_trigram_cache_directory())?;
        std::fs::create_dir_all(self.get_filter_directory())?;
        std::fs::create_dir_all(self.get_database_directory())?;
        return Ok(())
    }

    /// Get a path object for the trigram directory
    pub fn get_trigram_cache_directory(&self) -> PathBuf {
        self.data_path.join(TRIGRAM_DIRECTORY)
    }

    /// Get a path object for the filter directory
    pub fn get_filter_directory(&self) -> PathBuf {
        self.data_path.join(FILTER_DIRETORY)
    }

    /// Geth a path for the database directory
    pub fn get_database_directory(&self) -> PathBuf {
        self.data_path.join(DATABASE_DIRETORY)
    }
}

/// Default path where workers will store data.
/// Assuming the system is running in a container by default a root directory
/// /data/ is used, assuming that is some sort of meaningful mount.
fn default_data_path() -> PathBuf { PathBuf::from("/data/") }
/// Default soft cap data limit
fn default_data_limit() -> u64 { 1 << 40 }
/// Number of bytes that will be reserved from the soft cap for transient purposes.
fn default_data_reserve() -> u64 { 5 << 30 }
/// The default size for the initial segments of the filter files
fn default_initial_segment_size() -> u32 { 128 }
/// The default size for extended segments for the filter files
fn default_extended_segment_size() -> u32 { 2048 }
/// How many files to ingest into a filter in a single pass
fn default_ingest_batch_size() -> u32 { 100 }
/// How many files to download into the trigram cache concurrently
fn default_parallel_file_downloads() -> usize { 100 }

/// Settings for the worker server
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerSettings {
    /// Where should files be stored temporarily while processing occurs
    pub file_cache: CacheConfig,
    /// Where are the files being indexed stored
    pub files: crate::storage::BlobStorageConfig,
    /// What address should this server bind to
    pub bind_address: Option<String>,
    /// What TLS settings should the server use
    pub tls: Option<TLSConfig>,
    /// Which path should be used for local data storage
    #[serde(default="default_data_path")]
    pub data_path: PathBuf,
    /// A soft cap on the number of bytes of data to be saved by the worker.
    /// The worker should stop accepting new items when the size gets near this.
    #[serde(default="default_data_limit")]
    pub data_limit: u64,
    /// How much space (bytes) should be reserved for transient storage
    #[serde(default="default_data_reserve")]
    pub data_reserve: u64,
    /// Default size for initial data segments in the filter files
    #[serde(default="default_initial_segment_size")]
    pub initial_segment_size: u32,
    /// Default size for extended segments in the filter files
    #[serde(default="default_extended_segment_size")]
    pub extended_segment_size: u32,
    /// How many files should be ingested per batch
    #[serde(default="default_ingest_batch_size")]
    pub ingest_batch_size: u32,
    /// How many files should be downloaded concurrently
    #[serde(default="default_parallel_file_downloads")]
    pub parallel_file_downloads: usize,

    #[serde(flatten)]
    pub classification: ClassificationConfig,
}


impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            file_cache: Default::default(),
            files: Default::default(),
            data_path: default_data_path(),
            data_limit: default_data_limit(),
            classification: Default::default(),
            data_reserve: default_data_reserve(),
            initial_segment_size: default_initial_segment_size(),
            extended_segment_size: default_extended_segment_size(),
            ingest_batch_size: default_ingest_batch_size(),
            parallel_file_downloads: default_parallel_file_downloads(),
            bind_address: Some("localhost:4444".to_owned()),
            tls: None,
        }
    }
}

/// Apply environment variable substitution to a string
pub fn apply_env(data: &str) -> Result<String> {
    apply_variables(data, &std::env::vars().collect())
}

/// Regex used by the apply_variables method
const REGEX: &str = r#"(^|.)\$\{([0-9[:alpha:]_]+)(?::-?((?:\\\}|[^}])*))?\}"#;

/// Apply variable substitution to the string using the bash syntax
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
        let values = HashMap::from([
            ("Abc".to_owned(), "cats".to_owned()),
            ("A_b".to_owned(), "rats".to_owned())
        ]);
        assert_eq!(apply_variables("abc123", &values).unwrap(), "abc123".to_owned());
        assert_eq!(apply_variables("abc${Abc}123", &values).unwrap(), "abccats123".to_owned());
        assert_eq!(apply_variables("${Abc}", &values).unwrap(), "cats".to_owned());
        assert_eq!(apply_variables("${Abc}123", &values).unwrap(), "cats123".to_owned());
        assert_eq!(apply_variables("abc${Abc}", &values).unwrap(), "abccats".to_owned());
        assert_eq!(apply_variables("\\${Abc}123", &values).unwrap(), "${Abc}123".to_owned());
        assert_eq!(apply_variables("abc\\${Abc}123", &values).unwrap(), "abc${Abc}123".to_owned());

        // underscore
        assert_eq!(apply_variables("abc${A_b}123", &values).unwrap(), "abcrats123".to_owned());

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