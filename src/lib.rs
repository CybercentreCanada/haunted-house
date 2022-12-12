pub mod error;
pub mod ursadb;
pub mod varint;
mod access;
mod storage;
mod core;
mod database;
mod database_sqlite;
mod interface;
mod auth;
mod blob_cache;
mod query;
mod filter;
mod worker;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use auth::{Authenticator, Role};
use chrono::{DateTime, Utc};
use database::Database;
use log::{info};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::PyModule;
use pyo3::{Python, pymodule, PyResult, pyclass, pymethods, PyAny, Py, PyObject};
use sha2::Digest;
use storage::BlobStorageConfig;
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot;

use crate::blob_cache::BlobCache;
use crate::core::{IngestMessage, Config, HouseCore};
use crate::access::AccessControl;
use crate::worker::WorkerBuilder;


#[pyclass]
struct ServerStatus {
    pub indices: Vec<(String, String)>,
    pub ingest_buffer: usize,
    pub ingest_batch_active: bool,
}

#[pymethods]
impl ServerStatus {
    #[getter]
    fn indices(&self) -> Vec<(String, String)> {
        self.indices.clone()
    }

    #[getter]
    fn ingest_buffer(&self) -> usize {
        self.ingest_buffer
    }

    #[getter]
    fn ingest_batch_active(&self) -> bool {
        self.ingest_batch_active
    }
}

#[pyclass]
struct ServerInterface {
    core: Arc<HouseCore>
}

#[pymethods]
impl ServerInterface {
    pub fn upload(&self, py: Python, path: PathBuf) -> PyResult<PyObject> {
        let core = self.core.clone();
        Ok(pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut file = tokio::fs::File::open(&path).await?;

            let mut buffer = vec![0; 1 << 20];
            let mut hasher = sha2::Sha256::new();
            loop {
                let x = file.read(&mut buffer).await?;
                if x == 0 {break;}
                hasher.update(&buffer[0..x]);
            }
            let hash = hasher.finalize();

            // let hash = base64::encode(hash);
            core.file_storage.upload(&hex::encode(hash), path).await?;
            return Ok(hash.to_vec())
        })?.into())
    }

    pub fn ingest_file(&self, py: Python, hash: Vec<u8>, access: String, expiry: Option<DateTime<Utc>>) -> PyResult<PyObject> {
        let access = AccessControl::parse(&access, "/", ",");
        let (send, recv) = oneshot::channel();
        match self.core.ingest_queue.send(IngestMessage::IngestMessage(hash, access, expiry, send)) {
            Ok(()) => Ok(pyo3_asyncio::tokio::future_into_py(py, async move {
                match recv.await {
                    Ok(res) => match res {
                        Ok(_) => Ok(()),
                        Err(err) => Err(PyRuntimeError::new_err(format!("ingest error: {err}"))),
                    },
                    Err(err) => Err(PyRuntimeError::new_err(format!("ingest error: {err}"))),
                }
            })?.into()),
            Err(err) => Err(PyRuntimeError::new_err(format!("Error queuing ingest: {err}"))),
        }
    }

    pub fn status(&self, py: Python) -> PyResult<PyObject> {
        let core = self.core.clone();
        Ok(pyo3_asyncio::tokio::future_into_py(py, async move {
            let (ingest_batch_active, ingest_buffer) = core.ingest_status().await?;
            return Ok(ServerStatus{
                indices: core.database.list_indices().await?.into_iter().map(|(a, b)|(a.as_str().to_owned(), b.as_str().to_owned())).collect(),
                ingest_buffer,
                ingest_batch_active,
            })
        })?.into())
    }
}

enum DatabaseConfig {
    SQLite(PathBuf),
    SQLiteTemp()
}


#[pyclass]
#[derive(Default)]
struct ServerBuilder {
    // runtime: Option<tokio::runtime::Runtime>,
    index_storage: Option<BlobStorageConfig>,
    file_storage: Option<BlobStorageConfig>,
    bind_address: String,
    authenticator: Option<Authenticator>,
    cache_space: Option<(PathBuf, usize)>,
    database_config: Option<DatabaseConfig>,
    config: Config,
}

#[pymethods]
impl ServerBuilder {
    #[new]
    fn new() -> Self {
        Default::default()
    }

    fn index_storage(&mut self, config: BlobStorageConfig) -> PyResult<()> {
        self.index_storage = Some(config);
        Ok(())
    }

    fn file_storage(&mut self, config: BlobStorageConfig) -> PyResult<()> {
        self.file_storage = Some(config);
        Ok(())
    }

    fn cache_directory(&mut self, path: PathBuf, capacity: usize) -> PyResult<()> {
        self.cache_space = Some((path, capacity));
        Ok(())
    }

    fn authentication_object(&mut self, object: Py<PyAny>) -> PyResult<()> {
        self.authenticator = Some(Authenticator::new_python(object)?);
        Ok(())
    }

    fn static_authentication(&mut self, assignments: HashMap<String, HashSet<Role>>) -> PyResult<()> {
        self.authenticator = Some(Authenticator::new_static(assignments)?);
        Ok(())
    }

    fn database_path(&mut self, path: PathBuf) -> PyResult<()> {
        self.database_config = Some(DatabaseConfig::SQLite(path));
        Ok(())
    }

    fn batch_limit_size(&mut self, size: usize) {
        self.config.batch_limit_size = size;
    }

    fn batch_limit_seconds(&mut self, seconds: u64) {
        self.config.batch_limit_seconds = seconds;
    }

    fn index_soft_bytes_max(&mut self, size: u64) {
        self.config.index_soft_bytes_max = size;
    }

    fn index_soft_entries_max(&mut self, size: u64) {
        self.config.index_soft_entries_max = size;
    }

    fn build(&mut self, py: Python) -> PyResult<PyObject> {
        // Initialize blob stores
        let index_storage_config = match self.index_storage.take() {
            Some(index) => index,
            None => return Err(PyValueError::new_err("index storage must be configured"))
        };
        let file_storage_config = match self.file_storage.take() {
            Some(index) => index,
            None => return Err(PyValueError::new_err("file storage must be configured"))
        };

        // Initialize authenticator
        let auth = self.authenticator.take().ok_or(anyhow::format_err!("An authentication module must be configured."))?;

        // Get cache
        let cache = self.cache_space.take().ok_or(anyhow::format_err!("A cache directory must be configured."))?;

        let db_config = match self.database_config.take() {
            Some(config) => config,
            None => DatabaseConfig::SQLiteTemp()
        };

        let bind_address = if self.bind_address.is_empty() {
            "localhost:8080".to_owned()
        } else {
            self.bind_address.clone()
        };
        let config = self.config.clone();

        Ok(pyo3_asyncio::tokio::future_into_py(py, async move {

            let index_storage = crate::storage::connect(index_storage_config).await?;
            let file_storage = crate::storage::connect(file_storage_config).await?;

            let cache = BlobCache::new(index_storage.clone(), cache.1, cache.0);

            // Initialize database
            info!("Connecting to database.");
            let database = match db_config {
                DatabaseConfig::SQLite(path) => Database::new_sqlite(config.clone(), path.to_str().unwrap()).await?,
                DatabaseConfig::SQLiteTemp() => Database::new_sqlite_temp(config.clone()).await?,
            };

            // Start server core
            info!("Starting server core.");
            let core = HouseCore::new(index_storage, file_storage, database, cache, auth, config)
                .context("Error launching core.")?;

            // Start http interface
            info!("Starting server interface.");
            tokio::task::spawn(crate::interface::serve(bind_address, core.clone()));

            // return internal interface to core
            Ok(ServerInterface {
                core
            })
        })?.into())
    }
}



#[pymodule]
fn haunted_house(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);

    m.add_class::<ServerBuilder>()?;
    m.add_class::<WorkerBuilder>()?;
    Ok(())
}