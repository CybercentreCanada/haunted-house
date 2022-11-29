pub mod error;
pub mod ursadb;
pub mod varint;
mod access;
mod storage;
mod core;
mod database;
mod database_rocksdb;
mod database_sqlite;
mod sqlite_kv;
mod interface;
mod auth;
mod cache;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use auth::{Authenticator, Role};
use chrono::{DateTime, Utc};
use database::Database;
use pyo3::exceptions::{PyRuntimeError};
use pyo3::types::PyModule;
use pyo3::{Python, pymodule, PyResult, pyclass, pymethods, PyAny, Py, PyObject};
use storage::{BlobStorage, LocalDirectory, PythonBlobStore};
use tokio::sync::oneshot;

use crate::core::HouseCore;
use crate::access::AccessControl;
use crate::cache::LocalCache;


#[pyclass]
struct ServerInterface {
    core: Arc<HouseCore>
}

#[pymethods]
impl ServerInterface {
    pub fn ingest_file(&self, py: Python, hash: Vec<u8>, access: String, expiry: Option<DateTime<Utc>>) -> PyResult<PyObject> {
        let access = AccessControl::parse(&access, "/", ",");
        let (send, recv) = oneshot::channel();
        match self.core.ingest_queue.send((hash, access, expiry, send)) {
            Ok(()) => Ok(pyo3_asyncio::tokio::local_future_into_py(py, async move {
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
}

const DEFAULT_SOFT_MAX_SIZE: usize = 50 << 30;

enum DatabaseConfig {
    SQLite(PathBuf)
}


#[pyclass]
#[derive(Default)]
struct ServerBuilder {
    index_storage: Option<BlobStorage>,
    file_storage: Option<BlobStorage>,
    bind_address: String,
    authenticator: Option<Authenticator>,
    cache_space: Option<LocalCache>,
    index_soft_max: Option<usize>,
    database_config: Option<DatabaseConfig>,
}

#[pymethods]
impl ServerBuilder {
    #[new]
    fn new() -> Self {
        Default::default()
    }

    fn index_storage_path(&mut self, path: PathBuf) -> PyResult<()> {
        self.index_storage = Some(BlobStorage::Local(LocalDirectory::new(path)));
        Ok(())
    }

    fn index_storage_object(&mut self, object: Py<PyAny>) -> PyResult<()> {
        self.index_storage = Some(BlobStorage::Python(PythonBlobStore::new(object)));
        Ok(())
    }

    fn file_storage_path(&mut self, path: PathBuf) -> PyResult<()> {
        self.file_storage = Some(BlobStorage::Local(LocalDirectory::new(path)));
        Ok(())
    }

    fn file_storage_object(&mut self, object: Py<PyAny>) -> PyResult<()> {
        self.file_storage = Some(BlobStorage::Python(PythonBlobStore::new(object)));
        Ok(())
    }

    fn cache_directory(&mut self, path: PathBuf, capacity: usize) -> PyResult<()> {
        self.cache_space = Some(LocalCache::new(capacity, path));
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

    fn build(&mut self) -> PyResult<ServerInterface> {
        // Initialize blob stores
        let index_storage = match self.index_storage.take() {
            Some(index) => index,
            None => LocalDirectory::new_temp()?
        };
        let file_storage = match self.file_storage.take() {
            Some(index) => index,
            None => LocalDirectory::new_temp()?
        };

        // Launch runtime
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        // Initialize authenticator
        let auth = self.authenticator.take().ok_or(anyhow::format_err!("An authentication module must be configured."))?;

        // Get cache
        let cache = self.cache_space.take().ok_or(anyhow::format_err!("A cache directory must be configured."))?;

        // Initialize database
        let db_config = match self.database_config.take() {
            Some(config) => config,
            None => DatabaseConfig::SQLite(tempfile::tempdir()?.into_path().join("house.sqlite"))
        };
        let database = match db_config {
            DatabaseConfig::SQLite(path) => runtime.block_on(Database::new_sqlite(self.index_soft_max.unwrap_or(DEFAULT_SOFT_MAX_SIZE), path.to_str().unwrap()))?
        };

        // Start server core
        let core = HouseCore::new(runtime, index_storage, file_storage, database, cache, auth)?;

        // Start http interface
        core.runtime.spawn(crate::interface::serve(self.bind_address.clone(), core.clone()));

        // return internal interface to core
        Ok(ServerInterface {
            core
        })
    }
}



#[pymodule]
fn haunted_house(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<ServerBuilder>()?;
    Ok(())
}