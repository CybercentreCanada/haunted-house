pub mod error;
pub mod ursadb;
pub mod varint;
mod access;
mod storage;
mod core;
mod database;
mod interface;
mod auth;
mod cache;

use std::path::PathBuf;
use std::sync::Arc;

use auth::Authenticator;
use pyo3::types::PyModule;
use pyo3::{Python, pymodule, PyResult, pyclass, pymethods, PyAny, Py};
use storage::{BlobStorage, LocalDirectory, PythonBlobStore};

use crate::core::HouseCore;
use crate::database::LocalDatabase;



#[pyclass]
struct ServerInterface {
    core: Arc<HouseCore>
}


#[pyclass]
#[derive(Default)]
struct ServerBuilder {
    index_storage: Option<BlobStorage>,
    file_storage: Option<BlobStorage>,
    bind_address: String,
    authenticator: Option<Authenticator>
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

        // Initialize authenticator
        let auth = self.authenticator.take().ok_or(anyhow::format_err!("An authentication module must be configured."))?;

        // Initialize database
        let database = LocalDatabase::new()?;

        // Start server core
        let core = Arc::new(HouseCore::new(index_storage, file_storage, database, auth)?);

        // Start http interface
        tokio::spawn(crate::interface::serve(self.bind_address.clone(), core.clone()));

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