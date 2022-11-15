pub mod error;
pub mod ursadb;
pub mod varint;
mod access;
mod storage;
mod core;
mod database;
mod interface;

use std::path::PathBuf;

use pyo3::types::PyModule;
use pyo3::{Python, pymodule, PyResult, pyclass, pymethods, PyAny, Py};
use storage::{BlobStorage, LocalDirectory, PythonBlobStore};

use crate::database::LocalDatabase;



#[pyclass]
struct ServerInterface {


}


#[pyclass]
#[derive(Default)]
struct ServerBuilder {
    index_storage: Option<BlobStorage>,
    file_storage: Option<BlobStorage>,
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

        // Initialize database
        let database = LocalDatabase::new()?;

        // Start server core
        let core = HouseCore::new(index_storage, file_storage, database);

        // Start http interface
        todo!("initialize http interface");

        // return internal interface to core
        Ok(ServerInterface {

        })
    }
}



#[pymodule]
fn haunted_house(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<ServerBuilder>()?;
    Ok(())
}