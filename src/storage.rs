
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use pyo3::{Python, PyAny, Py};
use pyo3::types::PyTuple;
use serde::Deserialize;
use tempfile::TempDir;


pub type BlobID = String;

// #[derive(Clone, PartialEq, Eq, Hash, Debug)]
// pub struct BlobID(pub uuid::Uuid);

// impl ToString for BlobID {
//     fn to_string(&self) -> String {
//         self.0.to_string()
//     }
// }


#[derive(Deserialize)]
pub enum BlobStorageConfig {
    Directory {
        path: String,
    }
}

pub async fn connect(config: BlobStorageConfig) -> Result<BlobStorage> {
    match config {
        BlobStorageConfig::Directory { path } => {
            let path = PathBuf::from(path);
            tokio::fs::create_dir_all(&path).await?;
            Ok(BlobStorage::Local(LocalDirectory::new(path)))
        },
    }
}

#[derive(Clone)]
pub enum BlobStorage {
    Local(LocalDirectory),
    Python(PythonBlobStore),
}

impl BlobStorage {
    pub async fn size(&self, label: &BlobID) -> Result<Option<usize>> {
        match self {
            BlobStorage::Local(obj) => obj.size(label).await,
            BlobStorage::Python(obj) => obj.size(label).await,
        }
    }
    pub async fn download(&self, label: &BlobID, path: PathBuf) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.download(label, path).await,
            BlobStorage::Python(obj) => obj.download(label, path).await,
        }
    }
    pub async fn upload(&self, label: BlobID, path: PathBuf) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.upload(label, path).await,
            BlobStorage::Python(obj) => obj.upload(label, path).await,
        }
    }
    pub async fn put(&self, label: BlobID, data: &[u8]) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.put(label, data).await,
            BlobStorage::Python(obj) => obj.put(label, data).await,
        }
    }
    pub async fn get(&self, label: BlobID) -> Result<Vec<u8>> {
        match self {
            BlobStorage::Local(obj) => obj.get(label).await,
            BlobStorage::Python(obj) => obj.get(label).await,
        }
    }
}


#[derive(Clone)]
pub struct LocalDirectory {
    path: PathBuf,
    temp: Option<Arc<TempDir>>
}

impl LocalDirectory {
    pub fn new(path: PathBuf) -> Self {
        Self {path, temp: None}
    }

    pub fn new_temp() -> Result<BlobStorage> {
        let temp = tempfile::tempdir()?;
        Ok(BlobStorage::Local(Self {
            path: temp.path().to_path_buf(),
            temp: Some(Arc::new(temp))
        }))
    }

    fn get_path(&self, label: &BlobID) -> PathBuf {
        let dest = self.path.with_file_name(label.to_string());
        return dest;
    }

    async fn size(&self, label: &BlobID) -> Result<Option<usize>> {
        let path = self.get_path(label);
        match tokio::fs::metadata(path).await {
            Ok(meta) => Ok(Some(meta.len() as usize)),
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => Ok(None),
                _ => Err(err.into())
            },
        }
    }

    async fn download(&self, label: &BlobID, dest: PathBuf) -> Result<()> {
        let path = self.get_path(label);
        if let Ok(_) = tokio::fs::hard_link(&path, &dest).await {
            return Ok(());
        }
        tokio::fs::copy(path, dest).await?;
        return Ok(())
    }

    async fn upload(&self, label: BlobID, source: PathBuf) -> Result<()> {
        let dest = self.get_path(&label);
        if let Ok(_) = tokio::fs::hard_link(&source, &dest).await {
            return Ok(());
        }
        tokio::fs::copy(source, dest).await?;
        return Ok(())
    }

    async fn put(&self, label: BlobID, data: &[u8]) -> Result<()> {
        let dest = self.get_path(&label);
        tokio::fs::write(dest, data).await?;
        return Ok(())
    }

    async fn get(&self, label: BlobID) -> Result<Vec<u8>> {
        let path = self.get_path(&label);
        Ok(tokio::fs::read(path).await?)
    }
}


#[derive(Clone)]
pub struct PythonBlobStore {
    object: Py<PyAny>
}

impl PythonBlobStore {
    pub fn new(object: Py<PyAny>) -> Self {
        Self {object}
    }

    async fn size(&self, label: &BlobID) -> Result<Option<usize>> {
        // Invoke method
        let future = Python::with_gil(|py| {
            // calling the py_sleep method like a normal function returns a coroutine
            let args = PyTuple::new(py, &[label.to_string()]);
            let coroutine = self.object.call_method1(py, "size", args)?;

            // convert the coroutine into a Rust future
            pyo3_asyncio::tokio::into_future(coroutine.as_ref(py))
        })?;

        // await the future
        let result = future.await?;

        // Convert the python value back to rust
        return Python::with_gil(|py| {
            let result: Option<usize> = result.extract(py)?;
            Ok(result)
        });
    }

    async fn download(&self, label: &BlobID, path: PathBuf) -> Result<()> {
        // Invoke method
        let future = Python::with_gil(|py| {
            // calling the py_sleep method like a normal function returns a coroutine
            let coroutine = self.object.call_method1(py, "download", (label.to_string(), path))?;

            // convert the coroutine into a Rust future
            pyo3_asyncio::tokio::into_future(coroutine.as_ref(py))
        })?;

        // await the future
        future.await?;
        return Ok(())
    }

    async fn upload(&self, label: BlobID, path: PathBuf) -> Result<()> {
        // Invoke method
        let future = Python::with_gil(|py| {
            // calling the py_sleep method like a normal function returns a coroutine
            let coroutine = self.object.call_method1(py, "upload", (label.to_string(), path))?;

            // convert the coroutine into a Rust future
            pyo3_asyncio::tokio::into_future(coroutine.as_ref(py))
        })?;

        // await the future
        future.await?;
        return Ok(())
    }

    async fn put(&self, label: BlobID, data: &[u8]) -> Result<()> {
        // Invoke method
        let future = Python::with_gil(|py| {
            // calling the py_sleep method like a normal function returns a coroutine
            let coroutine = self.object.call_method1(py, "put", (label.to_string(), data))?;

            // convert the coroutine into a Rust future
            pyo3_asyncio::tokio::into_future(coroutine.as_ref(py))
        })?;

        // await the future
        future.await?;
        return Ok(())
    }

    async fn get(&self, label: BlobID) -> Result<Vec<u8>> {
        // Invoke method
        let future = Python::with_gil(|py| {
            // calling the py_sleep method like a normal function returns a coroutine
            let args = PyTuple::new(py, &[label.to_string()]);
            let coroutine = self.object.call_method1(py, "get", args)?;

            // convert the coroutine into a Rust future
            pyo3_asyncio::tokio::into_future(coroutine.as_ref(py))
        })?;

        // await the future
        let result = future.await?;

        // Convert the python value back to rust
        return Python::with_gil(|py| {
            let result: Vec<u8> = result.extract(py)?;
            Ok(result)
        });
    }
}