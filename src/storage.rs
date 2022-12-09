
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use azure_storage::StorageCredentials;
use azure_storage_blobs::container::Container;
use azure_storage_blobs::prelude::{BlobClient, ClientBuilder, ContainerClient};
use futures::StreamExt;
use pyo3::{Python, PyAny, Py};
use pyo3::types::{PyTuple, PyBytes};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;


#[derive(Deserialize)]
pub enum BlobStorageConfig {
    Directory {
        path: String,
    },
    Azure (AzureBlobConfig)
}

pub async fn connect(config: BlobStorageConfig) -> Result<BlobStorage> {
    match config {
        BlobStorageConfig::Directory { path } => {
            let path = PathBuf::from(path);
            tokio::fs::create_dir_all(&path).await?;
            Ok(BlobStorage::Local(LocalDirectory::new(path)))
        },
        BlobStorageConfig::Azure(azure) =>
            Ok(BlobStorage::Azure(AzureBlobStore::new(azure).await?)),
    }
}

#[derive(Clone)]
pub enum BlobStorage {
    Local(LocalDirectory),
    Python(PythonBlobStore),
    Azure(AzureBlobStore)
}

impl BlobStorage {
    pub async fn size(&self, label: &str) -> Result<Option<usize>> {
        match self {
            BlobStorage::Local(obj) => obj.size(label).await,
            BlobStorage::Python(obj) => obj.size(label).await,
            BlobStorage::Azure(obj) => obj.size(label).await,
        }
    }
    pub async fn stream(&self, label: &str) -> Result<Box<dyn std::io::Read + Send>> {
        match self {
            BlobStorage::Local(obj) => obj.stream(label).await,
            BlobStorage::Python(obj) => obj.stream(label).await,
            BlobStorage::Azure(obj) => obj.stream(label).await,
        }
    }
    pub async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.download(label, path).await,
            BlobStorage::Python(obj) => obj.download(label, path).await,
            BlobStorage::Azure(obj) => obj.download(label, path).await,
        }
    }
    pub async fn upload(&self, label: &str, path: PathBuf) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.upload(label, path).await,
            BlobStorage::Python(obj) => obj.upload(label, path).await,
            BlobStorage::Azure(obj) => obj.upload(label, path).await,
        }
    }
    pub async fn put(&self, label: &str, data: Vec<u8>) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.put(label, &data).await,
            BlobStorage::Python(obj) => obj.put(label, &data).await,
            BlobStorage::Azure(obj) => obj.put(label, data).await,
        }
    }
    pub async fn get(&self, label: &str) -> Result<Vec<u8>> {
        match self {
            BlobStorage::Local(obj) => obj.get(label).await,
            BlobStorage::Python(obj) => obj.get(label).await,
            BlobStorage::Azure(obj) => obj.get(label).await,
        }
    }
    pub async fn delete(&self, label: &str) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.delete(label).await,
            BlobStorage::Python(obj) => obj.delete(label).await,
            BlobStorage::Azure(obj) => obj.delete(label).await,
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

    fn get_path(&self, label: &str) -> PathBuf {
        let dest = self.path.with_file_name(label.to_string());
        return dest;
    }

    async fn size(&self, label: &str) -> Result<Option<usize>> {
        let path = self.get_path(label);
        match tokio::fs::metadata(path).await {
            Ok(meta) => Ok(Some(meta.len() as usize)),
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => Ok(None),
                _ => Err(err.into())
            },
        }
    }

    async fn stream(&self, label: &str) -> Result<Box<dyn std::io::Read + Send>> {
        let path = self.get_path(label);
        let file = Box::new(std::fs::File::open(path)?);
        return Ok(file)
    }

    async fn download(&self, label: &str, dest: PathBuf) -> Result<()> {
        let path = self.get_path(label);
        if let Ok(_) = tokio::fs::hard_link(&path, &dest).await {
            return Ok(());
        }
        tokio::fs::copy(path, dest).await?;
        return Ok(())
    }

    async fn upload(&self, label: &str, source: PathBuf) -> Result<()> {
        let dest = self.get_path(&label);
        if let Ok(_) = tokio::fs::hard_link(&source, &dest).await {
            return Ok(());
        }
        tokio::fs::copy(source, dest).await?;
        return Ok(())
    }

    async fn put(&self, label: &str, data: &[u8]) -> Result<()> {
        let dest = self.get_path(&label);
        tokio::fs::write(dest, data).await?;
        return Ok(())
    }

    async fn get(&self, label: &str) -> Result<Vec<u8>> {
        let path = self.get_path(&label);
        Ok(tokio::fs::read(path).await?)
    }

    async fn delete(&self, label: &str) -> Result<()> {
        let path = self.get_path(&label);
        Ok(tokio::fs::remove_file(path).await?)
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

    async fn size(&self, label: &str) -> Result<Option<usize>> {
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

    async fn stream(&self, label: &str) -> Result<Box<dyn std::io::Read + Send>> {
        // Invoke method
        let future = Python::with_gil(|py| {
            let coroutine = self.object.call_method1(py, "stream", (label.to_string(),))?;

            // convert the coroutine into a Rust future
            pyo3_asyncio::tokio::into_future(coroutine.as_ref(py))
        })?;

        let result = future.await?;

        // Convert the python value back to rust
        let object: Result<Py<PyAny>> = Python::with_gil(|py| {
            let result: Py<PyAny> = result.extract(py)?;
            Ok(result)
        });

        return Ok(Box::new(PythonStream{object: object?}))
    }

    async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
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

    async fn upload(&self, label: &str, path: PathBuf) -> Result<()> {
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

    async fn put(&self, label: &str, data: &[u8]) -> Result<()> {
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

    async fn get(&self, label: &str) -> Result<Vec<u8>> {
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

    async fn delete(&self, label: &str) -> Result<()> {
        // Invoke method
        let future = Python::with_gil(|py| {
            // calling the py_sleep method like a normal function returns a coroutine
            let coroutine = self.object.call_method1(py, "delete", (label.to_string(), ))?;

            // convert the coroutine into a Rust future
            pyo3_asyncio::tokio::into_future(coroutine.as_ref(py))
        })?;

        // await the future
        future.await?;
        return Ok(())
    }
}

#[derive(Clone)]
pub struct PythonStream {
    object: Py<PyAny>
}

impl std::io::Read for PythonStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let copied = Python::with_gil(|py| {
            let args = PyTuple::new(py, &[buf.len()]);
            let read_buf = self.object.call_method1(py, "read", args)?;
            let read_buf: &PyBytes = read_buf.extract(py)?;
            let read_buf_len = read_buf.len()?;
            if read_buf_len > buf.len() {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Python returned too many bytes for buffer read"))
            } else if read_buf_len > 0 {
                buf[0..read_buf_len].copy_from_slice(read_buf.as_bytes());
            }

            return Ok(read_buf_len)
        })?;

        return Ok(copied)
    }
}


#[derive(Clone)]
pub struct AzureBlobStore {
    config: AzureBlobConfig,
    client: ContainerClient,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AzureBlobConfig {
    pub account: String,
    pub access_key: String,
    pub container: String,
    pub use_emulator: bool,
}

impl AzureBlobStore {
    async fn new(config: AzureBlobConfig) -> Result<Self> {
        let client = Self::get_container_client(&config)?;
        client.create().await?;
        Ok(Self{ config, client })
    }

    fn get_container_client(config: &AzureBlobConfig) -> Result<ContainerClient> {
        let client_builder = if config.use_emulator {
            ClientBuilder::emulator()
        } else {
            let storage_credentials = if config.access_key.is_empty() {
                StorageCredentials::Anonymous
            } else {
                StorageCredentials::Key(config.account.clone(), config.access_key.clone())
            };
            ClientBuilder::new(config.account.clone(), storage_credentials)
        };
        Ok(client_builder.container_client(config.container.clone()))
    }

    pub async fn size(&self, label: &str) -> Result<Option<usize>> {
        let client = self.client.blob_client(label);
        let data = match client.get_properties().await {
            Ok(metadata) => metadata,
            Err(err) => {
                match err.kind() {
                    azure_storage::ErrorKind::HttpResponse { status, .. } => {
                        if let azure_core::StatusCode::NotFound = status {
                            return Ok(None)
                        } else {
                            return Err(err.into())
                        }
                    },
                    _ => return Err(err.into())
                }
            },
        };
        Ok(Some(data.blob.properties.content_length as usize))
    }
    
    pub async fn stream(&self, label: &str) -> Result<Box<dyn std::io::Read + Send>> {
        todo!()
        // let mut stream = client.get().into_stream();
        // let mut data = vec![];
        // while let Some(chunk) = stream.next().await {
        //     let chunk = chunk?;
        //     data.extend(&chunk.data.collect().await?);
        // }
        // Ok(data)
    }

    pub async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
        todo!()
    }
    pub async fn upload(&self, label: &str, path: PathBuf) -> Result<()> {
        todo!()
    }

    pub async fn put(&self, label: &str, data: Vec<u8>) -> Result<()> {
        let client = self.client.blob_client(label);
        client.put_block_blob(data).await?;
        return Ok(())
    }
    
    pub async fn get(&self, label: &str) -> Result<Vec<u8>> {
        let client = self.client.blob_client(label);
        Ok(client.get_content().await?)
    }

    pub async fn delete(&self, label: &str) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::storage::{AzureBlobStore, AzureBlobConfig};


    #[tokio::test]
    async fn anon_access() {
        let store = AzureBlobStore::new(AzureBlobConfig{
            account: "devstoreaccount1".to_owned(),
            access_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_owned(),
            container: "test".to_owned(),
            use_emulator: true,
        }).await.unwrap();

        let body = b"a body".repeat(10);
        assert!(store.size("not-a-blob").await.unwrap().is_none());
        store.put("test", body.clone()).await.unwrap();
        assert_eq!(store.size("test").await.unwrap().unwrap(), 60);
        assert_eq!(store.get("test").await.unwrap(), body);
        
        let mut file_like = store.stream("test").await.unwrap();
        let mut buff = vec![0u8; 1000];
        let bytes_read = file_like.read_to_end(&mut buff).unwrap();
        assert_eq!(buff[0..bytes_read], body);

        todo!();
    }

}