
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{PathBuf};
use std::sync::{Arc};

use anyhow::{Result, Context};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{ClientBuilder, ContainerClient};
use futures::StreamExt;
use log::error;
#[cfg(feature = "python")]
use pyo3::exceptions::PyValueError;
#[cfg(feature = "python")]
use pyo3::{Python, PyAny, Py, FromPyObject};
#[cfg(feature = "python")]
use pyo3::types::{PyTuple, PyBytes};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::mpsc;


#[derive(Debug, Serialize, Deserialize)]
pub enum BlobStorageConfig {
    TempDir{size: u64},
    Directory {path: PathBuf, size: u64},
    Azure (AzureBlobConfig)
}

#[cfg(feature = "python")]
impl<'source> FromPyObject<'source> for BlobStorageConfig {
    fn extract(ob: &'source PyAny) -> pyo3::PyResult<Self> {
        todo!();
        // Try to interpret it as a string
        // if let Ok(value) = ob.extract::<String>() {
        //     { // Check for keywords after lowercasing
        //         let value = value.to_lowercase();
        //         if value == "temp" || value == "temporary" {
        //             return Ok(BlobStorageConfig::TempDir)
        //         }
        //     }

        //     // Try interpreting it as a path
        //     if value.starts_with("/") || value.starts_with("./") {
        //         let path = PathBuf::from(value);
        //         std::fs::create_dir_all(&path)?;
        //         return Ok(BlobStorageConfig::Directory { path })
        //     }
        // }

        // // Try interpreting it as an azure configuration
        // else if let Ok(config) = ob.extract::<AzureBlobConfig>() {
        //     return Ok(BlobStorageConfig::Azure(config))
        // }

        // // Try interpreting it as a dict
        // else if let Ok(config) = ob.extract::<HashMap<String, String>>() {
        //     if let Some(value) = config.get("path") {
        //         let path = PathBuf::from(value);
        //         std::fs::create_dir_all(&path)?;
        //         return Ok(BlobStorageConfig::Directory { path })
        //     }
        // }

        return Err(PyValueError::new_err("Provided blob storage configuration was not valid"));
    }
}


pub async fn connect(config: BlobStorageConfig) -> Result<BlobStorage> {
    match config {
        BlobStorageConfig::TempDir { size } => {
            LocalDirectory::new_temp().context("Error setting up local blob store")
        }
        BlobStorageConfig::Directory { path, size } => {
            Ok(BlobStorage::Local(LocalDirectory::new(path)))
        },
        BlobStorageConfig::Azure(azure) =>
            Ok(BlobStorage::Azure(AzureBlobStore::new(azure).await?)),
    }
}

#[derive(Clone)]
pub enum BlobStorage {
    Local(LocalDirectory),
    #[cfg(feature = "python")]
    Python(PythonBlobStore),
    Azure(AzureBlobStore)
}

impl BlobStorage {
    pub async fn size(&self, label: &str) -> Result<Option<u64>> {
        match self {
            BlobStorage::Local(obj) => obj.size(label).await,
            #[cfg(feature = "python")]
            BlobStorage::Python(obj) => obj.size(label).await,
            BlobStorage::Azure(obj) => obj.size(label).await,
        }
    }
    pub async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>>>> {
        match self {
            BlobStorage::Local(obj) => obj.stream(label).await,
            #[cfg(feature = "python")]
            BlobStorage::Python(obj) => obj.stream(label).await,
            BlobStorage::Azure(obj) => obj.stream(label).await,
        }
    }
    pub async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.download(label, path).await,
            #[cfg(feature = "python")]
            BlobStorage::Python(obj) => obj.download(label, path).await,
            BlobStorage::Azure(obj) => obj.download(label, path).await,
        }
    }
    pub async fn upload(&self, label: &str, path: PathBuf) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.upload(label, path).await,
            #[cfg(feature = "python")]
            BlobStorage::Python(obj) => obj.upload(label, path).await,
            BlobStorage::Azure(obj) => obj.upload(label, path).await,
        }
    }
    pub async fn put(&self, label: &str, data: Vec<u8>) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.put(label, &data).await,
            #[cfg(feature = "python")]
            BlobStorage::Python(obj) => obj.put(label, &data).await,
            BlobStorage::Azure(obj) => obj.put(label, data).await,
        }
    }
    pub async fn get(&self, label: &str) -> Result<Vec<u8>> {
        match self {
            BlobStorage::Local(obj) => obj.get(label).await,
            #[cfg(feature = "python")]
            BlobStorage::Python(obj) => obj.get(label).await,
            BlobStorage::Azure(obj) => obj.get(label).await,
        }
    }
    pub async fn delete(&self, label: &str) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.delete(label).await,
            #[cfg(feature = "python")]
            BlobStorage::Python(obj) => obj.delete(label).await,
            BlobStorage::Azure(obj) => obj.delete(label).await,
        }
    }
}

pub fn read_chunks(path: PathBuf) -> mpsc::Receiver<Result<Vec<u8>>> {
    let (send, recv) = mpsc::channel::<Result<Vec<u8>>>(8);
    tokio::task::spawn_blocking(move ||{
        let mut file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(err) => {
                _ = send.blocking_send(Err(err.into()));
                return;
            },
        };

        loop {
            let mut buffer = vec![0u8; 1 << 20];
            let bytes_read = match file.read(&mut buffer) {
                Ok(bytes_read) => bytes_read,
                Err(err) => {
                    _ = send.blocking_send(Err(err.into()));
                    return;
                },
            };

            buffer.resize(bytes_read, 0);
            if let Err(_) = send.blocking_send(Ok(buffer)) {
                return;
            }
            if bytes_read == 0 {
                break;
            }
        }
    });
    return recv
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

    async fn size(&self, label: &str) -> Result<Option<u64>> {
        let path = self.get_path(label);
        match tokio::fs::metadata(path).await {
            Ok(meta) => Ok(Some(meta.len())),
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => Ok(None),
                _ => Err(err.into())
            },
        }
    }

    async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>>>> {
        let path = self.get_path(label);
        return Ok(read_chunks(path))
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


#[cfg(feature = "python")]
#[derive(Clone)]
pub struct PythonBlobStore {
    object: Py<PyAny>
}

#[cfg(feature = "python")]
impl PythonBlobStore {
    pub fn new(object: Py<PyAny>) -> Self {
        Self {object}
    }

    async fn size(&self, label: &str) -> Result<Option<u64>> {
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
            let result: Option<u64> = result.extract(py)?;
            Ok(result)
        });
    }

    async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>>>> {
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
        let object = object?;

        let (send, recv) = mpsc::channel::<Result<Vec<u8>>>(8);
        tokio::spawn(async move {
            loop {
                let future = Python::with_gil(|py| {
                    let args = PyTuple::new(py, &[1u64 << 20]);
                    let coroutine = object.call_method1(py, "read", args)?;

                    // convert the coroutine into a Rust future
                    pyo3_asyncio::tokio::into_future(coroutine.as_ref(py))
                });
                let future = match future {
                    Ok(future) => future,
                    Err(err) => {
                        _ = send.send(Err(err.into())).await;
                        break;
                    },
                };

                let result = match future.await {
                    Ok(future) => future,
                    Err(err) => {
                        _ = send.send(Err(err.into())).await;
                        break;
                    },
                };

                // Convert the python value back to rust
                let outcome: Result<Vec<u8>> = Python::with_gil(|py| {
                    let result: Vec<u8> = result.extract(py)?;
                    Ok(result)
                });

                match outcome {
                    Ok(buffer) => {
                        let buffer_len = buffer.len();
                        if let Err(_) = send.send(Ok(buffer)).await {
                            break;
                        }
                        if buffer_len == 0 {
                            break;
                        }
                    },
                    Err(err) => {
                        _ = send.send(Err(err.into())).await;
                        break;
                    },
                };
            }
        });

        return Ok(recv)
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

#[cfg(feature = "python")]
#[derive(Clone)]
pub struct PythonStream {
    object: Py<PyAny>
}

#[cfg(feature = "python")]
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
    http_client: reqwest::Client,
    client: ContainerClient,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureBlobConfig {
    pub account: String,
    pub access_key: String,
    pub container: String,
    #[serde(default)]
    pub use_emulator: bool,
}

#[cfg(feature = "python")]
impl<'source> FromPyObject<'source> for AzureBlobConfig {
    fn extract(ob: &'source PyAny) -> pyo3::PyResult<Self> {
        Python::with_gil(|py| {
            let use_emulator_key = pyo3::intern!(py, "use_emulator");
            let use_emulator = if ob.hasattr(use_emulator_key)? {
                ob.get_item(use_emulator_key)?.extract()?
            } else {
                false
            };

            Ok(AzureBlobConfig{
                account: ob.get_item(pyo3::intern!(py, "account"))?.extract()?,
                access_key: ob.get_item(pyo3::intern!(py, "access_key"))?.extract()?,
                container: ob.get_item(pyo3::intern!(py, "container"))?.extract()?,
                use_emulator,
            })
        })
    }
}

impl AzureBlobStore {
    async fn new(config: AzureBlobConfig) -> Result<Self> {
        let client = Self::get_container_client(&config)?;
        if !client.exists().await? {
            if let Err(err) = client.create().await {
                let http_error = err.as_http_error();
                let ignore = match http_error {
                    Some(http) => http.error_code() == Some("ContainerAlreadyExists"),
                    None => false,
                };
                if !ignore {
                    return Err(err.into())
                }
            };
        }
        let http_client = reqwest::ClientBuilder::new().build()?;

        Ok(Self{ config, http_client, client })
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

    pub async fn size(&self, label: &str) -> Result<Option<u64>> {
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
        Ok(Some(data.blob.properties.content_length))
    }

    pub async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>>>> {
        let mut stream = self.client.blob_client(label).get().into_stream();
        let (send, recv) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        _ = send.send(Err(anyhow::anyhow!(err))).await;
                        return;
                    },
                };

                let mut body = chunk.data;
                while let Some(data) = body.next().await {
                    let data = match data {
                        Ok(data) => data,
                        Err(err) => {
                            _ = send.send(Err(anyhow::anyhow!(err))).await;
                            return;
                        },
                    };
                    if let Err(_) = send.send(anyhow::Ok(data.to_vec())).await {
                        return;
                    }
                };
            }
        });
        Ok(recv)
    }

    pub async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
        let mut recv = self.stream(label).await?;
        Ok(tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::options().write(true).open(path)?;
            while let Some(data) = recv.blocking_recv() {
                file.write(&data?)?;
            }
            return anyhow::Ok(())
        }).await??)
    }

    pub async fn upload(&self, label: &str, path: PathBuf) -> Result<()> {
        let client = self.client.blob_client(label);
        let sas = client.shared_access_signature(azure_storage::prelude::BlobSasPermissions {
            read: true,
            add: true,
            create: true,
            write: true,
            delete: false,
            delete_version: false,
            permanent_delete: false,
            list: false,
            tags: false,
            move_: false,
            execute: false,
            ownership: false,
            permissions: false
        }, time::OffsetDateTime::now_utc() + time::Duration::HOUR)?;

        let url = client.generate_signed_blob_url(&sas)?;

        loop {
            let request = self.http_client.put(url.clone())
                .header("x-ms-blob-type", "BlockBlob")
                .header("Date", chrono::Utc::now().to_rfc3339())
                .header("Content-Length", tokio::fs::metadata(&path).await?.len().to_string())
                .body(tokio::fs::File::open(&path).await?)
                .send().await?;

            if request.status().is_success() {
                break
            }

            error!("HTTP error uploading blob to azure: {}; {}", request.status(), request.text().await?);
        }

        return Ok(())
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
        let client = self.client.blob_client(label);
        if let Err(err) = client.delete().await {
            let mut ignore = match err.as_http_error() {
                Some(err) => {
                    println!("{}", err);
                    err.status() == 404
                },
                None => false,
            };
            if &azure_core::error::ErrorKind::DataConversion == err.kind() {
                if err.to_string() == "header not found x-ms-delete-type-permanent" {
                    ignore = true;
                }
            }
            // println!("{}", err);
            // println!("{:?}", err);
            if !ignore {
                return Err(err.into())
            }
        };
        return Ok(())
    }
}


#[cfg(test)]
mod test {
    use std::io::Write;

    use crate::storage::{AzureBlobStore, AzureBlobConfig};

    async fn connect() -> AzureBlobStore {
        AzureBlobStore::new(AzureBlobConfig{
            account: "devstoreaccount1".to_owned(),
            access_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_owned(),
            container: "test".to_owned(),
            use_emulator: true,
        }).await.unwrap()
    }

    #[tokio::test]
    async fn get_put_size() {
        let store = connect().await;

        let body = b"a body".repeat(10);
        assert!(store.size("not-a-blob").await.unwrap().is_none());
        store.put("test", body.clone()).await.unwrap();
        assert_eq!(store.size("test").await.unwrap().unwrap(), 60);
        assert_eq!(store.get("test").await.unwrap(), body);

    }

    #[tokio::test]
    async fn delete_file() {
        let store = connect().await;
        let body = b"a body".repeat(10);

        store.delete("delete-file-test").await.unwrap();
        assert!(store.size("delete-file-test").await.unwrap().is_none());
        store.delete("delete-file-test").await.unwrap();
        assert!(store.size("delete-file-test").await.unwrap().is_none());
        store.put("delete-file-test", body.clone()).await.unwrap();
        assert!(store.size("delete-file-test").await.unwrap().is_some());
        store.delete("delete-file-test").await.unwrap();
        assert!(store.size("delete-file-test").await.unwrap().is_none());
        store.delete("delete-file-test").await.unwrap();
        assert!(store.size("delete-file-test").await.unwrap().is_none());
    }


    #[tokio::test]
    async fn stream() {
        let store = connect().await;

        let body = b"a body".repeat(10);
        store.put("test", body.clone()).await.unwrap();

        let mut data_stream = store.stream("test").await.unwrap();
        let mut buff = vec![];
        while let Some(data) = data_stream.recv().await {
            buff.extend(data.unwrap());
        }
        assert_eq!(buff, body);
    }


    #[tokio::test]
    async fn upload_download() {
        let store = connect().await;

        for _ in 0 .. 3 {
            let input_file = tempfile::NamedTempFile::new().unwrap();
            {
                let mut handle = input_file.as_file();
                for _ in 0..100 {
                    assert_eq!(handle.write(&(b"123".repeat(100))).unwrap(), 300);
                }
                println!("file length {}", handle.metadata().unwrap().len());
                handle.flush().unwrap();
            }
            store.upload("label", input_file.path().to_owned()).await.unwrap();
            assert_eq!(store.size("label").await.unwrap().unwrap(), 3 * 100 * 100);

            let output_file = tempfile::NamedTempFile::new().unwrap();
            store.download("label", output_file.path().to_owned()).await.unwrap();

            assert_eq!(std::fs::read(input_file.path()).unwrap(), std::fs::read(output_file.path()).unwrap())
        }
    }
}