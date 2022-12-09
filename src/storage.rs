
use std::io::{Read, Write};
use std::path::{PathBuf, Path};
use std::sync::{Arc};

use anyhow::{Result, Context};
use azure_storage::StorageCredentials;
use azure_storage_blobs::container::Container;
use azure_storage_blobs::prelude::{BlobClient, ClientBuilder, ContainerClient};
use bytes::BytesMut;
use futures::StreamExt;
use pyo3::{Python, PyAny, Py};
use pyo3::types::{PyTuple, PyBytes};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;


#[derive(Deserialize)]
pub enum BlobStorageConfig {
    TempDir,
    Directory {path: String},
    // Azure (AzureBlobConfig)
}


impl<'source> FromPyObject<'source> for Role {
    fn extract(ob: &'source PyAny) -> pyo3::PyResult<Self> {
        let value: String = ob.extract()?;
        let value = value.to_lowercase();
        if value == "search" {
            Ok(Role::Search)
        } else if value == "worker" {
            Ok(Role::Worker)
        } else {
            Err(PyValueError::new_err(format!("Not accepted role catagory: {value}")))
        }
    }
}


pub async fn connect(config: BlobStorageConfig) -> Result<BlobStorage> {
    match config {
        BlobStorageConfig::TempDir => {
            LocalDirectory::new_temp().context("Error setting up local blob store")
        }
        BlobStorageConfig::Directory { path } => {
            let path = PathBuf::from(path);
            tokio::fs::create_dir_all(&path).await?;
            Ok(BlobStorage::Local(LocalDirectory::new(path)))
        },
        // BlobStorageConfig::Azure(azure) =>
        //     Ok(BlobStorage::Azure(AzureBlobStore::new(azure).await?)),
    }
}

#[derive(Clone)]
pub enum BlobStorage {
    Local(LocalDirectory),
    Python(PythonBlobStore),
    // Azure(AzureBlobStore)
}

impl BlobStorage {
    pub async fn size(&self, label: &str) -> Result<Option<usize>> {
        match self {
            BlobStorage::Local(obj) => obj.size(label).await,
            BlobStorage::Python(obj) => obj.size(label).await,
            //BlobStorage::Azure(obj) => obj.size(label).await,
        }
    }
    pub async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>>>> {
        match self {
            BlobStorage::Local(obj) => obj.stream(label).await,
            BlobStorage::Python(obj) => obj.stream(label).await,
            //BlobStorage::Azure(obj) => obj.stream(label).await,
        }
    }
    pub async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.download(label, path).await,
            BlobStorage::Python(obj) => obj.download(label, path).await,
            //BlobStorage::Azure(obj) => obj.download(label, path).await,
        }
    }
    pub async fn upload(&self, label: &str, path: PathBuf) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.upload(label, path).await,
            BlobStorage::Python(obj) => obj.upload(label, path).await,
            //BlobStorage::Azure(obj) => obj.upload(label, path).await,
        }
    }
    pub async fn put(&self, label: &str, data: Vec<u8>) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.put(label, &data).await,
            BlobStorage::Python(obj) => obj.put(label, &data).await,
            //BlobStorage::Azure(obj) => obj.put(label, data).await,
        }
    }
    pub async fn get(&self, label: &str) -> Result<Vec<u8>> {
        match self {
            BlobStorage::Local(obj) => obj.get(label).await,
            BlobStorage::Python(obj) => obj.get(label).await,
            //BlobStorage::Azure(obj) => obj.get(label).await,
        }
    }
    pub async fn delete(&self, label: &str) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.delete(label).await,
            BlobStorage::Python(obj) => obj.delete(label).await,
            //BlobStorage::Azure(obj) => obj.delete(label).await,
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


// #[derive(Clone)]
// pub struct AzureBlobStore {
//     config: AzureBlobConfig,
//     client: ContainerClient,
// }

// #[derive(Clone, Serialize, Deserialize)]
// pub struct AzureBlobConfig {
//     pub account: String,
//     pub access_key: String,
//     pub container: String,
//     pub use_emulator: bool,
// }

// impl AzureBlobStore {
//     async fn new(config: AzureBlobConfig) -> Result<Self> {
//         let client = Self::get_container_client(&config)?;
//         if !client.exists().await? {
//             client.create().await?;
//         }
//         Ok(Self{ config, client })
//     }

//     fn get_container_client(config: &AzureBlobConfig) -> Result<ContainerClient> {
//         let client_builder = if config.use_emulator {
//             ClientBuilder::emulator()
//         } else {
//             let storage_credentials = if config.access_key.is_empty() {
//                 StorageCredentials::Anonymous
//             } else {
//                 StorageCredentials::Key(config.account.clone(), config.access_key.clone())
//             };
//             ClientBuilder::new(config.account.clone(), storage_credentials)
//         };
//         Ok(client_builder.container_client(config.container.clone()))
//     }

//     pub async fn size(&self, label: &str) -> Result<Option<usize>> {
//         let client = self.client.blob_client(label);
//         let data = match client.get_properties().await {
//             Ok(metadata) => metadata,
//             Err(err) => {
//                 match err.kind() {
//                     azure_storage::ErrorKind::HttpResponse { status, .. } => {
//                         if let azure_core::StatusCode::NotFound = status {
//                             return Ok(None)
//                         } else {
//                             return Err(err.into())
//                         }
//                     },
//                     _ => return Err(err.into())
//                 }
//             },
//         };
//         Ok(Some(data.blob.properties.content_length as usize))
//     }

//     pub async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>>>> {
//         let mut stream = self.client.blob_client(label).get().into_stream();
//         let (send, recv) = mpsc::channel(8);
//         tokio::spawn(async move {
//             while let Some(chunk) = stream.next().await {
//                 let chunk = match chunk {
//                     Ok(chunk) => chunk,
//                     Err(err) => {
//                         _ = send.send(Err(anyhow::anyhow!(err))).await;
//                         return;
//                     },
//                 };

//                 let mut body = chunk.data;
//                 while let Some(data) = body.next().await {
//                     let data = match data {
//                         Ok(data) => data,
//                         Err(err) => {
//                             _ = send.send(Err(anyhow::anyhow!(err))).await;
//                             return;
//                         },
//                     };
//                     if let Err(_) = send.send(anyhow::Ok(data.to_vec())).await {
//                         return;
//                     }
//                 };
//             }
//         });
//         Ok(recv)
//     }

//     pub async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
//         let mut recv = self.stream(label).await?;
//         Ok(tokio::task::spawn_blocking(move || {
//             let mut file = std::fs::File::open(path)?;
//             while let Some(data) = recv.blocking_recv() {
//                 file.write(&data?)?;
//             }
//             return anyhow::Ok(())
//         }).await??)
//     }

//     pub async fn upload(&self, label: &str, path: PathBuf) -> Result<()> {
//         let client = self.client.blob_client(label);

//         // self.client.

//         if !client.exists().await? {
//             client.put_append_blob().await
//                 .context("Error setting block to append")?;
//         //     client.delete().lease_id(lease_id).await.context("error deleting blob for replacement")?;
//         }

//         let lease = client.acquire_lease(std::time::Duration::from_secs(60 * 5)).await
//             .context("Aquire lease error")?;
//         let lease_id = lease.lease_id;

//         let mut file = tokio::fs::File::open(path).await?;

//         loop {
//             let mut buffer = BytesMut::zeroed(1 << 20);
//             let bytes_read = file.read(&mut buffer).await?;
//             println!("read {bytes_read} bytes");
//             if bytes_read == 0 {
//                 break
//             }
//             buffer.resize(bytes_read, 0);
//             client.append_block(buffer)
//                 .lease_id(lease_id).await
//                 .context("Error appending to block.")?;
//         }

//         client.break_lease().lease_id(lease_id).await.context("error breaking lease")?;
//         return Ok(())
//     }

//     pub async fn put(&self, label: &str, data: Vec<u8>) -> Result<()> {
//         let client = self.client.blob_client(label);
//         client.put_block_blob(data).await?;
//         return Ok(())
//     }

//     pub async fn get(&self, label: &str) -> Result<Vec<u8>> {
//         let client = self.client.blob_client(label);
//         Ok(client.get_content().await?)
//     }

//     pub async fn delete(&self, label: &str) -> Result<()> {
//         let client = self.client.blob_client(label);
//         client.delete().await?;
//         return Ok(())
//     }
// }


// #[cfg(test)]
// mod test {
//     use std::io::Write;

//     use crate::storage::{AzureBlobStore, AzureBlobConfig};

//     async fn connect() -> AzureBlobStore {
//         AzureBlobStore::new(AzureBlobConfig{
//             account: "devstoreaccount1".to_owned(),
//             access_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_owned(),
//             container: "test".to_owned(),
//             use_emulator: true,
//         }).await.unwrap()
//     }

//     #[tokio::test]
//     async fn get_put_size() {
//         let store = connect().await;

//         let body = b"a body".repeat(10);
//         assert!(store.size("not-a-blob").await.unwrap().is_none());
//         store.put("test", body.clone()).await.unwrap();
//         assert_eq!(store.size("test").await.unwrap().unwrap(), 60);
//         assert_eq!(store.get("test").await.unwrap(), body);

//     }

//     #[tokio::test]
//     async fn stream() {
//         let store = connect().await;

//         let body = b"a body".repeat(10);
//         store.put("test", body.clone()).await.unwrap();

//         let mut data_stream = store.stream("test").await.unwrap();
//         let mut buff = vec![];
//         while let Some(data) = data_stream.recv().await {
//             buff.extend(data.unwrap());
//         }
//         assert_eq!(buff, body);
//     }


//     #[tokio::test]
//     async fn upload_download() {
//         let store = connect().await;

//         for _ in 0 .. 3 {
//             let input_file = tempfile::NamedTempFile::new().unwrap();
//             {
//                 let mut handle = input_file.as_file();
//                 for _ in 0..100 {
//                     assert_eq!(handle.write(&(b"123".repeat(100))).unwrap(), 300);
//                 }
//                 handle.flush().unwrap();
//             }
//             store.upload("label", input_file.path().to_owned()).await.unwrap();
//             assert_eq!(store.size("label").await.unwrap().unwrap(), 3 * 100 * 100);

//             let output_file = tempfile::NamedTempFile::new().unwrap();
//             store.download("label", output_file.path().to_owned()).await.unwrap();

//             assert_eq!(std::fs::read(input_file.path()).unwrap(), std::fs::read(output_file.path()).unwrap())
//         }
//     }
// }