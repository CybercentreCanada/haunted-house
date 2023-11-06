
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, Context};
use aws_sdk_s3::types::ByteStream;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{ClientBuilder, ContainerClient};
use futures::StreamExt;
use log::error;
// use reqwest_middleware::ClientWithMiddleware;
// use reqwest_retry::RetryTransientMiddleware;
// use reqwest_retry::policies::ExponentialBackoff;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::error::ErrorKinds;


/// Configures a blob storage location
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum BlobStorageConfig {
    /// Store blobs in a local temporary directory
    TempDir{
        /// How much space to dedicate to the temporary directory
        size: u64
    },
    /// Store blobs in a local directory
    Directory {
        /// Location on disk for the blob store
        path: PathBuf,
        /// How much space to dedicate to the blob store
        size: u64
    },
    /// Use an azure blob storage account
    Azure (AzureBlobConfig),
    /// Use an s3 server
    S3(S3Config),
}

impl Default for BlobStorageConfig {
    fn default() -> Self {
        BlobStorageConfig::Azure(AzureBlobConfig {
            account: "azure-account".to_string(),
            access_key: "azure-key".to_string(),
            container: "retrohunt".to_owned(),
            use_emulator: false
        })
    }
}

/// Setup a blob storage
pub async fn connect(config: &BlobStorageConfig) -> Result<BlobStorage> {
    match config {
        BlobStorageConfig::TempDir { .. } => {
            LocalDirectory::new_temp().context("Error setting up local blob store")
        }
        BlobStorageConfig::Directory { path, .. } => {
            Ok(BlobStorage::Local(LocalDirectory::new(path.clone())))
        },
        BlobStorageConfig::Azure(azure) =>
            Ok(BlobStorage::Azure(AzureBlobStore::new(azure.clone()).await?)),
        BlobStorageConfig::S3(config) =>
            Ok(BlobStorage::S3(S3BlobStore::new(config.clone()).await?)),
    }
}

/// A unified type holding a blob store
#[derive(Clone)]
pub enum BlobStorage {
    /// A local file based storage
    Local(LocalDirectory),
    /// An Azure blob storage account
    Azure(AzureBlobStore),
    /// An s3 server
    S3(S3BlobStore),
}

impl BlobStorage {
    /// Fetch the size of a blob
    pub async fn size(&self, label: &str) -> Result<Option<u64>> {
        match self {
            BlobStorage::Local(obj) => obj.size(label).await,
            BlobStorage::Azure(obj) => obj.size(label).await,
            BlobStorage::S3(obj) => obj.size(label).await,
        }
    }
    /// Download the blob as a stream of chunks
    pub async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>, ErrorKinds>>, ErrorKinds> {
        match self {
            BlobStorage::Local(obj) => obj.stream(label).await,
            BlobStorage::Azure(obj) => obj.stream(label).await,
            BlobStorage::S3(obj) => obj.stream(label).await,
        }
    }
    /// Download the blob into a local file
    pub async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.download(label, path).await,
            BlobStorage::Azure(obj) => obj.download(label, path).await,
            BlobStorage::S3(obj) => obj.download(label, path).await,
        }
    }
    // Upload a blob from a local file
    // pub async fn upload(&self, label: &str, path: PathBuf) -> Result<()> {
    //     match self {
    //         BlobStorage::Local(obj) => obj.upload(label, path).await,
    //         BlobStorage::Azure(obj) => obj.upload(label, path).await,
    //         BlobStorage::S3(obj) => obj.upload(label, path).await,
    //     }
    // }
    #[cfg(test)]
    pub async fn put(&self, label: &str, data: Vec<u8>) -> Result<()> {
        match self {
            BlobStorage::Local(obj) => obj.put(label, &data).await,
            BlobStorage::Azure(obj) => obj.put(label, data).await,
            BlobStorage::S3(obj) => obj.put(label, data).await,
        }
    }
    #[cfg(test)]
    pub async fn get(&self, label: &str) -> Result<Vec<u8>> {
        match self {
            BlobStorage::Local(obj) => obj.get(label).await,
            BlobStorage::Azure(obj) => obj.get(label).await,
            BlobStorage::S3(obj) => obj.get(label).await,
        }
    }
    // Delete a blob from storage
    // pub async fn delete(&self, label: &str) -> Result<()> {
    //     match self {
    //         BlobStorage::Local(obj) => obj.delete(label).await,
    //         BlobStorage::Azure(obj) => obj.delete(label).await,
    //         BlobStorage::S3(obj) => obj.delete(label).await,
    //     }
    // }
}

/// Helper function used in streaming local files
fn read_chunks(path: PathBuf) -> mpsc::Receiver<Result<Vec<u8>, ErrorKinds>> {
    // Create the channel outside the blocking section so we can only pass in half
    let (send, recv) = mpsc::channel::<Result<Vec<u8>, ErrorKinds>>(8);

    // Run the actual fine interaction in the block
    tokio::task::spawn_blocking(move ||{
        // Open the file
        let mut file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(err) => {
                _ = send.blocking_send(Err(err.into()));
                return;
            },
        };

        // loop until the file read
        loop {
            // Read out a block into a fresh buffer
            let mut buffer = vec![0u8; 1 << 20];
            let bytes_read = match file.read(&mut buffer) {
                Ok(bytes_read) => bytes_read,
                Err(err) => {
                    _ = send.blocking_send(Err(err.into()));
                    return;
                },
            };

            // Resize the length value to communicate the amount of actual data
            buffer.resize(bytes_read, 0);

            // send the data to the socket, if the socket refuses the data
            // stop sending
            if send.blocking_send(Ok(buffer)).is_err() {
                return;
            }

            // if a read returned zero bytes, finish
            if bytes_read == 0 {
                break;
            }
        }
    });
    return recv
}

/// A local storage directory, may or may not be temporary directory
#[derive(Clone)]
pub struct LocalDirectory {
    /// Location of the storage directory
    path: PathBuf,
    /// If the directory is a temporary directory this is the scope guard that
    /// erases it on destruction
    _temp: Option<Arc<TempDir>>
}

impl LocalDirectory {
    /// Open a standard directory as a blob storage
    pub fn new(path: PathBuf) -> Self {
        Self {path, _temp: None}
    }

    /// Setup a temporary directory as a blob store
    pub fn new_temp() -> Result<BlobStorage> {
        let temp = tempfile::tempdir()?;
        Ok(BlobStorage::Local(Self {
            path: temp.path().to_path_buf(),
            _temp: Some(Arc::new(temp))
        }))
    }

    /// Get the local path where a blob will be stored
    fn get_path(&self, label: &str) -> PathBuf {
        let dest = self.path.with_file_name(label);
        return dest;
    }

    /// fetch the size of a blob by checking the file size
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

    /// Read the local file into a stream of chunks
    async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>, ErrorKinds>>, ErrorKinds> {
        let path = self.get_path(label);
        return Ok(read_chunks(path))
    }

    /// "download" the file by copying the local file
    async fn download(&self, label: &str, dest: PathBuf) -> Result<()> {
        let path = self.get_path(label);
        if tokio::fs::hard_link(&path, &dest).await.is_ok() {
            return Ok(());
        }
        tokio::fs::copy(path, dest).await?;
        return Ok(())
    }

    // "upload" the file by copying the local source file
    // #[cfg(test)]
    // async fn upload(&self, label: &str, source: PathBuf) -> Result<()> {
    //     let dest = self.get_path(label);
    //     if tokio::fs::hard_link(&source, &dest).await.is_ok() {
    //         return Ok(());
    //     }
    //     tokio::fs::copy(source, dest).await?;
    //     return Ok(())
    // }
    #[cfg(test)]
    async fn put(&self, label: &str, data: &[u8]) -> Result<()> {
        let dest = self.get_path(label);
        tokio::fs::write(dest, data).await?;
        return Ok(())
    }
    #[cfg(test)]
    async fn get(&self, label: &str) -> Result<Vec<u8>> {
        let path = self.get_path(label);
        Ok(tokio::fs::read(path).await?)
    }

    // erase file from storage
    // #[cfg(test)]
    // async fn delete(&self, label: &str) -> Result<()> {
    //     let path = self.get_path(label);
    //     Ok(tokio::fs::remove_file(path).await?)
    // }
}

/// Use an azure blob store
#[derive(Clone)]
pub struct AzureBlobStore {
    // config: AzureBlobConfig,
    // http_client: ClientWithMiddleware,
    /// http client used to interact with the blob storage in cases the azure client doesn't cover
    _http_client: reqwest::Client,
    /// An azure blob storage client
    client: ContainerClient,
}

/// Configure access to an azure blob store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureBlobConfig {
    /// Azure account identifier
    pub account: String,
    /// Azure access key
    pub access_key: String,
    /// Name of the container within storage account
    pub container: String,
    /// Whether the blob storage system is being run on the development emulator
    #[serde(default)]
    pub use_emulator: bool,
}

impl AzureBlobStore {
    /// Connect to an azure blob store
    async fn new(config: AzureBlobConfig) -> Result<Self> {
        let client = Self::get_container_client(&config)?;
        if !client.exists().await? {
            if let Err(err) = client.create().await {
                let http_error = err.as_http_error();
                let ignore = match http_error {
                    Some(http) => http.error_code() == Some("ContainerAlreadyExists") || http.error_code() == Some("BucketAlreadyOwnedByYou"),
                    None => false,
                };
                if !ignore {
                    return Err(err.into())
                }
            };
        }

        // let retry_policy = ExponentialBackoff::builder().build_with_total_retry_duration(chrono::Duration::days(1).to_std()?);
        // let http_client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
        //     .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        //     .build();
        let _http_client = reqwest::Client::new();

        Ok(Self{ _http_client, client })
    }

    /// Setup the azure container client
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

    /// Measure the size of the blob
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

    /// Download the file in chunks and stream them into the channel
    pub async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>, ErrorKinds>>, ErrorKinds> {
        let mut stream = self.client.blob_client(label).get().into_stream();
        let (send, recv) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        _ = send.send(Err(err.into())).await;
                        return;
                    },
                };

                let mut body = chunk.data;
                while let Some(data) = body.next().await {
                    let data = match data {
                        Ok(data) => data,
                        Err(err) => {
                            _ = send.send(Err(err.into())).await;
                            return;
                        },
                    };
                    if send.send(Ok(data.to_vec())).await.is_err() {
                        return;
                    }
                };
            }
        });
        Ok(recv)
    }

    /// Download a file to disk
    pub async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
        let mut recv = self.stream(label).await?;
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::options().write(true).open(path)?;
            while let Some(data) = recv.blocking_recv() {
                file.write_all(&data?)?;
            }
            return anyhow::Ok(())
        }).await?
    }

    /// Upload a file from disk
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
            let request = self._http_client.put(url.clone())
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

    #[cfg(test)]
    pub async fn put(&self, label: &str, data: Vec<u8>) -> Result<()> {
        let client = self.client.blob_client(label);
        client.put_block_blob(data).await?;
        return Ok(())
    }

    #[cfg(test)]
    pub async fn get(&self, label: &str) -> Result<Vec<u8>> {
        let client = self.client.blob_client(label);
        Ok(client.get_content().await?)
    }

    /// Delete blob
    #[cfg(test)]
    pub async fn delete(&self, label: &str) -> Result<()> {
        let client = self.client.blob_client(label);
        if let Err(err) = client.delete().await {
            let mut ignore = match err.as_http_error() {
                Some(err) => {
                    // println!("{}", err);
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

/// Wrapper for an s3 client
#[derive(Clone)]
pub struct S3BlobStore {
    /// Underlying blob client
    client: aws_sdk_s3::Client,
    /// Name of the bucket
    bucket: String,
    // region_name: String,
}

/// Configuration to access s3 bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint_url: Option<String>,
    region_name: String,
    bucket: String,
    #[serde(default)]
    no_tls_verify: bool,
}

/// A dummy certificate verifier that just accepts anything
pub struct NoCertificateVerification {}

impl rustls::client::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

impl S3BlobStore {
    /// Connect to the s3 store and ensure resources exist
    async fn new(config: S3Config) -> Result<Self> {
        // Ok(S3BlobStore { client: bucket })
        let mut loader = aws_config::from_env();

        // Override the region
        loader = loader.region(aws_types::region::Region::new(config.region_name));

        // configure endpoint
        if let Some(endpoint_url) = config.endpoint_url {
            loader = loader.endpoint_url(endpoint_url);
        }

        // Configure keys
        if let Some(key) = config.access_key_id {
            std::env::set_var("AWS_ACCESS_KEY_ID", key)
        }
        if let Some(secret) = config.secret_access_key {
            std::env::set_var("AWS_SECRET_ACCESS_KEY", secret)
        }

        // Configure the use of ssl
        loader = loader.http_connector({

            let https_connector = if config.no_tls_verify {
                let root_store = rustls::RootCertStore::empty();
                let mut tls_config = rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();
                tls_config
                    .dangerous()
                    .set_certificate_verifier(Arc::new(NoCertificateVerification {}));

                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_tls_config(tls_config)
                    .https_or_http()
                    .enable_http1()
                    .enable_http2()
                    .build()
            } else {
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http1()
                    .enable_http2()
                    .build()
            };

            aws_smithy_client::hyper_ext::Adapter::builder()
                .build(https_connector)
        });

        // Build the client
        let sdk_config = loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(true)
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);

        if let Err(err) = client.head_bucket().bucket(&config.bucket).send().await {
            let err = err.into_service_error();
            if err.is_not_found() {
                if let Err(err) = client.create_bucket().bucket(&config.bucket).send().await {
                    let x = err.into_service_error();
                    if !x.is_bucket_already_exists() && !x.is_bucket_already_owned_by_you() {
                        return Err(x.into())
                    }
                }
            } else {
                return Err(err.into())
            }
        }

        Ok(S3BlobStore {
            client,
            bucket: config.bucket,
        })
    }

    /// read blob size via API query
    pub async fn size(&self, label: &str) -> Result<Option<u64>> {
        let request = self.client
            .head_object()
            .bucket(&self.bucket)
            .key(label)
            .send().await;

        let res = match request {
            Ok(res) => res,
            Err(err) => {
                let err = err.into_service_error();
                if err.is_not_found() {
                    return Ok(None)
                }
                return Err(err.into())
            },
        };

        return Ok(Some(res.content_length() as u64))
    }

    /// read blob into stream
    /// The api already provides block based reading, so just spawn a task
    /// to read from the respones and shovel data into the channel
    pub async fn stream(&self, label: &str) -> Result<mpsc::Receiver<Result<Vec<u8>, ErrorKinds>>, ErrorKinds> {
        let mut request = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(label)
            .send().await?;

        let (send, recv) = mpsc::channel(64);
        tokio::spawn(async move {
            // let mut chunks = request.body.chunks(1 << 20);
            while let Some(buffer) = request.body.next().await {
                _ = match buffer {
                    Ok(data) => send.send(Ok(data.to_vec())).await,
                    Err(err) => send.send(Err(ErrorKinds::OtherBlobError(err.to_string()))).await,
                };
            }
        });

        return Ok(recv)
    }

    /// Download the data to disk, use tokio fs to wrap blocking operation
    pub async fn download(&self, label: &str, path: PathBuf) -> Result<()> {
        let mut request = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(label)
            .send().await?;

        let mut out = tokio::fs::OpenOptions::new().write(true).open(path).await?;
        while let Some(buffer) = request.body.next().await {
            out.write_all(&buffer?).await?;
        }
        out.flush().await?;
        return Ok(())
    }

    /// Upload a file, let the library handle streaming read
    pub async fn upload(&self, label: &str, path: PathBuf) -> Result<()> {
        self.client
            .put_object()
            .content_type("application/octet-stream")
            .bucket(&self.bucket)
            .key(label)
            .body(ByteStream::from_path(path).await?)
            .send().await?;
        return Ok(())
    }

    #[cfg(test)]
    pub async fn put(&self, label: &str, data: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .content_type("application/octet-stream")
            .content_length(data.len() as i64)
            .bucket(&self.bucket)
            .key(label)
            .body(data.into())
            .send().await?;
        return Ok(())
    }

    #[cfg(test)]
    pub async fn get(&self, label: &str) -> Result<Vec<u8>> {
        let request = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(label)
            .send().await?;
        let bytes = request.body.collect().await?;
        return Ok(bytes.to_vec())
    }

    /// Delete the block by api call
    pub async fn delete(&self, label: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(label)
            .send().await?;
        Ok(())
    }
}


#[cfg(test)]
mod test {
    use std::io::Write;

    use crate::storage::{AzureBlobStore, AzureBlobConfig};

    use super::S3BlobStore;

    async fn connect() -> AzureBlobStore {
        AzureBlobStore::new(AzureBlobConfig{
            account: "devstoreaccount1".to_owned(),
            access_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_owned(),
            container: "test".to_owned(),
            use_emulator: true,
        }).await.unwrap()
    }

    #[tokio::test]
    async fn azure_get_put_size() {
        let store = connect().await;

        let body = b"a body".repeat(10);
        assert!(store.size("not-a-blob").await.unwrap().is_none());
        store.put("test", body.clone()).await.unwrap();
        assert_eq!(store.size("test").await.unwrap().unwrap(), 60);
        assert_eq!(store.get("test").await.unwrap(), body);

    }

    #[tokio::test]
    async fn azure_delete_file() {
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
    async fn azure_stream() {
        let store = connect().await;

        let body = b"a body".repeat(10);
        store.put("test", body.clone()).await.unwrap();

        let mut data_stream = store.stream("test").await.unwrap();
        let mut buff = vec![];
        while let Some(data) = data_stream.recv().await {
            buff.extend(data.unwrap());
        }
        assert_eq!(buff, body);

        let mut stream = store.stream("Not a file").await.unwrap();
        while let Some(data) = stream.recv().await {
            assert!(data.is_err());
        }
    }


    #[tokio::test]
    async fn azure_upload_download() {
        let store = connect().await;

        for _ in 0 .. 3 {
            let input_file = tempfile::NamedTempFile::new().unwrap();
            {
                let mut handle = input_file.as_file();
                for _ in 0..100 {
                    assert_eq!(handle.write(&(b"123".repeat(100))).unwrap(), 300);
                }
                handle.flush().unwrap();
                assert_eq!(30000, handle.metadata().unwrap().len());
            }
            store.upload("label", input_file.path().to_owned()).await.unwrap();
            assert_eq!(store.size("label").await.unwrap().unwrap(), 3 * 100 * 100);

            let output_file = tempfile::NamedTempFile::new().unwrap();
            store.download("label", output_file.path().to_owned()).await.unwrap();

            assert_eq!(std::fs::read(input_file.path()).unwrap(), std::fs::read(output_file.path()).unwrap())
        }

        assert!(store.upload("Not a file", std::path::PathBuf::from("/not-a-file")).await.is_err());
        let output_file = tempfile::NamedTempFile::new().unwrap();
        assert!(store.download("Not a file", output_file.path().to_owned()).await.is_err());
    }

    async fn s3_connect() -> S3BlobStore {
        S3BlobStore::new(super::S3Config {
            access_key_id: Some("al_storage_key".to_owned()),
            secret_access_key: Some("Ch@ngeTh!sPa33w0rd".to_owned()),
            endpoint_url: Some("http://localhost:9000".to_owned()),
            region_name: "west".to_owned(),
            bucket: "test".to_owned(),
            no_tls_verify: true
        }).await.unwrap()
    }

    #[tokio::test]
    async fn s3_get_put_size() {
        let store = s3_connect().await;

        let body = b"a body".repeat(10);
        assert!(store.size("not-a-blob").await.unwrap().is_none());
        store.put("get-put-size-test", body.clone()).await.unwrap();
        assert_eq!(store.size("get-put-size-test").await.unwrap().unwrap(), 60);
        assert_eq!(store.get("get-put-size-test").await.unwrap(), body);

    }

    #[tokio::test]
    async fn s3_delete_file() {
        let store = s3_connect().await;
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
    async fn s3_azure_stream() {
        let store = s3_connect().await;

        let body = b"a body".repeat(10);
        store.put("stream-test", body.clone()).await.unwrap();

        let mut data_stream = store.stream("stream-test").await.unwrap();
        let mut buff = vec![];
        while let Some(data) = data_stream.recv().await {
            buff.extend(data.unwrap());
        }
        assert_eq!(buff, body);

        assert!(store.stream("Not a file").await.is_err());
    }


    #[tokio::test]
    async fn s3_upload_download() {
        let store = s3_connect().await;

        for _ in 0 .. 3 {
            let input_file = tempfile::NamedTempFile::new().unwrap();
            {
                let mut handle = input_file.as_file();
                for _ in 0..100 {
                    assert_eq!(handle.write(&(b"123".repeat(100))).unwrap(), 300);
                }
                handle.flush().unwrap();
                assert_eq!(30000, handle.metadata().unwrap().len());
            }
            store.upload("upload-download-label", input_file.path().to_owned()).await.unwrap();
            assert_eq!(store.size("upload-download-label").await.unwrap().unwrap(), 3 * 100 * 100);

            let output_file = tempfile::NamedTempFile::new().unwrap();
            store.download("upload-download-label", output_file.path().to_owned()).await.unwrap();

            let expected = std::fs::read(input_file.path()).unwrap();
            let output = std::fs::read(output_file.path()).unwrap();
            assert_eq!(expected.len(), output.len());
            assert_eq!(expected, output)
        }

        assert!(store.upload("Not a file", std::path::PathBuf::from("/not-a-file")).await.is_err());
        let output_file = tempfile::NamedTempFile::new().unwrap();
        assert!(store.download("Not a file", output_file.path().to_owned()).await.is_err());
    }
}