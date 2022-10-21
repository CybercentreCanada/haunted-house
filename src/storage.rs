
use std::fs::File;
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;


#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BlobID(pub uuid::Uuid);

impl ToString for BlobID {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}


#[derive(Deserialize)]
pub enum BlobStorageConfig {
    Directory {
        path: String,
    }
}

pub async fn connect(config: BlobStorageConfig) -> Result<impl BlobStorage> {
    match config {
        BlobStorageConfig::Directory { path } => {
            let path = PathBuf::from(path);
            tokio::fs::create_dir_all(&path).await?;
            Ok(LocalDirectory { path })
        },
    }
}


#[async_trait]
pub trait BlobStorage: Clone + Send + Sync + 'static {
    async fn size(&self, label: &BlobID) -> Result<Option<usize>>;
    async fn download(&self, label: BlobID, path: PathBuf) -> Result<()>;
    async fn upload(&self, label: BlobID, path: PathBuf) -> Result<()>;
    async fn put(&self, label: BlobID, data: Vec<u8>) -> Result<()>;
    async fn get(&self, label: BlobID) -> Result<Vec<u8>>;
}


#[derive(Clone)]
struct LocalDirectory {
    path: PathBuf
}

#[async_trait]
impl BlobStorage for LocalDirectory {
    async fn size(&self, label: &BlobID) -> Result<Option<usize>> {
        let path = self.path.with_file_name(label.to_string());
        match tokio::fs::metadata(path).await {
            Ok(meta) => Ok(Some(meta.len() as usize)),
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => Ok(None),
                _ => Err(err.into())
            },
        }
    }
    
    async fn download(&self, label: BlobID, dest: PathBuf) -> Result<()> {
        let path = self.path.with_file_name(label.to_string());
        if let Ok(_) = tokio::fs::hard_link(&path, &dest).await {
            return Ok(());
        }
        tokio::fs::copy(path, dest).await?;
        return Ok(())
    }

    async fn upload(&self, label: BlobID, source: PathBuf) -> Result<()> {
        let dest = self.path.with_file_name(label.to_string());
        if let Ok(_) = tokio::fs::hard_link(&source, &dest).await {
            return Ok(());
        }
        tokio::fs::copy(source, dest).await?;
        return Ok(())
    }
}

