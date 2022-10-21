
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;


#[derive(Clone, PartialEq, Eq, Hash, Debug)]
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
    async fn download(&self, label: &BlobID, path: PathBuf) -> Result<()>;
    async fn upload(&self, label: BlobID, path: PathBuf) -> Result<()>;
    async fn put(&self, label: BlobID, data: &[u8]) -> Result<()>;
    async fn get(&self, label: BlobID) -> Result<Vec<u8>>;
}


#[derive(Clone)]
struct LocalDirectory {
    path: PathBuf
}

impl LocalDirectory {
    fn get_path(&self, label: &BlobID) -> PathBuf {
        let dest = self.path.with_file_name(label.to_string());
        return dest;
    }
}

#[async_trait]
impl BlobStorage for LocalDirectory {
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

