
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;
// use crate::database_rocksdb::RocksInterface;
use crate::database_sqlite::SQLiteInterface;


#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Hash)]
pub struct IndexGroup(String);

impl IndexGroup {
    pub fn create(expiry: &Option<DateTime<Utc>>) -> IndexGroup {
        IndexGroup(match expiry {
            Some(date) => format!("{}", date.format("%Y0%j")),
            None => format!("99990999"),
        })
    }

    pub fn max() -> IndexGroup {
        IndexGroup(format!("99999999"))
    }

    pub fn as_bytes<'a>(&'a self) -> &'a [u8] {
        self.0.as_bytes()
    }

    pub fn as_str<'a>(&'a self) -> &'a str {
        &self.0
    }
}

impl std::fmt::Display for IndexGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct IndexID(String);

impl IndexID {
    pub fn new() -> Self {
        IndexID(format!("{:x}", uuid::Uuid::new_v4().as_u128()))
    }

    pub fn as_bytes<'a>(&'a self) -> &'a [u8] {
        self.0.as_bytes()
    }

    pub fn as_str<'a>(&'a self) -> &'a str {
        &self.0
    }
}

impl From<&str> for IndexID {
    fn from(value: &str) -> Self {
        IndexID(value.to_owned())
    }
}

impl std::fmt::Display for IndexID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct BlobID(String);

impl BlobID {
    pub fn new() -> Self {
        Self(format!("{:x}", uuid::Uuid::new_v4().as_u128()))
    }

    pub fn as_bytes<'a>(&'a self) -> &'a [u8] {
        self.0.as_bytes()
    }

    pub fn as_str<'a>(&'a self) -> &'a str {
        &self.0
    }
}

pub enum Database {
    // Rocks(RocksInterface),
    SQLite(SQLiteInterface),
}

impl Database {

    // pub async fn new_rocks(index_soft_max: usize) -> Result<Self> {
    //     Ok(Database::Rocks(RocksInterface::new(index_soft_max)?))
    // }

    pub async fn new_sqlite(index_soft_max: u64, path: &str) -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new(index_soft_max, path).await?))
    }

    pub async fn new_sqlite_temp(index_soft_max: u64) -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new_temp(index_soft_max).await?))
    }

    pub async fn update_file_access(&self, hash: &[u8], access: &AccessControl, index_group: &IndexGroup) -> Result<bool> {
        match self {
            // Database::Rocks(local) => local.update_file_access(hash, access, index_group).await,
            Database::SQLite(local) => local.update_file_access(hash, access, index_group).await,
        }
    }

    pub async fn select_index_to_grow(&self, index_group: &IndexGroup) -> Result<Option<(IndexID, BlobID, u64)>> {
        match self {
            // Database::Rocks(local) => local.select_index_to_grow(index_group).await,
            Database::SQLite(local) => local.select_index_to_grow(index_group).await,
        }
    }

    pub async fn create_index_data(&self, index_group: &IndexGroup, blob_id: BlobID, meta: Vec<(Vec<u8>, AccessControl)>, new_size: u64) -> Result<()> {
        match self {
            // Database::Rocks(local) => local.create_index_data(index_group, index_id, old_blob_id, blob_id, meta, new_size).await,
            Database::SQLite(local) => local.create_index_data(index_group, blob_id, meta, new_size).await,
        }
    }

    pub async fn update_index_data(&self, index_group: &IndexGroup, index_id: IndexID, old_blob_id: BlobID, blob_id: BlobID, meta: Vec<(Vec<u8>, AccessControl)>, index_offset: u64, new_size: u64) -> Result<()> {
        match self {
            // Database::Rocks(local) => local.update_index_data(index_group, index_id, old_blob_id, blob_id, meta, index_offset, new_size).await,
            Database::SQLite(local) => local.update_index_data(index_group, index_id, &old_blob_id, &blob_id, meta, index_offset, new_size).await,
        }
    }

    pub async fn list_indices(&self) -> Result<Vec<(IndexGroup, IndexID)>> {
        match self {
            Database::SQLite(local) => local.list_indices().await,
        }
    }
}

