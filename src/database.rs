
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;
use crate::core::{SearchCache, CoreConfig};
// use crate::database_rocksdb::RocksInterface;
use crate::database_sqlite::SQLiteInterface;
use crate::interface::{SearchRequest, SearchRequestResponse, WorkRequest, WorkPackage, WorkError};


#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Hash, PartialOrd, Ord)]
pub struct IndexGroup(String);

impl IndexGroup {
    pub fn create(expiry: &Option<DateTime<Utc>>) -> IndexGroup {
        IndexGroup(match expiry {
            Some(date) => format!("{}", date.format("%Y0%j")),
            None => format!("99990999"),
        })
    }

    pub fn from(data: &str) -> Self {
        Self(data.to_owned())
    }

    pub fn min() -> IndexGroup {
        IndexGroup(format!(""))
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

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct IndexID(String);

impl IndexID {
    pub fn new() -> Self {
        IndexID(format!("{:x}", uuid::Uuid::new_v4().as_u128()))
    }

    pub fn from(data: &str) -> Self {
        Self(data.to_owned())
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

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Hash, Debug)]
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

impl std::fmt::Display for BlobID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for BlobID { fn from(value: String) -> Self { Self(value) } }
impl From<&str> for BlobID { fn from(value: &str) -> Self { Self(value.to_owned()) } }

pub enum Database {
    // Rocks(RocksInterface),
    SQLite(SQLiteInterface),
}

impl Database {

    // pub async fn new_rocks(index_soft_max: usize) -> Result<Self> {
    //     Ok(Database::Rocks(RocksInterface::new(index_soft_max)?))
    // }

    pub async fn new_sqlite(config: CoreConfig, path: &str) -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new(config, path).await?))
    }

    pub async fn new_sqlite_temp(config: CoreConfig) -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new_temp(config).await?))
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

    pub async fn count_files(&self, id: &IndexID) -> Result<u64> {
        match self {
            Database::SQLite(local) => local.count_files(id).await,
        }
    }

    pub async fn lease_blob(&self) -> Result<BlobID> {
        match self {
            Database::SQLite(local) => local.lease_blob().await,
        }
    }

    pub async fn list_garbage_blobs(&self) -> Result<Vec<BlobID>> {
        match self {
            Database::SQLite(local) => local.list_garbage_blobs().await,
        }
    }

    pub async fn release_blob(&self, id: BlobID) -> Result<()> {
        match self {
            Database::SQLite(local) => local.release_blob(id).await,
        }
    }

    pub async fn release_groups(&self, id: IndexGroup) -> Result<()> {
        match self {
            Database::SQLite(local) => local.release_groups(id).await,
        }
    }

    pub async fn initialize_search(&self, req: SearchRequest) -> Result<SearchRequestResponse> {
        match self {
            Database::SQLite(local) => local.initialize_search(req).await
        }
    }

    pub async fn search_status(&self, code: String) -> Result<SearchRequestResponse> {
        match self {
            Database::SQLite(local) => local.search_status(code).await
        }
    }

    pub async fn get_work(&self, req: &WorkRequest) -> Result<WorkPackage> {
        match self {
            Database::SQLite(local) => local.get_work(req).await
        }
    }

    pub async fn get_work_notification(&self) -> Result<()> {
        match self {
            Database::SQLite(local) => local.get_work_notification().await
        }
    }

    pub async fn finish_filter_work(&self, id: i64, search: &String, cache: &mut SearchCache, index: IndexID, blob: BlobID, file_ids: Vec<u64>) -> Result<()> {
        match self {
            Database::SQLite(local) => local.finish_filter_work(id, search, cache, index, blob, file_ids).await
        }
    }

    pub async fn finish_yara_work(&self, id: i64, search: &String, hashes: Vec<Vec<u8>>) -> Result<()> {
        match self {
            Database::SQLite(local) => local.finish_yara_work(id, search, hashes).await
        }
    }

    pub async fn work_error(&self, err: WorkError) -> Result<()> {
        match self {
            Database::SQLite(local) => local.work_error(err).await
        }
    }

}

