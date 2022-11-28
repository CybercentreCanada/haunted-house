
use anyhow::Result;
use log::error;
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;
use crate::database_rocksdb::RocksInterface;
use crate::database_sqlite::SQLiteInterface;


pub struct IndexGroup(pub String);

pub struct IndexID(pub String);

pub struct BlobID(pub String);

pub enum Database {
    // Rocks(RocksInterface),
    SQLite(SQLiteInterface),
}

impl Database {

    // pub async fn new_rocks(index_soft_max: usize) -> Result<Self> {
    //     Ok(Database::Rocks(RocksInterface::new(index_soft_max)?))
    // }

    pub async fn new_sqlite(index_soft_max: usize, path: &str) -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new(index_soft_max, path).await?))
    }

    pub async fn update_file_access(&self, hash: &[u8], access: &AccessControl, index_group: &IndexGroup) -> Result<bool> {
        match self {
            // Database::Rocks(local) => local.update_file_access(hash, access, index_group).await,
            Database::SQLite(local) => local.update_file_access(hash, access, index_group).await,
        }
    }

    pub async fn select_index_to_grow(&self, index_group: &IndexGroup) -> Result<(IndexID, Option<BlobID>, BlobID)> {
        match self {
            // Database::Rocks(local) => local.select_index_to_grow(index_group).await,
            Database::SQLite(local) => local.select_index_to_grow(index_group).await,
        }
    }

    pub async fn create_index_data(&self, index_group: &IndexGroup, index_id: IndexID, old_blob_id: BlobID, blob_id: BlobID, meta: Vec<(Vec<u8>, AccessControl)>, new_size: usize) -> Result<()> {
        match self {
            // Database::Rocks(local) => local.create_index_data(index_group, index_id, old_blob_id, blob_id, meta, new_size).await,
            Database::SQLite(local) => local.create_index_data(index_group, index_id, old_blob_id, blob_id, meta, new_size).await,
        }
    }

    pub async fn update_index_data(&self, index_group: &IndexGroup, index_id: IndexID, old_blob_id: BlobID, blob_id: BlobID, meta: Vec<(Vec<u8>, AccessControl)>, index_offset: usize, new_size: usize) -> Result<()> {
        match self {
            // Database::Rocks(local) => local.update_index_data(index_group, index_id, old_blob_id, blob_id, meta, index_offset, new_size).await,
            Database::SQLite(local) => local.update_index_data(index_group, index_id, old_blob_id, blob_id, meta, index_offset, new_size).await,
        }
    }
}

