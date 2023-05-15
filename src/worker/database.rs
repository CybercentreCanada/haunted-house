
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;
use crate::core::{SearchCache, CoreConfig};
// use crate::database_rocksdb::RocksInterface;
use crate::database_sqlite::SQLiteInterface;
use crate::interface::{SearchRequest, InternalSearchStatus, WorkRequest, WorkPackage, WorkError};
use crate::types::ExpiryGroup;


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

    pub async fn create_filter(&self, expiry_group: &ExpiryGroup, id: FilterID) -> Result<bool> {
        todo!();
    }

    pub async fn update_file_access(&self, hash: &[u8], access: &AccessControl, index_group: &IndexGroup) -> Result<bool> {
        match self {
            // Database::Rocks(local) => local.update_file_access(hash, access, index_group).await,
            Database::SQLite(local) => local.update_file_access(hash, access, index_group).await,
        }
    }

    // pub async fn select_index_to_grow(&self, index_group: &IndexGroup) -> Result<Option<(IndexID, BlobID, u64)>> {
    //     match self {
    //         // Database::Rocks(local) => local.select_index_to_grow(index_group).await,
    //         Database::SQLite(local) => local.select_index_to_grow(index_group).await,
    //     }
    // }

    // pub async fn create_index_data(&self, index_group: &IndexGroup, blob_id: BlobID, meta: Vec<(Vec<u8>, AccessControl)>, new_size: u64) -> Result<()> {
    //     match self {
    //         // Database::Rocks(local) => local.create_index_data(index_group, index_id, old_blob_id, blob_id, meta, new_size).await,
    //         Database::SQLite(local) => local.create_index_data(index_group, blob_id, meta, new_size).await,
    //     }
    // }

    // pub async fn update_index_data(&self, index_group: &IndexGroup, index_id: IndexID, old_blob_id: BlobID, blob_id: BlobID, meta: Vec<(Vec<u8>, AccessControl)>, index_offset: u64, new_size: u64) -> Result<()> {
    //     match self {
    //         // Database::Rocks(local) => local.update_index_data(index_group, index_id, old_blob_id, blob_id, meta, index_offset, new_size).await,
    //         Database::SQLite(local) => local.update_index_data(index_group, index_id, &old_blob_id, &blob_id, meta, index_offset, new_size).await,
    //     }
    // }

    // pub async fn list_indices(&self) -> Result<Vec<(IndexGroup, IndexID)>> {
    //     match self {
    //         Database::SQLite(local) => local.list_indices().await,
    //     }
    // }

    // pub async fn count_files(&self, id: &IndexID) -> Result<u64> {
    //     match self {
    //         Database::SQLite(local) => local.count_files(id).await,
    //     }
    // }

    // pub async fn lease_blob(&self) -> Result<BlobID> {
    //     match self {
    //         Database::SQLite(local) => local.lease_blob().await,
    //     }
    // }

    // pub async fn list_garbage_blobs(&self) -> Result<Vec<BlobID>> {
    //     match self {
    //         Database::SQLite(local) => local.list_garbage_blobs().await,
    //     }
    // }

    // pub async fn release_blob(&self, id: BlobID) -> Result<()> {
    //     match self {
    //         Database::SQLite(local) => local.release_blob(id).await,
    //     }
    // }

    // pub async fn release_groups(&self, id: IndexGroup) -> Result<()> {
    //     match self {
    //         Database::SQLite(local) => local.release_groups(id).await,
    //     }
    // }

    // pub async fn initialize_search(&self, req: SearchRequest) -> Result<InternalSearchStatus> {
    //     match self {
    //         Database::SQLite(local) => local.initialize_search(req).await
    //     }
    // }

    // pub async fn search_status(&self, code: String) -> Result<Option<InternalSearchStatus>> {
    //     match self {
    //         Database::SQLite(local) => local.search_status(code).await
    //     }
    // }

    // pub async fn get_work(&self, req: &WorkRequest) -> Result<WorkPackage> {
    //     match self {
    //         Database::SQLite(local) => local.get_work(req).await
    //     }
    // }

    // pub async fn release_assignments_before(&self, time: chrono::DateTime<chrono::Utc>) -> Result<u64> {
    //     match self {
    //         Database::SQLite(local) => local.release_tasks_assigned_before(time).await
    //     }
    // }

    // pub async fn release_filter_task(&self, id: i64) -> Result<bool> {
    //     match self {
    //         Database::SQLite(local) => local.release_filter_task(id).await
    //     }
    // }

    // pub async fn release_yara_task(&self, id: i64) -> Result<bool> {
    //     match self {
    //         Database::SQLite(local) => local.release_yara_task(id).await
    //     }
    // }

    // pub async fn get_filter_assignments_before(&self, time: chrono::DateTime<chrono::Utc>) -> Result<Vec<(String, i64)>> {
    //     match self {
    //         Database::SQLite(local) => local.get_filter_assignments_before(time).await
    //     }
    // }

    // pub async fn get_yara_assignments_before(&self, time: chrono::DateTime<chrono::Utc>) -> Result<Vec<(String, i64)>> {
    //     match self {
    //         Database::SQLite(local) => local.get_yara_assignments_before(time).await
    //     }
    // }

    // pub async fn get_work_notification(&self) -> Result<()> {
    //     match self {
    //         Database::SQLite(local) => local.get_work_notification().await
    //     }
    // }

    // pub async fn finish_filter_work(&self, id: i64, search: &String, cache: &mut SearchCache, index: IndexID, file_ids: Vec<u64>) -> Result<()> {
    //     match self {
    //         Database::SQLite(local) => local.finish_filter_work(id, search, cache, index, file_ids).await
    //     }
    // }

    // pub async fn finish_yara_work(&self, id: i64, search: &String, hashes: Vec<Vec<u8>>) -> Result<()> {
    //     match self {
    //         Database::SQLite(local) => local.finish_yara_work(id, search, hashes).await
    //     }
    // }

    // pub async fn work_error(&self, err: WorkError) -> Result<()> {
    //     match self {
    //         Database::SQLite(local) => local.work_error(err).await
    //     }
    // }

    // pub async fn tag_prompt(&self, req: PromptQuery) -> Result<PromptResult> {
    //     match self {
    //         Database::SQLite(local) => local.tag_prompt(req).await
    //     }
    // }
}

