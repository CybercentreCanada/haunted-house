
use std::collections::{HashSet, HashMap};

use anyhow::{Result, Context};
use bitvec::vec::BitVec;

use super::database_sqlite::SQLiteInterface;
use crate::error::ErrorKinds;
use crate::types::{ExpiryGroup, FilterID, Sha256, FileInfo};

pub enum IngestStatus {
    Ready,
    Pending(FilterID),
    Missing
}

pub enum Database {
    SQLite(SQLiteInterface),
}

impl Database {

    pub async fn new_sqlite(path: &str) -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new(path).await?))
    }

    // pub async fn new_sqlite_temp() -> Result<Self> {
    //     Ok(Database::SQLite(SQLiteInterface::new_temp().await?))
    // }

    pub async fn create_filter(&self, id: FilterID, expiry: &ExpiryGroup) -> Result<()> {
        match self {
            Database::SQLite(db) => db.create_filter(id, expiry).await.context("create_filter"),
        }
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>, ErrorKinds> {
        match self {
            Database::SQLite(db) => db.get_filters(first, last).await,
        }
    }

    pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>, ErrorKinds> {
        match self {
            Database::SQLite(db) => db.get_expiry(first, last).await,
        }
    }

    pub async fn delete_filter(&self, id: FilterID) -> Result<()> {
        match self {
            Database::SQLite(db) => db.delete_filter(id).await.context("delete_filter"),
        }
    }

    pub async fn filter_sizes(&self) -> HashMap<FilterID, u64> {
        match self {
            Database::SQLite(db) => db.filter_sizes().await,
        }
    }

    pub async fn filter_pending_count(&self) -> HashMap<FilterID, u64> {
        match self {
            Database::SQLite(db) => db.filter_pending_count().await,
        }
    }

    pub async fn filter_pending(&self) -> HashMap<FilterID, HashSet<Sha256>> {
        match self {
            Database::SQLite(db) => db.filter_pending().await,
        }
    }

    pub async fn update_file_access(&self, file: &FileInfo) -> Result<IngestStatus> {
        match self {
            Database::SQLite(db) => db.update_file_access(file).await.context("update_file_access"),
        }
    }

    pub async fn check_insert_status(&self, id: FilterID, file: &FileInfo) -> Result<IngestStatus, ErrorKinds> {
        match self {
            Database::SQLite(db) => db.check_insert_status(id, file).await,
        }
    }

    pub async fn ingest_file(&self, id: FilterID, file: &FileInfo, body: &BitVec) -> core::result::Result<bool, ErrorKinds> {
        match self {
            Database::SQLite(db) => db.ingest_file(id, file, body).await,
        }
    }

    pub async fn select_file_hashes(&self, id: FilterID, file_indices: &Vec<u64>, access: &HashSet<String>) -> Result<Vec<Sha256>> {
        match self {
            Database::SQLite(db) => db.select_file_hashes(id, file_indices, access).await.context("select_file_hashes"),
        }
    }

    pub async fn get_ingest_batch(&self, id: FilterID, limit: u32) -> Result<Vec<(u64, BitVec)>> {
        match self {
            Database::SQLite(db) => db.get_ingest_batch(id, limit).await.context("get_ingest_batch"),
        }
    }

    pub async fn finished_ingest(&self, id: FilterID, files: Vec<u64>) -> Result<()> {
        match self {
            Database::SQLite(db) => db.finished_ingest(id, files).await.context("finished_ingest"),
        }
    }
}

