
use std::collections::{HashSet, HashMap};

use anyhow::Result;

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

    pub async fn new_sqlite_temp() -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new_temp().await?))
    }

    pub async fn create_filter(&self, id: FilterID, expiry: &ExpiryGroup) -> Result<()> {
        match self {
            Database::SQLite(db) => db.create_filter(id, expiry).await,
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
            Database::SQLite(db) => db.delete_filter(id).await,
        }
    }

    pub async fn filter_sizes(&self) -> HashMap<FilterID, u64> {
        match self {
            Database::SQLite(db) => db.filter_sizes().await,
        }
    }

    pub async fn filter_pending(&self) -> Result<HashMap<FilterID, u64>> {
        match self {
            Database::SQLite(db) => db.filter_pending().await,
        }
    }

    pub async fn update_file_access(&self, file: &FileInfo) -> Result<IngestStatus> {
        match self {
            Database::SQLite(db) => db.update_file_access(file).await,
        }
    }

    pub async fn ingest_file(&self, id: FilterID, file: &FileInfo) -> core::result::Result<bool, ErrorKinds> {
        match self {
            Database::SQLite(db) => db.ingest_file(id, file).await,
        }
    }

    pub async fn select_file_hashes(&self, id: FilterID, file_indices: &Vec<u64>, access: &HashSet<String>) -> Result<Vec<Sha256>> {
        match self {
            Database::SQLite(db) => db.select_file_hashes(id, file_indices, access).await,
        }
    }
}

