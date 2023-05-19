
use std::collections::BTreeSet;

use anyhow::Result;

use crate::types::{ExpiryGroup, FilterID, WorkerID, Sha256};

use super::database_sqlite::{SQLiteInterface, SearchRecord};
use super::interface::{SearchRequest, InternalSearchStatus};


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

    pub async fn list_active_searches(&self) -> Result<Vec<String>> {
        match self {
            Database::SQLite(local) => local.list_active_searches().await
        }
    }

    pub async fn initialize_search(&self, code: &str, req: &SearchRequest) -> Result<InternalSearchStatus> {
        match self {
            Database::SQLite(local) => local.initialize_search(code, req).await
        }
    }

    pub async fn finalize_search(&self, code: &str, hits: BTreeSet<Sha256>, errors: Vec<String>, truncated: bool) -> Result<()> {
        match self {
            Database::SQLite(local) => local.finalize_search(code, hits, errors, truncated).await
        }
    }

    pub async fn search_record(&self, code: &str) -> Result<Option<SearchRecord>> {
        match self {
            Database::SQLite(local) => local.search_record(code).await
        }
    }

    pub async fn search_status(&self, code: &str) -> Result<Option<InternalSearchStatus>> {
        match self {
            Database::SQLite(local) => local.search_status(code).await
        }
    }

    pub async fn list_filters(&self) -> Result<Vec<(WorkerID, FilterID, ExpiryGroup)>> {
        match self {
            Database::SQLite(local) => local.list_filters().await
        }
    }

    pub async fn get_expiry(&self, filter: FilterID) -> Result<Option<ExpiryGroup>> {
        match self {
            Database::SQLite(local) => local.get_expiry(filter).await
        }
    }

    pub async fn create_filter(&self, worker: &WorkerID, expiry: &ExpiryGroup) -> Result<FilterID> {
        match self {
            Database::SQLite(local) => local.create_filter(worker, expiry).await
        }
    }

}

