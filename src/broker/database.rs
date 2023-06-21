//! Generic database interface that can be extended to multiple backends.
//! In development there were several supported, but only one has been retained.
use std::collections::BTreeSet;

use anyhow::{Result, Context};

use crate::types::{Sha256};

use super::database_sqlite::{SQLiteInterface, SearchRecord};
use super::interface::{SearchRequest, InternalSearchStatus};

/// A wrapper around a database module
pub enum Database {
    /// A database module implemented with sqlite
    SQLite(SQLiteInterface),
}

impl Database {
    /// Create an sqlite backed database instance at the given path.
    pub async fn new_sqlite(path: &str) -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new(path).await?))
    }

    /// Create an sqlite backed database instance in a temporary directory.
    pub async fn new_sqlite_temp() -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new_temp().await?))
    }

    /// List all searches that haven't completed yet.
    pub async fn list_active_searches(&self) -> Result<Vec<String>> {
        match self {
            Database::SQLite(local) => local.list_active_searches().await.context("list_active_searches")
        }
    }

    /// Add a search as pending.
    pub async fn initialize_search(&self, code: &str, req: &SearchRequest) -> Result<InternalSearchStatus> {
        match self {
            Database::SQLite(local) => local.initialize_search(code, req).await.context("initialize_search")
        }
    }

    /// Store the results for a complete search.
    pub async fn finalize_search(&self, code: &str, hits: BTreeSet<Sha256>, errors: Vec<String>, truncated: bool) -> Result<()> {
        match self {
            Database::SQLite(local) => local.finalize_search(code, hits, errors, truncated).await.context("finalize_search")
        }
    }

    /// Get the system internal view of a search 
    pub async fn search_record(&self, code: &str) -> Result<Option<SearchRecord>> {
        match self {
            Database::SQLite(local) => local.search_record(code).await.context("search_record")
        }
    }

    /// Get the the search status in a form ready to be presented to the API
    pub async fn search_status(&self, code: &str) -> Result<Option<InternalSearchStatus>> {
        match self {
            Database::SQLite(local) => local.search_status(code).await.context("search_status")
        }
    }
}

