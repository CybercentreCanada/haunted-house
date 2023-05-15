
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;

use super::database_sqlite::SQLiteInterface;
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

    pub async fn initialize_search(&self, code: String, req: SearchRequest) -> Result<InternalSearchStatus> {
        match self {
            Database::SQLite(local) => local.initialize_search(code, req).await
        }
    }

    pub async fn search_status(&self, code: String) -> Result<Option<InternalSearchStatus>> {
        match self {
            Database::SQLite(local) => local.search_status(code).await
        }
    }

}

