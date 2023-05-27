use std::collections::{BTreeSet, HashSet};
use std::path::{Path};

use anyhow::{Result, Context};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, query_as};
use sqlx::pool::PoolOptions;

use crate::access::AccessControl;
use crate::query::Query;
use crate::types::{ExpiryGroup, Sha256};

use super::interface::{SearchRequest, InternalSearchStatus, SearchRequestResponse};

pub struct SQLiteInterface {
    db: SqlitePool,
    // work_notification: tokio::sync::Notify,
    _temp_dir: Option<tempfile::TempDir>,
}

// #[derive(Serialize, Deserialize)]
// struct FileEntry {
//     access: AccessControl,
//     hash: Vec<u8>
// }

// fn filter_table_name(name: &IndexID) -> String {
//     format!("filter_{name}")
// }

#[derive(Serialize, Deserialize)]
pub struct SearchRecord {
    pub code: String,
    pub yara_signature: String,
    pub query: Query,
    pub view: AccessControl,
    pub access: HashSet<String>,
    pub errors: Vec<String>,
    pub start_date: ExpiryGroup,
    pub end_date: ExpiryGroup,
    pub hit_files: BTreeSet<Sha256>,
    pub truncated: bool,
    pub finished: bool,
}


impl SQLiteInterface {
    pub async fn new(url: &str) -> Result<Self> {

        let url = if url == "memory" {
            format!("sqlite::memory:")
        } else {
            let path = Path::new(url);

            if let Some(parent) = path.parent() {
                if parent != Path::new("") && parent != Path::new("/") && !parent.exists() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }

            if path.is_absolute() {
                format!("sqlite://{}?mode=rwc", url)
            } else {
                format!("sqlite:{}?mode=rwc", url)
            }
        };

        let pool = PoolOptions::new()
            .max_connections(200)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&url).await?;

        Self::initialize(&pool).await?;

        Ok(Self {
            db: pool,
            // work_notification: Default::default(),
            _temp_dir: None,
        })
    }

    pub async fn new_temp() -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let path = tempdir.path().join("house.db");

        let mut obj = Self::new(path.to_str().unwrap()).await?;
        obj._temp_dir = Some(tempdir);

        Ok(obj)
    }

    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;
        sqlx::query("PRAGMA foreign_keys=ON").execute(&mut con).await?;
        sqlx::query("PRAGMA busy_timeout=600000").execute(&mut *con).await?;

        sqlx::query(&format!("create table if not exists searches (
            code TEXT PRIMARY KEY,
            data BLOB NOT NULL,
            finished BOOLEAN NOT NULL,
            start_time TEXT
        )")).execute(&mut con).await.context("error creating table searches")?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS searches_finished ON searches(finished)")).execute(&mut con).await?;

        return Ok(())
    }

    pub async fn list_active_searches(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = query_as("SELECT code FROM searches WHERE finished IS FALSE").fetch_all(&self.db).await?;
        Ok(rows.into_iter().map(|(code, )|code).collect_vec())
    }


    pub async fn initialize_search(&self, code: &str, req: &SearchRequest) -> Result<InternalSearchStatus> {
        // Turn the expiry dates into a group range
        let start = match req.start_date {
            Some(value) => ExpiryGroup::create(&Some(value)),
            None => ExpiryGroup::min(),
        };
        let end = match req.end_date {
            Some(value) => ExpiryGroup::create(&Some(value)),
            None => ExpiryGroup::max(),
        };

        // Add operation to the search table
        sqlx::query("INSERT INTO searches(code, data, start_time) VALUES(?, ?, ?)")
            .bind(&code)
            .bind(&postcard::to_allocvec(&SearchRecord{
                code: code.to_owned(),
                yara_signature: req.yara_signature.clone(),
                errors: Default::default(),
                access: req.access.clone(),
                view: req.view.clone(),
                query: req.query.clone(),
                hit_files: Default::default(),
                truncated: false,
                finished: false,
                start_date: start,
                end_date: end,
            })?)
            .bind(chrono::Utc::now().to_rfc3339())
            .execute(&self.db).await?;

        match self.search_status(code).await? {
            Some(result) => Ok(result),
            None => Err(anyhow::anyhow!("Result status could not be read.")),
        }
    }

    pub async fn finalize_search(&self, code: &str, hits: BTreeSet<Sha256>, errors: Vec<String>, truncated: bool) -> Result<()> {
        let mut record = match self.search_record(code).await? {
            Some(record) => record,
            None => return Err(anyhow::anyhow!(""))
        };

        record.hit_files = hits;
        record.errors = errors;
        record.truncated = truncated;
        record.finished = true;

        sqlx::query("UPDATE searches SET data = ?, finished = TRUE WHERE code = ?")
            .bind(&postcard::to_allocvec(&record)?)
            .bind(&code)
            .execute(&self.db).await?;

        return Ok(())
    }

    pub async fn search_record(&self, code: &str) -> Result<Option<SearchRecord>> {
        let row: Option<(Vec<u8>, )> = sqlx::query_as("SELECT data FROM searches WHERE code = ?")
            .bind(&code)
            .fetch_optional(&self.db).await?;

        Ok(match row {
            Some((data, )) => Some(postcard::from_bytes(&data)?),
            None => None,
        })
    }

    pub async fn search_status(&self, code: &str) -> Result<Option<InternalSearchStatus>> {
        let record = self.search_record(code).await?;
        Ok(match record {
            Some(record) => Some(InternalSearchStatus {
                view: record.view,
                resp: SearchRequestResponse {
                    code: record.code,
                    finished: record.finished,
                    errors: record.errors,
                    hits: record.hit_files.into_iter().map(|hash|hash.hex()).collect(),
                    truncated: record.truncated,
                }
            }),
            None => None,
        })
    }
}

