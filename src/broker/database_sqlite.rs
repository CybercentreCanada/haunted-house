//! An implementation of the broker database in sqlite.
use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use anyhow::{Result, Context};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, query_as};
use sqlx::pool::PoolOptions;

use crate::access::AccessControl;
use crate::query::Query;
use crate::types::{ExpiryGroup, Sha256};

use super::interface::{SearchRequest, InternalSearchStatus, SearchRequestResponse, SearchProgress};


/// An implementation of the broker database in sqlite.
pub struct SQLiteInterface {
    /// sqlite database connection pool
    db: SqlitePool,
    /// optionally a scope guard ensuring that any temporary directories are deleted on exit
    _temp_dir: Option<tempfile::TempDir>,
}

/// Database record describing a search, either in progress or completed.
#[derive(Serialize, Deserialize)]
pub struct SearchRecord {
    /// ID code that identifies a search
    pub code: String,
    /// Signature that is being run for this search
    pub yara_signature: String,
    /// The filter query derived from the yara signature
    pub query: Query,
    /// control describing what permissions are required to view this search
    pub view: AccessControl,
    /// permissions describing what files can be seen by by this search
    pub access: HashSet<String>,
    /// List of warnings encountered by running this search
    pub warnings: Vec<String>,
    /// List of errors encountered by running this search
    pub errors: Vec<String>,
    /// Earliest expiry group this search will include
    pub start_date: ExpiryGroup,
    /// Latest expiry group this search will include
    pub end_date: ExpiryGroup,
    /// List of files hit on by this search
    pub hit_files: BTreeSet<Sha256>,
    /// Is the list of hit_files truncated
    pub truncated: bool,
    /// Is this search finished
    pub finished: bool,
}


impl SQLiteInterface {
    /// Open or create the database at the given path.
    pub async fn new(url: &str) -> Result<Self> {

        let url = if url == "memory" {
            String::from("sqlite::memory:")
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

    /// Open a new database in a temporary directory
    pub async fn new_temp() -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let path = tempdir.path().join("house.db");

        let mut obj = Self::new(path.to_str().unwrap()).await?;
        obj._temp_dir = Some(tempdir);

        Ok(obj)
    }

    /// Setup the tables and configuration of the database.
    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;
        sqlx::query("PRAGMA foreign_keys=ON").execute(&mut con).await?;
        sqlx::query("PRAGMA busy_timeout=600000").execute(&mut *con).await?;

        sqlx::query("create table if not exists searches (
            code TEXT PRIMARY KEY,
            data BLOB NOT NULL,
            finished BOOLEAN NOT NULL,
            start_time TEXT
        )").execute(&mut con).await.context("error creating table searches")?;
        sqlx::query("CREATE INDEX IF NOT EXISTS searches_finished ON searches(finished)").execute(&mut con).await?;

        sqlx::query("create table if not exists config_values (
            key TEXT PRIMARY KEY,
            data BLOB NOT NULL
        )").execute(&mut con).await.context("error creating table config_values")?;

        return Ok(())
    }

    /// List all searches that aren't finished.
    pub async fn list_active_searches(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = query_as("SELECT code FROM searches WHERE finished IS FALSE").fetch_all(&self.db).await?;
        Ok(rows.into_iter().map(|(code, )|code).collect_vec())
    }

    /// Store a search that has yet to run
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
        sqlx::query("INSERT INTO searches(code, data, start_time, finished) VALUES(?, ?, ?, FALSE)")
            .bind(code)
            .bind(&postcard::to_allocvec(&SearchRecord{
                code: code.to_owned(),
                yara_signature: req.yara_signature.clone(),
                errors: Default::default(),
                access: req.access.clone(),
                view: req.view.clone(),
                query: req.query.clone(),
                warnings: req.warnings.clone(),
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

    /// Save the results and complete a search
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
            .bind(code)
            .execute(&self.db).await?;

        return Ok(())
    }

    /// Get the system internal search record
    pub async fn search_record(&self, code: &str) -> Result<Option<SearchRecord>> {
        let row: Option<(Vec<u8>, )> = sqlx::query_as("SELECT data FROM searches WHERE code = ?")
            .bind(code)
            .fetch_optional(&self.db).await?;

        Ok(match row {
            Some((data, )) => Some(postcard::from_bytes(&data)?),
            None => None,
        })
    }

    /// Get the search status approprate for the API
    pub async fn search_status(&self, code: &str) -> Result<Option<InternalSearchStatus>> {
        let record = self.search_record(code).await?;
        Ok(match record {
            Some(record) => Some(InternalSearchStatus {
                view: record.view,
                resp: SearchRequestResponse {
                    code: record.code,
                    finished: record.finished,
                    phase: if record.finished { SearchProgress::Finished } else { SearchProgress::Unknown },
                    progress: (0, 1),
                    warnings: record.warnings,
                    errors: record.errors,
                    hits: record.hit_files.into_iter().map(|hash|hash.hex()).collect(),
                    truncated: record.truncated,
                }
            }),
            None => None,
        })
    }

    /// Write a transient configuration value to the database
    pub async fn store_value(&self, key: &str, value: &[u8]) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO config_values(key, data) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&self.db).await?;
        Ok(())
    }

    /// Read a transient configuration value from the database
    pub async fn fetch_value(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row: Option<(Vec<u8>, )> = sqlx::query_as("SELECT data FROM config_values WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.db).await?;

        Ok(row.map(|(data, )| data))
    }

}

