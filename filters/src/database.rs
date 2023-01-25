use std::path::Path;

use bitvec::vec::BitVec;
use anyhow::{Result, Context};
use sqlx::SqlitePool;
use sqlx::pool::PoolOptions;

use crate::filters::{Filter, load, SimpleFilter};


#[derive(Clone)]
pub struct Database {
    db: SqlitePool,
    // config: Config,
    // work_notification: tokio::sync::Notify,
    // _temp_dir: Option<tempfile::TempDir>,
}

impl Database {
    pub async fn new(url: &str) -> Result<Self> {

        let url = if url == "memory" {
            format!("sqlite::memory:")
        } else {
            let path = Path::new(url);
            if path.is_absolute() {
                format!("sqlite://{}?mode=rwc", url)
            } else {
                format!("sqlite:{}?mode=rwc", url)
            }
        };

        let pool = PoolOptions::new()
            .max_connections(50)
            .acquire_timeout(std::time::Duration::from_secs(60))
            .connect(&url).await?;

        Self::initialize(&pool).await?;

        Ok(Self {
            db: pool,
            // config,
            // work_notification: Default::default(),
            // _temp_dir: None,
        })
    }

    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists filter_table (
            id INT PRIMARY KEY,
            label TEXT NOT NULL,
            filter BLOB NOT NULL,
            hash BLOB NOT NULL
        )")).execute(&mut con).await.context("error creating table filter_table")?;
        sqlx::query(&format!("CREATE INDEX if not exists filter_table_hash_label ON filter_table(hash, label)")).execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists paths (
            path TEXT PRIMARY KEY,
            hash BLOB NOT NULL
        )")).execute(&mut con).await.context("error creating table paths")?;

        return Ok(())
    }


    pub async fn get_cached_hash(&self, path: &Path) -> Result<Option<Vec<u8>>> {
        let row: Option<(Vec<u8>, )> = sqlx::query_as("SELECT hash FROM paths WHERE path = ?")
            .bind(path.as_os_str().to_str().unwrap())
            .fetch_optional(&self.db).await?;

        let row = match row {
            Some(row) => row.0,
            None => return Ok(None),
        };

        return Ok(Some(row))
    }

    pub async fn cache_hash(&self, path: &Path, hash: &Vec<u8>) -> Result<()> {
        sqlx::query("INSERT INTO paths(path, hash) VALUES(?, ?)")
            .bind(path.as_os_str().to_str().unwrap())
            .bind(hash)
            .execute(&self.db).await?;
        return Ok(())
    }

    pub async fn get(&self, hash: &Vec<u8>, label: &str) -> Result<Option<Box<dyn Filter>>> {
        let row: Option<(Vec<u8>, )> = sqlx::query_as("SELECT filter FROM filter_table WHERE hash = ? AND label = ?")
            .bind(hash)
            .bind(label)
            .fetch_optional(&self.db).await?;

        let row = match row {
            Some(row) => row.0,
            None => return Ok(None),
        };

        let mut data = BitVec::new();
        data.append(&mut BitVec::<_, bitvec::order::Lsb0>::from_vec(row));

        let filter = load(label, data)?;

        return Ok(Some(filter))
    }

    pub async fn get_simple(&self, hash: &Vec<u8>, label: &str) -> Result<Option<SimpleFilter>> {
        let row: Option<(Vec<u8>, )> = sqlx::query_as("SELECT filter FROM filter_table WHERE hash = ? AND label = ?")
            .bind(hash)
            .bind(label)
            .fetch_optional(&self.db).await?;

        let row = match row {
            Some(row) => row.0,
            None => return Ok(None),
        };

        let mut data = BitVec::new();
        data.append(&mut BitVec::<_, bitvec::order::Lsb0>::from_vec(row));

        let filter = SimpleFilter::load(label, data)?;

        return Ok(Some(filter))
    }

    pub async fn insert(&self, hash: &Vec<u8>, label: &str, filter: &Vec<u8>) -> Result<()> {
        let mut cursor = self.db.acquire().await?;
        sqlx::query("INSERT INTO filter_table(filter, hash, label) VALUES(?, ?, ?)")
            .bind(filter)
            .bind(hash)
            .bind(label)
            .execute(&mut cursor).await?;
        return Ok(())
    }

    pub async fn list_hashes(&self) -> Result<Vec<Vec<u8>>> {
        todo!();
    }
}