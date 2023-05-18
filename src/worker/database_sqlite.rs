use std::collections::{HashSet, HashMap};
use std::path::Path;
use std::str::FromStr;

use anyhow::{Result, Context};
use sqlx::{SqlitePool, query_as, query};
use sqlx::pool::PoolOptions;

use crate::access::AccessControl;
use crate::error::ErrorKinds;
use crate::types::{FilterID, FileInfo, ExpiryGroup, Sha256};

use super::database::IngestStatus;


pub struct SQLiteInterface {
    db: SqlitePool,
    work_notification: tokio::sync::Notify,
    filter_sizes: tokio::sync::Mutex<HashMap<FilterID, u64>>,
    _temp_dir: Option<tempfile::TempDir>,
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

        let db = Self {
            db: pool,
            work_notification: Default::default(),
            _temp_dir: None,
            filter_sizes: tokio::sync::Mutex::new(Default::default()),
        };

        {
            let mut sizes = db.filter_sizes.lock().await;
            for name in db.get_filters(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
                let (count, ): (i64, ) = sqlx::query_as(&format!("SELECT COUNT(1) FROM filter_{name}")).fetch_one(&db.db).await?;
                sizes.insert(name, count as u64);
            }
        }

        Ok(db)
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

        sqlx::query(&format!("create table if not exists filters (
            id INTEGER PRIMARY KEY,
            expiry CHAR(8) NOT NULL,
        )")).execute(&mut con).await.context("error creating table filters")?;

        return Ok(())
    }

    pub async fn create_filter(&self, name: FilterID, expiry: &ExpiryGroup) -> Result<()> {
        let mut con = self.db.begin().await?;

        sqlx::query(&format!("INSERT INTO filters(id, expiry) VALUES(?, ?) ON CONFLICT DO NOTHING"))
            .bind(name.to_string())
            .bind(expiry.as_str())
            .execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists filter_{name} (
            hash BLOB PRIMARY KEY,
            number INTEGER NOT NULL UNIQUE,
            access BLOB NOT NULL,
            ingested BOOLEAN DEFAULT FALSE,
        )")).execute(&mut con).await?;
        sqlx::query(&format!("create index if not exists ingested ON filter_{name}(ingested)")).execute(&mut con).await?;
        con.commit().await?;

        let mut sizes = self.filter_sizes.lock().await;
        sizes.insert(name, 0);

        return Ok(())
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>, ErrorKinds> {
        let rows : Vec<(String, )> = sqlx::query_as("SELECT id FROM filters WHERE ? <= expiry AND expiry <= ?")
            .bind(first.as_str())
            .bind(last.as_str())
            .fetch_all(&self.db).await?;
        rows.into_iter().map(|(id, )|FilterID::from_str(&id)).collect()
    }

    pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>, ErrorKinds> {
        let rows : Vec<(String, String)> = sqlx::query_as("SELECT id, expiry FROM filters WHERE ? <= expiry AND expiry <= ?")
            .bind(first.as_str())
            .bind(last.as_str())
            .fetch_all(&self.db).await?;
        rows.into_iter().map(|(id, expiry)|Ok((FilterID::from_str(&id)?, ExpiryGroup::from(&expiry)))).collect()
    }

    pub async fn delete_filter(&self, name: FilterID) -> Result<()> {
        let mut con = self.db.begin().await?;

        sqlx::query(&format!("DELETE FROM filters WHERE name = ?"))
            .bind(name.to_string())
            .execute(&mut con).await?;
        sqlx::query(&format!("DROP TABLE filter_{name}")).execute(&mut con).await?;
        con.commit().await?;

        let mut sizes = self.filter_sizes.lock().await;
        sizes.remove(&name);

        return Ok(())
    }

    pub async fn filter_sizes(&self) -> HashMap<FilterID, u64> {
        let sizes = self.filter_sizes.lock().await;
        sizes.clone()
    }

    pub async fn filter_pending(&self) -> Result<HashMap<FilterID, u64>> {
        let mut output: HashMap<FilterID, u64> = Default::default();
        let mut conn = self.db.acquire().await?;
        for id in self.get_filters(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
            let (count, ): (i64, ) = query_as(&format!("SELECT COUNT(1) FROM filter_{id} WHERE ingested IS FALSE")).fetch_one(&mut conn).await?;
            output.insert(id, count as u64);
        }
        Ok(output)
    }

    pub async fn update_file_access(&self, file: &FileInfo) -> Result<IngestStatus> {
        let mut filters = self.get_expiry(&file.expiry, &ExpiryGroup::max()).await?;
        filters.sort_unstable_by(|a, b|b.1.cmp(&a.1));

        let mut conn = self.db.acquire().await?;
        'filters: for (id, _) in filters {
            loop {
                let (access_string, ingested): (String, bool) = match query_as(&format!("SELECT access, ingested FROM filter_{id} WHERE hash = ?")).bind(file.hash.as_bytes()).fetch_optional(&mut conn).await? {
                    Some(row) => row,
                    None => continue 'filters
                };

                let access = AccessControl::from_str(&access_string)?.or(&file.access).simplify();

                let result = query(&format!("UPDATE filter_{id} SET access = ? WHERE access = ?"))
                    .bind(access.to_string())
                    .bind(access_string)
                    .execute(&mut conn).await?;
                if result.rows_affected() > 0 {
                    if ingested {
                        return Ok(IngestStatus::Ready)
                    } else {
                        return Ok(IngestStatus::Pending(id))
                    }
                }
            }
        }
        return Ok(IngestStatus::Missing)
    }

    pub async fn ingest_file(&self, id: FilterID, file: &FileInfo) -> Result<bool, ErrorKinds> {
        // Hold the locked filter sizes
        let mut sizes = self.filter_sizes.lock().await;
        let size = match sizes.get_mut(&id) {
            Some(size) => size,
            None => return Err(ErrorKinds::FilterUnknown),
        };

        // Try to load the file if its already in the DB
        let mut conn = self.db.acquire().await?;
        let row: Option<(bool, )> = sqlx::query_as(&format!("SELECT ingested FROM filter_{id} WHERE hash = ?"))
            .bind(file.hash.as_bytes()).fetch_optional(&mut conn).await?;
        if let Some((ingested, )) = row {
            return Ok(ingested)
        }

        // Insert the new file if it is not there
        sqlx::query(&format!("INSERT INTO filter_{id}(hash, number, access) VALUES(?, ?, ?)"))
            .bind(file.hash.as_bytes())
            .bind((*size + 1) as i64)
            .bind(file.access.to_string())
            .execute(&mut conn).await?;
        *size += 1;
        return Ok(false)
    }


    pub async fn select_file_hashes(&self, id: FilterID, indices: &Vec<i64>, view: &HashSet<String>) -> Result<Vec<Sha256>> {
        let mut selected: Vec<Sha256> = vec![];
        let mut conn = self.db.acquire().await?;
        for file_id in indices {
            let row: Option<(String, Vec<u8>)> = sqlx::query_as(&format!("SELECT access, hash FROM filter_{id} WHERE number = ?"))
                .bind(file_id).fetch_optional(&mut conn).await?;
            if let Some((access, hash)) = row {
                let access = AccessControl::from_str(&access)?;
                if access.can_access(&view) {
                    selected.push(Sha256::try_from(&hash[..])?);
                }
            }
        }
        return Ok(selected)
    }

}

