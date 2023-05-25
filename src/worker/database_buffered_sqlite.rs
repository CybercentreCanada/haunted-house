use std::collections::{HashSet, HashMap};
use std::path::Path;
use std::str::FromStr;

use anyhow::{Result, Context};
use log::{info, error};
use sqlx::{SqlitePool, query_as, query};
use sqlx::pool::PoolOptions;
use tokio::sync::{mpsc, oneshot};

use crate::access::AccessControl;
use crate::error::ErrorKinds;
use crate::types::{FilterID, FileInfo, ExpiryGroup, Sha256};

use super::database::IngestStatus;

#[derive(Debug)]
pub enum BSQLCommand {
    CreateFilter{id: FilterID, expiry: ExpiryGroup, response: oneshot::Sender<Result<()>>},
    GetFilters{first: ExpiryGroup, last: ExpiryGroup, response: oneshot::Sender<Result<Vec<FilterID>, ErrorKinds>>},
    GetExpiry{first: ExpiryGroup, last: ExpiryGroup, response: oneshot::Sender<Result<Vec<(FilterID, ExpiryGroup)>, ErrorKinds>>},
    DeleteFilter{id: FilterID, response: oneshot::Sender<Result<()>>},
    GetFileAccess{id: FilterID, hash: Sha256, response: oneshot::Sender<Result<Option<AccessControl>>>},
    FilterSizes{response: oneshot::Sender<HashMap<FilterID, u64>>},
    FilterPendingCount{response: oneshot::Sender<HashMap<FilterID, u64>>},
    FilterPending{response: oneshot::Sender<HashMap<FilterID, HashSet<Sha256>>>},
    UpdateFileAccess{file: FileInfo, response: oneshot::Sender<Result<IngestStatus>>},
    CheckInsertStatus{id: FilterID, file: FileInfo, response: oneshot::Sender<Result<IngestStatus, ErrorKinds>>},
    IngestFile{id: FilterID, file: FileInfo, response: oneshot::Sender<core::result::Result<bool, ErrorKinds>>},
    SelectFileHashes{id: FilterID, file_indices: Vec<u64>, access: HashSet<String>, response: oneshot::Sender<Result<Vec<Sha256>>>},
    GetIngestBatch{id: FilterID, limit: u32, response: oneshot::Sender<Result<Vec<(u64, Sha256)>>>},
    FinishedIngest{id: FilterID, files: Vec<(u64, Sha256)>, response: oneshot::Sender<Result<()>>},
}

pub struct BufferedSQLite {
    db: SqlitePool,
    channel: mpsc::Receiver<BSQLCommand>,
    // work_notification: tokio::sync::Notify,
    filter_sizes: tokio::sync::Mutex<HashMap<FilterID, u64>>,
    filter_pending: tokio::sync::RwLock<HashMap<FilterID, HashSet<Sha256>>>,
    _temp_dir: Option<tempfile::TempDir>,
}


impl BufferedSQLite {

    pub async fn new(url: &str) -> Result<mpsc::Sender<BSQLCommand>> {

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
            .acquire_timeout(std::time::Duration::from_secs(600))
            .connect(&url).await?;

        Self::initialize(&pool).await?;

        let (sender, reciver) = mpsc::channel(128);

        let db = Self {
            db: pool,
            // work_notification: Default::default(),
            channel: reciver,
            _temp_dir: None,
            filter_sizes: tokio::sync::Mutex::new(Default::default()),
            filter_pending: tokio::sync::RwLock::new(Default::default())
        };

        {
            let mut sizes = db.filter_sizes.lock().await;
            for name in db.get_filters(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
                let (count, ): (i64, ) = sqlx::query_as(&format!("SELECT COUNT(1) FROM filter_{name}")).fetch_one(&db.db).await?;
                sizes.insert(name, count as u64);
            }
        }

        {
            let mut pending = db.filter_pending.write().await;
            for name in db.get_filters(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
                let mut items = vec![];
                let data: Vec<(Vec<u8>, )> = sqlx::query_as(&format!("SELECT hash FROM filter_{name} WHERE ingested IS FALSE")).fetch_all(&db.db).await?;
                for (row,) in data {
                    if let Ok(hash) = Sha256::try_from(&row[..]) {
                        items.push(hash)
                    }
                }
                pending.insert(name, HashSet::from_iter(items.into_iter()));
            }
        }

        tokio::spawn(db.run());

        Ok(sender)
    }

    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;
        sqlx::query("PRAGMA foreign_keys=ON").execute(&mut con).await?;
        sqlx::query("PRAGMA busy_timeout=600000").execute(&mut *con).await?;

        sqlx::query(&format!("create table if not exists filters (
            id INTEGER PRIMARY KEY,
            expiry CHAR(8) NOT NULL
        )")).execute(&mut con).await.context("error creating table filters")?;
        sqlx::query(&format!("create index if not exists expiry ON filters(expiry)")).execute(&mut con).await?;

        return Ok(())
    }

    async fn run(mut self) {
        loop {
            let message = match self.channel.recv().await {
                Some(message) => message,
                None => break
            };

            if let Err(err) = self.handle_message(message).await {
                error!("DB error: {err}")
            }
        }
    }

    async fn handle_message(&mut self, message: BSQLCommand) -> Result<()> {
        match message {
            BSQLCommand::CreateFilter { id, expiry, response } => { _ = response.send(self.create_filter(id, &expiry).await); },
            BSQLCommand::GetFilters { first, last, response } => { _ = response.send(self.get_filters(&first, &last).await); },
            BSQLCommand::GetExpiry { first, last, response } => { _ = response.send(self.get_expiry(&first, &last).await); },
            BSQLCommand::DeleteFilter { id, response } => { _ = response.send(self.delete_filter(id).await); },
            BSQLCommand::GetFileAccess { id, hash, response } => { _ = response.send(self.get_file_access(id, &hash).await); },
            BSQLCommand::FilterSizes { response } => { _ = response.send(self.filter_sizes().await); },
            BSQLCommand::FilterPendingCount { response } => { _ = response.send(self.filter_pending_count().await); },
            BSQLCommand::FilterPending { response } => { _ = response.send(self.filter_pending().await); },
            BSQLCommand::UpdateFileAccess { file, response } => { _ = response.send(self.update_file_access(&file).await); },
            BSQLCommand::CheckInsertStatus { id, file, response } => { _ = response.send(self.check_insert_status(id, &file).await); },
            BSQLCommand::IngestFile { id, file, response } => { _ = response.send(self.ingest_file(id, &file).await); },
            BSQLCommand::SelectFileHashes { id, file_indices, access, response } => { _ = response.send(self.select_file_hashes(id, &file_indices, &access).await); },
            BSQLCommand::GetIngestBatch { id, limit, response } => { _ = response.send(self.get_ingest_batch(id, limit).await); },
            BSQLCommand::FinishedIngest { id, files, response } => { _ = response.send(self.finished_ingest(id, files).await); },
        };
        Ok(())
    }

    pub async fn create_filter(&self, name: FilterID, expiry: &ExpiryGroup) -> Result<()> {
        let mut con = self.db.begin().await?;
        info!("Creating filter {name} ({})", name.to_i64());

        sqlx::query(&format!("INSERT INTO filters(id, expiry) VALUES(?, ?) ON CONFLICT DO NOTHING"))
            .bind(name.to_i64())
            .bind(expiry.as_str())
            .execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists filter_{name} (
            number INTEGER PRIMARY KEY AUTOINCREMENT,
            hash BLOB NOT NULL UNIQUE,
            access BLOB NOT NULL,
            ingested BOOLEAN DEFAULT FALSE
        )")).execute(&mut con).await?;
        sqlx::query(&format!("create index if not exists ingested ON filter_{name}(ingested)")).execute(&mut con).await?;
        sqlx::query(&format!("create index if not exists hash ON filter_{name}(hash)")).execute(&mut con).await?;
        con.commit().await?;

        let mut sizes = self.filter_sizes.lock().await;
        sizes.insert(name, 0);

        return Ok(())
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>, ErrorKinds> {
        let rows : Vec<(i64, )> = sqlx::query_as("SELECT id FROM filters WHERE ? <= expiry AND expiry <= ?")
            .bind(first.as_str())
            .bind(last.as_str())
            .fetch_all(&self.db).await?;
        Ok(rows.into_iter().map(|(id, )|FilterID::from(id)).collect())
    }

    pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>, ErrorKinds> {
        let rows : Vec<(i64, String)> = sqlx::query_as("SELECT id, expiry FROM filters WHERE ? <= expiry AND expiry <= ?")
            .bind(first.as_str())
            .bind(last.as_str())
            .fetch_all(&self.db).await?;
        rows.into_iter().map(|(id, expiry)|Ok((FilterID::from(id), ExpiryGroup::from(&expiry)))).collect()
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

    pub async fn get_file_access(&self, id: FilterID, hash: &Sha256) -> Result<Option<AccessControl>> {
        let row: Option<(String, )> = query_as(&format!("SELECT access FROM filter_{id} WHERE hash = ?")).bind(hash.as_bytes()).fetch_optional(&self.db).await?;

        match row {
            Some((row, )) => Ok(Some(AccessControl::from_str(&row)?)),
            None => Ok(None)
        }
    }

    pub async fn filter_sizes(&self) -> HashMap<FilterID, u64> {
        let sizes = self.filter_sizes.lock().await;
        sizes.clone()
    }

    pub async fn filter_pending(&self) -> HashMap<FilterID, HashSet<Sha256>> {
        let pending = self.filter_pending.read().await;
        return pending.clone()
    }

    pub async fn filter_pending_count(&self) -> HashMap<FilterID, u64> {
        let pending = self.filter_pending.read().await;
        return pending.iter().map(|(key, values)|(*key, values.len() as u64)).collect()
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
                let new_string = access.to_string();
                if access_string == new_string {
                    if ingested {
                        return Ok(IngestStatus::Ready)
                    } else {
                        return Ok(IngestStatus::Pending(id))
                    }
                }

                let result = query(&format!("UPDATE filter_{id} SET access = ? WHERE access = ? AND hash = ?"))
                    .bind(new_string)
                    .bind(access_string)
                    .bind(file.hash.as_bytes())
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

    pub async fn check_insert_status(&self, id: FilterID, file: &FileInfo) -> Result<IngestStatus, ErrorKinds> {
        let mut filters = self.get_expiry(&file.expiry, &ExpiryGroup::max()).await?;
        filters.sort_unstable_by(|a, b|b.1.cmp(&a.1));

        let mut conn = self.db.acquire().await?;
        let (ingested, ): (bool, ) = match query_as(&format!("SELECT ingested FROM filter_{id} WHERE hash = ?")).bind(file.hash.as_bytes()).fetch_optional(&mut conn).await? {
            Some(row) => row,
            None => return Ok(IngestStatus::Missing)
        };

        if ingested {
            return Ok(IngestStatus::Ready)
        } else {
            return Ok(IngestStatus::Pending(id))
        }
    }

    pub async fn ingest_file(&self, id: FilterID, file: &FileInfo) -> Result<bool, ErrorKinds> {
        // Try to load the file if its already in the DB
        let mut conn = self.db.acquire().await?;
        let row: Result<Option<(bool, )>, sqlx::Error> = sqlx::query_as(&format!("SELECT ingested FROM filter_{id} WHERE hash = ?"))
            .bind(file.hash.as_bytes()).fetch_optional(&mut conn).await;
        match row {
            Ok(Some((ingested, ))) => return Ok(ingested),
            Ok(None) => {},
            Err(err) => {
                if let Some(err) = err.as_database_error() {
                    if err.message().contains("no such table") {
                        return Err(ErrorKinds::FilterUnknown(id))
                    }
                }
            }
        }

        // Insert the new file if it is not there
        sqlx::query(&format!("INSERT INTO filter_{id}(hash, access) VALUES(?, ?)"))
            .bind(file.hash.as_bytes())
            .bind(file.access.to_string())
            .execute(&mut conn).await?;

        match self.filter_sizes.lock().await.get_mut(&id) {
            Some(size) => { *size += 1; },
            None => {},
        };

        let mut pending = self.filter_pending.write().await;
        match pending.entry(id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => { entry.get_mut().insert(file.hash.clone()); },
            std::collections::hash_map::Entry::Vacant(entry) => { entry.insert(HashSet::from([file.hash.clone()])); },
        }

        return Ok(false)
    }


    pub async fn select_file_hashes(&self, id: FilterID, indices: &Vec<u64>, view: &HashSet<String>) -> Result<Vec<Sha256>> {
        let mut selected: Vec<Sha256> = vec![];
        let mut conn = self.db.acquire().await?;
        for file_id in indices {
            let row: Option<(String, Vec<u8>)> = sqlx::query_as(&format!("SELECT access, hash FROM filter_{id} WHERE number = ?"))
                .bind(*file_id as i64).fetch_optional(&mut conn).await?;
            if let Some((access, hash)) = row {
                let access = AccessControl::from_str(&access)?;
                if access.can_access(&view) {
                    selected.push(Sha256::try_from(&hash[..])?);
                }
            }
        }
        return Ok(selected)
    }

    pub async fn get_ingest_batch(&self, id: FilterID, limit: u32) -> Result<Vec<(u64, Sha256)>> {
        let row: Vec<(i64, Vec<u8>)> = sqlx::query_as(&format!("SELECT number, hash FROM filter_{id} WHERE ingested = FALSE ORDER BY number LIMIT {limit}"))
        .fetch_all(&self.db).await?;

        let mut output = vec![];
        for (id, hash) in row {
            output.push((id as u64, Sha256::try_from(&hash[..])?));
        }
        return Ok(output)
    }

    pub async fn finished_ingest(&self, id: FilterID, files: Vec<(u64, Sha256)>) -> Result<()> {
        let mut conn = self.db.acquire().await.context("aquire")?;
        for (number, _) in &files {
            sqlx::query(&format!("UPDATE filter_{id} SET ingested = TRUE WHERE number = ?")).bind(*number as i64)
            .execute(&mut conn).await.context("update")?;
        }
        let mut pending = self.filter_pending.write().await;
        if let Some(pending) = pending.get_mut(&id) {
            for (_, hash) in files {
                pending.remove(&hash);
            }
        }
        return Ok(())
    }
}

