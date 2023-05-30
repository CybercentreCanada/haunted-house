use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, Context};
use log::{info, error};
use sqlx::{SqlitePool, query_as, query};
use sqlx::pool::{PoolOptions, PoolConnection};
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::access::AccessControl;
use crate::error::ErrorKinds;
use crate::types::{FilterID, FileInfo, ExpiryGroup, Sha256};

use super::database::{IngestStatus, IngestStatusBundle};

#[derive(Debug)]
pub enum BSQLCommand {
    CreateFilter{id: FilterID, expiry: ExpiryGroup, response: oneshot::Sender<Result<()>>},
    GetFilters{first: ExpiryGroup, last: ExpiryGroup, response: oneshot::Sender<Result<Vec<FilterID>, ErrorKinds>>},
    GetExpiry{first: ExpiryGroup, last: ExpiryGroup, response: oneshot::Sender<Result<Vec<(FilterID, ExpiryGroup)>, ErrorKinds>>},
    DeleteFilter{id: FilterID, response: oneshot::Sender<Result<()>>},
    FilterSizes{response: oneshot::Sender<HashMap<FilterID, u64>>},
    // FilterPendingCount{response: oneshot::Sender<HashMap<FilterID, u64>>},
    FilterPending{response: oneshot::Sender<HashMap<FilterID, HashSet<Sha256>>>},
    UpdateFileAccess{files: Vec<FileInfo>, response: oneshot::Sender<Result<IngestStatusBundle>>},
    GetFileAccess{id: FilterID, hash: Sha256, response: oneshot::Sender<Result<Option<AccessControl>>>},
    CheckInsertStatus{id: FilterID, file: FileInfo, response: oneshot::Sender<Result<IngestStatus, ErrorKinds>>},
    IngestFile{id: FilterID, file: FileInfo, response: oneshot::Sender<core::result::Result<bool, ErrorKinds>>},
    SelectFileHashes{id: FilterID, file_indices: Vec<u64>, access: HashSet<String>, response: oneshot::Sender<Result<Vec<Sha256>>>},
    GetIngestBatch{id: FilterID, limit: u32, response: oneshot::Sender<Result<Vec<(u64, Sha256)>>>},
    FinishedIngest{id: FilterID, files: Vec<(u64, Sha256)>, response: oneshot::Sender<Result<()>>},
}

impl BSQLCommand {
    pub fn get_id(&self) -> Option<FilterID> {
        match self {
            BSQLCommand::CreateFilter { id, .. } => Some(*id),
            BSQLCommand::GetFilters { .. } => None,
            BSQLCommand::GetExpiry { .. } => None,
            BSQLCommand::DeleteFilter { id, .. } => Some(*id),
            BSQLCommand::FilterSizes { .. } => None,
            // BSQLCommand::FilterPendingCount { .. } => None,
            BSQLCommand::FilterPending { .. } => None,
            BSQLCommand::UpdateFileAccess { .. } => None,
            BSQLCommand::GetFileAccess { id, .. } => Some(*id),
            BSQLCommand::CheckInsertStatus { id, .. } => Some(*id),
            BSQLCommand::IngestFile { id, .. } => Some(*id),
            BSQLCommand::SelectFileHashes { id, .. } => Some(*id),
            BSQLCommand::GetIngestBatch { id, .. } => Some(*id),
            BSQLCommand::FinishedIngest { id, .. } => Some(*id),
        }
    }
}

pub struct BufferedSQLite {
    db: SqlitePool,
    channel: mpsc::Receiver<BSQLCommand>,
    // work_notification: tokio::sync::Notify,
    workers: HashMap<FilterID, mpsc::Sender<BSQLCommand>>,
    filter_sizes: Arc<RwLock<HashMap<FilterID, u64>>>,
    filter_pending: Arc<RwLock<HashMap<FilterID, HashSet<Sha256>>>>,
    _temp_dir: Option<tempfile::TempDir>,
    database_directory: PathBuf,
}


impl BufferedSQLite {

    pub async fn new(database_directory: PathBuf) -> Result<mpsc::Sender<BSQLCommand>> {

        let path = database_directory.join("directory.sqlite");

        let url = if path.is_absolute() {
            format!("sqlite://{}?mode=rwc", path.to_str().unwrap())
        } else {
            format!("sqlite:{}?mode=rwc", path.to_str().unwrap())
        };

        let pool = PoolOptions::new()
            .max_connections(200)
            .acquire_timeout(std::time::Duration::from_secs(600))
            .connect(&url).await?;

        Self::initialize(&pool).await?;

        let (sender, reciver) = mpsc::channel(128);

        let mut db = Self {
            db: pool,
            workers: HashMap::new(),
            channel: reciver,
            _temp_dir: None,
            database_directory: database_directory.clone(),
            filter_sizes: Arc::new(RwLock::new(Default::default())),
            filter_pending: Arc::new(RwLock::new(Default::default()))
        };

        for (name, expiry) in db.get_expiry(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
            db.workers.insert(name, FilterSQLWorker::new(&database_directory, name, expiry, db.filter_sizes.clone(), db.filter_pending.clone()).await?);
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
            expiry INTEGER NOT NULL
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
            BSQLCommand::FilterSizes { response } => { _ = response.send(self.filter_sizes().await); },
            // BSQLCommand::FilterPendingCount { response } => { _ = response.send(self.filter_pending_count().await); },
            BSQLCommand::FilterPending { response } => { _ = response.send(self.filter_pending().await); },
            BSQLCommand::UpdateFileAccess { files, response } => { _ = response.send(self.update_file_access(files).await); },
            other => {
                if let Some(id) = other.get_id() {
                    if let Some(channel) = self.workers.get(&id) {
                        channel.send(other).await?;
                    }

                    // match self.workers.get(id) {
                    //     Some(channel) => channel.send(other),
                    //     None => other.response(),
                    // };
                };
            }
        };
        Ok(())
    }

    pub async fn create_filter(&mut self, name: FilterID, expiry: &ExpiryGroup) -> Result<()> {
        let mut con = self.db.begin().await?;
        info!("Creating filter {name} ({})", name.to_i64());

        sqlx::query(&format!("INSERT INTO filters(id, expiry) VALUES(?, ?) ON CONFLICT DO NOTHING"))
            .bind(name.to_i64())
            .bind(expiry.to_u32())
            .execute(&mut con).await?;

        self.workers.insert(name, FilterSQLWorker::new(&self.database_directory, name, expiry.clone(), self.filter_sizes.clone(), self.filter_pending.clone()).await?);

        con.commit().await?;

        return Ok(())
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>, ErrorKinds> {
        let rows : Vec<(i64, )> = sqlx::query_as("SELECT id FROM filters WHERE ? <= expiry AND expiry <= ?")
            .bind(first.to_u32())
            .bind(last.to_u32())
            .fetch_all(&self.db).await?;
        Ok(rows.into_iter().map(|(id, )|FilterID::from(id)).collect())
    }

    pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>, ErrorKinds> {
        let rows : Vec<(i64, u32)> = sqlx::query_as("SELECT id, expiry FROM filters WHERE ? <= expiry AND expiry <= ?")
            .bind(first.to_u32())
            .bind(last.to_u32())
            .fetch_all(&self.db).await?;
        rows.into_iter().map(|(id, expiry)|Ok((FilterID::from(id), ExpiryGroup::from(expiry)))).collect()
    }

    pub async fn update_file_access(&self, files: Vec<FileInfo>) -> Result<IngestStatusBundle> {
        // Ask all the filters if they know anything about these files
        let mut sub_results = vec![];
        for channel in self.workers.values() {
            let (send, resp) = oneshot::channel();
            channel.send(BSQLCommand::UpdateFileAccess { files: files.clone(), response: send }).await?;
            sub_results.push(resp);
        }

        let mut files: HashSet<Sha256> = files.into_iter().map(|file|file.hash).collect();

        // Collect the results
        let mut unproc_ready = vec![];
        let mut unproc_pending = vec![];
        for resp in sub_results {
            let IngestStatusBundle{ready, pending, ..} = resp.await??;
            unproc_ready.push(ready);
            unproc_pending.push(pending);
        }

        // Select the files that are ready
        let mut output = IngestStatusBundle::default();
        for ready in unproc_ready {
            for hash in ready {
                if files.remove(&hash) {
                    output.ready.push(hash);
                }
            }
        }

        // Select the files that are pending
        for pending in unproc_pending {
            for (hash, id) in pending {
                if files.remove(&hash) {
                    output.pending.insert(hash, id);
                }
            }
        }

        // All the rest are missing
        for hash in files {
            output.missing.push(hash);
        }

        return Ok(output)
    }

    pub async fn delete_filter(&mut self, name: FilterID) -> Result<()> {
        let mut con = self.db.begin().await?;

        sqlx::query(&format!("DELETE FROM filters WHERE name = ?"))
            .bind(name.to_string())
            .execute(&mut con).await?;

        if let Some(channel) = self.workers.remove(&name) {
            let (send, recv) = oneshot::channel();
            channel.send(BSQLCommand::DeleteFilter { id: name, response: send }).await?;
            recv.await??;
        }

        con.commit().await?;

        self.filter_sizes.write().await.remove(&name);
        self.filter_pending.write().await.remove(&name);

        return Ok(())
    }

    pub async fn filter_sizes(&self) -> HashMap<FilterID, u64> {
        self.filter_sizes.read().await.clone()
    }

    pub async fn filter_pending(&self) -> HashMap<FilterID, HashSet<Sha256>> {
        self.filter_pending.read().await.clone()
    }

    // pub async fn filter_pending_count(&self) -> HashMap<FilterID, u64> {
    //     let pending = self.filter_pending.read().await;
    //     return pending.iter().map(|(key, values)|(*key, values.len() as u64)).collect()
    // }


}



struct FilterSQLWorker {
    id: FilterID,
    expiry: ExpiryGroup,
    db: SqlitePool,
    directory: PathBuf,
    channel: mpsc::Receiver<BSQLCommand>,
    filter_sizes: Arc<RwLock<HashMap<FilterID, u64>>>,
    filter_pending: Arc<RwLock<HashMap<FilterID, HashSet<Sha256>>>>,
}


impl FilterSQLWorker {

    pub async fn new(database_directory: &Path, id: FilterID, expiry: ExpiryGroup, filter_sizes: Arc<RwLock<HashMap<FilterID, u64>>>, filter_pending: Arc<RwLock<HashMap<FilterID, HashSet<Sha256>>>>) -> Result<mpsc::Sender<BSQLCommand>> {

        let directory = database_directory.join(id.to_string());
        tokio::fs::create_dir_all(&directory).await?;
        let path = directory.join("filter_files.sqlite");

        let url = if path.is_absolute() {
            format!("sqlite://{}?mode=rwc", path.to_str().unwrap())
        } else {
            format!("sqlite:{}?mode=rwc", path.to_str().unwrap())
        };

        let pool = PoolOptions::new()
            .max_connections(10)
            .acquire_timeout(std::time::Duration::from_secs(600))
            .connect(&url).await?;

        Self::initialize(&pool).await?;

        let (sender, reciver) = mpsc::channel(128);

        let db = Self {
            db: pool,
            directory,
            id,
            expiry,
            // work_notification: Default::default(),
            channel: reciver,
            // _temp_dir: None,
            filter_sizes,
            filter_pending
        };

        {
            let (count, ): (i64, ) = sqlx::query_as(&format!("SELECT COUNT(1) FROM files")).fetch_one(&db.db).await?;
            let mut sizes = db.filter_sizes.write().await;
            sizes.insert(id, count as u64);
        }

        {
            let mut items = vec![];
            let data: Vec<(Vec<u8>, )> = sqlx::query_as(&format!("SELECT hash FROM files WHERE ingested IS FALSE")).fetch_all(&db.db).await?;
            for (row,) in data {
                if let Ok(hash) = Sha256::try_from(&row[..]) {
                    items.push(hash)
                }
            }
            let mut pending = db.filter_pending.write().await;
            pending.insert(id, HashSet::from_iter(items.into_iter()));
        }

        tokio::spawn(db.run());

        Ok(sender)
    }

    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;
        sqlx::query("PRAGMA foreign_keys=ON").execute(&mut con).await?;
        sqlx::query("PRAGMA busy_timeout=600000").execute(&mut *con).await?;

        sqlx::query(&format!("create table if not exists files (
            number INTEGER PRIMARY KEY AUTOINCREMENT,
            hash BLOB NOT NULL UNIQUE,
            access BLOB NOT NULL,
            ingested BOOLEAN DEFAULT FALSE
        )")).execute(&mut con).await?;
        sqlx::query(&format!("create index if not exists ingested ON files(ingested)")).execute(&mut con).await?;
        sqlx::query(&format!("create index if not exists hash ON files(hash)")).execute(&mut con).await?;

        return Ok(())
    }

    async fn run(mut self) {
        loop {
            let message = match self.channel.recv().await {
                Some(message) => message,
                None => break
            };

            match self.handle_message(message).await {
                Ok(stop) => if stop {
                    return;
                },
                Err(err) => {
                    error!("DB error: {err}")
                }
            }
        }
    }

    async fn handle_message(&mut self, message: BSQLCommand) -> Result<bool> {
        match message {
            BSQLCommand::DeleteFilter { response, .. } => { _ = response.send(self.delete_filter().await); return Ok(true); },
            BSQLCommand::GetFileAccess { id, hash, response } => { _ = response.send(self.get_file_access(id, &hash).await); },
            BSQLCommand::CheckInsertStatus { id, file, response } => { _ = response.send(self.check_insert_status(id, &file).await); },
            BSQLCommand::IngestFile { id, file, response } => { _ = response.send(self.ingest_file(id, &file).await); },
            BSQLCommand::SelectFileHashes { id, file_indices, access, response } => { _ = response.send(self.select_file_hashes(id, &file_indices, &access).await); },
            BSQLCommand::GetIngestBatch { id, limit, response } => { _ = response.send(self.get_ingest_batch(id, limit).await); },
            BSQLCommand::FinishedIngest { id, files, response } => { _ = response.send(self.finished_ingest(id, files).await); },
            BSQLCommand::UpdateFileAccess { files, response } => { _ = response.send(self.update_file_access(files).await); },
            _other => {
                error!("Message in wrong place?");
            }
        };
        Ok(false)
    }

    pub async fn delete_filter(&self) -> Result<()> {
        self.db.close().await;
        tokio::fs::remove_dir_all(&self.directory).await?;
        Ok(())
    }

    pub async fn get_file_access(&self, _id: FilterID, hash: &Sha256) -> Result<Option<AccessControl>> {
        let row: Option<(String, )> = query_as(&format!("SELECT access FROM files WHERE hash = ?")).bind(hash.as_bytes()).fetch_optional(&self.db).await?;

        match row {
            Some((row, )) => Ok(Some(AccessControl::from_str(&row)?)),
            None => Ok(None)
        }
    }

    pub async fn check_insert_status(&self, id: FilterID, file: &FileInfo) -> Result<IngestStatus, ErrorKinds> {
        // let mut filters = self.get_expiry(&file.expiry, &ExpiryGroup::max()).await?;
        // filters.sort_unstable_by(|a, b|b.1.cmp(&a.1));

        let mut conn = self.db.acquire().await?;
        let (ingested, ): (bool, ) = match query_as(&format!("SELECT ingested FROM files WHERE hash = ?")).bind(file.hash.as_bytes()).fetch_optional(&mut conn).await? {
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
        let row: Result<Option<(bool, )>, sqlx::Error> = sqlx::query_as(&format!("SELECT ingested FROM files WHERE hash = ?"))
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
        sqlx::query(&format!("INSERT INTO files(hash, access) VALUES(?, ?)"))
            .bind(file.hash.as_bytes())
            .bind(file.access.to_string())
            .execute(&mut conn).await?;

        match self.filter_sizes.write().await.get_mut(&id) {
            Some(size) => { *size += 1; },
            None => {},
        };

        match self.filter_pending.write().await.entry(id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => { entry.get_mut().insert(file.hash.clone()); },
            std::collections::hash_map::Entry::Vacant(entry) => { entry.insert(HashSet::from([file.hash.clone()])); },
        }

        return Ok(false)
    }

    pub async fn update_file_access(&self, files: Vec<FileInfo>) -> Result<IngestStatusBundle> {
        let mut output = IngestStatusBundle::default();
        let mut conn = self.db.acquire().await?;

        for file in files {
            if file.expiry < self.expiry {
                output.missing.push(file.hash);
                continue
            }

            match self._update_file_access(&mut conn, &file).await? {
                IngestStatus::Ready => output.ready.push(file.hash),
                IngestStatus::Pending(id) => { output.pending.insert(file.hash, id); },
                IngestStatus::Missing => output.missing.push(file.hash),
            }
        }
        return Ok(output);
    }

    pub async fn _update_file_access(&self, conn: &mut PoolConnection<sqlx::Sqlite>, file: &FileInfo) -> Result<IngestStatus> {
        let (access_string, ingested): (String, bool) = match query_as(&format!("SELECT access, ingested FROM files WHERE hash = ?")).bind(file.hash.as_bytes()).fetch_optional(&mut *conn).await? {
            Some(row) => row,
            None => return Ok(IngestStatus::Missing)
        };

        let access = AccessControl::from_str(&access_string)?.or(&file.access).simplify();
        let new_string = access.to_string();
        if access_string == new_string {
            if ingested {
                return Ok(IngestStatus::Ready)
            } else {
                return Ok(IngestStatus::Pending(self.id))
            }
        }

        let result = query(&format!("UPDATE files SET access = ? WHERE access = ? AND hash = ?"))
            .bind(new_string)
            .bind(access_string)
            .bind(file.hash.as_bytes())
            .execute(&mut *conn).await?;
        if result.rows_affected() > 0 {
            if ingested {
                return Ok(IngestStatus::Ready)
            } else {
                return Ok(IngestStatus::Pending(self.id))
            }
        }
        return Ok(IngestStatus::Missing)
    }


    pub async fn select_file_hashes(&self, _id: FilterID, indices: &Vec<u64>, view: &HashSet<String>) -> Result<Vec<Sha256>> {
        let mut selected: Vec<Sha256> = vec![];
        let mut conn = self.db.acquire().await?;
        for file_id in indices {
            let row: Option<(String, Vec<u8>)> = sqlx::query_as(&format!("SELECT access, hash FROM files WHERE number = ?"))
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

    pub async fn get_ingest_batch(&self, _id: FilterID, limit: u32) -> Result<Vec<(u64, Sha256)>> {
        let row: Vec<(i64, Vec<u8>)> = sqlx::query_as(&format!("SELECT number, hash FROM files WHERE ingested = FALSE ORDER BY number LIMIT {limit}"))
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
            sqlx::query(&format!("UPDATE files SET ingested = TRUE WHERE number = ?")).bind(*number as i64)
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