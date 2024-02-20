use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use assemblyline_markings::classification::ClassificationParser;
use log::{info, error};
use sqlx::{SqlitePool, query_as, query};
use sqlx::pool::{PoolOptions, PoolConnection};
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::access::AccessControl;
use crate::error::ErrorKinds;
use crate::types::{FilterID, FileInfo, ExpiryGroup, Sha256};

use super::database::{IngestStatus, IngestStatusBundle};


type Result<T> = core::result::Result<T, ErrorKinds>;
type Respond<T> = oneshot::Sender<Result<T>>;
type BatchRespond<T> = oneshot::Sender<Vec<oneshot::Receiver<Result<T>>>>;

#[derive(Debug)]
pub enum SQLiteCommand {
    CreateFilter{id: FilterID, expiry: ExpiryGroup, response: Respond<()>},
    GetFilters{first: ExpiryGroup, last: ExpiryGroup, response: Respond<Vec<FilterID>>},
    GetExpiry{first: ExpiryGroup, last: ExpiryGroup, response: Respond<Vec<(FilterID, ExpiryGroup)>>},
    DeleteFilter{id: FilterID, response: Respond<()>},
    FilterSizes{response: oneshot::Sender<HashMap<FilterID, u64>>},
    // FilterPendingCount{response: oneshot::Sender<HashMap<FilterID, u64>>},
    FilterPending{response: oneshot::Sender<HashMap<FilterID, HashSet<Sha256>>>},
    UpdateFileAccess{files: Vec<FileInfo>, response: BatchRespond<IngestStatusBundle>},
    GetFileAccess{id: FilterID, hash: Sha256, response: Respond<Option<(AccessControl, String)>>},
    CheckInsertStatus{files: Vec<(FilterID, FileInfo)>, response: BatchRespond<Vec<(FilterID, FileInfo, IngestStatus)>>},
    IngestFiles{files: Vec<(FilterID, FileInfo)>, response: BatchRespond<Vec<(FilterID, FileInfo)>>},
    SelectFiles{id: FilterID, file_indices: Vec<u64>, access: HashSet<String>, response: Respond<Vec<FileInfo>>},
    GetIngestBatch{id: FilterID, limit: u32, response: Respond<Vec<(u64, Sha256)>>},
    FinishedIngest{id: FilterID, files: Vec<(u64, Sha256)>, response: Respond<f64>},
    AbandonFiles{files: Vec<(FilterID, Sha256)>, response: Respond<()>},
}

#[derive(Debug)]
pub enum FilterCommand {
    DeleteFilter{response: oneshot::Sender<Result<()>>},
    GetFileAccess{hash: Sha256, response: oneshot::Sender<Result<Option<(AccessControl, String)>>>},
    CheckInsertStatus{files: Vec<FileInfo>, response: oneshot::Sender<Result<Vec<(FilterID, FileInfo, IngestStatus)>>>},
    IngestFiles{files: Vec<FileInfo>, response: oneshot::Sender<Result<Vec<(FilterID, FileInfo)>>>},
    SelectFiles{file_indices: Vec<u64>, access: HashSet<String>, response: oneshot::Sender<Result<Vec<FileInfo>>>},
    GetIngestBatch{limit: u32, response: oneshot::Sender<Result<Vec<(u64, Sha256)>>>},
    FinishedIngest{files: Vec<(u64, Sha256)>, response: oneshot::Sender<Result<f64>>},
    UpdateFileAccess{files: Vec<FileInfo>, response: oneshot::Sender<Result<IngestStatusBundle>>},
    AbandonFile{file: Sha256, response: oneshot::Sender<Result<()>>},
}

impl FilterCommand {
    pub fn error(self, error: ErrorKinds) {
        match self {
            FilterCommand::DeleteFilter { response } => _ = response.send(Err(error)),
            FilterCommand::GetFileAccess { response, .. } => _ = response.send(Err(error)),
            FilterCommand::CheckInsertStatus { response, .. } => _ = response.send(Err(error)),
            FilterCommand::IngestFiles { response, .. } => _ = response.send(Err(error)),
            FilterCommand::SelectFiles { response, .. } => _ = response.send(Err(error)),
            FilterCommand::GetIngestBatch { response, .. } => _ = response.send(Err(error)),
            FilterCommand::FinishedIngest { response, .. } => _ = response.send(Err(error)),
            FilterCommand::UpdateFileAccess { response, .. } => _ = response.send(Err(error)),
            FilterCommand::AbandonFile { response, .. } => _ = response.send(Err(error)),
        }
    }
}


pub struct BufferedSQLite {
    db: SqlitePool,
    channel: mpsc::Receiver<SQLiteCommand>,
    // work_notification: tokio::sync::Notify,
    workers: HashMap<FilterID, mpsc::Sender<FilterCommand>>,
    filter_sizes: Arc<RwLock<HashMap<FilterID, u64>>>,
    filter_pending: Arc<RwLock<HashMap<FilterID, HashSet<Sha256>>>>,
    _temp_dir: Option<tempfile::TempDir>,
    database_directory: PathBuf,
    ce: Arc<ClassificationParser>,
}


impl BufferedSQLite {

    pub async fn start(database_directory: PathBuf, ce: Arc<ClassificationParser>) -> Result<mpsc::Sender<SQLiteCommand>> {

        let path = database_directory.join("directory.sqlite");

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

        let mut db = Self {
            db: pool,
            workers: HashMap::new(),
            channel: reciver,
            _temp_dir: None,
            database_directory: database_directory.clone(),
            filter_sizes: Arc::new(RwLock::new(Default::default())),
            filter_pending: Arc::new(RwLock::new(Default::default())),
            ce: ce.clone()
        };

        for (name, expiry) in db.get_expiry(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
            db.workers.insert(name, FilterSQLWorker::start(&database_directory, ce.clone(), name, expiry, db.filter_sizes.clone(), db.filter_pending.clone()).await?);
        }

        tokio::spawn(db.run());

        Ok(sender)
    }

    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;
        sqlx::query("PRAGMA foreign_keys=ON").execute(&mut con).await?;
        sqlx::query("PRAGMA busy_timeout=600000").execute(&mut *con).await?;

        sqlx::query("create table if not exists filters (
            id INTEGER PRIMARY KEY,
            expiry INTEGER NOT NULL
        )").execute(&mut con).await?;
        sqlx::query("create index if not exists expiry ON filters(expiry)").execute(&mut con).await?;

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

    async fn handle_message(&mut self, message: SQLiteCommand) -> Result<()> {
        match message {
            SQLiteCommand::CreateFilter { id, expiry, response } => { _ = response.send(self.create_filter(id, &expiry).await); },
            SQLiteCommand::GetFilters { first, last, response } => { _ = response.send(self.get_filters(&first, &last).await); },
            SQLiteCommand::GetExpiry { first, last, response } => { _ = response.send(self.get_expiry(&first, &last).await); },
            SQLiteCommand::DeleteFilter { id, response } => { _ = response.send(self.delete_filter(id).await); },
            SQLiteCommand::FilterSizes { response } => { _ = response.send(self.filter_sizes().await); },
            // BSQLCommand::FilterPendingCount { response } => { _ = response.send(self.filter_pending_count().await); },
            SQLiteCommand::FilterPending { response } => { _ = response.send(self.filter_pending().await); },
            SQLiteCommand::UpdateFileAccess { files, response } => {
                // Ask all the filters if they know anything about these files
                let mut sub_results = vec![];
                for channel in self.workers.values() {
                    let (send, resp) = oneshot::channel();
                    channel.send(FilterCommand::UpdateFileAccess { files: files.clone(), response: send }).await?;
                    sub_results.push(resp);
                }
                _ = response.send(sub_results);
            },
            SQLiteCommand::IngestFiles{ files, response } => {
                let mut batches: HashMap<FilterID, Vec<FileInfo>> = Default::default();
                for (filter, info) in files {
                    match batches.entry(filter) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => entry.get_mut().push(info),
                        std::collections::hash_map::Entry::Vacant(entry) => { entry.insert(vec![info]); },
                    }
                }

                let mut collectors = vec![];
                for (filter, batch) in batches {
                    if let Some(channel) = self.workers.get(&filter) {
                        let (send, recv) = oneshot::channel();
                        channel.send(FilterCommand::IngestFiles { files: batch, response: send }).await?;
                        collectors.push(recv)
                    }
                }

                _ = response.send(collectors);
            },
            SQLiteCommand::CheckInsertStatus{ files, response } => {
                let mut batches: HashMap<FilterID, Vec<FileInfo>> = Default::default();
                for (filter, info) in files {
                    match batches.entry(filter) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => entry.get_mut().push(info),
                        std::collections::hash_map::Entry::Vacant(entry) => { entry.insert(vec![info]); },
                    }
                }

                let mut collectors = vec![];
                for (filter, batch) in batches {
                    if let Some(channel) = self.workers.get(&filter) {
                        let (send, recv) = oneshot::channel();
                        channel.send(FilterCommand::CheckInsertStatus { files: batch, response: send }).await?;
                        collectors.push(recv)
                    }
                }

                _ = response.send(collectors);
            }
            SQLiteCommand::GetFileAccess { id, hash, response } => {
                self.send_filter(id, FilterCommand::GetFileAccess { hash, response }).await
            },
            SQLiteCommand::SelectFiles { id, file_indices, access, response } => {
                self.send_filter(id, FilterCommand::SelectFiles { file_indices, access, response }).await
            },
            SQLiteCommand::GetIngestBatch { id, limit, response } => {
                self.send_filter(id, FilterCommand::GetIngestBatch { limit, response }).await
            },
            SQLiteCommand::FinishedIngest { id, files, response } => {
                self.send_filter(id, FilterCommand::FinishedIngest { files, response }).await
            },
            SQLiteCommand::AbandonFiles {files, response } => {
                let mut collectors = vec![];
                for (filter, file) in files {
                    if let Some(channel) = self.workers.get(&filter) {
                        let (send, recv) = oneshot::channel();
                        channel.send(FilterCommand::AbandonFile { file, response: send }).await?;
                        collectors.push(recv)
                    }
                }

                for x in collectors {
                    x.await??;
                }
                _ = response.send(Ok(()));
            },
        };
        Ok(())
    }

    async fn send_filter(&self, id: FilterID, command: FilterCommand) {
        match self.workers.get(&id) {
            Some(channel) => if let Err(err) = channel.send(command).await {
                err.0.error(ErrorKinds::FilterUnknown(id));
            },
            None => command.error(ErrorKinds::FilterUnknown(id)),
        }
    }

    pub async fn create_filter(&mut self, name: FilterID, expiry: &ExpiryGroup) -> Result<()> {
        let mut con = self.db.begin().await?;
        info!("Creating filter {name} ({})", name.to_i64());

        sqlx::query("INSERT INTO filters(id, expiry) VALUES(?, ?) ON CONFLICT DO NOTHING")
            .bind(name.to_i64())
            .bind(expiry.to_u32())
            .execute(&mut con).await?;

        self.workers.insert(name, FilterSQLWorker::start(&self.database_directory, self.ce.clone(), name, expiry.clone(), self.filter_sizes.clone(), self.filter_pending.clone()).await?);

        con.commit().await?;

        return Ok(())
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>> {
        let rows : Vec<(i64, )> = sqlx::query_as("SELECT id FROM filters WHERE ? <= expiry AND expiry <= ?")
            .bind(first.to_u32())
            .bind(last.to_u32())
            .fetch_all(&self.db).await?;
        Ok(rows.into_iter().map(|(id, )|FilterID::from(id)).collect())
    }

    pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>> {
        let rows : Vec<(i64, u32)> = sqlx::query_as("SELECT id, expiry FROM filters WHERE ? <= expiry AND expiry <= ?")
            .bind(first.to_u32())
            .bind(last.to_u32())
            .fetch_all(&self.db).await?;
        rows.into_iter().map(|(id, expiry)|Ok((FilterID::from(id), ExpiryGroup::from(expiry)))).collect()
    }

    pub async fn delete_filter(&mut self, id: FilterID) -> Result<()> {
        let mut con = self.db.begin().await?;

        sqlx::query("DELETE FROM filters WHERE id = ?")
            .bind(id.to_string())
            .execute(&mut con).await?;

        if let Some(channel) = self.workers.remove(&id) {
            let (send, recv) = oneshot::channel();
            channel.send(FilterCommand::DeleteFilter { response: send }).await?;
            recv.await??;
        }

        con.commit().await?;

        self.filter_sizes.write().await.remove(&id);
        self.filter_pending.write().await.remove(&id);

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
    channel: mpsc::Receiver<FilterCommand>,
    filter_sizes: Arc<RwLock<HashMap<FilterID, u64>>>,
    filter_pending: Arc<RwLock<HashMap<FilterID, HashSet<Sha256>>>>,
    ce: Arc<ClassificationParser>,
}


impl FilterSQLWorker {

    pub async fn start(database_directory: &Path, ce: Arc<ClassificationParser>, id: FilterID, expiry: ExpiryGroup, filter_sizes: Arc<RwLock<HashMap<FilterID, u64>>>, filter_pending: Arc<RwLock<HashMap<FilterID, HashSet<Sha256>>>>) -> Result<mpsc::Sender<FilterCommand>> {

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
            filter_pending,
            ce,
        };

        {
            let (count, ): (i64, ) = sqlx::query_as("SELECT COUNT(1) FROM files").fetch_one(&db.db).await?;
            let mut sizes = db.filter_sizes.write().await;
            sizes.insert(id, count as u64);
        }

        {
            let mut items = vec![];
            let data: Vec<(Vec<u8>, )> = sqlx::query_as("SELECT hash FROM files WHERE ingested IS FALSE").fetch_all(&db.db).await?;
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

        sqlx::query("create table if not exists files (
            number INTEGER PRIMARY KEY AUTOINCREMENT,
            hash BLOB NOT NULL UNIQUE,
            access BLOB NOT NULL,
            access_string BLOB NOT NULL,
            ingested BOOLEAN DEFAULT FALSE
        )").execute(&mut con).await?;
        sqlx::query("create index if not exists ingested ON files(ingested)").execute(&mut con).await?;
        sqlx::query("create index if not exists hash ON files(hash)").execute(&mut con).await?;

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

    async fn handle_message(&mut self, message: FilterCommand) -> Result<bool> {
        match message {
            FilterCommand::DeleteFilter { response, .. } => { _ = response.send(self.delete_filter().await); return Ok(true); },
            FilterCommand::GetFileAccess { hash, response } => { _ = response.send(self.get_file_access(&hash).await); },
            FilterCommand::CheckInsertStatus {files, response } => { _ = response.send(self.check_insert_status(files).await); },
            FilterCommand::IngestFiles { files, response } => { _ = response.send(self.ingest_files(files).await); },
            FilterCommand::SelectFiles { file_indices, access, response } => { _ = response.send(self.select_files(&file_indices, &access).await); },
            FilterCommand::GetIngestBatch { limit, response } => { _ = response.send(self.get_ingest_batch(limit).await); },
            FilterCommand::FinishedIngest { files, response } => { _ = response.send(self.finished_ingest(files).await); },
            FilterCommand::UpdateFileAccess { files, response } => { _ = response.send(self.update_file_access(files).await); },
            FilterCommand::AbandonFile { file, response } => { _ = response.send(self.abandon_file(file).await); },
        };
        Ok(false)
    }

    pub async fn delete_filter(&self) -> Result<()> {
        self.db.close().await;
        tokio::fs::remove_dir_all(&self.directory).await?;
        Ok(())
    }

    pub async fn get_file_access(&self, hash: &Sha256) -> Result<Option<(AccessControl, String)>> {
        let row: Option<(String, String)> = query_as("SELECT access FROM files WHERE hash = ?").bind(hash.as_bytes()).fetch_optional(&self.db).await?;

        match row {
            Some((access, access_string)) => Ok(Some((AccessControl::from_str(&access)?, access_string))),
            None => Ok(None)
        }
    }

    pub async fn check_insert_status(&self, files: Vec<FileInfo>) -> Result<Vec<(FilterID, FileInfo, IngestStatus)>> {
        let mut conn = self.db.acquire().await?;
        let mut output = vec![];
        for file in files {
            let (ingested, ): (bool, ) = match query_as("SELECT ingested FROM files WHERE hash = ?").bind(file.hash.as_bytes()).fetch_optional(&mut conn).await? {
                Some(row) => row,
                None => {
                    output.push((self.id, file, IngestStatus::Missing));
                    continue
                }
            };

            if ingested {
                output.push((self.id, file, IngestStatus::Ready));
            } else {
                output.push((self.id, file, IngestStatus::Pending(self.id)));
            }
        }
        return Ok(output)
    }

    pub async fn ingest_files(&self, files: Vec<FileInfo>) -> Result<Vec<(FilterID, FileInfo)>> {
        let mut conn = self.db.begin().await?;
        let mut completed = vec![];
        for file in files {
            // Check if the file is already ingested
            let row: Option<(bool, )> = sqlx::query_as("SELECT ingested FROM files WHERE hash = ?")
                .bind(file.hash.as_bytes()).fetch_optional(&mut conn).await?;
            if let Some((ingested, )) = row {
                if ingested {
                    completed.push((self.id, file));
                }
                continue
            }

            // Insert the new file if it is not there
            sqlx::query("INSERT INTO files(hash, access, access_string) VALUES(?, ?, ?)")
                .bind(file.hash.as_bytes())
                .bind(file.access.to_string())
                .bind(file.access_string.to_string())
                .execute(&mut conn).await?;

            if let Some(size) = self.filter_sizes.write().await.get_mut(&self.id) {
                *size += 1;
            };

            match self.filter_pending.write().await.entry(self.id) {
                std::collections::hash_map::Entry::Occupied(mut entry) => { entry.get_mut().insert(file.hash.clone()); },
                std::collections::hash_map::Entry::Vacant(entry) => { entry.insert(HashSet::from([file.hash.clone()])); },
            }
        }

        conn.commit().await?;
        return Ok(completed)
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
        let (raw_access, old_access_string, ingested): (String, String, bool) = match query_as("SELECT access, access_string, ingested FROM files WHERE hash = ?").bind(file.hash.as_bytes()).fetch_optional(&mut *conn).await? {
            Some(row) => row,
            None => return Ok(IngestStatus::Missing)
        };

        let access = AccessControl::from_str(&raw_access)?.or(&file.access).simplify();
        let new_access = access.to_string();
        if raw_access == new_access {
            if ingested {
                return Ok(IngestStatus::Ready)
            } else {
                return Ok(IngestStatus::Pending(self.id))
            }
        }
        let new_access_string = self.ce.min_classification(&old_access_string, &file.access_string, false)?;

        let result = query("UPDATE files SET access = ?, access_string = ? WHERE access = ? AND hash = ?")
            .bind(new_access)
            .bind(new_access_string)
            .bind(raw_access)
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


    pub async fn select_files(&self, indices: &Vec<u64>, view: &HashSet<String>) -> Result<Vec<FileInfo>> {
        let mut selected: Vec<FileInfo> = vec![];
        let mut conn = self.db.acquire().await?;
        for file_id in indices {
            let row: Option<(String, String, Vec<u8>)> = sqlx::query_as("SELECT access, access_string, hash FROM files WHERE number = ?")
                .bind(*file_id as i64).fetch_optional(&mut conn).await?;
            if let Some((access, access_string, hash)) = row {
                let access = AccessControl::from_str(&access)?;
                if access.can_access(view) {
                    selected.push(FileInfo {
                        hash: Sha256::try_from(&hash[..])?,
                        access,
                        access_string,
                        expiry: self.expiry,
                    });
                }
            }
        }
        info!("File selection complete [Filter {}]: {} input, {} selected", self.id, view.len(), selected.len());
        return Ok(selected)
    }

    pub async fn get_ingest_batch(&self, limit: u32) -> Result<Vec<(u64, Sha256)>> {
        let row: Vec<(i64, Vec<u8>)> = sqlx::query_as(&format!("SELECT number, hash FROM files WHERE ingested = FALSE ORDER BY number LIMIT {limit}"))
        .fetch_all(&self.db).await?;

        let mut output = vec![];
        for (id, hash) in row {
            output.push((id as u64, Sha256::try_from(&hash[..])?));
        }
        return Ok(output)
    }

    pub async fn finished_ingest(&self, files: Vec<(u64, Sha256)>) -> Result<f64> {
        let stamp = std::time::Instant::now();
        let mut transaction = self.db.begin().await?;
        for (number, _) in &files {
            sqlx::query("UPDATE files SET ingested = TRUE WHERE number = ?").bind(*number as i64).execute(&mut transaction).await?;
        }
        let mut pending = self.filter_pending.write().await;
        if let Some(pending) = pending.get_mut(&self.id) {
            for (_, hash) in files {
                pending.remove(&hash);
            }
        }
        transaction.commit().await?;
        return Ok(stamp.elapsed().as_secs_f64())
    }

    pub async fn abandon_file(&self, file: Sha256) -> Result<()> {
        let mut transaction = self.db.begin().await?;
        sqlx::query("DELETE FROM files WHERE sha256 = ?").bind(file.as_bytes()).execute(&mut transaction).await?;
        let mut pending = self.filter_pending.write().await;
        if let Some(pending) = pending.get_mut(&self.id) {
            pending.remove(&file);
        }
        transaction.commit().await?;
        return Ok(())
    }
}