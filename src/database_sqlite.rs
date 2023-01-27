use std::collections::{BTreeSet, HashSet};
use std::path::{Path};

use anyhow::{Result, Context};
use futures::{TryStreamExt};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, query_as, Decode, Acquire, Row};
use sqlx::pool::{PoolOptions, PoolConnection};

use crate::access::AccessControl;
use crate::core::{SearchCache, CoreConfig};
use crate::database::{IndexGroup, BlobID, IndexID};
use crate::interface::{SearchRequest, SearchRequestResponse, WorkRequest, WorkPackage, FilterTask, YaraTask, WorkError};
use crate::query::Query;

impl<'r> Decode<'r, sqlx::Sqlite> for IndexGroup {
    fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <&str as Decode<sqlx::Sqlite>>::decode(value)?;
        Ok(Self::from(value))
    }
}

impl sqlx::Type<sqlx::Sqlite> for IndexGroup {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl<'r> Decode<'r, sqlx::Sqlite> for IndexID {
    fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <&str as Decode<sqlx::Sqlite>>::decode(value)?;
        Ok(Self::from(value))
    }
}

impl sqlx::Type<sqlx::Sqlite> for IndexID {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl<'r> Decode<'r, sqlx::Sqlite> for BlobID {
    fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <&str as Decode<sqlx::Sqlite>>::decode(value)?;
        Ok(Self::from(value))
    }
}

impl sqlx::Type<sqlx::Sqlite> for BlobID {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

pub struct SQLiteInterface {
    db: SqlitePool,
    config: CoreConfig,
    work_notification: tokio::sync::Notify,
    _temp_dir: Option<tempfile::TempDir>,
}

#[derive(Serialize, Deserialize, Clone)]
struct IndexEntry {
    group: IndexGroup,
    label: IndexID,
    current_blob: BlobID,
    size_bytes: u64,
    size_entries: u64
}

// impl IndexEntry {
//     fn prepare_key(group: &IndexGroup, label: &IndexID) -> String {
//         format!("{}:{}", group.as_str(), label.as_str())
//     }

//     fn key(&self) -> String {
//         IndexEntry::prepare_key(&self.group, &self.label)
//     }
// }

#[derive(Serialize, Deserialize)]
struct FileEntry {
    access: AccessControl,
    hash: Vec<u8>
}

fn filter_table_name(name: &IndexID) -> String {
    format!("filter_{name}")
}


#[derive(Serialize, Deserialize)]
struct SearchRecord {
    code: String,
    yara_signature: String,
    query: Query,
    access: HashSet<String>,
    errors: Vec<String>,
    // pending_indices: Vec<BlobID>,
    // pending_files: BTreeSet<Vec<u8>>,
    hit_files: BTreeSet<Vec<u8>>,
    truncated: bool,
}


impl SQLiteInterface {
    pub async fn new(config: CoreConfig, url: &str) -> Result<Self> {

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
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&url).await?;

        Self::initialize(&pool).await?;

        Ok(Self {
            db: pool,
            config,
            work_notification: Default::default(),
            _temp_dir: None,
        })
    }

    pub async fn new_temp(config: CoreConfig) -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let path = tempdir.path().join("house.db");

        let mut obj = Self::new(config, path.to_str().unwrap()).await?;
        obj._temp_dir = Some(tempdir);

        Ok(obj)
    }

    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists index_index (
            label TEXT PRIMARY KEY,
            expiry_group TEXT NOT NULL,
            data BLOB NOT NULL
        )")).execute(&mut con).await.context("error creating table index_index")?;

        sqlx::query(&format!("create table if not exists searches (
            code TEXT PRIMARY KEY,
            data BLOB NOT NULL
        )")).execute(&mut con).await.context("error creating table searches")?;

        sqlx::query(&format!("create table if not exists filter_tasks (
            id INTEGER PRIMARY KEY,
            query BLOB NOT NULL,
            search TEXT NOT NULL,
            filter_id TEXT NOT NULL,
            filter_blob TEXT NOT NULL,
            assigned_worker TEXT,
            assigned_time TEXT,
            FOREIGN KEY(search) REFERENCES searches(code),
            UNIQUE (search, filter_blob)
        )
        ")).execute(&mut con).await.context("error creating table filter_tasks")?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS filter_assigned_work_index ON filter_tasks(assigned_worker)")).execute(&mut con).await?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS filter_filter_index ON filter_tasks(filter_blob)")).execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists yara_tasks (
            id INTEGER PRIMARY KEY,
            search TEXT NOT NULL,
            hashes BLOB NOT NULL,
            hash_count INTEGER NOT NULL,
            assigned_worker TEXT,
            assigned_time TEXT,
            FOREIGN KEY(search) REFERENCES searches(code)
        )
        ")).execute(&mut con).await.context("Error creating table yara_tasks")?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS yara_assigned_work_index ON yara_tasks(assigned_worker)")).execute(&mut con).await?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS yara_search_index ON yara_tasks(search)")).execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists garbage (
            blob_id TEXT PRIMARY KEY,
            time TEXT NOT NULL
        )")).execute(&mut con).await.context("error creating table garbage")?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS garbage_blob_id ON garbage(time)")).execute(&mut con).await?;

        return Ok(())
    }

    async fn setup_filter_table(&self, name: &IndexID) -> Result<()> {
        let mut con = self.db.acquire().await?;

        sqlx::query(&format!("create table if not exists {} (
            hash BLOB PRIMARY KEY,
            number INTEGER NOT NULL UNIQUE,
            access BLOB NOT NULL
        )", filter_table_name(name))).execute(&mut con).await?;

        return Ok(())
    }

    // async fn open_index_column(&self, name: &IndexID) -> Result<Option<Collection<FileEntry>>> {
    //     Ok(self.kv.open_collection(&format!("index_{}", name.as_str())).await?)
    // }

    // async fn create_index_column(&self, name: &IndexID) -> Result<Collection<FileEntry>> {
    //     Ok(self.kv.create_collection(&format!("index_{}", name.as_str())).await?)
    // }

    pub async fn update_file_access(&self, hash: &[u8], access: &AccessControl, new_index_group: &IndexGroup) -> Result<bool> {
        let mut conn = self.db.acquire().await?;

        // Get all the groups that expire later than this one
        let list: Vec<(IndexID, IndexGroup)> = query_as("
            SELECT label, expiry_group FROM index_index
            WHERE ? <= expiry_group ORDER BY expiry_group DESC")
            .bind(new_index_group.as_str())
            .fetch_all(&mut conn).await?;

        for (label, _group) in list {
            let table_name = filter_table_name(&label);

            loop {
                // Fetch current data
                let item: Option<(Vec<u8>, )> = sqlx::query_as(&format!(
                    "SELECT access FROM {table_name} WHERE hash = ? LIMIT 1"))
                    .bind(hash).fetch_optional(&mut conn).await?;

                // Update entry
                let (old_access, old_buffer): (AccessControl, Vec<u8>) = match item {
                    Some(item) => (postcard::from_bytes(&item.0)?, item.0),
                    None => break,
                };
                let new_access = old_access.or(access).simplify();
                if new_access == old_access {
                    return Ok(true)
                }

                // Apply update
                let res = sqlx::query(&format!(
                    "UPDATE {table_name} SET access = ? WHERE hash = ? AND access = ?"))
                    .bind(&postcard::to_allocvec(&new_access)?).bind(hash).bind(old_buffer)
                    .execute(&mut conn).await?;
                if res.rows_affected() > 0 {
                    return Ok(true)
                }
            }
        }

        return Ok(false)
    }

    pub async fn select_index_to_grow(&self, index_group: &IndexGroup) -> Result<Option<(IndexID, BlobID, u64)>> {
        let mut conn = self.db.acquire().await?;

        // Get all the groups that expire later than this one
        let list: Vec<(IndexID, Vec<u8>)> = query_as("
            SELECT label, data FROM index_index
            WHERE expiry_group = ?")
            .bind(index_group.as_str())
            .fetch_all(&mut conn).await?;

        // loop until we find an index
        let mut best: Option<(IndexID, IndexEntry)> = None;
        for (key, value) in list {
            let value: IndexEntry = postcard::from_bytes(&value)?;

            if value.size_bytes >= self.config.index_soft_bytes_max {
                debug!("index {} chunk over size {}/{} skipping", key, value.size_bytes, self.config.index_soft_bytes_max);
                continue;
            }

            if value.size_entries >= self.config.index_soft_entries_max {
                debug!("index {} chunk over capacity {}/{} skipping", key, value.size_entries, self.config.index_soft_entries_max);
                continue;
            }

            match best {
                Some(old_best) => {
                    if old_best.1.size_bytes > value.size_bytes {
                        best = Some((key, value))
                    } else {
                        best = Some(old_best)
                    }
                },
                None => best = Some((key, value)),
            }
        }

        match best {
            Some((label, best)) => Ok(Some((label, best.current_blob, best.size_entries))),
            None => Ok(None),
        }
    }

    pub async fn create_index_data(&self, index_group: &IndexGroup, blob_id: BlobID, meta: Vec<(Vec<u8>, AccessControl)>, new_size: u64) -> Result<()> {
        let index_id = IndexID::new();
        debug!("create collection for new index {index_group} {index_id}");
        self.setup_filter_table(&index_id).await?;
        self.update_index_data(index_group, index_id, &blob_id, &blob_id, meta, 0, new_size).await
    }

    pub async fn update_index_data(&self, index_group: &IndexGroup, index_id: IndexID, old_blob_id: &BlobID, blob_id: &BlobID, meta: Vec<(Vec<u8>, AccessControl)>, index_offset: u64, new_size: u64) -> Result<()> {
        debug!("update collection for {index_group} {index_id}");
        let mut conn = self.db.acquire().await?;
        // Open column family for the index meta data
        let table_name = filter_table_name(&index_id);

        // Add all the new file entries
        let new_entries = meta.len() as u64;
        for (index, (hash, access)) in meta.into_iter().enumerate() {
            let index = index as i64 + index_offset as i64;
            sqlx::query(&format!("INSERT INTO {table_name}(hash, number, access) VALUES(?, ?, ?)"))
                .bind(hash).bind(index).bind(&postcard::to_allocvec(&access)?)
                .execute(&mut conn).await?;
        }
        debug!("update {index_group} {index_id} file records updated");

        // Update size in index table
        loop {
            debug!("update {index_group} {index_id} get old index entry");
            // Get
            let old: Option<(Vec<u8>, )> = sqlx::query_as(
                "SELECT data FROM index_index WHERE label = ?")
                .bind(index_id.as_str())
                .fetch_optional(&mut conn).await?;

            // modify
            let entry = match &old {
                Some((old, )) => {
                    debug!("update {index_group} {index_id} update index entry");
                    let old: IndexEntry = postcard::from_bytes(old)?;
                    if &old.current_blob != old_blob_id {
                        return Err(anyhow::anyhow!("Blob replaced"));
                    }
                    let mut entry = old.clone();
                    entry.size_bytes = new_size as u64;
                    entry.size_entries += new_entries;
                    entry.current_blob = blob_id.clone();
                    entry
                },
                None => {
                    debug!("update {index_group} {index_id} define index entry");
                    IndexEntry{
                        current_blob: blob_id.clone(),
                        size_bytes: new_size as u64,
                        size_entries: new_entries,
                        group: index_group.clone(),
                        label: index_id.clone(),
                    }
                },
            };
            let entry = postcard::to_allocvec(&entry)?;

            // write
            let mut trans = conn.begin().await?;
            debug!("update {index_group} {index_id} add old blob to garbage collection");
            self._schedule_blob_gc(&mut trans, old_blob_id, chrono::Utc::now()).await?;
            self._release_blob_gc(&mut trans, blob_id).await?;

            debug!("update {index_group} {index_id} insert new index entry");
            let res = match old {
                Some((old, )) => sqlx::query(
                    "UPDATE index_index SET data = ? WHERE label = ? AND data = ?")
                    .bind(entry).bind(index_id.as_str()).bind(old)
                    .execute(&mut trans).await?,
                None => sqlx::query(
                    "INSERT OR REPLACE INTO index_index(data, label, expiry_group) VALUES(?, ?, ?)")
                    .bind(entry).bind(index_id.as_str()).bind(index_group.as_str())
                    .execute(&mut trans).await?,
            };

            debug!("update {index_group} {index_id} try to finalize");
            if res.rows_affected() > 0 {
                trans.commit().await?;
                return Ok(())
            }
        }
    }

    pub async fn list_indices(&self) -> Result<Vec<(IndexGroup, IndexID)>> {
        let mut conn = self.db.acquire().await?;
        let list: Vec<(IndexGroup, IndexID)> = query_as("
            SELECT expiry_group, label FROM index_index")
            .fetch_all(&mut conn).await?;
        Ok(list)
    }

    pub async fn lease_blob(&self) -> Result<BlobID> {
        let id = BlobID::new();
        self._schedule_blob_gc(&self.db, &id, chrono::Utc::now() + chrono::Duration::days(1)).await?;
        return Ok(id)
    }

    pub async fn list_garbage_blobs(&self) -> Result<Vec<BlobID>> {
        let rows: Vec<(BlobID, )> = sqlx::query_as(
            "SELECT blob_id FROM garbage WHERE time < ?")
            .bind(chrono::Utc::now().to_rfc3339())
            .fetch_all(&self.db).await?;
        return Ok(rows.into_iter().map(|row|row.0).collect())
    }

    pub async fn release_blob(&self, id: BlobID) -> Result<()> {
        self._release_blob_gc(&self.db, &id).await
    }

    async fn _schedule_blob_gc<'e, E>(&self, conn: E, id: &BlobID, when: chrono::DateTime<chrono::Utc>) -> Result<()>
    where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
    {
        sqlx::query("INSERT OR REPLACE INTO garbage(blob_id, time) VALUES(?, ?)")
            .bind(id.as_str())
            .bind(&when.to_rfc3339())
            .execute(conn).await?;
        return Ok(())
    }

    async fn _release_blob_gc<'e, E>(&self, conn: E, id: &BlobID) -> Result<()>
    where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
    {
        sqlx::query("DELETE FROM garbage WHERE blob_id = ?").bind(id.as_str()).execute(conn).await?;
        return Ok(())
    }

    pub async fn release_groups(&self, id: IndexGroup) -> Result<()> {
        // Search for groups older than that id
        let rows: Vec<(Vec<u8>, )> = sqlx::query_as(
            "SELECT data FROM index_index WHERE expiry_group <= ?")
            .bind(id.as_str())
            .fetch_all(&self.db).await?;

        for data in rows {
            let entry = postcard::from_bytes(&data.0)?;
            self._release_group(entry).await?;
        }

        return Ok(())
    }

    async fn _release_group(&self, entry: IndexEntry) -> Result<()> {
        info!("Garbage collecting index for {}", entry.group);

        // In a transaction
        let mut trans = self.db.begin().await?;

        // Delete from index_index
        sqlx::query("DELETE FROM index_index WHERE label = ?")
            .bind(entry.label.as_str())
            .execute(&mut trans).await?;

        // Add blob to GC
        self._schedule_blob_gc(&mut trans, &entry.current_blob, chrono::Utc::now()).await?;

        // Delete group table
        let table_name = filter_table_name(&entry.label);
        sqlx::query(&format!("DROP TABLE IF EXISTS {table_name}")).execute(&mut trans).await?;

        // Commit
        Ok(trans.commit().await?)
    }

    pub async fn initialize_search(&self, req: SearchRequest) -> Result<SearchRequestResponse> {
        // Turn the expiry dates into a group range
        let start = match req.start_date {
            Some(value) => IndexGroup::create(&Some(value)),
            None => IndexGroup::min(),
        };
        let end = match req.end_date {
            Some(value) => IndexGroup::create(&Some(value)),
            None => IndexGroup::max(),
        };

        // Get a list of currently active blobs in the range of groups
        let mut conn = self.db.acquire().await?;
        let pending: Vec<(Vec<u8>, )> = query_as("
            SELECT data FROM index_index
            WHERE ? <= expiry_group AND expiry_group <= ?")
            .bind(start.as_str()).bind(end.as_str())
            .fetch_all(&mut conn).await?;
        let pending: Vec<(BlobID, IndexID)> = pending.into_iter()
            .filter_map(|(data, )|postcard::from_bytes::<IndexEntry>(&data).ok())
            .map(|entry|(entry.current_blob, entry.label))
            .collect();

        // Add operation to the search table
        let code = hex::encode(uuid::Uuid::new_v4().as_bytes());
        sqlx::query("INSERT INTO searches(code, data) VALUES(?, ?)")
            .bind(&code)
            .bind(&postcard::to_allocvec(&SearchRecord{
                code: code.clone(),
                yara_signature: req.yara_signature,
                errors: Default::default(),
                access: req.access,
                query: req.query.clone(),
                hit_files: Default::default(),
                truncated: false,
            })?)
            .execute(&mut conn).await?;

        for (blob, index) in pending {
            sqlx::query(
                "INSERT INTO filter_tasks(search, query, filter_id, filter_blob) VALUES(?,?,?,?)")
                .bind(&code)
                .bind(&postcard::to_allocvec(&req.query)?)
                .bind(index.as_str())
                .bind(blob.as_str())
                .execute(&mut conn).await?;
        }

        self.work_notification.notify_waiters();
        self.search_status(code).await
    }

    pub async fn search_status(&self, code: String) -> Result<SearchRequestResponse> {
        let mut conn = self.db.acquire().await?;
        let data: Option<SearchRecord> = self._get_search(&mut conn, &code).await?;

        let search: SearchRecord = match data {
            Some(search) => search,
            None => return Err(anyhow::anyhow!("Search code not found"))
        };

        let (pending_indices, ): (i64, ) = sqlx::query_as(
            "SELECT COUNT(filter_blob) FROM filter_tasks WHERE search = ?")
            .bind(&code)
            .fetch_one(&mut conn).await.context("Couldn't list filter tasks for search")?;
        let (pending_files, ): (i64, ) = sqlx::query_as(
            "SELECT SUM(hash_count) FROM yara_tasks WHERE search = ?")
            .bind(&code)
            .fetch_one(&mut conn).await.context("Couldn't list yara tasks for search")?;

        Ok(SearchRequestResponse{
            code,
            finished: pending_indices == 0 && pending_files == 0,
            errors: search.errors,
            pending_indices: pending_indices as u64,
            pending_candidates: pending_files as u64,
            hits: search.hit_files.into_iter().map(|hash|hex::encode(hash)).collect(),
            truncated: search.truncated,
        })
    }

    async fn claim_filter_task(&self, id: i64, worker: &String) -> Result<bool> {
        let mut conn = self.db.acquire().await?;
        let req = sqlx::query(
            "UPDATE filter_tasks SET assigned_worker = ?, assigned_time = ?
            WHERE id = ?")
            .bind(worker.as_str())
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(id)
            .execute(&mut conn).await?;
        return Ok(req.rows_affected() > 0)
    }

    async fn claim_yara_task(&self, id: i64, worker: &String) -> Result<bool> {
        let mut conn = self.db.acquire().await?;
        let req = sqlx::query(
            "UPDATE yara_tasks SET assigned_worker = ?, assigned_time = ?
            WHERE id = ?")
            .bind(worker.as_str())
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(id)
            .execute(&mut conn).await?;
        return Ok(req.rows_affected() > 0)
    }

    async fn release_yara_task<'e, E>(&self, conn: E, id: i64) -> Result<bool>
    where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
    {
        let req = sqlx::query(
            "UPDATE yara_tasks SET assigned_worker = NULL, assigned_time = NULL
            WHERE id = ?")
            .bind(id)
            .execute(conn).await?;
        return Ok(req.rows_affected() > 0)
    }

    async fn _get_search<'e, E>(&self, conn: E, code: &String) -> Result<Option<SearchRecord>>
    where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
    {
        let search: Option<(Vec<u8>, )> = sqlx::query_as(
            "SELECT data FROM searches WHERE code = ? LIMIT 1")
            .bind(&code)
            .fetch_optional(conn).await?;

        Ok(match search {
            Some(search) => Some(postcard::from_bytes(&search.0)?),
            None => None,
        })
    }

    async fn get_yara_task(&self, conn: &mut PoolConnection<sqlx::Sqlite>, worker: &String) -> Result<Vec<YaraTask>> {
        let search: Option<(i64, String, Vec<u8>)> = sqlx::query_as(
            "SELECT id, search, hashes FROM yara_tasks WHERE assigned_worker IS NULL LIMIT 1")
            .fetch_optional(&mut *conn).await?;

        let (id, search, hashes) = match search {
            Some(row) => row,
            None => return Ok(vec![]),
        };

        let record = match self._get_search(conn, &search).await? {
            Some(record) => record,
            None => return Ok(vec![])
        };

        let hashes: Vec<Vec<u8>> = postcard::from_bytes(&hashes)?;
        if self.claim_yara_task(id, &worker).await? {
            Ok(vec![YaraTask {
                id,
                search,
                yara_rule: record.yara_signature,
                hashes
            }])
        } else {
            Ok(vec![])
        }
    }


    pub async fn get_work(&self, req: &WorkRequest) -> Result<WorkPackage> {
        let mut conn = self.db.acquire().await?;
        // Look for related filter jobs
        let mut filter_tasks = {
            // : dyn Stream<>
            let mut raw = sqlx::query(
                "SELECT id, query, search, filter_id, filter_blob FROM filter_tasks WHERE assigned_worker IS NULL")
                .fetch(&mut conn);

            let mut filters = vec![];
            while let Some(row) = raw.try_next().await.context("Error listing assignable tasks.")? {
                let id: i64 = row.get(0);
                let query: Vec<u8> = row.get(1);
                let search: String = row.get(2);
                let filter_id: IndexID = row.get(3);
                let filter_blob: BlobID = row.get(4);
                let query: Query = postcard::from_bytes(&query)?;

                if !req.cached_filters.contains(&filter_blob) {
                    continue;
                }

                if self.claim_filter_task(id, &req.worker).await? {
                    filters.push(FilterTask {
                        id,
                        search,
                        filter_id,
                        filter_blob,
                        query
                    });
                }
            }
            filters
        };

        // Add a job from a new filter
        while filter_tasks.len() < 2 {
            let filter: Option<(BlobID, IndexID)> = sqlx::query_as(
                "SELECT filter_blob, filter_id FROM filter_tasks WHERE assigned_worker IS NULL LIMIT 1")
                .fetch_optional(&mut conn).await?;

            let (filter_blob, filter_id) = match filter {
                Some(filter) => filter,
                None => break,
            };

            let raw: Vec<(i64, Vec<u8>, String)> = sqlx::query_as(
                "SELECT id, query, search FROM filter_tasks WHERE assigned_worker IS NULL AND filter_blob = ?")
                .bind(filter_blob.as_str())
                .fetch_all(&mut conn).await?;

            for (id, query, search) in raw {
                let query: Query = postcard::from_bytes(&query)?;

                if self.claim_filter_task(id, &req.worker).await? {
                    filter_tasks.push(FilterTask {
                        id,
                        search,
                        filter_id: filter_id.clone(),
                        filter_blob: filter_blob.clone(),
                        query
                    });
                }
            }
        }

        // Grab some yara jobs
        let yara_tasks = self.get_yara_task(&mut conn, &req.worker).await?;

        if !filter_tasks.is_empty() || !yara_tasks.is_empty() {
            debug!("Returning work bundle with {} filters {} yara batches", filter_tasks.len(), yara_tasks.len());
        }
        return Ok(WorkPackage{
            filter: filter_tasks,
            yara: yara_tasks,
        })
    }

    pub async fn get_work_notification(&self) -> Result<()> {
        Ok(self.work_notification.notified().await)
    }

    pub async fn finish_filter_work(&self, id: i64, code: &String, search_cache: &mut SearchCache, index: IndexID, blob: BlobID, file_ids: Vec<u64>) -> Result<()> {
        let mut conn = self.db.acquire().await?;
        let table_name = filter_table_name(&index);

        let search_access = match &search_cache.access {
            Some(access) => access,
            None => {
                let search_entry = self._get_search(&mut conn, code).await?;
                match search_entry {
                    Some(search_entry) => {
                        search_cache.access = Some(search_entry.access.clone());
                        search_cache.access.as_ref().unwrap()
                    }
                    None => {
                        return Err(anyhow::anyhow!("Terminated search"));
                    }
                }
            },
        };

        // Get the information about the filter entry
        let mut found_hashes = vec![];
        for index in file_ids {
            // Load the hash and access
            let row: Option<(Vec<u8>, Vec<u8>)> = sqlx::query_as(&format!(
                "SELECT hash, access FROM {table_name} WHERE number = ?"))
                .bind(index as i64)
                .fetch_optional(&mut conn).await?;

            let (hash, access) = match row {
                Some((hash, access)) => {
                    if !search_cache.seen.insert(hash.clone()) {
                        continue;
                    }

                    (hash, match postcard::from_bytes::<AccessControl>(&access) {
                        Ok(access) => access,
                        Err(_) => continue,
                    })
                },
                None => continue,
            };

            // Filter on access
            if access.can_access(search_access) {
                found_hashes.push(hash);
            }
        }

        // Create yara job
        for hash_block in found_hashes.chunks(self.config.yara_job_size as usize) {
            let hashes_data = postcard::to_allocvec(&hash_block)?;
            sqlx::query(
                "INSERT INTO yara_tasks(search, hashes, hash_count) VALUES(?,?,?)")
                .bind(&code)
                .bind(hashes_data)
                .bind(hash_block.len() as i64)
                .execute(&mut conn).await?;
        }

        // Drop finished filter job
        sqlx::query(
            "DELETE FROM filter_tasks WHERE id = ?")
            .bind(id)
            .execute(&mut conn).await?;

        self.work_notification.notify_waiters();
        return Ok(())
    }

    pub async fn finish_yara_work(&self, id: i64, code: &String, hashes: Vec<Vec<u8>>) -> Result<()> {
        let mut conn = self.db.acquire().await?;

        // Store confirmed hashes into search object
        loop {
            // Get the old record value
            let buffer: Option<(Vec<u8>, )> = sqlx::query_as(
                "SELECT data FROM searches WHERE code = ? LIMIT 1")
                .bind(&code)
                .fetch_optional(&mut conn).await?;
            let buffer = match buffer {
                Some(buffer) => buffer.0,
                None => return Err(anyhow::anyhow!("results on missing search.")),
            };
            let mut search: SearchRecord = postcard::from_bytes(&buffer)?;

            // Update the record
            let before_count = search.hit_files.len();
            let before_truncated = search.truncated;
            for hash in hashes.iter() {
                if search.hit_files.contains(hash) { continue }
                if search.hit_files.len() >= self.config.max_result_set_size as usize {
                    search.truncated = true;
                    break
                }
                search.hit_files.insert(hash.clone());
            }

            if before_count == search.hit_files.len() && before_truncated == search.truncated {
                break;
            }

            // Apply the update
            let res = sqlx::query(
                "UPDATE searches SET data = ? WHERE code = ? AND data = ?")
                .bind(&postcard::to_allocvec(&search)?)
                .bind(&code)
                .bind(&buffer)
                .execute(&mut conn).await?;
            if res.rows_affected() > 0 {
                break;
            }
        }

        // remove finished yara jobs
        sqlx::query(
            "DELETE FROM yara_tasks WHERE id = ?")
            .bind(id)
            .execute(&mut conn).await?;
        return Ok(())
    }

    pub async fn work_error(&self, err: WorkError) -> Result<()> {
        let mut conn = self.db.acquire().await?;

        let (code, error_message) = match &err {
            WorkError::Yara(id, err) => {
                let (search, ): (String, ) = sqlx::query_as(
                    "SELECT search FROM yara_tasks WHERE id = ? LIMIT 1").bind(id)
                    .fetch_one(&mut conn).await?;
                (search, err)
            },
            WorkError::Filter(id, err) => {
                let (search, ): (String, ) = sqlx::query_as(
                    "SELECT search FROM filter_tasks WHERE id = ? LIMIT 1").bind(id)
                    .fetch_one(&mut conn).await?;
                (search, err)
            },
        };

        loop {
            // Get the old record value
            let buffer: Option<(Vec<u8>, )> = sqlx::query_as(
                "SELECT data FROM searches WHERE code = ? LIMIT 1")
                .bind(&code)
                .fetch_optional(&mut conn).await?;
            let buffer = match buffer {
                Some(buffer) => buffer.0,
                None => return Err(anyhow::anyhow!("results on missing search.")),
            };
            let mut search: SearchRecord = postcard::from_bytes(&buffer)?;

            // Update the record
            search.errors.push(error_message.clone());

            // Apply the update
            let res = sqlx::query(
                "UPDATE searches SET data = ? WHERE code = ? AND data = ?")
                .bind(&postcard::to_allocvec(&search)?)
                .bind(&code)
                .bind(&buffer)
                .execute(&mut conn).await?;
            if res.rows_affected() > 0 {
                break;
            }
        }

        match &err {
            WorkError::Yara(id, _) => {
                sqlx::query("DELETE FROM yara_tasks WHERE id = ?").bind(id).execute(&mut conn).await?;
            },
            WorkError::Filter(id, _) => {
                sqlx::query("DELETE FROM filter_tasks WHERE id = ?").bind(id).execute(&mut conn).await?;
            },
        };

        return Ok(())
    }
}

