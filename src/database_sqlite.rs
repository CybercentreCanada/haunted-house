use std::collections::{BTreeSet, HashSet};
use std::path::{Path};

use anyhow::Result;
use futures::{TryStreamExt};
use log::{debug};
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, query_as, Decode, Acquire, Row};
use sqlx::pool::PoolOptions;

use crate::access::AccessControl;
use crate::core::SearchCache;
use crate::database::{IndexGroup, BlobID, IndexID};
use crate::interface::{SearchRequest, SearchRequestResponse, WorkRequest, WorkPackage, WorkResult, FilterTask, YaraTask};
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
    index_soft_max: u64,
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
    // pending_indices: Vec<BlobID>,
    // pending_files: BTreeSet<Vec<u8>>,
    hit_files: BTreeSet<Vec<u8>>
}


impl SQLiteInterface {
    pub async fn new(index_soft_max: u64, url: &str) -> Result<Self> {

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
            index_soft_max,
            _temp_dir: None,
        })
    }

    pub async fn new_temp(index_soft_max: u64) -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let path = tempdir.path().join("house.db");

        let mut obj = Self::new(index_soft_max, path.to_str().unwrap()).await?;
        obj._temp_dir = Some(tempdir);

        Ok(obj)
    }

    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists index_index (
            label TEXT PRIMARY KEY,
            group TEXT NOT NULL,
            data BLOB NOT NULL
        )")).execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists searches (
            code TEXT PRIMARY KEY,
            data BLOB NOT NULL
        )")).execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists filter_tasks (
            query BLOB NOT NULL,
            search TEXT NOT NULL,
            FOREIGN KEY(search) REFERENCES searches(code),
            filter_id TEXT NOT NULL,
            filter_blob TEXT NOT NULL,
            assigned_worker TEXT,
            assign_time TEXT
        ) PRIMARY KEY(search, filter_blob)
        ")).execute(&mut con).await?;
        sqlx::query(&format!("CREATE INDEX filter_assigned_work_index ON filter_tasks(assigned_worker)")).execute(&mut con).await?;
        sqlx::query(&format!("CREATE INDEX filter_filter_index ON filter_tasks(filter_blob)")).execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists yara_tasks (
            search TEXT NOT NULL,
            FOREIGN KEY(search) REFERENCES searches(code),
            hash BLOB NOT NULL,
            assigned_worker TEXT,
            assign_time TEXT
        ) PRIMARY KEY(search, hash)
        ")).execute(&mut con).await?;

        sqlx::query(&format!("CREATE INDEX yara_assigned_work_index ON yara_tasks(assigned_worker)")).execute(&mut con).await?;
        sqlx::query(&format!("CREATE INDEX yara_search_index ON yara_tasks(search)")).execute(&mut con).await?;

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
            SELECT label, group FROM index_index 
            WHERE ? <= group SORT BY group DEC")
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
            WHERE group = ?")
            .bind(index_group.as_str())
            .fetch_all(&mut conn).await?;

        // loop until we find an index
        let mut best: Option<(IndexID, IndexEntry)> = None;
        for (key, value) in list {
            let value: IndexEntry = postcard::from_bytes(&value)?;

            if value.size_bytes >= self.index_soft_max {
                debug!("index {} chunk over size {}/{} skipping", key, value.size_bytes, self.index_soft_max);
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
        debug!("create {index_group} {index_id} file records updated");

        // Update size in index table
        loop {
            debug!("update {index_group} {index_id} index directory");
            // Get
            let old: Option<(Vec<u8>, )> = sqlx::query_as(
                "SELECT data FROM index_index WHERE label = ?")
                .bind(index_id.as_str())
                .fetch_optional(&mut conn).await?;

            // modify
            let entry = match &old {
                Some((old, )) => {
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
                None => IndexEntry{
                    current_blob: blob_id.clone(),
                    size_bytes: new_size as u64,
                    size_entries: new_entries,
                    group: index_group.clone(),
                    label: index_id.clone(),
                },
            };
            let entry = postcard::to_allocvec(&entry)?;

            // write
            let mut trans = conn.begin().await?;
            sqlx::query("INSERT OR IGNORE INTO garbage(blob_id) VALUES(?)")
                .bind(old_blob_id.as_str())
                .execute(&mut trans).await?;

            let res = match old {
                Some(old) => sqlx::query(
                    "UPDATE index_index SET data = ? WHERE label = ? AND data = ?")
                    .bind(entry).bind(index_id.as_str()).bind(old.0)
                    .execute(&mut trans).await?,
                None => sqlx::query(
                    "UPDATE index_index SET data = ? WHERE label = ?")
                    .bind(entry).bind(index_id.as_str())
                    .execute(&mut trans).await?,
            };
            if res.rows_affected() > 0 {
                trans.commit().await?;
                return Ok(())
            }
        }
    }

    pub async fn list_indices(&self) -> Result<Vec<(IndexGroup, IndexID)>> {
        let mut conn = self.db.acquire().await?;
        let list: Vec<(IndexGroup, IndexID)> = query_as("
            SELECT group, label FROM index_index")
            .fetch_all(&mut conn).await?;
        Ok(list)
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
            WHERE ? <= group AND group <= ?")
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
                access: req.access,
                query: req.query.clone(),
                hit_files: Default::default(),
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

        self.search_status(code).await
    }

    pub async fn search_status(&self, code: String) -> Result<SearchRequestResponse> {
        let mut conn = self.db.acquire().await?;
        let data: Option<(Vec<u8>, )> = sqlx::query_as("SELECT data FROM searches WHERE code = ?")
            .bind(&code)
            .fetch_optional(&mut conn).await?;

        let search: SearchRecord = match data {
            Some((data, )) => postcard::from_bytes(&data)?,
            None => return Err(anyhow::anyhow!("Search code not found"))
        };

        let (pending_indices, ): (i64, ) = sqlx::query_as("SELECT COUNT(filter_blob) FROM filter_tasks WHERE search = ? GROUP BY search")
            .bind(&code)
            .fetch_one(&mut conn).await?;
        let (pending_files, ): (i64, ) = sqlx::query_as("SELECT COUNT(hash) FROM yara_tasks WHERE search = ? GROUP BY search")
            .bind(&code)
            .fetch_one(&mut conn).await?;

        Ok(SearchRequestResponse{
            code,
            finished: pending_indices == 0 && pending_files == 0,
            errors: vec![],
            pending_indices: pending_indices as u64,
            pending_candidates: pending_files as u64,
            hits: search.hit_files.into_iter().map(|hash|hex::encode(hash)).collect(),
        })
    }

    async fn claim_filter_task(&self, search: &String, filter: &BlobID, worker: &String) -> Result<bool> {
        let mut conn = self.db.acquire().await?;
        let req = sqlx::query(
            "UPDATE filter_tasks SET assigned_worker = ?, assigned_time = ? 
            WHERE search = ? AND filter = ?")
            .bind(worker.as_str())
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(search).bind(filter.as_str())
            .execute(&mut conn).await?;
        return Ok(req.rows_affected() > 0)
    }

    async fn claim_yara_task(&self, search: &String, hash: &[u8], worker: &String) -> Result<bool> {
        let mut conn = self.db.acquire().await?;
        let req = sqlx::query(
            "UPDATE yara_tasks SET assigned_worker = ?, assigned_time = ? 
            WHERE search = ? AND hash = ?")
            .bind(worker.as_str())
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(search).bind(hash)
            .execute(&mut conn).await?;
        return Ok(req.rows_affected() > 0)
    }

    async fn get_search(&self, code: &String) -> Result<Option<SearchRecord>> {
        let search: Option<(Vec<u8>, )> = sqlx::query_as(
            "SELECT data FROM searches WHERE assigned_worker IS NULL LIMIT 1")
            .fetch_optional(&self.db).await?;

        Ok(match search {
            Some(search) => Some(postcard::from_bytes(&search.0)?),
            None => None,
        })
    }

    pub async fn get_work(&self, req: WorkRequest) -> Result<WorkPackage> {
        let mut conn = self.db.acquire().await?;
        // Look for related filter jobs
        let mut filter_tasks = {
            // : dyn Stream<> 
            let mut raw = sqlx::query(
                "SELECT query, search, filter_id, filter_blob FROM filter_tasks WHERE assigned_worker IS NULL")
                .fetch(&mut conn);

            let mut filters = vec![];
            while let Some(row) = raw.try_next().await? {
                let query: Vec<u8> = row.get(0);
                let search: String = row.get(1);
                let filter_id: IndexID = row.get(2);
                let filter_blob: BlobID = row.get(3);
                let query: Query = postcard::from_bytes(&query)?;

                if !req.cached_filters.contains(&filter_blob) {
                    continue;
                }

                if self.claim_filter_task(&search, &filter_blob, &req.worker).await? {
                    filters.push(FilterTask {
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
        while filter_tasks.len() < 10 {
            let filter: Option<(BlobID, IndexID)> = sqlx::query_as(
                "SELECT filter_blob, filter_id FROM filter_tasks WHERE assigned_worker IS NULL LIMIT 1")
                .fetch_optional(&mut conn).await?;

            let (filter_blob, filter_id) = match filter {
                Some(filter) => filter,
                None => break,
            };

            let raw: Vec<(Vec<u8>, String)> = sqlx::query_as(
                "SELECT query, search FROM filter_tasks WHERE assigned_worker IS NULL AND filter_blob = ?")
                .bind(filter_blob.as_str())
                .fetch_all(&mut conn).await?;

            for (query, search) in raw {
                let query: Query = postcard::from_bytes(&query)?;

                if self.claim_filter_task(&search, &filter_blob, &req.worker).await? {
                    filter_tasks.push(FilterTask {
                        search,
                        filter_id: filter_id.clone(),
                        filter_blob: filter_blob.clone(),
                        query
                    });
                }
            }
        }
        
        // Grab some yara jobs
        let mut yara_tasks = vec![];
        while yara_tasks.len() < 100 {
            let search: Option<(String, )> = sqlx::query_as(
                "SELECT search FROM yara_tasks WHERE assigned_worker IS NULL LIMIT 1")
                .fetch_optional(&mut conn).await?;

            let search = match search {
                Some(search) => search.0,
                None => break,
            };

            let record = match self.get_search(&search).await? {
                Some(record) => record,
                None => continue
            };

            let raw: Vec<(Vec<u8>, )> = sqlx::query_as(
                "SELECT hash FROM yara_tasks WHERE assigned_worker IS NULL AND search = ? LIMIT 100")
                .bind(&search)
                .fetch_all(&mut conn).await?;

            let mut hashes = vec![];
            for (hash, ) in raw {
                if self.claim_yara_task(&search, &hash, &req.worker).await? {
                    hashes.push(hash)
                }
            }

            if hashes.len() > 0 {
                yara_tasks.push(YaraTask {
                    search,
                    yara_rule: record.yara_signature,
                    hashes
                });
            }
        }

        return Ok(WorkPackage{
            filter: filter_tasks,
            yara: yara_tasks,
        })
    }

    pub async fn finish_filter_work(&self, code: &String, search_cache: &mut SearchCache, index: IndexID, blob: BlobID, file_ids: Vec<u64>) -> Result<()> {
        let mut conn = self.db.acquire().await?;
        let table_name = filter_table_name(&index);

        let search_access = match &search_cache.access {
            Some(access) => access,
            None => {
                let search_entry = self.get_search(code).await?;
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
            if !access.can_access(search_access) {
                continue
            }
            
            // Create yara job
            sqlx::query(
                "INSERT INTO yara_tasks(search, hash) VALUES(?,?)")
                .bind(&code)
                .bind(hash)
                .execute(&mut conn).await?;
        }

        // Drop finished filter job
        sqlx::query(
            "DELETE FROM filter_tasks WHERE search = ? filter_blob = ?")
            .bind(&code)
            .bind(blob.as_str())
            .execute(&mut conn).await?;

        return Ok(())
    }

    pub async fn finish_yara_work(&self, search: &String, hashes: Vec<Vec<u8>>) -> Result<()> {
        todo!();
    }

}

