use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet, HashMap};
use std::path::{Path};

use anyhow::{Result, Context};
use log::{info, error, warn};
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, query_as, Decode, Acquire, Sqlite, Encode};
use sqlx::pool::{PoolOptions, PoolConnection};

use crate::access::AccessControl;
use crate::bloom::SimpleFilter;
use crate::core::{CoreConfig};
use crate::database::{IndexGroup, SearchStage, IndexStatus};
use crate::interface::{SearchRequest, SearchRequestResponse, InternalSearchStatus, WorkRequest, WorkPackage, YaraTask, WorkError};
use crate::query::Query;

// impl<'r> Decode<'r, sqlx::Sqlite> for IndexGroup {
//     fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
//         let value = <&str as Decode<sqlx::Sqlite>>::decode(value)?;
//         Ok(Self::from(value))
//     }
// }

// impl sqlx::Type<sqlx::Sqlite> for IndexGroup {
//     fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
//         <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
//     }
// }

// impl<'r> Decode<'r, sqlx::Sqlite> for IndexID {
//     fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
//         let value = <&str as Decode<sqlx::Sqlite>>::decode(value)?;
//         Ok(Self::from(value))
//     }
// }

// impl sqlx::Type<sqlx::Sqlite> for IndexID {
//     fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
//         <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
//     }
// }

impl<'r> Encode<'r, sqlx::Sqlite> for SearchStage {
    fn encode_by_ref(&self, buf: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'r>>) -> sqlx::encode::IsNull {
        self.to_string().encode_by_ref(buf)
    }
}

impl<'r> Decode<'r, sqlx::Sqlite> for SearchStage {
    fn decode(value: <sqlx::Sqlite as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <&str as Decode<sqlx::Sqlite>>::decode(value)?;
        Ok(Self::from(value))
    }
}

impl sqlx::Type<sqlx::Sqlite> for SearchStage {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}



#[derive(Serialize, Deserialize)]
struct SearchRecord {
    code: String,
    group: String,
    yara_signature: String,
    query: Query,
    view: AccessControl,
    access: HashSet<String>,
    errors: Vec<String>,
    filtered_files: u64,
    suspect_files: u64,
    hit_files: BTreeSet<Vec<u8>>,
    truncated: bool,
    start: IndexGroup,
    end: IndexGroup,
}



pub struct SQLiteInterface {
    db: SqlitePool,
    config: CoreConfig,
    work_notification: tokio::sync::Notify,
    _temp_dir: Option<tempfile::TempDir>,
    cant_split: std::sync::Mutex<std::cell::RefCell<HashSet<i64>>>,
}

// #[derive(Serialize, Deserialize, Clone)]
// struct IndexEntry {
//     group: IndexGroup,
//     label: IndexID,
//     current_blob: BlobID,
//     size_bytes: u64,
//     size_entries: u64
// }

// // impl IndexEntry {
// //     fn prepare_key(group: &IndexGroup, label: &IndexID) -> String {
// //         format!("{}:{}", group.as_str(), label.as_str())
// //     }

// //     fn key(&self) -> String {
// //         IndexEntry::prepare_key(&self.group, &self.label)
// //     }
// // }

// #[derive(Serialize, Deserialize)]
// struct FileEntry {
//     access: AccessControl,
//     hash: Vec<u8>
// }

fn file_table_name(name: &IndexGroup) -> String {
    format!("file_{name}")
}

fn filter_table_name(name: &IndexGroup) -> String {
    format!("filter_{name}")
}


const GROUP_SIZE: usize = 128;

impl SQLiteInterface {
    pub async fn new(config: CoreConfig, url: &str) -> Result<Self> {

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
            config,
            work_notification: Default::default(),
            _temp_dir: None,
            cant_split: std::sync::Mutex::new(RefCell::new(Default::default()))
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
        sqlx::query("PRAGMA foreign_keys=ON").execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists searches (
            code TEXT PRIMARY KEY,
            stage TEXT NOT NULL,
            data BLOB NOT NULL,
            search_group TEXT,
            start_time TEXT
        )")).execute(&mut con).await.context("error creating table searches")?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS searches_group_start ON searches(search_group, start_time)")).execute(&mut con).await?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS searches_stage ON searches(stage)")).execute(&mut con).await?;

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

    async fn setup_filter_table(&self, name: &IndexGroup) -> Result<()> {
        let mut con = self.db.acquire().await?;
        let mut trans = con.begin().await?;

        let file_table = file_table_name(name);
        let filter_table = filter_table_name(name);

        sqlx::query(&format!("create table if not exists {filter_table} (
            id INTEGER PRIMARY KEY,
            leaves BOOLEAN,
            block INTEGER,
            filter BLOB NOT NULL,
            kind TEXT NOT NULL,
            FOREIGN KEY(block) REFERENCES {filter_table}(id)
        )")).execute(&mut trans).await.context("Create filter table")?;

        sqlx::query(&format!("create table if not exists {file_table} (
            hash BLOB PRIMARY KEY,
            access BLOB NOT NULL,
            block INTEGER NOT NULL,
            filter BLOB NOT NULL,
            kind TEXT NOT NULL,
            FOREIGN KEY(block) REFERENCES {filter_table}(id)
        )")).execute(&mut trans).await.context("Create file table")?;

        trans.commit().await?;
        return Ok(())
    }

    pub async fn list_indices(&self) -> Result<Vec<IndexGroup>> {
        // Get tables
        let tables: Vec<(String, )> = sqlx::query_as("SELECT name FROM sqlite_schema WHERE type='table'")
            .fetch_all(&self.db).await?;

        // Figure out which ones are index groups
        let mut file = HashSet::new();
        let mut filter = HashSet::new();
        for (name, ) in tables {
            if name.starts_with("file_") {
                file.insert(name[5..].to_owned());
            } else if name.starts_with("filter_") {
                filter.insert(name[7..].to_owned());
            }
        }

        let groups: Vec<IndexGroup> = file.intersection(&filter)
            .map(|date| IndexGroup::from(date))
            .collect();
        return Ok(groups)
    }

    pub async fn status(&self) -> Result<IndexStatus> {
        let tables = self.list_indices().await?;
        let mut conn = self.db.acquire().await?;

        let mut sizes: HashMap<String, u64> = Default::default();
        let mut kinds: HashMap<String, u64> = Default::default();

        for group in tables {
            let file_table = file_table_name(&group);
            // let (count, ): (i64, ) = sqlx::query_as(&format!(
            //     "SELECT COUNT(1) FROM {file_table}"))
            //     .fetch_one(&mut conn).await?;
            // sizes.insert(group.to_string(), count as u64);

            let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
                "SELECT kind, COUNT(1) FROM {file_table} GROUP BY kind"))
                .fetch_all(&mut conn).await?;

            for (kind, size) in rows {
                sizes.insert(group.to_string(), *sizes.get(&group.to_string()).unwrap_or(&0) + size as u64);
                kinds.insert(kind.to_string(), *kinds.get(&kind.to_string()).unwrap_or(&0) + size as u64);
            }
        }

        Ok(IndexStatus{
            group_files: sizes,
            filter_sizes: kinds
        })
    }

    pub async fn update_file_access(&self, hash: &[u8], access: &AccessControl, new_index_group: &IndexGroup) -> Result<bool> {
        let mut conn = self.db.acquire().await?;

        // Get all the groups that expire later than this one
        let indices = self.list_indices().await?;
        let mut indices: Vec<IndexGroup> = indices.into_iter()
            .filter(|index| index >= new_index_group)
            .collect();
        indices.sort_unstable();

        while let Some(group) = indices.pop() {
            let file_table = file_table_name(&group);

            loop {
                // Fetch current data
                let item: Option<(String, )> = sqlx::query_as(&format!(
                    "SELECT access FROM {file_table} WHERE hash = ? LIMIT 1"))
                    .bind(hash).fetch_optional(&mut conn).await?;

                // Update entry
                let (old_access, old_buffer): (AccessControl, String) = match item {
                    Some(item) => (item.0.parse()?, item.0),
                    None => break,
                };
                let new_access = old_access.or(access).simplify();
                if new_access == old_access {
                    return Ok(true)
                }

                // Apply update
                let res = sqlx::query(&format!(
                    "UPDATE {file_table} SET access = ? WHERE hash = ? AND access = ?"))
                    .bind(new_access.to_string()).bind(hash).bind(old_buffer)
                    .execute(&mut conn).await?;
                if res.rows_affected() > 0 {
                    return Ok(true)
                }
            }
        }

        return Ok(false)
    }

    pub async fn insert_file(&self, hash: &[u8], access: &AccessControl, index_group: &IndexGroup, filter: &SimpleFilter) -> Result<()> {
        // Setup tables to insert to
        self.setup_filter_table(index_group).await.context("setup_filter_table")?;
        let mut conn = self.db.acquire().await?;
        let file_table = file_table_name(index_group);

        let mut stack = loop {
            // Get the stack down to the group we want to insert into
            let stack = self._find_insert_group(&mut conn, index_group, filter).await.context("_find_insert_group")?;

            // Expand the items in the stack down to the insertion point
            if !self._expand_filters(&mut conn, index_group, filter, &stack).await.context("expand_filters")? {
                continue
            }

            // Insert file entry
            let result = sqlx::query(&format!("INSERT INTO {file_table}(hash, access, block, filter, kind) VALUES(?, ?, ?, ?, ?)"))
                .bind(hash)
                .bind(access.to_string())
                .bind(stack.last().unwrap().0)
                .bind(filter.to_buffer()?)
                .bind(filter.kind())
                .execute(&mut conn).await;
            match result {
                Ok(_) => break stack,
                Err(err) => {
                    error!("Could not insert file: {err}");
                },
            };
        };

        // Check each group (bottom up) to see if it needs to be split
        while let Some((group_id, leaf, _mask)) = stack.pop() {
            if leaf {
                let count = self._count_files_in_group(&mut conn, index_group, group_id).await?;
                if count > GROUP_SIZE {
                    self._split_leaf_group(&mut conn, index_group, group_id).await?;
                }
            } else {
                let count = self._count_groups_in_group(&mut conn, index_group, group_id).await?;
                if count > GROUP_SIZE {
                    self._split_inner_group(&mut conn, index_group, group_id).await?;
                }
            }
        }
        return Ok(())
    }

    async fn _find_roots(&self, conn: &mut PoolConnection<Sqlite>, index_group: &IndexGroup, filter: &SimpleFilter) -> Result<Vec<(i64, bool, SimpleFilter)>> {
        let filter_table = filter_table_name(index_group);

        // Load the root group
        let mut rows: Vec<(i64, bool, String, Vec<u8>)> = query_as(&format!("
            SELECT id, leaves, kind, filter FROM {filter_table}
            WHERE kind = ? AND block IS NULL"))
            .bind(filter.kind())
            .fetch_all(&mut *conn).await?;

        // Try to atomically create it
        if rows.is_empty() {
            sqlx::query(&format!("INSERT INTO {filter_table}(leaves, block, filter, kind) VALUES(TRUE, NULL, ?, ?)"))
                .bind(filter.to_buffer()?)
                .bind(filter.kind())
                .execute(&mut *conn).await?;

            rows = query_as(&format!("
            SELECT id, leaves, kind, filter FROM {filter_table}
            WHERE kind = ? AND block IS NULL"))
            .bind(filter.kind())
            .fetch_all(&mut *conn).await?;
        }

        let mut output = vec![];
        for (id, leaves, kind, filter) in rows {
            let filter = match SimpleFilter::load(&kind, &filter) {
                Ok(filter) => filter,
                Err(err) => {
                    error!("Couldn't load filter {id}: {err}");
                    continue
                },
            };
            output.push((id, leaves, filter))
        }
        Ok(output)
    }

    async fn _find_all_roots(&self, conn: &mut PoolConnection<Sqlite>, index_group: &IndexGroup) -> Result<Vec<(i64, bool, SimpleFilter)>> {
        let filter_table = filter_table_name(index_group);

        // Load the root group
        let rows: Vec<(i64, bool, String, Vec<u8>)> = query_as(&format!("
            SELECT id, leaves, kind, filter FROM {filter_table}
            WHERE block IS NULL"))
            .fetch_all(&mut *conn).await?;

        let mut output = vec![];
        for (id, leaves, kind, filter) in rows {
            let filter = match SimpleFilter::load(&kind, &filter) {
                Ok(filter) => filter,
                Err(err) => {
                    error!("Couldn't load filter {id}: {err}");
                    continue
                },
            };
            output.push((id, leaves, filter))
        }
        Ok(output)
    }

    fn _pick_best(target: &SimpleFilter, options: &Vec<&SimpleFilter>) -> Result<usize> {
        let mut best = 0;
        let mut best_score = 0;
        for (index, other) in options.iter().enumerate() {
            let score = target.overlap(other)?.data.count_zeros();
            if score > best_score {
                best = index;
                best_score = score;
            }
        }
        Ok(best)
    }

    async fn _find_insert_group(&self, conn: &mut PoolConnection<Sqlite>, index_group: &IndexGroup, filter: &SimpleFilter) -> Result<Vec<(i64, bool, SimpleFilter)>> {
        let mut group_stack: Vec<(i64, bool, SimpleFilter)> = vec![{
            // Get the root groups
            let mut roots = self._find_roots(&mut *conn, index_group, filter).await?;
            assert!(!roots.is_empty());

            // Select the best of the root groups
            let index = Self::_pick_best(filter, &roots.iter().map(|r|&r.2).collect())?;
            roots.swap_remove(index)
        }];

        while !group_stack.last().unwrap().1 {
            // Get the subgroups of this group
            let mut subgroups = self._load_groups_in_group(&mut *conn, index_group, group_stack.last().unwrap().0).await?;
            assert!(!subgroups.is_empty());

            // select the best of the subgroups
            let index = Self::_pick_best(filter, &subgroups.iter().map(|r|&r.2).collect())?;
            group_stack.push(subgroups.swap_remove(index));
        }
        return Ok(group_stack)
    }

    async fn _expand_filters(&self, conn: &mut PoolConnection<Sqlite>, index_group: &IndexGroup, filter: &SimpleFilter, stack: &Vec<(i64, bool, SimpleFilter)>) -> Result<bool> {
        let filter_table = filter_table_name(index_group);
        for (group_id, _, old_filter) in stack {
            // calculate the new filter for this group
            let new = filter.overlap(old_filter)?;

            // Try to swap it in
            let result = sqlx::query(&format!("
                UPDATE {filter_table} SET filter = ?
                WHERE id = ? AND filter = ?"))
            .bind(new.to_buffer()?)
            .bind(group_id)
            .bind(old_filter.to_buffer()?)
            .execute(&mut *conn).await?;

            // if we could insert it, continue to the next
            // if we failed abort and restart the insertion process
            if result.rows_affected() == 0 {
                return Ok(false)
            }
        }
        return Ok(true)
    }

    async fn _count_files_in_group(&self, conn: &mut PoolConnection<Sqlite>, index_group: &IndexGroup, group: i64) -> Result<usize> {
        let file_table = file_table_name(&index_group);
        let (count, ) : (i64, ) = query_as(&format!("
            SELECT count(*) FROM {file_table}
            WHERE block = ?"))
            .bind(group)
            .fetch_one(&mut *conn).await?;
        return Ok(count as usize)
    }

    async fn _count_groups_in_group(&self, conn: &mut PoolConnection<Sqlite>, index_group: &IndexGroup, group: i64) -> Result<usize> {
        let filter_table = filter_table_name(&index_group);
        let (count, ) : (i64, ) = query_as(&format!("
            SELECT count(*) FROM {filter_table}
            WHERE block = ?"))
            .bind(group)
            .fetch_one(&mut *conn).await?;
        return Ok(count as usize)
    }

    async fn _split_leaf_group(&self, conn: &mut PoolConnection<Sqlite>, index_group: &IndexGroup, group: i64) -> Result<bool> {
        if let Ok(mut table) = self.cant_split.lock() {
            let table = table.get_mut();
            if table.contains(&group) {
                return Ok(false)
            }
        }

        loop {
            let mut trans = conn.begin().await?;
            let filter_table = filter_table_name(&index_group);
            let file_table = file_table_name(&index_group);

            // Get the group info
            let query = query_as(&format!("
                SELECT block, kind FROM {filter_table}
                WHERE id = ?"))
                .bind(group)
                .fetch_optional(&mut trans).await?;
            let (parent, kind): (Option<i64>, String) = match query {
                Some(row) => row,
                None => return Ok(false),
            };

            // Load all the files in the group
            let items = self._load_files_in_group(&mut trans, index_group, group).await?;
            if items.len() < GROUP_SIZE {
                return Ok(false)
            }

            // Partition the data
            let (a_items, b_items) = match Self::_partition(items) {
                Some(index) => index,
                None => {
                    if !parent.is_none() {
                        warn!("A filter group could not be split. {parent:?} {kind}");
                    }
                    if let Ok(mut table) = self.cant_split.lock() {
                        let table = table.get_mut();
                        table.insert(group);
                    }
                    return Ok(false)
                },
            };

            // build the new covering filters
            let size = SimpleFilter::parse_kind(&kind)?;
            let a_cover = a_items.iter()
                .fold(SimpleFilter::empty(size), |a, b|a.overlap(&b.2).unwrap());
            let b_cover = b_items.iter()
                .fold(SimpleFilter::empty(size), |a, b|a.overlap(&b.2).unwrap());

            // Check for root
            let parent = match parent {
                Some(parent) => parent,
                None => {
                    let peak = a_cover.overlap(&b_cover)?;
                    self._new_group(&mut trans, index_group, false, None, &peak).await?
                },
            };

            // Add new groups
            let group_a = self._new_group(&mut trans, index_group, true, Some(parent), &a_cover).await?;
            let group_b = self._new_group(&mut trans, index_group, true, Some(parent), &b_cover).await?;

            // Move the ones that should be moved
            for (hash, _, _) in a_items {
                sqlx::query(&format!("UPDATE {file_table} SET block = ? WHERE hash = ?"))
                    .bind(group_a).bind(hash)
                    .execute(&mut trans).await?;
            }
            for (hash, _, _) in b_items {
                sqlx::query(&format!("UPDATE {file_table} SET block = ? WHERE hash = ?"))
                    .bind(group_b).bind(hash)
                    .execute(&mut trans).await?;
            }

            // try to remove the old group
            sqlx::query(&format!("DELETE FROM {filter_table} WHERE id = ?"))
                .bind(group)
                .execute(&mut trans).await?;

            // Commit
            match trans.commit().await {
                Ok(_) => return Ok(true),
                Err(err) => {
                    warn!("Split failure: {err}")
                },
            };
        }
    }

    async fn _split_inner_group(&self, conn: &mut PoolConnection<Sqlite>, index_group: &IndexGroup, group: i64) -> Result<bool> {
        if let Ok(mut table) = self.cant_split.lock() {
            let table = table.get_mut();
            if table.contains(&group) {
                return Ok(false)
            }
        }

        loop {
            let mut trans = conn.begin().await?;
            let filter_table = filter_table_name(&index_group);

            // Get the group info
            let query = query_as(&format!("
                SELECT block, kind FROM {filter_table}
                WHERE id = ?"))
                .bind(group)
                .fetch_optional(&mut trans).await?;
            let (parent, kind): (Option<i64>, String) = match query {
                Some(row) => row,
                None => return Ok(false),
            };

            // Load all the files in the group
            let items = self._load_groups_in_group(&mut trans, index_group, group).await?;
            if items.len() < GROUP_SIZE {
                return Ok(false)
            }

            // Partition the data
            let (a_items, b_items) = match Self::_partition(items) {
                Some(index) => index,
                None => {
                    if !parent.is_none() {
                        warn!("A filter group could not be split. {parent:?} {kind}");
                    }
                    if let Ok(mut table) = self.cant_split.lock() {
                        let table = table.get_mut();
                        table.insert(group);
                    }
                    return Ok(false)
                },
            };

            // build the new covering filters
            let size = SimpleFilter::parse_kind(&kind)?;
            let a_cover = a_items.iter()
                .fold(SimpleFilter::empty(size), |a, b|a.overlap(&b.2).unwrap());
            let b_cover = b_items.iter()
                .fold(SimpleFilter::empty(size), |a, b|a.overlap(&b.2).unwrap());

            // Check for root
            let parent = match parent {
                Some(parent) => parent,
                None => {
                    let peak = a_cover.overlap(&b_cover)?;
                    self._new_group(&mut trans, index_group, false, None, &peak).await?
                },
            };

            // Add new groups
            let group_a = self._new_group(&mut trans, index_group, false, Some(parent), &a_cover).await?;
            let group_b = self._new_group(&mut trans, index_group, false, Some(parent), &b_cover).await?;

            // Move the ones that should be moved
            for (id, _, _) in a_items {
                sqlx::query(&format!("UPDATE {filter_table} SET block = ? WHERE id = ?"))
                    .bind(group_a).bind(id)
                    .execute(&mut trans).await?;
            }
            for (id, _, _) in b_items {
                sqlx::query(&format!("UPDATE {filter_table} SET block = ? WHERE id = ?"))
                    .bind(group_b).bind(id)
                    .execute(&mut trans).await?;
            }

            // try to remove the old group
            sqlx::query(&format!("DELETE FROM {filter_table} WHERE id = ?"))
                .bind(group)
                .execute(&mut trans).await?;

            // Commit
            match trans.commit().await {
                Ok(_) => return Ok(true),
                Err(err) => {
                    warn!("Split failure: {err}")
                },
            };
        }
    }

    fn _partition<A, B>(items: Vec<(A, B, SimpleFilter)>) -> Option<(Vec<(A, B, SimpleFilter)>, Vec<(A, B, SimpleFilter)>)> {
        // Find the best partition
        let split_index = match Self::_find_partition(&items.iter().map(|r|&r.2).collect()) {
            Some(index) => index,
            None => {
                return None
            },
        };

        // Split the items that will move
        let (a_items, b_items): (Vec<(A, B, SimpleFilter)>, Vec<(A, B, SimpleFilter)>) = items.into_iter()
            .partition(|row| row.2.data.get(split_index).as_deref() == Some(&true));
        Some((a_items, b_items))
    }


    fn _find_partition(items: &Vec<&SimpleFilter>) -> Option<usize> {
        let mut counts: HashMap<usize, i64> = Default::default();
        for filter in items.iter() {
            for bit in filter.data.iter_ones() {
                counts.insert(bit, *counts.get(&bit).unwrap_or(&0));
            }
        }
        let mut best_index: Option<usize> = None;
        let even_split = (items.len() as i64)/2;
        let mut best_distance = even_split;
        for (index, hits) in counts {
            let distance = (hits - even_split).abs();
            if distance < best_distance {
                best_distance = distance;
                best_index = Some(index)
            }
        }
        best_index
    }

    async fn _new_group<'e, E>(&self, conn: E, index_group: &IndexGroup, leaves: bool, parent: Option<i64>, filter: &SimpleFilter) -> Result<i64>
    where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
    {
        let filter_table = filter_table_name(index_group);
        Ok(sqlx::query(&format!("INSERT INTO {filter_table}(leaves, block, filter, kind) VALUES(?, ?, ?, ?)"))
            .bind(leaves)
            .bind(parent)
            .bind(filter.to_buffer()?)
            .bind(filter.kind())
            .execute(conn).await?.last_insert_rowid())
    }


    async fn _load_files_in_group<'e, E>(&self, conn: E, index_group: &IndexGroup, group: i64) -> Result<Vec<(Vec<u8>, AccessControl, SimpleFilter)>>
    where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
    {
        let file_table = file_table_name(&index_group);
        let rows: Vec<(Vec<u8>, String, String, Vec<u8>)> = query_as(&format!("
            SELECT hash, access, kind, filter FROM {file_table}
            WHERE block = ?"))
            .bind(group)
            .fetch_all(conn).await?;
        let mut out = vec![];
        for (hash, access, kind, filter) in rows {
            let filter = match SimpleFilter::load(&kind, &filter) {
                Ok(filter) => filter,
                Err(err) => {
                    error!("Corrupt filter entry: {err}");
                    continue
                },
            };
            let access: AccessControl = match access.parse() {
                Ok(access) => access,
                Err(err) => {
                    error!("Corrupt access entry: {err}");
                    continue
                }
            };
            out.push((hash, access, filter))
        }
        return Ok(out)
    }

    async fn _load_groups_in_group<'e, E>(&self, conn: E, index_group: &IndexGroup, group: i64) -> Result<Vec<(i64, bool, SimpleFilter)>>
    where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
    {
        let filter_table = filter_table_name(&index_group);
        let rows: Vec<(i64, bool, String, Vec<u8>)> = query_as(&format!("
            SELECT id, leaves, kind, filter FROM {filter_table}
            WHERE block = ?"))
            .bind(group)
            .fetch_all(conn).await?;
        let mut out = vec![];
        for (id, leave, kind, filter) in rows {
            let filter = match SimpleFilter::load(&kind, &filter) {
                Ok(filter) => filter,
                Err(err) => {
                    error!("Corrupt filter entry: {err}");
                    continue
                },
            };
            out.push((id, leave, filter))
        }
        return Ok(out)
    }

//     pub async fn select_index_to_grow(&self, index_group: &IndexGroup) -> Result<Option<(IndexID, BlobID, u64)>> {
//         let mut conn = self.db.acquire().await?;

//         // Get all the groups that expire later than this one
//         let list: Vec<(IndexID, Vec<u8>)> = query_as("
//             SELECT label, data FROM index_index
//             WHERE block = ?")
//             .bind(index_group.as_str())
//             .fetch_all(&mut conn).await?;

//         // loop until we find an index
//         let mut best: Option<(IndexID, IndexEntry)> = None;
//         for (key, value) in list {
//             let value: IndexEntry = postcard::from_bytes(&value)?;

//             if value.size_bytes >= self.config.index_soft_bytes_max {
//                 debug!("index {} chunk over size {}/{} skipping", key, value.size_bytes, self.config.index_soft_bytes_max);
//                 continue;
//             }

//             if value.size_entries >= self.config.index_soft_entries_max {
//                 debug!("index {} chunk over capacity {}/{} skipping", key, value.size_entries, self.config.index_soft_entries_max);
//                 continue;
//             }

//             match best {
//                 Some(old_best) => {
//                     if old_best.1.size_bytes > value.size_bytes {
//                         best = Some((key, value))
//                     } else {
//                         best = Some(old_best)
//                     }
//                 },
//                 None => best = Some((key, value)),
//             }
//         }

//         match best {
//             Some((label, best)) => Ok(Some((label, best.current_blob, best.size_entries))),
//             None => Ok(None),
//         }
//     }

//     pub async fn create_index_data(&self, index_group: &IndexGroup, blob_id: BlobID, meta: Vec<(Vec<u8>, AccessControl)>, new_size: u64) -> Result<()> {
//         let index_id = IndexID::new();
//         debug!("create collection for new index {index_group} {index_id}");
//         self.setup_filter_table(&index_id).await?;
//         self.update_index_data(index_group, index_id, &blob_id, &blob_id, meta, 0, new_size).await
//     }

//     pub async fn update_index_data(&self, index_group: &IndexGroup, index_id: IndexID, old_blob_id: &BlobID, blob_id: &BlobID, meta: Vec<(Vec<u8>, AccessControl)>, index_offset: u64, new_size: u64) -> Result<()> {
//         debug!("update collection for {index_group} {index_id}");
//         let mut conn = self.db.acquire().await?;
//         // Open column family for the index meta data
//         let table_name = filter_table_name(&index_id);

//         // Add all the new file entries
//         let new_entries = meta.len() as u64;
//         for (index, (hash, access)) in meta.into_iter().enumerate() {
//             let index = index as i64 + index_offset as i64;
//             sqlx::query(&format!("INSERT INTO {table_name}(hash, number, access) VALUES(?, ?, ?)"))
//                 .bind(hash).bind(index).bind(&postcard::to_allocvec(&access)?)
//                 .execute(&mut conn).await?;
//         }
//         debug!("update {index_group} {index_id} file records updated");

//         // Update size in index table
//         loop {
//             debug!("update {index_group} {index_id} get old index entry");
//             let mut trans = conn.begin().await?;
//             // Get
//             let old: Option<(Vec<u8>, )> = sqlx::query_as(
//                 "SELECT data FROM index_index WHERE label = ?")
//                 .bind(index_id.as_str())
//                 .fetch_optional(&mut trans).await?;

//             // modify
//             let entry = match &old {
//                 Some((old, )) => {
//                     debug!("update {index_group} {index_id} update index entry");
//                     let old: IndexEntry = postcard::from_bytes(old)?;
//                     if &old.current_blob != old_blob_id {
//                         return Err(anyhow::anyhow!("Blob replaced"));
//                     }
//                     let mut entry = old.clone();
//                     entry.size_bytes = new_size as u64;
//                     entry.size_entries += new_entries;
//                     entry.current_blob = blob_id.clone();
//                     entry
//                 },
//                 None => {
//                     debug!("update {index_group} {index_id} define index entry");
//                     IndexEntry{
//                         current_blob: blob_id.clone(),
//                         size_bytes: new_size as u64,
//                         size_entries: new_entries,
//                         group: index_group.clone(),
//                         label: index_id.clone(),
//                     }
//                 },
//             };
//             let entry = postcard::to_allocvec(&entry)?;

//             // write
//             debug!("update {index_group} {index_id} add old blob to garbage collection");
//             self._schedule_blob_gc(&mut trans, old_blob_id, chrono::Utc::now()).await?;
//             self._release_blob_gc(&mut trans, blob_id).await?;

//             debug!("update {index_group} {index_id} insert new index entry");
//             let res = match old {
//                 Some((old, )) => sqlx::query(
//                     "UPDATE index_index SET data = ? WHERE label = ? AND data = ?")
//                     .bind(entry).bind(index_id.as_str()).bind(old)
//                     .execute(&mut trans).await?,
//                 None => sqlx::query(
//                     "INSERT OR REPLACE INTO index_index(data, label, expiry_group) VALUES(?, ?, ?)")
//                     .bind(entry).bind(index_id.as_str()).bind(index_group.as_str())
//                     .execute(&mut trans).await?,
//             };

//             debug!("update {index_group} {index_id} try to finalize");
//             if res.rows_affected() > 0 {
//                 trans.commit().await?;
//                 return Ok(())
//             } else {
//                 trans.rollback().await?;
//             }
//         }
//     }

//     pub async fn list_indices(&self) -> Result<Vec<(IndexGroup, IndexID)>> {
//         let mut conn = self.db.acquire().await?;
//         let list: Vec<(IndexGroup, IndexID)> = query_as("
//             SELECT expiry_group, label FROM index_index")
//             .fetch_all(&mut conn).await?;
//         Ok(list)
//     }

//     pub async fn count_files(&self, index: &IndexID) -> Result<u64> {
//         let mut conn = self.db.acquire().await?;

//         let table_name = filter_table_name(&index);

//         // Add all the new file entries
//         let (row, ): (i64, ) = sqlx::query_as(&format!("SELECT count(*) FROM {table_name}"))
//             .fetch_one(&mut conn).await?;

//         Ok(row as u64)
//     }

//     pub async fn lease_blob(&self) -> Result<BlobID> {
//         let id = BlobID::new();
//         self._schedule_blob_gc(&self.db, &id, chrono::Utc::now() + chrono::Duration::days(1)).await?;
//         return Ok(id)
//     }

//     pub async fn list_garbage_blobs(&self) -> Result<Vec<BlobID>> {
//         let rows: Vec<(BlobID, )> = sqlx::query_as(
//             "SELECT blob_id FROM garbage WHERE time < ?")
//             .bind(chrono::Utc::now().to_rfc3339())
//             .fetch_all(&self.db).await?;
//         return Ok(rows.into_iter().map(|row|row.0).collect())
//     }

//     pub async fn release_blob(&self, id: BlobID) -> Result<()> {
//         self._release_blob_gc(&self.db, &id).await
//     }

//     async fn _schedule_blob_gc<'e, E>(&self, conn: E, id: &BlobID, when: chrono::DateTime<chrono::Utc>) -> Result<()>
//     where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
//     {
//         sqlx::query("INSERT OR REPLACE INTO garbage(blob_id, time) VALUES(?, ?)")
//             .bind(id.as_str())
//             .bind(&when.to_rfc3339())
//             .execute(conn).await?;
//         return Ok(())
//     }

//     async fn _release_blob_gc<'e, E>(&self, conn: E, id: &BlobID) -> Result<()>
//     where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
//     {
//         sqlx::query("DELETE FROM garbage WHERE blob_id = ?").bind(id.as_str()).execute(conn).await?;
//         return Ok(())
//     }

    pub async fn release_groups(&self, id: IndexGroup) -> Result<()> {
        for group in self.list_indices().await? {
            if group <= id {
                self._release_group(group).await?;
            }
        }
        return Ok(())
    }

    async fn _release_group(&self, entry: IndexGroup) -> Result<()> {
        info!("Garbage collecting index for {}", entry);

        // In a transaction
        let mut trans = self.db.begin().await?;

        // Delete group table
        let filter_table = filter_table_name(&entry);
        sqlx::query(&format!("DROP TABLE IF EXISTS {filter_table}")).execute(&mut trans).await?;
        let file_table = file_table_name(&entry);
        sqlx::query(&format!("DROP TABLE IF EXISTS {file_table}")).execute(&mut trans).await?;

        // Commit
        Ok(trans.commit().await?)
    }

    pub async fn initialize_search(&self, req: SearchRequest) -> Result<InternalSearchStatus> {
        // Turn the expiry dates into a group range
        let start = match req.start_date {
            Some(value) => IndexGroup::create(&Some(value)),
            None => IndexGroup::min(),
        };
        let end = match req.end_date {
            Some(value) => IndexGroup::create(&Some(value)),
            None => IndexGroup::max(),
        };

        // Add operation to the search table
        let code = hex::encode(uuid::Uuid::new_v4().as_bytes());
        sqlx::query("INSERT INTO searches(code, stage, data, search_group, start_time) VALUES(?, ?, ?, ?, ?)")
            .bind(&code)
            .bind(SearchStage::Queued)
            .bind(&postcard::to_allocvec(&SearchRecord{
                code: code.clone(),
                yara_signature: req.yara_signature,
                errors: Default::default(),
                access: req.access,
                view: req.view,
                group: req.group.clone(),
                query: req.query.clone(),
                start,
                end,
                suspect_files: 0,
                filtered_files: 0,
                hit_files: Default::default(),
                truncated: false,
            })?)
            .bind(req.group)
            .bind(chrono::Utc::now().to_rfc3339())
            .execute(&self.db).await.context("inserting search")?;


        self.work_notification.notify_waiters();
        match self.search_status(code).await.context("first search status")? {
            Some(result) => Ok(result),
            None => Err(anyhow::anyhow!("Result status could not be read.")),
        }
    }

    pub async fn search_status(&self, code: String) -> Result<Option<InternalSearchStatus>> {
        let mut conn = self.db.acquire().await?;
        let data = self._get_search(&mut conn, &code).await.context("_get_search")?;

        let (stage, search) = match data {
            Some(row) => row,
            None => return Ok(None)
        };

        let pending_files = if stage == SearchStage::Yara {
            let (pending_files, ): (i64, ) = sqlx::query_as(
                "SELECT SUM(hash_count) FROM yara_tasks WHERE search = ?")
                .bind(&code)
                .fetch_one(&mut conn).await.context("Couldn't list yara tasks for search")?;
            pending_files
        } else {
            0
        };

        Ok(Some(
            InternalSearchStatus {
                view: search.view,
                resp: SearchRequestResponse{
                    code,
                    group: search.group,
                    stage,
                    errors: search.errors,
                    suspect_files: search.suspect_files,
                    filtered_files: search.filtered_files,
                    pending_files: pending_files as u64,
                    hits: search.hit_files.into_iter().map(|hash|hex::encode(hash)).collect(),
                    truncated: search.truncated,
                }
            }
        ))
    }

    pub async fn get_yara_assignments_before(&self, time: chrono::DateTime<chrono::Utc>) -> Result<Vec<(String, i64)>> {
        let mut conn = self.db.acquire().await?;
        let req: Vec<(String, i64)> = sqlx::query_as(
            "SELECT assigned_worker, id FROM yara_tasks WHERE assigned_time < ?")
            .bind(time.to_rfc3339())
            .fetch_all(&mut conn).await?;
        return Ok(req)
    }

    pub async fn release_tasks_assigned_before(&self, time: chrono::DateTime<chrono::Utc>) -> Result<u64> {
        let mut conn = self.db.acquire().await?;
        let yara_req = sqlx::query(
            "UPDATE yara_tasks SET assigned_worker = NULL, assigned_time = NULL
            WHERE assigned_time < ?")
            .bind(time.to_rfc3339())
            .execute(&mut conn).await?;
        return Ok(yara_req.rows_affected())
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

    pub async fn release_yara_task(&self, id: i64) -> Result<bool>{
        self._release_yara_task(&self.db, id).await
    }

    async fn _release_yara_task<'e, E>(&self, conn: E, id: i64) -> Result<bool>
    where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
    {
        let req = sqlx::query(
            "UPDATE yara_tasks SET assigned_worker = NULL, assigned_time = NULL
            WHERE id = ?")
            .bind(id)
            .execute(conn).await?;
        return Ok(req.rows_affected() > 0)
    }

    async fn _get_search<'e, E>(&self, conn: E, code: &str) -> Result<Option<(SearchStage, SearchRecord)>>
    where E: sqlx::Executor<'e, Database = sqlx::Sqlite>
    {
        let search: Option<(SearchStage, Vec<u8>)> = sqlx::query_as(
            "SELECT stage, data FROM searches WHERE code = ? LIMIT 1")
            .bind(&code)
            .fetch_optional(conn).await?;

        Ok(match search {
            Some((stage, search)) => Some((stage, postcard::from_bytes(&search)?)),
            None => None,
        })
    }

    pub async fn get_queued_or_filtering_searches(&self) -> Result<(Vec<String>, Vec<String>)> {
        let searches: Vec<(String, SearchStage)> = sqlx::query_as(
            "SELECT code, stage FROM searches WHERE stage = ? OR stage = ?")
            .bind(SearchStage::Queued)
            .bind(SearchStage::Filtering)
            .fetch_all(&self.db).await?;

        let mut queued = vec![];
        let mut filtering = vec![];

        for (code, stage) in searches {
            match stage {
                SearchStage::Queued => queued.push(code),
                SearchStage::Filtering => filtering.push(code),
                _ => {}
            }
        }

        return Ok((queued, filtering))
    }


    pub async fn set_search_stage(&self, code: &str, stage: SearchStage) -> Result<()> {
        sqlx::query(
            "UPDATE searches SET stage = ? WHERE code = ?")
            .bind(stage)
            .bind(code)
            .execute(&self.db).await?;
        return Ok(())
    }

    pub async fn filter_search(&self, code: &str) -> Result<()> {
        // Get the search in question
        let mut conn = self.db.acquire().await?;
        let record = match self._get_search(&mut conn, code).await? {
            None => return Ok(()),
            Some((stage, record)) => {
                if stage == SearchStage::Filtering {
                    record
                } else {
                    return Ok(())
                }
            }
        };

        // Load the set of indices in the range for this job
        let indices = self.list_indices().await?;
        let indices: Vec<IndexGroup> = indices.into_iter()
            .filter(|index| &record.start <= index && index <= &record.end).collect();

        let mut files: HashSet<Vec<u8>> = Default::default();
        let mut rejected_files: HashSet<Vec<u8>> = Default::default();
        for index_group in indices {
            // Load the base filters for this index
            let mut outstanding_groups = self._find_all_roots(&mut conn, &index_group).await?;

            // Apply filters recursively until we only have file entries left
            while let Some((group_id, leaves, filter)) = outstanding_groups.pop() {
                if !filter.query(&record.query) {
                    continue
                }

                if leaves {
                    for (hash, access, filter) in self._load_files_in_group(&mut conn, &index_group, group_id).await? {
                        if rejected_files.contains(&hash) { continue }
                        if !access.can_access(&record.access) || !filter.query(&record.query) {
                            rejected_files.insert(hash);
                            continue
                        }
                        files.insert(hash);
                    }
                } else {
                    outstanding_groups.extend(self._load_groups_in_group(&mut conn, &index_group, group_id).await?);
                }
            }
        }

        let mut trans = conn.begin().await?;

        // Create yara jobs for these files
        let files: Vec<Vec<u8>> = files.into_iter().collect();
        for hash_block in files.chunks(self.config.yara_job_size as usize) {
            let hashes_data = postcard::to_allocvec(&hash_block)?;
            sqlx::query(
                "INSERT INTO yara_tasks(search, hashes, hash_count) VALUES(?,?,?)")
                .bind(&code)
                .bind(hashes_data)
                .bind(hash_block.len() as i64)
                .execute(&mut trans).await?;
        }

        let stage = if files.is_empty() {
            SearchStage::Finished
        } else {
            SearchStage::Yara
        };

        // Update job status
        let mut record = record;
        record.suspect_files = files.len() as u64;
        record.filtered_files = rejected_files.len() as u64;
        sqlx::query(
            "UPDATE searches SET data = ?, stage = ? WHERE code = ?")
            .bind(&postcard::to_allocvec(&record)?)
            .bind(stage)
            .bind(&code)
            .execute(&mut trans).await?;

        // Apply search result
        Ok(trans.commit().await?)
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
            Some((_stage, record)) => record,
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

        // Grab some yara jobs
        let yara_tasks = self.get_yara_task(&mut conn, &req.worker).await?;

        return Ok(WorkPackage{
            yara: yara_tasks,
        })
    }

    pub async fn get_work_notification(&self) -> Result<()> {
        Ok(self.work_notification.notified().await)
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

        // Check if the yara phase is done
        let (count, ): (i64, ) = sqlx::query_as("SELECT count(1) FROM yara_tasks WHERE search = ? LIMIT 1").bind(code).fetch_one(&mut conn).await?;
        if count == 0 {
            self.set_search_stage(code, SearchStage::Finished).await?;
        }
        return Ok(());
    }


    pub async fn add_work_error(&self, err: WorkError) -> Result<()> {
        let (search, ): (String, ) = sqlx::query_as(
            "SELECT search FROM yara_tasks WHERE id = ? LIMIT 1")
            .bind(err.job_id).fetch_one(&self.db).await?;
        self.add_search_error(&search, &err.error).await?;
        sqlx::query("DELETE FROM yara_tasks WHERE id = ?").bind(err.job_id).execute(&self.db).await?;
        return Ok(())
    }

    pub async fn add_search_error(&self, code: &str, error_message: &str) -> Result<()> {
        let mut conn = self.db.acquire().await?;

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
            search.errors.push(error_message.to_owned());

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

        return Ok(())

    }

    fn _new_partition<A, B>(items: Vec<(A, B, SimpleFilter)>) -> Option<(Vec<(A, B, SimpleFilter)>, Vec<(A, B, SimpleFilter)>)> {
        // Find the best partition
        let split_index = match Self::_find_partition(&items.iter().map(|r|&r.2).collect()) {
            Some(index) => index,
            None => {
                return None
            },
        };

        // Split the items that will move
        let (a_items, b_items): (Vec<(A, B, SimpleFilter)>, Vec<(A, B, SimpleFilter)>) = items.into_iter()
            .partition(|row| row.2.data.get(split_index).as_deref() == Some(&true));
        Some((a_items, b_items))
    }

    pub async fn partition_test(&self) -> Result<()> {
        let mut conn = self.db.acquire().await?;

        for index in self.list_indices().await? {
            let filter_table = filter_table_name(&index);
            let groups: Vec<(i64, bool)> = query_as(&format!("
            SELECT id, leaves FROM {filter_table}"))
            .fetch_all(&mut conn).await?;

            for (group_id, leaves) in groups {
                let items: Vec<((), (), SimpleFilter)> = if leaves {
                    self._load_files_in_group(&mut conn, &index, group_id).await?
                        .into_iter()
                        .map(|(_, _, filter)|((), (), filter)).collect()
                } else {
                    self._load_groups_in_group(&mut conn, &index, group_id).await?
                        .into_iter()
                        .map(|(_, _, filter)|((), (), filter)).collect()
                };

                println!("{group_id}");

                if let Some((a_items, b_items)) = Self::_partition(items.clone()) {
                    let kind = a_items[0].2.kind();
                    let size = SimpleFilter::parse_kind(&kind)?;
                    let a_cover = a_items.iter()
                        .fold(SimpleFilter::empty(size), |a, b|a.overlap(&b.2).unwrap());
                    let b_cover = b_items.iter()
                        .fold(SimpleFilter::empty(size), |a, b|a.overlap(&b.2).unwrap());
                    println!("Split ")
                }


                if let Some((a_items, b_items)) = Self::_new_partition(items.clone()) {
                    let kind = a_items[0].2.kind();
                    let size = SimpleFilter::parse_kind(&kind)?;
                    let a_cover = a_items.iter()
                        .fold(SimpleFilter::empty(size), |a, b|a.overlap(&b.2).unwrap());
                    let b_cover = b_items.iter()
                        .fold(SimpleFilter::empty(size), |a, b|a.overlap(&b.2).unwrap());
                }

            }
        }
        return Ok(())
    }
}

