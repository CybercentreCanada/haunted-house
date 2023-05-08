use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet, HashMap};
use std::path::{Path};

use anyhow::{Result, Context};
use async_trait::async_trait;
use itertools::Itertools;
use log::{info, error, warn};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use sqlx::{query_as, Acquire};
use tempfile::TempDir;

use crate::access::AccessControl;
use crate::core::{CoreConfig};
use crate::database::{IndexGroup, SearchStage, IndexStatus};
use crate::db_imp_mysql::MySQLImp;
use crate::db_imp_sqlite::SqliteImp;
use crate::filter::{Filter, LoadFilter, GenericFilter};
use crate::interface::{SearchRequest, SearchRequestResponse, InternalSearchStatus, WorkRequest, WorkPackage, YaraTask, WorkError};
use crate::query::Query;

pub type PoolCon = sqlx::any::AnyConnection;

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
    group_hits: u64,
    group_misses: u64,
    hit_files: BTreeSet<Vec<u8>>,
    truncated: bool,
    start: IndexGroup,
    end: IndexGroup,
}

#[async_trait]
pub trait ImplDetails {
    async fn initialize(&self, conn: &mut PoolCon) -> Result<()>;
    async fn list_tables(&self, conn: &mut PoolCon) -> Result<Vec<String>>;
    async fn initialize_filter(&self, conn: &mut PoolCon, index: &IndexGroup) -> Result<()>;
}

// pub struct SQLInterface<DB: sqlx::Database, Imp: ImplDetails<DB>> {
pub struct SQLInterface {
    db: sqlx::AnyPool,
    config: CoreConfig,
    work_notification: tokio::sync::Notify,
    _temp_dir: Option<TempDir>,
    imp: Box<dyn ImplDetails + Sync + Send>,
    // _db: std::marker::PhantomData<DB>,
    cant_split: std::sync::Mutex<std::cell::RefCell<HashSet<i64>>>,
    status: tokio::sync::Mutex<std::cell::RefCell<IndexStatus>>
}

pub fn file_table_name(name: &IndexGroup) -> String {
    format!("file_{name}")
}

pub fn filter_table_name(name: &IndexGroup) -> String {
    format!("filter_{name}")
}

const GROUP_SIZE: usize = 48;

fn layer_density(height: i32, target: f64) -> f64 {
    let minimum = 1.0 - (1.0 - target).powi(3);
    let factor = 1.0 - (0.95f64).powi(height.pow(2));
    return minimum + (1.0-minimum)*factor;
}


impl SQLInterface {
    pub async fn new(config: CoreConfig, dbconf: crate::config::Database) -> Result<Self> {

        let (url, imp, temp): (String, Box<dyn ImplDetails + Sync + Send>, Option<TempDir>) = match dbconf {
            crate::config::Database::SQLite { path } => {
                let url = path;
                let url = if url == "memory" {
                    format!("sqlite::memory:")
                } else {
                    let path = Path::new(&url);

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

                (url, Box::new(SqliteImp{}), None)
            },
            crate::config::Database::SQLiteTemp => {
                let tempdir = tempfile::tempdir()?;
                let path = tempdir.path().join("house.db");

                let url = if path.is_absolute() {
                    format!("sqlite://{}?mode=rwc", path.to_str().unwrap())
                } else {
                    format!("sqlite:{}?mode=rwc", path.to_str().unwrap())
                };

                (url, Box::new(SqliteImp{}), Some(tempdir))
            },
            crate::config::Database::MySQL { username, password, host, database } => {
                let url = format!("mysql://{username}:{password}@{host}/{database}");

                (url, Box::new(MySQLImp{}), None)
            }
        };


        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(200)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&url).await?;
        let mut conn = pool.acquire().await?;
        imp.initialize(&mut conn).await?;

        let db = Self {
            db: pool,
            config,
            work_notification: Default::default(),
            imp,
            _temp_dir: temp,
            cant_split: std::sync::Mutex::new(RefCell::new(Default::default())),
            status: tokio::sync::Mutex::new(RefCell::new(Default::default()))
        };
        {
            let mut status = db.status.lock().await;
            *status.get_mut() = db._init_status().await?;
        }

        return Ok(db)
    }

    async fn setup_filter_table(&self, name: &IndexGroup) -> Result<()> {
        let mut con = self.db.acquire().await?;
        return self.imp.initialize_filter(&mut con, name).await
    }

    pub async fn list_indices(&self) -> Result<Vec<IndexGroup>> {
        // Get tables
        let mut conn = self.db.acquire().await?;
        let tables = self.imp.list_tables(&mut conn).await?;

        // Figure out which ones are index groups
        let mut file = HashSet::new();
        let mut filter = HashSet::new();
        for name in tables {
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

    // pub async fn list_filters<F: Filter>(&self, kind: &str, goal: usize) -> Result<Vec<F>> {
    //     let mut filters: Vec<F> = vec![];
    //     let indices = self.list_indices().await?;

    //     for index in indices {
    //         if filters.len() >= goal {
    //             break
    //         }

    //         let file_table = file_table_name(&index);

    //         let rows: Vec<(Vec<u8>, )> = sqlx::query_as(&format!("
    //             SELECT filter FROM {file_table} WHERE kind = ? LIMIT {goal}"))
    //             .bind(kind)
    //             .fetch_all(&self.db).await?;

    //         for (data, ) in rows {
    //             filters.push(F::load(kind, &data)?)
    //         }
    //     }

    //     return Ok(filters)
    // }

    pub async fn _init_status(&self) -> Result<IndexStatus> {
        let tables = self.list_indices().await?;
        let mut conn = self.db.acquire().await?;

        let mut sizes: HashMap<String, u64> = Default::default();
        let mut kinds: HashMap<String, u64> = Default::default();

        for group in tables {
            let file_table = file_table_name(&group);

            let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
                "SELECT kind, COUNT(1) FROM {file_table} GROUP BY kind"))
                .fetch_all( &mut conn).await?;

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

    pub async fn status(&self) -> Result<IndexStatus> {
        let mut data = self.status.lock().await;
        return Ok(data.get_mut().clone());
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

    pub async fn insert_file<F: Filter>(&self, hash: &[u8], access: &AccessControl, index_group: &IndexGroup, filter: &F, target_density: f64) -> Result<()> {
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
                    let constraint_failed = err.to_string().contains("UNIQUE constraint failed")
                        || err.to_string().contains("1062 (23000)");

                    if constraint_failed {
                        self.update_file_access(hash, access, index_group).await?;
                        return Ok(());
                    }
                    error!("Could not insert file: {err}");
                },
            };
        };

        {
            let mut count = self.status.lock().await;
            let count = count.get_mut();
            let group = index_group.to_string();
            let kind = filter.kind();
            let kind_count = *count.filter_sizes.get(&kind).unwrap_or(&0);
            count.filter_sizes.insert(kind, kind_count + 1);
            let group_count = *count.group_files.get(&group).unwrap_or(&0);
            count.group_files.insert(group, group_count + 1);
        }

        // Check each group (bottom up) to see if it needs to be split
        let mut depth = 0;
        while let Some((group_id, leaf, mask)) = stack.pop() {
            let target_density = if stack.is_empty() {
                1.0
            } else {
                layer_density(depth, target_density)
            };

            if leaf {
                let count = self._count_files_in_group(&mut conn, index_group, group_id).await?;
                if count > GROUP_SIZE || mask.density() > target_density {
                    self._split_leaf_group::<F>(&mut conn, index_group, group_id).await?;
                }
            } else {
                let count = self._count_groups_in_group(&mut conn, index_group, group_id).await?;
                if count > GROUP_SIZE || mask.density() > target_density {
                    self._split_inner_group::<F>(&mut conn, index_group, group_id).await?;
                }
            }
            depth += 1;
        }
        return Ok(())
    }

    async fn _find_roots<F: Filter>(&self, conn: &mut PoolCon, index_group: &IndexGroup, filter: &F) -> Result<Vec<(i64, bool, F)>> {
        let filter_table = filter_table_name(index_group);

        // Load the root group
        let mut rows: Vec<(i64, bool, String, Vec<u8>)> = query_as(&format!("
            SELECT id, leaves, kind, filter FROM {filter_table}
            WHERE kind = ? AND block IS NULL"))
            .bind(filter.kind())
            .fetch_all(&mut *conn).await.context("search for existing")?;

        // Try to atomically create it
        if rows.is_empty() {
            sqlx::query(&format!("INSERT INTO {filter_table}(leaves, block, filter, kind) VALUES(TRUE, NULL, ?, ?)"))
                .bind(filter.to_buffer()?)
                .bind(filter.kind())
                .execute(&mut *conn).await.context("insert new")?;

            rows = query_as(&format!("
            SELECT id, leaves, kind, filter FROM {filter_table}
            WHERE kind = ? AND block IS NULL"))
            .bind(filter.kind())
            .fetch_all(&mut *conn).await.context("reload existing")?;
        }

        let mut output = vec![];
        for (id, leaves, kind, filter) in rows {
            let filter = match F::load(&kind, &filter) {
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

    async fn _find_all_roots<F: LoadFilter>(&self, conn: &mut PoolCon, index_group: &IndexGroup) -> Result<Vec<(i64, bool, F)>> {
        let filter_table = filter_table_name(index_group);

        // Load the root group
        let rows: Vec<(i64, bool, String, Vec<u8>)> = query_as(&format!("
            SELECT id, leaves, kind, filter FROM {filter_table}
            WHERE block IS NULL"))
            .fetch_all(&mut *conn).await?;

        let mut output = vec![];
        for (id, leaves, kind, filter) in rows {
            let filter = match F::load(&kind, &filter) {
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

    async fn _find_insert_group<F: Filter>(&self, conn: &mut PoolCon, index_group: &IndexGroup, filter: &F) -> Result<Vec<(i64, bool, F)>> {
        // Get roots
        let roots = self._find_roots(&mut *conn, index_group, filter).await.context("_find_roots")?;
        let margin = filter.data().len()/20;

        // sort the roots
        let mut active = vec![];
        for node in roots {
            let (before, after) = node.2.overlap_count_ones(&filter)?;
            let increase = after - before;
            active.push(((increase, after), vec![node]));
        }
        active.sort_unstable_by_key(|(a, _)|a.clone());

        let stack = loop {
            // break out if we are finished
            if active[0].1.last().unwrap().1 {
                break active.swap_remove(0).1;
            }

            // prune bad options
            let top = active[0].0.0;
            while active.last().unwrap().0.0 > top + margin {
                active.pop();
            }

            // expand groups
            let mut expanded = vec![];
            while let Some((_, stack)) = active.pop() {
                let subgroups = self._load_groups_in_group(&mut *conn, index_group, stack.last().unwrap().0).await?;
                for tail in subgroups {
                    let mut stack = stack.clone();
                    stack.push(tail);
                    expanded.push(stack);
                }
            }

            // update active
            for stack in expanded {
                let (before, after) = stack.last().unwrap().2.overlap_count_ones(&filter)?;
                let increase = after - before;
                active.push(((increase, after), stack));
            }
            active.sort_unstable_by_key(|(a, _)|a.clone());
        };

        return Ok(stack)
    }

    async fn _expand_filters<F: Filter>(&self, conn: &mut PoolCon, index_group: &IndexGroup, filter: &F, stack: &Vec<(i64, bool, F)>) -> Result<bool> {
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
            .execute(&mut *conn).await;

            let result = match result {
                Ok(ok) => ok,
                Err(err) => if let Some(dberr) = err.as_database_error() {
                    if dberr.code() == Some(Cow::Borrowed("5")) {
                        return Ok(false)
                    } else {
                        return Err(err.into())
                    }
                } else {
                    return Err(err.into())
                },
            };

            // if we could insert it, continue to the next
            // if we failed abort and restart the insertion process
            if result.rows_affected() == 0 {
                return Ok(false)
            }
        }
        return Ok(true)
    }

    async fn _count_files_in_group(&self, conn: &mut PoolCon, index_group: &IndexGroup, group: i64) -> Result<usize> {
        let file_table = file_table_name(&index_group);
        let (count, ) : (i64, ) = query_as(&format!("
            SELECT count(*) FROM {file_table}
            WHERE block = ?"))
            .bind(group)
            .fetch_one(&mut *conn).await?;
        return Ok(count as usize)
    }

    async fn _count_groups_in_group(&self, conn: &mut PoolCon, index_group: &IndexGroup, group: i64) -> Result<usize> {
        let filter_table = filter_table_name(&index_group);
        let (count, ) : (i64, ) = query_as(&format!("
            SELECT count(*) FROM {filter_table}
            WHERE block = ?"))
            .bind(group)
            .fetch_one(&mut *conn).await?;
        return Ok(count as usize)
    }

    async fn _split_leaf_group<F: Filter>(&self, conn: &mut PoolCon, index_group: &IndexGroup, group: i64) -> Result<bool> {
        if let Ok(mut table) = self.cant_split.lock() {
            let table = table.get_mut();
            if table.contains(&group) {
                return Ok(false)
            }
        }

        loop {
            let filter_table = filter_table_name(&index_group);
            let file_table = file_table_name(&index_group);

            // Get the group info
            let query = query_as(&format!("
                SELECT block, kind FROM {filter_table}
                WHERE id = ?"))
                .bind(group)
                .fetch_optional(&mut *conn).await?;
            let (parent, kind): (Option<i64>, String) = match query {
                Some(row) => row,
                None => return Ok(false),
            };

            // Load all the files in the group
            let items = self._load_files_in_group(&mut *conn, index_group, group).await?;
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
            let a_cover = F::merge(a_items.iter().map(|(_, _, f)|f).collect_vec())?;
            let b_cover = F::merge(b_items.iter().map(|(_, _, f)|f).collect_vec())?;

            let mut trans = conn.begin().await?;
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

    async fn _split_inner_group<F: Filter>(&self, conn: &mut PoolCon, index_group: &IndexGroup, group: i64) -> Result<bool> {
        if let Ok(mut table) = self.cant_split.lock() {
            let table = table.get_mut();
            if table.contains(&group) {
                return Ok(false)
            }
        }

        loop {
            let filter_table = filter_table_name(&index_group);

            // Get the group info
            let query = query_as(&format!("
                SELECT block, kind FROM {filter_table}
                WHERE id = ?"))
                .bind(group)
                .fetch_optional(&mut *conn).await?;
            let (parent, kind): (Option<i64>, String) = match query {
                Some(row) => row,
                None => return Ok(false),
            };

            // Load all the files in the group
            let items = self._load_groups_in_group(&mut *conn, index_group, group).await?;
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
            let a_cover = F::merge(a_items.iter().map(|(_, _, f)|f).collect_vec())?;
            let b_cover = F::merge(b_items.iter().map(|(_, _, f)|f).collect_vec())?;

            let mut trans = conn.begin().await?;
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

    fn _partition<A, B, F: Filter>(mut items: Vec<(A, B, F)>) -> Option<(Vec<(A, B, F)>, Vec<(A, B, F)>)> {
        // Pick a starting item
        let mut group = vec![{
            let (_, index) = items.iter().enumerate()
                .map(|row|(row.1.2.count_ones(), row.0))
                .min()?;
            let item = items.swap_remove(index);
            if item.2.count_zeros() == 0 {
                return None
            }
            item
        }];

        // Expand until we take half or  might lose more than half our zeros
        let mut group_cover = group[0].2.clone();
        let initial_zeros = group_cover.count_zeros();
        if initial_zeros <= 2 {
            return None
        }

        while group.len() + 1 < items.len() {
            let (new_cover, index) = items.iter().enumerate()
                .map(|row|(row.1.2.overlap(&group_cover).unwrap(), row.0))
                .min_by_key(|row|row.0.count_zeros())?;

            if group.len() > items.len()/3 {
                if new_cover.count_zeros() < initial_zeros/2 {
                    break
                };
            }

            group_cover = new_cover;
            group.push(items.swap_remove(index));
        }

        if group_cover.full() {
            return None
        }

        // Return remains
        return Some((group, items))
    }

    // fn _cover_on<A, B>(items: &Vec<(A, B, BloomFilter)>) -> BloomFilter {
    //     let size = items[0].2.params();
    //     items.iter().fold(BloomFilter::empty(size.0, size.1, size.2), |a, b|a.overlap(&b.2).unwrap())
    // }

    async fn _new_group<'e, E, F: Filter>(&self, conn: E, index_group: &IndexGroup, leaves: bool, parent: Option<i64>, filter: &F) -> Result<i64>
    where E: sqlx::Executor<'e, Database = sqlx::Any>
    {
        let filter_table = filter_table_name(index_group);
        let result = sqlx::query(&format!("INSERT INTO {filter_table}(leaves, block, filter, kind) VALUES(?, ?, ?, ?)"))
            .bind(leaves)
            .bind(parent)
            .bind(filter.to_buffer()?)
            .bind(filter.kind())
            .execute(conn).await?;
        match result.last_insert_id() {
            Some(id) => Ok(id),
            None => Err(anyhow::anyhow!("Could not insert")),
        }
    }


    async fn _load_files_in_group<'e, E, F: LoadFilter>(&self, conn: E, index_group: &IndexGroup, group: i64) -> Result<Vec<(Vec<u8>, AccessControl, F)>>
    where E: sqlx::Executor<'e, Database = sqlx::Any>
    {
        let file_table = file_table_name(&index_group);
        let rows: Vec<(Vec<u8>, String, String, Vec<u8>)> = query_as(&format!("
            SELECT hash, access, kind, filter FROM {file_table}
            WHERE block = ?"))
            .bind(group)
            .fetch_all(conn).await?;
        let mut out = vec![];
        for (hash, access, kind, filter) in rows {
            let filter = match F::load(&kind, &filter) {
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

    async fn _load_groups_in_group<'e, E, F: LoadFilter>(&self, conn: E, index_group: &IndexGroup, group: i64) -> Result<Vec<(i64, bool, F)>>
    where E: sqlx::Executor<'e, Database = sqlx::Any>
    {
        let filter_table = filter_table_name(&index_group);
        let rows: Vec<(i64, bool, String, Vec<u8>)> = query_as(&format!("
            SELECT id, leaves, kind, filter FROM {filter_table}
            WHERE block = ?"))
            .bind(group)
            .fetch_all(conn).await?;
        let mut out = vec![];
        for (id, leave, kind, filter) in rows {
            let filter = match F::load(&kind, &filter) {
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


    pub async fn release_groups(&self, id: IndexGroup) -> Result<()> {
        let mut changed = false;
        for group in self.list_indices().await? {
            if group <= id {
                self._release_group(group).await?;
                changed = true;
            }
        }

        // adjust cache
        if changed {
            let mut count = self.status.lock().await;
            *count.get_mut() = self._init_status().await?;
        }

        return Ok(())
    }

    async fn _release_group(&self, entry: IndexGroup) -> Result<()> {
        info!("Garbage collecting index for {}", entry);

        // In a transaction
        let mut trans = self.db.begin().await?;

        // Delete group table
        let file_table = file_table_name(&entry);
        sqlx::query(&format!("DROP TABLE IF EXISTS {file_table}")).execute(&mut trans).await?;
        let filter_table = filter_table_name(&entry);
        sqlx::query(&format!("DROP TABLE IF EXISTS {filter_table}")).execute(&mut trans).await?;

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
            .bind(SearchStage::Queued.to_string())
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
                group_hits: 0,
                group_misses: 0,
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

        let pending_files: i64 = if stage == SearchStage::Yara {
            let (pending_files, ): (f64, ) = sqlx::query_as(
                "SELECT CAST(SUM(hash_count) as DOUBLE) FROM yara_tasks WHERE search = ?")
                .bind(&code)
                .fetch_one(&mut conn).await.context("Couldn't list yara tasks for search")?;
            pending_files as i64
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
                    group_hits: search.group_hits,
                    group_misses: search.group_misses,
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
    where E: sqlx::Executor<'e, Database = sqlx::Any>
    {
        let req = sqlx::query(
            "UPDATE yara_tasks SET assigned_worker = NULL, assigned_time = NULL
            WHERE id = ?")
            .bind(id)
            .execute(conn).await?;
        return Ok(req.rows_affected() > 0)
    }

    async fn _get_search<'e, E>(&self, conn: E, code: &str) -> Result<Option<(SearchStage, SearchRecord)>>
    where E: sqlx::Executor<'e, Database = sqlx::Any>
    {
        let search: Option<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT stage, data FROM searches WHERE code = ? LIMIT 1")
            .bind(&code)
            .fetch_optional(conn).await?;

        Ok(match search {
            Some((stage, search)) => Some((SearchStage::from(stage.as_str()), postcard::from_bytes(&search)?)),
            None => None,
        })
    }

    pub async fn get_queued_or_filtering_searches(&self) -> Result<(Vec<String>, Vec<String>)> {
        let searches: Vec<(String, String)> = sqlx::query_as(
            "SELECT code, stage FROM searches WHERE stage = ? OR stage = ?")
            .bind(SearchStage::Queued.as_str())
            .bind(SearchStage::Filtering.as_str())
            .fetch_all(&self.db).await?;

        let mut queued = vec![];
        let mut filtering = vec![];

        for (code, stage) in searches {
            let stage = SearchStage::from(stage.as_str());
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
            .bind(stage.as_str())
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

        let mut accepted_files: HashSet<Vec<u8>> = Default::default();
        let mut rejected_files: HashSet<Vec<u8>> = Default::default();
        let mut group_misses: u64 = 0;
        let mut group_hits: u64 = 0;
        for index_group in indices {
            // Load the base filters for this index
            let mut outstanding_groups = self._find_all_roots::<GenericFilter>(&mut *conn, &index_group).await?;

            // Apply filters recursively until we only have file entries left
            while let Some((group_id, leaves, filter)) = outstanding_groups.pop() {
                if !filter.query(&record.query) {
                    group_misses += 1;
                    continue
                }
                group_hits += 1;

                if leaves {
                    for (hash, access, filter) in self._load_files_in_group::<_, GenericFilter>(&mut *conn, &index_group, group_id).await? {
                        if rejected_files.contains(&hash) { continue }
                        if !access.can_access(&record.access) || !filter.query(&record.query) {
                            rejected_files.insert(hash);
                            continue
                        }
                        accepted_files.insert(hash);
                    }
                } else {
                    outstanding_groups.extend(self._load_groups_in_group::<_, GenericFilter>(&mut *conn, &index_group, group_id).await?);
                }
            }
        }

        let mut trans = conn.begin().await?;

        // Create yara jobs for these files
        let files: Vec<Vec<u8>> = accepted_files.into_iter().collect();
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
        record.group_hits = group_hits;
        record.group_misses = group_misses;
        sqlx::query(
            "UPDATE searches SET data = ?, stage = ? WHERE code = ?")
            .bind(&postcard::to_allocvec(&record)?)
            .bind(stage.as_str())
            .bind(&code)
            .execute(&mut trans).await?;

        // Apply search result
        Ok(trans.commit().await?)
    }

    pub async fn simulate_filter(&self, query: &Query) -> Result<()> {
        // Get the search in question
        let mut conn = self.db.acquire().await?;

        // Load the set of indices in the range for this job
        let mut indices = self.list_indices().await?;
        indices.sort_unstable();
        let status = self.status().await?;

        for index_group in indices {
            println!("Group: {} {}", index_group, status.group_files.get(index_group.as_str()).unwrap_or(&0));
            let mut keys: BTreeSet<String> = Default::default();
            let mut group_misses: HashMap<String, u64> = Default::default();
            let mut group_hits: HashMap<String, u64> = Default::default();
            let mut accepted_files: HashMap<String, u64> = Default::default();
            let mut rejected_files: HashMap<String, u64> = Default::default();

            // Load the base filters for this index
            let mut outstanding_groups = self._find_all_roots::<GenericFilter>(&mut *conn, &index_group).await?;

            // Apply filters recursively until we only have file entries left
            while let Some((group_id, leaves, filter)) = outstanding_groups.pop() {
                let key = filter.kind();
                keys.insert(key.clone());

                if !filter.query(&query) {
                    group_misses.insert(key.clone(), group_misses.get(&key).unwrap_or(&0) + 1);
                    continue
                }
                group_hits.insert(key.clone(), group_hits.get(&key).unwrap_or(&0) + 1);

                if leaves {
                    for (hash, access, filter) in self._load_files_in_group::<_, GenericFilter>(&mut *conn, &index_group, group_id).await? {
                        // if rejected_files.contains(&hash) { continue }
                        if !filter.query(&query) {
                            rejected_files.insert(key.clone(), rejected_files.get(&key).unwrap_or(&0) + 1);
                            continue
                        }
                        accepted_files.insert(key.clone(), accepted_files.get(&key).unwrap_or(&0) + 1);
                    }
                } else {
                    outstanding_groups.extend(self._load_groups_in_group::<_, GenericFilter>(&mut *conn, &index_group, group_id).await?);
                }
            }
            for key in keys {
                println!("\t{key} group {}/{} files {}/{}", group_hits.get(&key).unwrap_or(&0), group_misses.get(&key).unwrap_or(&0), accepted_files.get(&key).unwrap_or(&0), rejected_files.get(&key).unwrap_or(&0));
            }
            println!("");
        }
        return Ok(())
    }


    pub async fn traverse_sample(&self) -> Result<()> {
        let mut conn = self.db.acquire().await?;
        let mut indices = self.list_indices().await?;
        indices.sort_unstable();

        for index_group in indices {
            println!("{}", index_group.as_str());
            // Get the root groups
            let mut roots = self._find_all_roots::<GenericFilter>(&mut *conn, &index_group).await.context("_find_all_roots")?;
            if roots.is_empty() {
                continue
            }
            roots.sort_unstable_by_key(|row| row.2.kind());

            for root in roots {
                let root_kind = root.2.kind();
                let mut  group_stack = vec![root];
                while !group_stack.last().unwrap().1 {
                    // Get the subgroups of this group
                    let mut subgroups = self._load_groups_in_group(&mut *conn, &index_group, group_stack.last().unwrap().0).await?;
                    assert!(!subgroups.is_empty());

                    // select the best of the subgroups
                    let index = thread_rng().gen_range(0..subgroups.len());
                    group_stack.push(subgroups.swap_remove(index));
                }

                println!("\t{}", root_kind);
                for (id, leaf, mask) in group_stack {
                    println!("\t\t{}", mask.density())
                }
            }
        }

        return Ok(())
    }

    pub async fn test_shallow_group(&self, query: Query) -> Result<()> {
        todo!();
        // let mut conn = self.db.acquire().await?;
        // let mut indices = self.list_indices().await?;
        // indices.sort_unstable();
        // indices.truncate(12);

        // // let group_id_counter: i64 = 0;
        // let mut grouping: Vec<(String, GenericFilter, u64)> = Default::default();
        // let status = self.status().await?;

        // for index_group in indices {
        //     println!("Group: {} {}", index_group, status.group_files.get(index_group.as_str()).unwrap_or(&0));

        //     // Load the base filters for this index
        //     let mut outstanding_groups = self._find_all_roots::<GenericFilter>(&mut *conn, &index_group).await?;

        //     let mut active: BTreeMap<String, Vec<InsertCursor>> = Default::default();

        //     // Apply filters recursively until we only have file entries left
        //     while let Some((group_id, leaves, filter)) = outstanding_groups.pop() {
        //         let key = filter.kind();

        //         let group_target: f64 = if key.starts_with("in") && key.ends_with(":1:1") {
        //             0.01
        //         } else if key.starts_with("in") && key.ends_with(":2:3") {
        //             0.05
        //         } else if key.starts_with("in") && key.ends_with(":3:5") {
        //             0.10
        //         } else if key.starts_with("tri") {
        //             0.10
        //         } else {
        //             todo!();
        //         };
        //         let group_target = 1.0-(1.0-group_target).powi(6);

        //         if leaves {
        //             for (_hash, _access, filter) in self._load_files_in_group::<_, GenericFilter>(&mut *conn, &index_group, group_id).await? {
        //                 match active.entry(key.clone()) {
        //                     std::collections::btree_map::Entry::Vacant(entry) => {
        //                         entry.insert(vec![InsertCursor::new(filter, group_target)]);
        //                     },
        //                     std::collections::btree_map::Entry::Occupied(mut entry) => {
        //                         entry.get_mut().push(InsertCursor::new(filter, group_target));
        //                     },
        //                 }
        //             }
        //         } else {
        //             outstanding_groups.extend(self._load_groups_in_group::<_, GenericFilter>(&mut *conn, &index_group, group_id).await?);
        //         }
        //     }

        //     println!("Loaded, starting grouping");
        //     let mut batches: Vec<(String, Vec<InsertCursor>)> = vec![];
        //     for (kind, items) in active {
        //         for b in &items.into_iter().chunks(100) {
        //             batches.push((kind.clone(), b.collect_vec()));
        //         }
        //     }

        //     while let Some((kind, mut active)) = batches.pop() {
        //         let mut ready_items: HashSet<usize> = Default::default();
        //         for (id, (group_kind, filter, _count)) in grouping.iter().enumerate() {
        //             if kind != *group_kind { continue; }
        //             for (index, item) in active.iter_mut().enumerate() {
        //                 if ready_items.contains(&index) { continue }
        //                 if item.add(id as i64, filter) {
        //                     ready_items.insert(index);
        //                 }
        //             }
        //         }
        //         let mut ready_items = ready_items.into_iter().collect_vec();
        //         ready_items.sort_unstable();
        //         while let Some(index) = ready_items.pop() {
        //             let mut item = active.swap_remove(index);
        //             let (group_id, _, _) = item.next().unwrap();
        //             grouping[group_id as usize].2 += 1;
        //         }

        //         println!("{} {} {}", index_group, grouping.len(), batches.len());
        //         while let Some(mut item) = active.pop() {
        //             match item.next() {
        //                 Some((id, original, changed)) => {
        //                     if original != grouping[id as usize].1.count_ones() {
        //                         item.add(id, &grouping[id as usize].1);
        //                         active.push(item);
        //                         continue;
        //                     } else {
        //                         grouping[id as usize].2 += 1;
        //                         if changed != 0 {
        //                             grouping[id as usize].1 = grouping[id as usize].1.overlap(&item.filter);
        //                         }
        //                     }
        //                 },
        //                 None => {
        //                     let index = grouping.len() as i64;
        //                     for other in active.iter_mut() {
        //                         other.add(index, &item.filter);
        //                     }
        //                     grouping.push((kind.clone(), item.filter, 1));
        //                 },
        //             }
        //         }
        //     }
        // }

        // let mut output = std::fs::OpenOptions::new().write(true).create(true).open("./groups2.csv")?;

        // for (kind, filter, items) in grouping {
        //     output.write_fmt(format_args!("{kind}, {}, {items}, {}\n", filter.density(), filter.query(&query)))?;
        // }
        // output.flush()?;

        return Ok(())
    }


    async fn get_yara_task(&self, conn: &mut PoolCon, worker: &String) -> Result<Vec<YaraTask>> {
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

    // pub async fn report_density(&self) -> Result<()> {
    //     let indices = self.list_indices().await?;
    //     let mut conn = self.db.acquire().await?;

    //     for index_group in indices {
    //         // Load the base filters for this index
    //         let outstanding_groups = self._find_all_roots(&mut conn, &index_group).await?;
    //         let mut outstanding_groups = outstanding_groups.into_iter().map(|row| (row.0, row.1, row.2, 0)).collect_vec();
    //         let mut densities: BTreeMap<i32, Vec<f64>> = Default::default();
    //         let mut file_count = 0.0;

    //         // Apply filters recursively until we only have file entries left
    //         while let Some((group_id, leaves, filter, height)) = outstanding_groups.pop() {
    //             let density = filter.density();
    //             match densities.get_mut(&height) {
    //                 Some(list) => { list.push(density); },
    //                 None => { densities.insert(height, vec![density]); },
    //             }

    //             if leaves {
    //                 let mut found = vec![];
    //                 for (_, _, filter) in self._load_files_in_group(&mut conn, &index_group, group_id).await? {
    //                     found.push(filter.density());
    //                     file_count += 1.0;
    //                 }
    //                 match densities.get_mut(&height) {
    //                     Some(list) => { list.extend(found); },
    //                     None => { densities.insert(height, found); },
    //                 }
    //             } else {
    //                 let new = self._load_groups_in_group(&mut conn, &index_group, group_id).await?;
    //                 outstanding_groups.extend(new.into_iter().map(|row| (row.0, row.1, row.2, height+1)));
    //             }
    //         }

    //         let densities = densities.into_iter().rev().map(|(a, b)|{
    //             let len = b.len() as f64;
    //             (a, b.into_iter().fold(0.0, |a, b|a+b)/len)
    //         }).collect_vec();
    //         println!("{file_count}  {:?}", densities);
    //     }

    //     return Ok(())
    // }

}

