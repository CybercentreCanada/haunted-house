
use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;
use crate::bloom::Filter;
use crate::core::{CoreConfig};
// use crate::database_rocksdb::RocksInterface;
use crate::database_sqlite::SQLiteInterface;
use crate::interface::{SearchRequest, InternalSearchStatus, WorkRequest, WorkPackage, WorkError};


#[derive(Default, Clone, Serialize, Deserialize)]
pub struct IndexStatus {
    pub group_files: HashMap<String, u64>,
    pub filter_sizes: HashMap<String, u64>
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Hash, PartialOrd, Ord, Debug)]
pub struct IndexGroup(String);

impl IndexGroup {
    pub fn create(expiry: &Option<DateTime<Utc>>) -> IndexGroup {
        IndexGroup(match expiry {
            Some(date) => format!("{}", date.format("%Y0%j")),
            None => format!("99990999"),
        })
    }

    pub fn from(data: &str) -> Self {
        Self(data.to_owned())
    }

    pub fn min() -> IndexGroup {
        IndexGroup(format!(""))
    }

    pub fn max() -> IndexGroup {
        IndexGroup(format!("99999999"))
    }

    // pub fn as_bytes<'a>(&'a self) -> &'a [u8] {
    //     self.0.as_bytes()
    // }

    pub fn as_str<'a>(&'a self) -> &'a str {
        &self.0
    }
}

impl std::fmt::Display for IndexGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// #[derive(Serialize, Deserialize, PartialEq, Eq, Clone, PartialOrd, Ord)]
// pub struct IndexID(String);

// impl IndexID {
//     pub fn new() -> Self {
//         IndexID(format!("{:x}", uuid::Uuid::new_v4().as_u128()))
//     }

//     pub fn from(data: &str) -> Self {
//         Self(data.to_owned())
//     }

//     // pub fn as_bytes<'a>(&'a self) -> &'a [u8] {
//     //     self.0.as_bytes()
//     // }

//     pub fn as_str<'a>(&'a self) -> &'a str {
//         &self.0
//     }
// }

// impl From<&str> for IndexID {
//     fn from(value: &str) -> Self {
//         IndexID(value.to_owned())
//     }
// }

// impl std::fmt::Display for IndexID {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.write_str(&self.0)
//     }
// }

// #[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Hash, Debug)]
// pub struct BlobID(String);

// impl BlobID {
//     pub fn new() -> Self {
//         Self(format!("{:x}", uuid::Uuid::new_v4().as_u128()))
//     }

//     // pub fn as_bytes<'a>(&'a self) -> &'a [u8] {
//     //     self.0.as_bytes()
//     // }

//     pub fn as_str<'a>(&'a self) -> &'a str {
//         &self.0
//     }
// }

// impl std::fmt::Display for BlobID {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.write_str(&self.0)
//     }
// }

// impl From<String> for BlobID { fn from(value: String) -> Self { Self(value) } }
// impl From<&str> for BlobID { fn from(value: &str) -> Self { Self(value.to_owned()) } }

// // #[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Hash, Debug)]
// // struct YaraTaskId(i64);

// // struct FilterTaskId(i64);

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Hash, Debug)]
pub enum SearchStage {
    Queued,
    Filtering,
    Yara,
    Finished,
}

impl From<&str> for SearchStage {
    fn from(value: &str) -> Self {
        let value = value.to_lowercase();
        if value == "queued" {
            SearchStage::Queued
        } else if value == "filtering" {
            SearchStage::Filtering
        } else if value == "yara" {
            SearchStage::Yara
        } else {
            SearchStage::Finished
        }
    }
}

impl std::fmt::Display for SearchStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SearchStage::Queued => "queued",
            SearchStage::Filtering => "filtering",
            SearchStage::Yara => "yara",
            SearchStage::Finished => "finished",
        })
    }
}

pub enum Database {
    SQLite(SQLiteInterface),
}

impl Database {

    pub async fn new_sqlite(config: CoreConfig, path: &str) -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new(config, path).await?))
    }

    pub async fn new_sqlite_temp(config: CoreConfig) -> Result<Self> {
        Ok(Database::SQLite(SQLiteInterface::new_temp(config).await?))
    }

    pub async fn partition_test(&self) -> Result<()> {
        match self {
            Database::SQLite(local) => local.partition_test().await,
        }
    }

    pub async fn update_file_access(&self, hash: &[u8], access: &AccessControl, index_group: &IndexGroup) -> Result<bool> {
        match self {
            Database::SQLite(local) => local.update_file_access(hash, access, index_group).await,
        }
    }

    pub async fn insert_file(&self, hash: &[u8], access: &AccessControl, index_group: &IndexGroup, filter: &Filter) -> Result<()> {
        match self {
            Database::SQLite(local) => local.insert_file(hash, access, index_group, filter).await,
        }
    }

    // pub async fn list_indices(&self) -> Result<Vec<IndexGroup>> {
    //     match self {
    //         Database::SQLite(local) => local.list_indices().await,
    //     }
    // }

    pub async fn list_filters(&self, kind: &str, goal: usize) -> Result<Vec<Filter>> {
        match self {
            Database::SQLite(local) => local.list_filters(kind, goal).await,
        }
    }

    pub async fn release_groups(&self, id: IndexGroup) -> Result<()> {
        match self {
            Database::SQLite(local) => local.release_groups(id).await,
        }
    }

    pub async fn initialize_search(&self, req: SearchRequest) -> Result<InternalSearchStatus> {
        match self {
            Database::SQLite(local) => local.initialize_search(req).await
        }
    }

    pub async fn search_status(&self, code: String) -> Result<Option<InternalSearchStatus>> {
        match self {
            Database::SQLite(local) => local.search_status(code).await
        }
    }

    pub async fn get_queued_or_filtering_searches(&self) -> Result<(Vec<String>, Vec<String>)> {
        match self {
            Database::SQLite(local) => local.get_queued_or_filtering_searches().await
        }
    }

    pub async fn set_search_stage(&self, code: &str, stage: SearchStage) -> Result<()> {
        match self {
            Database::SQLite(local) => local.set_search_stage(code, stage).await
        }
    }

    pub async fn filter_search(&self, code: &str) -> Result<()> {
        match self {
            Database::SQLite(local) => local.filter_search(code).await
        }
    }

    pub async fn status(&self) -> Result<IndexStatus> {
        match self {
            Database::SQLite(local) => local.status().await
        }
    }

    pub async fn get_work(&self, req: &WorkRequest) -> Result<WorkPackage> {
        match self {
            Database::SQLite(local) => local.get_work(req).await
        }
    }

    pub async fn release_assignments_before(&self, time: chrono::DateTime<chrono::Utc>) -> Result<u64> {
        match self {
            Database::SQLite(local) => local.release_tasks_assigned_before(time).await
        }
    }

    pub async fn release_yara_task(&self, id: i64) -> Result<bool> {
        match self {
            Database::SQLite(local) => local.release_yara_task(id).await
        }
    }

    pub async fn get_yara_assignments_before(&self, time: chrono::DateTime<chrono::Utc>) -> Result<Vec<(String, i64)>> {
        match self {
            Database::SQLite(local) => local.get_yara_assignments_before(time).await
        }
    }

    pub async fn get_work_notification(&self) -> Result<()> {
        match self {
            Database::SQLite(local) => local.get_work_notification().await
        }
    }

    pub async fn finish_yara_work(&self, id: i64, search: &String, hashes: Vec<Vec<u8>>) -> Result<()> {
        match self {
            Database::SQLite(local) => local.finish_yara_work(id, search, hashes).await
        }
    }

    pub async fn add_work_error(&self, err: WorkError) -> Result<()> {
        match self {
            Database::SQLite(local) => local.add_work_error(err).await
        }
    }

    pub async fn add_search_error(&self, code: &str, error: &str) -> Result<()> {
        match self {
            Database::SQLite(local) => local.add_search_error(code, error).await
        }
    }
}

