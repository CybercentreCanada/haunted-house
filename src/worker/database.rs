
use std::collections::{HashSet, HashMap};
use std::path::PathBuf;

use anyhow::{Result, Context};
use tokio::sync::oneshot;

use super::database_buffered_sqlite::{BSQLCommand, BufferedSQLite};
use super::database_sqlite::SQLiteInterface;
use crate::access::AccessControl;
use crate::error::ErrorKinds;
use crate::types::{ExpiryGroup, FilterID, Sha256, FileInfo};

#[derive(Debug)]
pub enum IngestStatus {
    Ready,
    Pending(FilterID),
    Missing
}

#[derive(Debug, Default)]
pub struct IngestStatusBundle {
    pub ready: Vec<Sha256>,
    pub pending: HashMap<Sha256, FilterID>,
    pub missing: Vec<Sha256>
}

pub enum Database {
    SQLite(SQLiteInterface),
    Buffered(tokio::sync::mpsc::Sender<BSQLCommand>)
}

impl Database {

    pub async fn new_sqlite(directory: PathBuf) -> Result<Self> {
        // Ok(Database::SQLite(SQLiteInterface::new(path).await?))
        Ok(Database::Buffered(BufferedSQLite::new(directory).await?))
    }

    // pub async fn new_sqlite_temp() -> Result<Self> {
    //     Ok(Database::SQLite(SQLiteInterface::new_temp().await?))
    // }

    pub async fn create_filter(&self, id: FilterID, expiry: &ExpiryGroup) -> Result<()> {
        match self {
            Database::SQLite(db) => db.create_filter(id, expiry).await.context("create_filter"),
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::CreateFilter { id, expiry: expiry.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>, ErrorKinds> {
        match self {
            Database::SQLite(db) => db.get_filters(first, last).await,
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::GetFilters { first: first.clone(), last: last.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>, ErrorKinds> {
        match self {
            Database::SQLite(db) => db.get_expiry(first, last).await,
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::GetExpiry { first: first.clone(), last: last.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn delete_filter(&self, id: FilterID) -> Result<()> {
        match self {
            Database::SQLite(db) => db.delete_filter(id).await.context("delete_filter"),
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::DeleteFilter { id, response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_file_access(&self, id: FilterID, hash: &Sha256) -> Result<Option<AccessControl>> {
        match self {
            Database::SQLite(db) => db.get_file_access(id, hash).await.context("get_fileinfo"),
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::GetFileAccess { id, hash: hash.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn filter_sizes(&self) -> Result<HashMap<FilterID, u64>> {
        Ok(match self {
            Database::SQLite(db) => db.filter_sizes().await,
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::FilterSizes { response: send }).await?;
                resp.await?
            }
        })
    }

    // pub async fn filter_pending_count(&self) -> Result<HashMap<FilterID, u64>> {
    //     Ok(match self {
    //         Database::SQLite(db) => db.filter_pending_count().await,
    //         Database::Buffered(chan) => {
    //             let (send, resp) = oneshot::channel();
    //             chan.send(BSQLCommand::FilterPendingCount { response: send }).await?;
    //             resp.await?
    //         }
    //     })
    // }

    pub async fn filter_pending(&self) -> Result<HashMap<FilterID, HashSet<Sha256>>> {
        Ok(match self {
            Database::SQLite(db) => db.filter_pending().await,
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::FilterPending { response: send }).await?;
                resp.await?
            }
        })
    }

    pub async fn update_file_access(&self, files: Vec<FileInfo>) -> Result<IngestStatusBundle> {
        match self {
            Database::SQLite(_db) => todo!(), //db.update_file_access(file).await.context("update_file_access"),
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::UpdateFileAccess { files, response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn check_insert_status(&self, id: FilterID, file: &FileInfo) -> Result<IngestStatus, ErrorKinds> {
        match self {
            Database::SQLite(db) => db.check_insert_status(id, file).await,
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::CheckInsertStatus { id, file: file.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn ingest_file(&self, id: FilterID, file: &FileInfo) -> core::result::Result<bool, ErrorKinds> {
        match self {
            Database::SQLite(db) => db.ingest_file(id, file).await,
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::IngestFile { id, file: file.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn select_file_hashes(&self, id: FilterID, file_indices: &Vec<u64>, access: &HashSet<String>) -> Result<Vec<Sha256>> {
        match self {
            Database::SQLite(db) => db.select_file_hashes(id, file_indices, access).await.context("select_file_hashes"),
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::SelectFileHashes { id, file_indices: file_indices.clone(), access: access.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_ingest_batch(&self, id: FilterID, limit: u32) -> Result<Vec<(u64, Sha256)>> {
        match self {
            Database::SQLite(db) => db.get_ingest_batch(id, limit).await.context("get_ingest_batch"),
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::GetIngestBatch { id, limit, response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn finished_ingest(&self, id: FilterID, files: Vec<(u64, Sha256)>) -> Result<()> {
        match self {
            Database::SQLite(db) => db.finished_ingest(id, files).await.context("finished_ingest"),
            Database::Buffered(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::FinishedIngest { id, files, response: send }).await?;
                resp.await?
            }
        }
    }
}

