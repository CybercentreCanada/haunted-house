
use std::collections::{HashSet, HashMap};
use std::path::PathBuf;

use anyhow::Result;
use tokio::sync::oneshot;

use super::database_sqlite::{BSQLCommand, BufferedSQLite};
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
    SQLite(tokio::sync::mpsc::Sender<BSQLCommand>)
}

impl Database {

    pub async fn new_sqlite(directory: PathBuf) -> Result<Self> {
        Ok(Database::SQLite(BufferedSQLite::new(directory).await?))
    }

    pub async fn create_filter(&self, id: FilterID, expiry: &ExpiryGroup) -> Result<()> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::CreateFilter { id, expiry: expiry.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>, ErrorKinds> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::GetFilters { first: first.clone(), last: last.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>, ErrorKinds> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::GetExpiry { first: first.clone(), last: last.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn delete_filter(&self, id: FilterID) -> Result<()> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::DeleteFilter { id, response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_file_access(&self, id: FilterID, hash: &Sha256) -> Result<Option<AccessControl>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::GetFileAccess { id, hash: hash.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn filter_sizes(&self) -> Result<HashMap<FilterID, u64>> {
        Ok(match self {
            Database::SQLite(chan) => {
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
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::FilterPending { response: send }).await?;
                resp.await?
            }
        })
    }

    pub async fn update_file_access(&self, files: Vec<FileInfo>) -> Result<IngestStatusBundle> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::UpdateFileAccess { files, response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn check_insert_status(&self, files: Vec<(FilterID, FileInfo)>) -> Result<Vec<(FilterID, FileInfo, IngestStatus)>, ErrorKinds> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::CheckInsertStatus { files, response: send }).await?;

                let mut results = vec![];
                for part in resp.await? {
                    let part = part.await??;
                    results.extend(part.into_iter());
                }
                return Ok(results)
            }
        }
    }

    pub async fn ingest_files(&self, files: Vec<(FilterID, FileInfo)>) -> Result<Vec<FileInfo>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::IngestFiles { files, response: send }).await?;

                let mut results = vec![];
                for part in resp.await? {
                    let part = part.await??;
                    results.extend(part.into_iter());
                }
                return Ok(results)
            }
        }
    }

    pub async fn select_file_hashes(&self, id: FilterID, file_indices: &Vec<u64>, access: &HashSet<String>) -> Result<Vec<Sha256>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::SelectFileHashes { id, file_indices: file_indices.clone(), access: access.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_ingest_batch(&self, id: FilterID, limit: u32) -> Result<Vec<(u64, Sha256)>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::GetIngestBatch { id, limit, response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn finished_ingest(&self, id: FilterID, files: Vec<(u64, Sha256)>) -> Result<f64> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(BSQLCommand::FinishedIngest { id, files, response: send }).await?;
                resp.await?
            }
        }
    }
}

