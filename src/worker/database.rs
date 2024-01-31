
use std::collections::{HashSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use assemblyline_markings::classification::ClassificationParser;
use tokio::sync::oneshot;

use super::database_sqlite::{SQLiteCommand, BufferedSQLite};
use crate::access::AccessControl;
use crate::error::ErrorKinds;
use crate::types::{ExpiryGroup, FilterID, Sha256, FileInfo};

type Result<T> = core::result::Result<T, ErrorKinds>;

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
    SQLite(tokio::sync::mpsc::Sender<SQLiteCommand>)
}

impl Database {
    pub async fn new_sqlite(directory: PathBuf, ce: Arc<ClassificationParser>) -> Result<Self> {
        Ok(Database::SQLite(BufferedSQLite::start(directory, ce).await?))
    }

    pub async fn create_filter(&self, id: FilterID, expiry: &ExpiryGroup) -> Result<()> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::CreateFilter { id, expiry: expiry.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::GetFilters { first: first.clone(), last: last.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::GetExpiry { first: first.clone(), last: last.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn delete_filter(&self, id: FilterID) -> Result<()> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::DeleteFilter { id, response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_file_access(&self, id: FilterID, hash: &Sha256) -> Result<Option<(AccessControl, String)>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::GetFileAccess { id, hash: hash.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn filter_sizes(&self) -> Result<HashMap<FilterID, u64>> {
        Ok(match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::FilterSizes { response: send }).await?;
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
                chan.send(SQLiteCommand::FilterPending { response: send }).await?;
                resp.await?
            }
        })
    }

    pub async fn update_file_access(&self, files: Vec<FileInfo>) -> Result<IngestStatusBundle> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::UpdateFileAccess { files: files.clone(), response: send }).await?;

                let mut files: HashSet<Sha256> = files.into_iter().map(|file|file.hash).collect();

                // Collect the results
                let mut unproc_ready = vec![];
                let mut unproc_pending = vec![];
                for resp in resp.await? {
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
        }
    }

    pub async fn check_insert_status(&self, files: Vec<(FilterID, FileInfo)>) -> Result<Vec<(FilterID, FileInfo, IngestStatus)>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::CheckInsertStatus { files, response: send }).await?;

                let mut results = vec![];
                for part in resp.await? {
                    let part = part.await??;
                    results.extend(part.into_iter());
                }
                return Ok(results)
            }
        }
    }

    pub async fn ingest_files(&self, files: Vec<(FilterID, FileInfo)>) -> Result<Vec<(FilterID, FileInfo)>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::IngestFiles { files, response: send }).await?;

                let mut results = vec![];
                for part in resp.await? {
                    results.extend(part.await??);
                }
                return Ok(results)
            }
        }
    }

    pub async fn select_files(&self, id: FilterID, file_indices: &[u64], access: &HashSet<String>) -> Result<Vec<FileInfo>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::SelectFiles { id, file_indices: file_indices.to_owned(), access: access.clone(), response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn get_ingest_batch(&self, id: FilterID, limit: u32) -> Result<Vec<(u64, Sha256)>> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::GetIngestBatch { id, limit, response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn finished_ingest(&self, id: FilterID, files: Vec<(u64, Sha256)>) -> Result<f64> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::FinishedIngest { id, files, response: send }).await?;
                resp.await?
            }
        }
    }

    pub async fn abandon_files(&self, files: Vec<(FilterID, Sha256)>) -> Result<()> {
        match self {
            Database::SQLite(chan) => {
                let (send, resp) = oneshot::channel();
                chan.send(SQLiteCommand::AbandonFiles { files, response: send }).await?;
                resp.await?
            }
        }
    }
}

