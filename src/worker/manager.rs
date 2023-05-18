use std::collections::{HashSet, HashMap};
use std::path::PathBuf;
// use std::collections::HashMap;
// use std::fmt::Display;
use std::sync::Arc;
// use std::time::Duration;
use anyhow::{Context, Result};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::blob_cache::{BlobHandle, BlobCache};
use crate::error::ErrorKinds;
use crate::query::Query;
use crate::types::{ExpiryGroup, Sha256, FilterID, FileInfo};

use super::YaraTask;
use super::database::{IngestStatus, Database};
use super::filter_worker::FilterWorker;
use super::interface::{FilterSearchResponse, UpdateFileInfoResponse};


#[derive(Serialize, Deserialize)]
struct WorkerConfig {
    filter_item_limit: u64,
    data_path: PathBuf,
    data_limit: u64,
    data_reserve: u64
}

pub struct WorkerState {
    database: Database,
    filters: tokio::sync::RwLock<HashMap<FilterID, FilterWorker>>,
    file_cache: BlobCache,
    config: WorkerConfig
}

impl WorkerState {

    pub async fn new(database: Database, file_cache: BlobCache, config: WorkerConfig) -> Result<Self> {
        // Start workers for every filter
        let mut filters: HashMap<FilterID, FilterWorker> = Default::default();
        for id in database.get_filters(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
            filters.insert(id, FilterWorker::open(&config.data_path, id)?);
        }

        Ok(Self {
            database,
            filters: tokio::sync::RwLock::new(filters),
            file_cache,
            config,
        })
    }

    pub async fn create_index(&self, id: FilterID, expiry: ExpiryGroup) -> Result<()> {
        self.database.create_filter(id, &expiry).await?;
        let worker = FilterWorker::new(&self.config.data_path, id)?;
        let mut filters = self.filters.write().await;
        filters.insert(id, worker);
        return Ok(())
    }

    pub async fn delete_index(&self, id: FilterID) -> Result<()> {
        self.database.delete_filter(id).await
    }

    pub async fn update_files(&self, files: Vec<FileInfo>) -> Result<UpdateFileInfoResponse> {
        //
        let mut processed = vec![];
        let mut pending: HashMap<Sha256, FilterID> = Default::default();
        let mut missing = vec![];
        for file in files {
            match self.database.update_file_access(&file).await? {
                IngestStatus::Ready => processed.push(file.hash),
                IngestStatus::Pending(filter) => { pending.insert(file.hash, filter); },
                IngestStatus::Missing => missing.push(file),
            }
        }

        //
        let filter_sizes = self.database.filter_sizes().await;
        let filters = self.get_expiry(&ExpiryGroup::min(), &ExpiryGroup::max()).await?;
        let mut assignments: HashMap<Sha256, Vec<FilterID>> = Default::default();
        let storage_pressure = self.check_storage_pressure().await?;
        if !storage_pressure {
            for file in missing {
                let mut selected = vec![];
                for (id, expiry) in &filters {
                    if *expiry == file.expiry {
                        if *filter_sizes.get(id).unwrap_or(&u64::MAX) < self.config.filter_item_limit {
                            selected.push(*id);
                        }
                    }
                }
                assignments.insert(file.hash, selected);
            }
        }

        Ok(UpdateFileInfoResponse {
            processed,
            pending,
            assignments,
            storage_pressure,
            filter_sizes,
            filter_pending: self.database.filter_pending().await?,
        })
    }

    pub async fn check_storage_pressure(&self) -> Result<bool> {
        let mut total = 0u64;
        let mut dirs = vec![self.config.data_path.clone()];
        while let Some(dir) = dirs.pop() {
            let mut listing = tokio::fs::read_dir(&dir).await?;
            while let Some(file) = listing.next_entry().await? {
                let file_type = file.file_type().await?;
                if file_type.is_dir() {
                    dirs.push(file.path())
                }
                else if file_type.is_file() {
                    total += file.metadata().await?.len();
                }
            }
        }

        return Ok(total >= self.config.data_limit - self.config.data_reserve)
    }

    pub async fn ingest_file(&self, id: FilterID, info: &FileInfo) -> core::result::Result<bool, ErrorKinds> {
        self.database.ingest_file(id, info).await
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>> {
        Ok(self.database.get_filters(first, last).await?)
    }

    pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>> {
        Ok(self.database.get_expiry(first, last).await?)
    }

    pub async fn is_ready(&self) -> Result<bool> {
        // Get the most comprehensive list of filters we can
        let mut ids = self.get_filters(&ExpiryGroup::min(), &ExpiryGroup::max()).await?;
        let filters = self.filters.read().await;
        ids.extend(filters.keys());

        // Make sure they are all ready
        for id in ids {
            match filters.get(&id) {
                Some(filter) => if !filter.is_ready() {
                    return Ok(false);
                }
                None => return Ok(false)
            }
        }
        return Ok(true)
    }

    pub async fn query_filter(self: Arc<Self>, id: FilterID, query: Query, access: HashSet<String>, respond: mpsc::Sender<FilterSearchResponse>) {
        if let Some(filter) = self.filters.read().await.get(&id) {
            match filter.query(query).await {
                Ok(file_indices) => {
                    match self.database.select_file_hashes(id, &file_indices, &access).await {
                        Ok(files) => respond.send(FilterSearchResponse::Candidates(files)),
                        Err(err) => respond.send(FilterSearchResponse::Error(err.to_string()))

                    };
                }
                Err(err) => { respond.send(FilterSearchResponse::Error(err.to_string())); },
            };
        }
    }

    pub async fn run_yara(&self, yara_task: YaraTask) -> Result<(Vec<Sha256>, Vec<String>)> {
        debug!("yara task {} starting", yara_task.id);
        let mut errors = vec![];
        let filter_handle = {
            let (file_send, mut file_recv) = mpsc::unbounded_channel::<(Sha256, BlobHandle)>();

            // Run the interaction with yara in a blocking thread
            let filter_handle = tokio::task::spawn_blocking(move || -> Result<Vec<Sha256>> {
                debug!("yara task {} launched yara worker", yara_task.id);
                // Compile the yara rules
                let compiler = yara::Compiler::new()?
                    .add_rules_str(&yara_task.yara_rule)?;
                let rules = compiler.compile_rules()?;
                debug!("yara task {} yara ready", yara_task.id);

                // Try
                let mut selected = vec![];
                while let Some((hash, handle)) = file_recv.blocking_recv() {
                    debug!("yara task {} processing {}", yara_task.id, handle.id());
                    let result = rules.scan_file(handle.path(), 60 * 30)?;
                    if !result.is_empty() {
                        selected.push(hash);
                    }
                }
                debug!("yara task {} yara finished", yara_task.id);
                return Ok(selected);
            });

            // Load the files and send them to the worker
            for hash in yara_task.hashes {
                // download the file
                let hash_string = hash.hex();
                debug!("yara task {} waiting for {}", yara_task.id, hash_string);
                match self.file_cache.open(hash_string.clone()).await {
                    Ok(blob) => file_send.send((hash, blob))?,
                    Err(err) => {
                        let error_string = format!("File not available: {hash_string} {err}");
                        info!("{error_string}");
                        errors.push(error_string);
                    },
                }
            }

            filter_handle
        };

        // Wait for worker to finish
        let filtered = filter_handle.await??;

        // Report the result to the broker
        info!("yara task {} finished ({} hits)", yara_task.id, filtered.len());
        return Ok((filtered, errors))
    }

}



