use std::collections::{HashSet, HashMap};
use std::path::PathBuf;
// use std::collections::HashMap;
// use std::fmt::Display;
use std::sync::Arc;
// use std::time::Duration;
use anyhow::Result;
use bitvec::vec::BitVec;
use itertools::Itertools;
use log::{debug, info, error};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch, Notify, oneshot};
use tokio::task::JoinSet;

use crate::blob_cache::{BlobHandle, BlobCache};
use crate::error::ErrorKinds;
use crate::query::Query;
use crate::storage::BlobStorage;
use crate::types::{ExpiryGroup, Sha256, FilterID, FileInfo};

use super::YaraTask;
use super::database::{IngestStatus, Database};
use super::filter_worker::{FilterWorker, WriterCommand};
use super::interface::{FilterSearchResponse, UpdateFileInfoResponse, IngestFilesResponse};


#[derive(Serialize, Deserialize, Clone)]
pub struct WorkerConfig {
    pub filter_item_limit: u64,
    pub data_path: PathBuf,
    pub data_limit: u64,
    pub data_reserve: u64,
    pub initial_segment_size: u32,
    pub extended_segment_size: u32,
    pub ingest_batch_size: u32,
}

pub struct WorkerState {
    pub database: Arc<Database>,
    pub running: watch::Receiver<bool>,
    pub filters: tokio::sync::RwLock<HashMap<FilterID, (FilterWorker, Arc<Notify>)>>,
    pub file_storage: BlobStorage,
    pub file_cache: BlobCache,
    pub config: WorkerConfig,
}

impl WorkerState {

    pub async fn new(database: Arc<Database>, file_storage: BlobStorage, file_cache: BlobCache, config: WorkerConfig, running: watch::Receiver<bool>) -> Result<Arc<Self>> {
        let new = Arc::new(Self {
            database,
            running,
            filters: tokio::sync::RwLock::new(Default::default()),
            file_cache,
            file_storage,
            config,
        });

        // Start workers for every filter
        {
            let mut filters = new.filters.write().await;
            for id in new.database.get_filters(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
                let notify = Arc::new(tokio::sync::Notify::new());
                let worker = FilterWorker::open(new.config.clone(), id)?;
                tokio::spawn(new.clone().ingest_feeder(id, worker.writer_connection.clone(), notify.clone()));
                filters.insert(id, (worker, notify));
            }
        }

        Ok(new)
    }

    pub async fn create_index(self: &Arc<Self>, id: FilterID, expiry: ExpiryGroup) -> Result<()> {
        self.database.create_filter(id, &expiry).await?;
        let worker = FilterWorker::open(self.config.clone(), id)?;
        let notify = Arc::new(tokio::sync::Notify::new());
        tokio::spawn(self.clone().ingest_feeder(id, worker.writer_connection.clone(), notify.clone()));
        let mut filters = self.filters.write().await;
        filters.insert(id, (worker, notify));
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
            filter_pending: self.database.filter_pending_count().await,
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

    pub async fn ingest_file(self: &Arc<Self>, mut files: Vec<(FilterID, FileInfo)>) -> Result<IngestFilesResponse> {
        // Filter those we already have information for quickly
        // the ones that are currently pending can be dropped
        let pending = self.database.filter_pending().await;
        files.retain(|(filter, file)|!pending.get(filter).unwrap_or(&Default::default()).contains(&file.hash));

        // Gather information for each file individually next
        let mut workers: JoinSet<std::result::Result<(bool, FilterID, Sha256), ErrorKinds>> = JoinSet::new();
        let mut completed = vec![];
        let mut modified_filters = vec![];
        let mut unknown_filters = vec![];
        while !workers.is_empty() || !files.is_empty() {
            while !files.is_empty() && workers.len() < 10 {
                let core = self.clone();
                if let Some((filter, file)) = files.pop() {
                    workers.spawn(async move {
                        // Check if the file is already inserted
                        let status = core.database.check_insert_status(filter, &file).await?;
                        match status {
                            IngestStatus::Ready => return Ok((true, filter, file.hash)),
                            IngestStatus::Pending(filter) => return Ok((false, filter, file.hash)),
                            IngestStatus::Missing => {}
                        }

                        // Gather the file content
                        let stream = core.file_storage.stream(&file.hash.hex()).await?;
                        let trigrams = match build_file(stream).await {
                            Ok(trigrams) => trigrams,
                            Err(_) => return Err(ErrorKinds::UnableToBuildTrigrams),
                        };

                        // Ingest the file
                        let complete = core.database.ingest_file(filter, &file, &trigrams).await?;
                        return Ok((complete, filter, file.hash))
                    });
                }
            }

            let result = match workers.join_next().await {
                Some(result) => result?,
                None => continue,
            };

            match result {
                Ok((complete, filter, hash)) => if complete {
                    completed.push(hash);
                } else {
                    modified_filters.push(filter)
                },
                Err(err) => if let ErrorKinds::FilterUnknown(filter) = err {
                    unknown_filters.push(filter);
                } else {
                    error!("{err}");
                }
            }
        }

        self.notify_ingest_feeders(modified_filters).await;
        return Ok(IngestFilesResponse { completed, unknown_filters })
    }

    pub async fn notify_ingest_feeders(&self, ids: Vec<FilterID>) {
        let filters = self.filters.read().await;
        for id in ids {
            if let Some((_, notify)) = filters.get(&id) {
                notify.notify_waiters()
            }
        }
    }

    pub async fn ingest_feeder(self: Arc<Self>, id: FilterID, writer: mpsc::Sender<WriterCommand>, notify: Arc<Notify>) {
        while let Err(err) = self._ingest_feeder(id, &writer, notify.clone()).await {
            error!("{err}");
        }
    }

    pub async fn _ingest_feeder(self: &Arc<Self>, id: FilterID, writer: &mpsc::Sender<WriterCommand>, notify: Arc<Notify>) -> Result<()> {
        loop {
            // Get next set of incomplete files
            let batch = self.database.get_ingest_batch(id, self.config.ingest_batch_size).await?;
            if batch.is_empty() {
                tokio::time::timeout(tokio::time::Duration::from_secs(300), notify.notified()).await;
                continue
            }

            // Send the files to the writer
            let files = batch.iter().map(|row|row.0).collect_vec();
            let batch = batch.into_iter().map(|row|(row.0, row.1)).collect_vec();
            let (finished_send, finished_recv) = oneshot::channel();
            writer.send(WriterCommand::Ingest(batch, finished_send)).await;

            // Wait for a positive response
            if let Ok(()) = finished_recv.await {
                // Commit those file ids
                self.database.finished_ingest(id, files).await?;
            }
        }
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
                Some((filter, _)) => if !filter.is_ready() {
                    return Ok(false);
                }
                None => return Ok(false)
            }
        }
        return Ok(true)
    }

    pub async fn query_filter(self: Arc<Self>, id: FilterID, query: Query, access: HashSet<String>, respond: mpsc::Sender<FilterSearchResponse>) {
        if let Some((filter, _)) = self.filters.read().await.get(&id) {
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


pub async fn build_file(mut input: mpsc::Receiver<Result<Vec<u8>>>) -> Result<BitVec> {
    // Prepare accumulators
    let mut mask = BitVec::repeat(false, 1 << 24);

    // Read the initial block
    let mut buffer = vec![];
    while buffer.len() <= 2 {
        let sub_buffer = match input.recv().await {
            Some(sub_buffer) => sub_buffer?,
            None => return Ok(mask),
        };

        buffer.extend(sub_buffer);
    }

    // Initialize trigram
    let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
    let mut index_start = 2;

    loop {
        for byte in &buffer[index_start..] {
            trigram = (trigram & 0x00FFFF) << 8 | (*byte as u32);
            mask.set(trigram as usize, true);
        }

        buffer = match input.recv().await {
            Some(buffer) => buffer?,
            None => return Ok(mask),
        };
        if buffer.is_empty() {
            break;
        }
        index_start = 0;
    }

    return Ok(mask)
}
