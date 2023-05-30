use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use anyhow::{Result, Context};
use log::{debug, info, error};
use tokio::sync::{mpsc, watch, Notify, oneshot};

use crate::blob_cache::{BlobHandle, BlobCache};
use crate::config::WorkerSettings;
use crate::query::Query;
use crate::storage::BlobStorage;
use crate::types::{ExpiryGroup, Sha256, FilterID, FileInfo};

use super::YaraTask;
use super::database::{IngestStatus, Database, IngestStatusBundle};
use super::filter::ExtensibleTrigramFile;
use super::filter_worker::{FilterWorker, WriterCommand};
use super::interface::{FilterSearchResponse, UpdateFileInfoResponse, IngestFilesResponse};
use super::trigram_cache::TrigramCache;

pub struct WorkerState {
    pub database: Database,
    pub running: watch::Receiver<bool>,
    pub filters: tokio::sync::RwLock<HashMap<FilterID, (FilterWorker, Arc<Notify>, watch::Sender<bool>)>>,
    pub file_storage: BlobStorage,
    pub file_cache: BlobCache,
    pub trigrams: Arc<TrigramCache>,
    pub config: WorkerSettings,
}


impl WorkerState {

    pub async fn new(database: Database, file_storage: BlobStorage, file_cache: BlobCache, config: WorkerSettings, running: watch::Receiver<bool>) -> Result<Arc<Self>> {

        let trigrams = TrigramCache::new(&config, file_storage.clone()).await.context("setting up trigram cache")?;

        let new = Arc::new(Self {
            database,
            running,
            filters: tokio::sync::RwLock::new(Default::default()),
            file_cache,
            file_storage,
            config,
            trigrams,
        });

        // Start workers for every filter
        let mut to_delete = vec![];
        {
            let today = ExpiryGroup::today();
            let mut filters = new.filters.write().await;
            for (id, expiry) in new.database.get_expiry(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
                if expiry < today {
                    to_delete.push(id);
                    continue
                }

                if filters.contains_key(&id) {
                    error!("Duplicate filter?");
                    continue
                }
                let notify = Arc::new(tokio::sync::Notify::new());
                let (send_running, recv_running) = watch::channel(true);
                let worker = FilterWorker::open(new.config.clone(), id)?;
                tokio::spawn(new.clone().ingest_feeder(id, worker.writer_connection.clone(), notify.clone(), recv_running));
                filters.insert(id, (worker, notify, send_running));
            }
        }

        for id in to_delete {
            new.delete_index(id).await.context("expiry")?;
        }

        Ok(new)
    }

    pub async fn stop(&self) {
        let mut filters = self.filters.write().await;
        for (_, (_, notify, running)) in filters.iter() {
            _ = running.send(false);
            notify.notify_waiters();
        }
        for (_, (worker, _, _)) in filters.drain() {
            worker.join().await;
        }
    }

    pub async fn create_index(self: &Arc<Self>, id: FilterID, expiry: ExpiryGroup) -> Result<()> {
        self.database.create_filter(id, &expiry).await?;
        let mut filters = self.filters.write().await;
        if filters.contains_key(&id) { return Ok(()); }
        let (send_running, recv_running) = watch::channel(true);
        let worker = FilterWorker::open(self.config.clone(), id)?;
        let notify = Arc::new(tokio::sync::Notify::new());
        tokio::spawn(self.clone().ingest_feeder(id, worker.writer_connection.clone(), notify.clone(), recv_running));
        filters.insert(id, (worker, notify, send_running));
        return Ok(())
    }

    pub async fn delete_index(&self, id: FilterID) -> Result<()> {
        info!("Deleting index {id}");
        // Stop the worker if running
        if let Some((worker, notify, running)) = self.filters.write().await.remove(&id) {
            _ = running.send(false);
            notify.notify_waiters();
            worker.join().await;
        }

        // Delete data behind the worker
        ExtensibleTrigramFile::delete(self.config.get_filter_directory(), id)?;

        // Delete the trigram cache data
        self.trigrams.expire(id).await?;

        // Delete the database info
        self.database.delete_filter(id).await
    }

    pub async fn update_files(&self, files: Vec<FileInfo>) -> Result<UpdateFileInfoResponse> {
        //
        let IngestStatusBundle{missing: _, ready, pending} = self.database.update_file_access(files.clone()).await.context("update_file_access")?;

        Ok(UpdateFileInfoResponse {
            processed: ready,
            pending,
        })
    }

    pub async fn check_storage_pressure(&self) -> Result<bool> {
        let mut total = 0u64;
        let mut dirs = vec![self.config.data_path.clone()];
        while let Some(dir) = dirs.pop() {
            let mut listing = tokio::fs::read_dir(&dir).await?;
            while let Some(file) = listing.next_entry().await? {
                if let Ok(file_type) =  file.file_type().await {
                    if file_type.is_dir() {
                        dirs.push(file.path())
                    }
                    else if file_type.is_file() {
                        if let Ok(meta) = file.metadata().await {
                            total += meta.len();
                        }
                    }
                }
            }
        }

        return Ok(total >= self.config.data_limit - self.config.data_reserve)
    }

    pub async fn ingest_file(self: &Arc<Self>, mut files: Vec<(FilterID, FileInfo)>) -> Result<IngestFilesResponse> {
        // Filter those we already have information for quickly
        // the ones that are currently pending can be dropped
        let pending = self.database.filter_pending().await?;
        files.retain(|(filter, file)| {
            match pending.get(filter) {
                Some(hashes) => !hashes.contains(&file.hash),
                None => true
            }
        });
        // The ones we currently waiting for their file download can also be dropped
        self.trigrams.strip_pending(&mut files).await;

        // Check for any completed files
        let mut completed = vec![];
        let files = self.database.check_insert_status(files).await?;
        let mut outstanding = vec![];
        let mut modified_filters = vec![];
        for (filter, file, status) in files {
            match status {
                IngestStatus::Ready => {
                    completed.push(file.hash);
                    continue
                },
                IngestStatus::Pending(_) => continue,
                IngestStatus::Missing => {
                    // if its not complete, either start downloading it, or pass it
                    // to the next step
                    if !self.trigrams.is_ready(filter, &file.hash).await? {
                        self.trigrams.start_fetch(filter, file.hash).await?;
                    } else {
                        outstanding.push((filter, file));
                        modified_filters.push(filter);
                    }
                }
            }
        }

        // Ingest the file
        let complete = self.database.ingest_files(outstanding).await?;
        for completed_file in complete {
            completed.push(completed_file.hash);
        }

        // Let ingest feeders know files are ready
        self.notify_ingest_feeders(modified_filters).await;

        // gather a list of which filters are assigned to what
        let mut expiry_groups: HashMap<ExpiryGroup, Vec<FilterID>> = Default::default();
        for (id, group) in self.database.get_expiry(&ExpiryGroup::min(), &ExpiryGroup::max()).await? {
            match expiry_groups.entry(group) {
                std::collections::hash_map::Entry::Occupied(mut entry) => entry.get_mut().push(id),
                std::collections::hash_map::Entry::Vacant(entry) => { entry.insert(vec![id]); },
            }
        }

        //
        let mut filter_pending = self.database.filter_pending().await?;
        self.trigrams.add_pending(&mut filter_pending).await;

        // return progress, with metadata about our filters
        return Ok(IngestFilesResponse {
            completed,
            // unknown_filters,
            filter_pending,
            expiry_groups,
            storage_pressure: self.check_storage_pressure().await?,
            filter_size: self.database.filter_sizes().await?,
        })
    }

    pub async fn notify_ingest_feeders(&self, mut ids: Vec<FilterID>) {
        ids.sort_unstable();
        ids.dedup();
        let filters = self.filters.read().await;
        for id in ids {
            if let Some((_, notify, _)) = filters.get(&id) {
                notify.notify_waiters()
            }
        }
    }

    pub async fn ingest_feeder(self: Arc<Self>, id: FilterID, writer: mpsc::Sender<WriterCommand>, notify: Arc<Notify>, running: watch::Receiver<bool>) {
        while let Err(err) = self._ingest_feeder(id, &writer, notify.clone(), running.clone()).await {
            error!("ingest feeder crash {err:?}");
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        }
        info!("Stopping ingest feeder for {id}");
    }

    pub async fn _ingest_feeder(self: &Arc<Self>, id: FilterID, writer: &mpsc::Sender<WriterCommand>, notify: Arc<Notify>, running: watch::Receiver<bool>) -> Result<()> {
        info!("Starting ingest feeder for {id}");
        while *self.running.borrow() && *running.borrow() {
            // Get next set of incomplete files
            let stamp = std::time::Instant::now();
            let batch = self.database.get_ingest_batch(id, self.config.ingest_batch_size).await?;
            let time_get_batch = stamp.elapsed().as_secs_f64();
            if batch.is_empty() {
                if let Err(_) = tokio::time::timeout(tokio::time::Duration::from_secs(600), notify.notified()).await {
                    writer.send(WriterCommand::Flush).await?;
                }
                continue
            }

            let mut min = u64::MAX;
            let mut max = u64::MIN;
            for (number, _) in &batch {
                min = min.min(*number);
                max = max.max(*number);
            }
            info!("Ingesting batch to {id} ({min} to {max})");

            let stamp = std::time::Instant::now();
            // Load the file trigrams
            let mut trigrams = vec![];
            let mut hashes = vec![];
            for (number, hash) in &batch {
                hashes.push(hash.clone());
                let data = self.trigrams.get(id, hash).await?;
                trigrams.push((*number, data));
            }
            let time_load_trigrams = stamp.elapsed().as_secs_f64();

            let stamp = std::time::Instant::now();
            // Send the files to the writer
            let (finished_send, finished_recv) = oneshot::channel();
            writer.send(WriterCommand::Ingest(trigrams, finished_send)).await?;

            // Wait for a positive response
            finished_recv.await?;
            let time_install = stamp.elapsed().as_secs_f64();

            // Commit those file ids
            let stamp = std::time::Instant::now();
            let processing = self.database.finished_ingest(id, batch).await?;
            let time_finish = stamp.elapsed().as_secs_f64();

            let stamp = std::time::Instant::now();
            for hash in hashes {
                if let Err(err) = self.trigrams.release(id, &hash).await {
                    error!("{err}");
                }
            }
            let time_cleanup = stamp.elapsed().as_secs_f64();
            info!("{id} Batch timing; get batch {time_get_batch}; trigrams {time_load_trigrams}; install {time_install}; finish {time_finish} ({processing}); cleanup {time_cleanup}");
        }
        return Ok(())
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>> {
        Ok(self.database.get_filters(first, last).await?)
    }

    // pub async fn get_expiry(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<(FilterID, ExpiryGroup)>> {
    //     Ok(self.database.get_expiry(first, last).await?)
    // }

    pub async fn is_ready(&self) -> Result<bool> {
        // Get the most comprehensive list of filters we can
        let mut ids = self.get_filters(&ExpiryGroup::min(), &ExpiryGroup::max()).await?;
        let filters = self.filters.read().await;
        ids.extend(filters.keys());

        // Make sure they are all ready
        for id in ids {
            match filters.get(&id) {
                Some((filter, _, _)) => if !filter.is_ready() {
                    return Ok(false);
                }
                None => return Ok(false)
            }
        }
        return Ok(true)
    }

    pub async fn query_filter(self: Arc<Self>, id: FilterID, query: Query, access: HashSet<String>, respond: mpsc::Sender<FilterSearchResponse>) {
        if let Some((filter, _, _)) = self.filters.read().await.get(&id) {
            match filter.query(query).await {
                Ok(file_indices) => {
                    match self.database.select_file_hashes(id, &file_indices, &access).await {
                        Ok(files) => { _ = respond.send(FilterSearchResponse::Candidates(files)).await; },
                        Err(err) => { _ = respond.send(FilterSearchResponse::Error(err.to_string())).await; }
                    };
                }
                Err(err) => { _ = respond.send(FilterSearchResponse::Error(err.to_string())); },
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

