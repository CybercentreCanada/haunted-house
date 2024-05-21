use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use anyhow::{Result, Context};
use chrono::DurationRound;
use log::{debug, info, error};
use tokio::sync::{mpsc, watch, RwLock};

use crate::blob_cache::{BlobHandle, BlobCache};
use crate::config::WorkerSettings;
use crate::query::Query;
use crate::storage::BlobStorage;
use crate::timing::ResourceTracker;
use crate::types::{ExpiryGroup, FilterID, FileInfo};

use super::YaraTask;
use super::database::{IngestStatus, Database, IngestStatusBundle};
use super::journal::JournalFilter;
// use super::filter::ExtensibleTrigramFile;
// use super::filter_worker::{FilterWorker, WriterCommand};
use super::interface::{FilterSearchResponse, UpdateFileInfoResponse, IngestFilesResponse, StorageStatus};
use super::trigrams::TrigramCache;

pub struct WorkerState {
    pub database: Database,
    pub running: watch::Receiver<bool>,
    pub filters: RwLock<HashMap<FilterID, Arc<JournalFilter>>>,
    pub file_storage: BlobStorage,
    pub file_cache: BlobCache,
    pub trigrams: Arc<TrigramCache>,
    pub config: WorkerSettings,
    pub resource_tracker: ResourceTracker,
    pub defrag_token: tokio::sync::Semaphore,
}

// type FilterInfo = (FilterWorker, Arc<Notify>, watch::Sender<bool>);

impl WorkerState {

    pub async fn new(database: Database, file_storage: BlobStorage, file_cache: BlobCache, config: WorkerSettings, running: watch::Receiver<bool>) -> Result<Arc<Self>> {

        let trigrams = TrigramCache::new(&config, file_storage.clone()).await.context("setting up trigram cache")?;

        let new = Arc::new(Self {
            database,
            running,
            filters: RwLock::new(Default::default()),
            file_cache,
            file_storage,
            config,
            trigrams,
            resource_tracker: ResourceTracker::start(),
            defrag_token: tokio::sync::Semaphore::new(1),
        });

        // Start workers for every filter
        let mut to_delete = vec![];
        let filter_directory = new.config.get_filter_directory();

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
                let worker = JournalFilter::open(filter_directory.clone(), id).await?;
                tokio::spawn(new.clone().ingest_feeder(id, worker.clone()));
                filters.insert(id, worker);
            }
        }

        for id in to_delete {
            new.delete_index(id).await.context("expiry")?;
        }
        tokio::spawn(new.clone().garbage_collector());

        Ok(new)
    }

    pub async fn stop(&self) {
        let mut futures = vec![];
        {
            let filters = self.filters.read().await;
            for (_, worker) in filters.iter() {
                let worker = worker.clone();
                futures.push(tokio::spawn(async move {
                    worker.stop().await; 
                }));
            }
        }
        for future in futures {
            _ = future.await;
        }
    }

    pub async fn create_index(self: &Arc<Self>, id: FilterID, expiry: ExpiryGroup) -> Result<()> {
        self.database.create_filter(id, &expiry).await?;
        let mut filters = self.filters.write().await;
        if filters.contains_key(&id) { return Ok(()); }
        let worker = JournalFilter::new(self.config.get_filter_directory(), id).await?;
        tokio::spawn(self.clone().ingest_feeder(id, worker.clone()));
        filters.insert(id, worker);
        return Ok(())
    }

    async fn garbage_collector(self: Arc<Self>) {
        while let Err(err) = self._garbage_collector().await {
            error!("Garbage collector error: {err}");
        }
    }

    async fn _garbage_collector(&self) -> Result<()> {
        let day: chrono::Duration = chrono::Duration::days(1);
        loop {
            // get all filters
            for filter in self.database.get_filters(&ExpiryGroup::min(), &ExpiryGroup::yesterday()).await? {
                self.delete_index(filter).await?;
            }

            // wait for next run
            let tomorrow = chrono::Utc::now().duration_trunc(day)? + day;
            let delay = tomorrow - chrono::Utc::now();
            tokio::time::sleep(delay.to_std()?).await;
        }
    }

    pub async fn delete_index(&self, id: FilterID) -> Result<()> {
        info!("Deleting index {id}");
        // Stop the worker if running
        if let Some(worker) = self.filters.write().await.remove(&id) {
            debug!("Deleting index {id}: Stop worker");
            worker.stop().await;

            // Delete data behind the worker
            debug!("Deleting index {id}: delete trigram file");
            worker.delete().context("Delete trigram index")?;
        }

        // Delete the trigram cache data
        debug!("Deleting index {id}: expire trigram cache");
        self.trigrams.expire(id).await.context("flush trigram cache")?;

        // Delete the database info
        debug!("Deleting index {id}: remove database entry");
        self.database.delete_filter(id).await.context("Delete file info database")?;
        debug!("Deleting index {id}: finish");
        return Ok(())
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
        return Ok(self.get_free_storage().await? < self.config.data_reserve)
    }

    pub async fn get_used_storage(&self) -> Result<u64> {
        let stats = nix::sys::statvfs::statvfs(&self.config.data_path)?;
        let used_blocks = stats.blocks() - stats.blocks_available();
        Ok(used_blocks * stats.block_size())
    }

    pub async fn get_free_storage(&self) -> Result<u64> {
        let stats = nix::sys::statvfs::statvfs(&self.config.data_path)?;
        let all_free = stats.blocks_available() * stats.block_size();
        Ok(self.config.data_limit.min(all_free))
    }

    pub (crate) async fn storage_status(&self) -> Result<StorageStatus> {
        let capacity = self.get_free_storage().await?;
        Ok(StorageStatus{
            capacity,
            high_water: capacity.saturating_sub(self.config.data_reserve),
            used: self.get_used_storage().await?,
        })
    }

    /// Given a set of files expected to be ingested check the progress of the ones already underway
    /// and add new ones
    pub async fn ingest_files(self: &Arc<Self>, mut files: Vec<(FilterID, FileInfo)>) -> Result<IngestFilesResponse> {
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
        let mut rejected = vec![];
        let mut modified_filters = vec![];
        for (filter, file, status) in files {
            match status {
                IngestStatus::Ready => {
                    completed.push((filter, file.hash));
                    continue
                },
                IngestStatus::Pending(_) => continue,
                IngestStatus::Missing => {
                    // if its not complete, either start downloading it, or pass it
                    // to the next step
                    if self.trigrams.clear_rejected(filter, file.hash.clone()).await {
                        rejected.push((filter, file.hash));
                        continue
                    } else if !self.trigrams.is_ready(filter, &file.hash).await? {
                        self.trigrams.start_fetch(filter, file.hash).await?;
                    } else {
                        outstanding.push((filter, file));
                        modified_filters.push(filter);
                    }
                }
            }
        }

        // Clear out files that have been abandoned
        self.database.abandon_files(rejected.clone()).await?;

        // Ingest the file
        let complete = self.database.ingest_files(outstanding).await?;
        for (filter, completed_file) in complete {
            completed.push((filter, completed_file.hash));
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
            rejected,
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
            if let Some(worker) = filters.get(&id) {
                worker.notify();
            }
        }
    }
    
    /// Get the number of fragments we are willing to accept as a function of 
    /// how long its been since this index was written to.
    /// This curve has 100 at 2 hours.
    /// as the time decreases the number of generations climbs to infinity, we don't want to defrag an active index
    /// as the time increases the number of generations drops so that at a day of inactivity it will defrag 
    /// any with 4 or more generations, at around 4 days of inactivity defraging will be run for
    /// ANY fragmentation at all
    fn defrag_limit(since_last_write: std::time::Duration) -> u64 {
        let seconds = since_last_write.as_secs_f64();
        let hours = seconds/(60.0 * 60.0);
        let limit = 100.0/(hours - 1.0);
        limit.floor() as u64
    }

    pub async fn ingest_feeder(self: Arc<Self>, id: FilterID, worker: Arc<JournalFilter>) {
        while let Err(err) = self._ingest_feeder(id, worker.clone()).await {
            error!("ingest feeder crash {err:?}");
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        }
        info!("Stopping ingest feeder for {id}");
    }

    pub async fn _ingest_feeder(self: &Arc<Self>, id: FilterID, worker: Arc<JournalFilter>) -> Result<()> {
        info!("Starting ingest feeder for {id}");
        let batch_size = self.config.ingest_batch_size;
        let batch_delay = std::time::Duration::from_secs(self.config.ingest_batch_delay_seconds);
        let mut delay_from: Option<std::time::Instant> = None;

        while *self.running.borrow() && worker.is_running() {
            // Get next set of incomplete files
            let stamp = std::time::Instant::now();
            let batch = self.database.get_ingest_batch(id, batch_size).await?;
            let time_get_batch = stamp.elapsed().as_secs_f64();

            // figure out if we have enough files to process
            if batch.is_empty() {
                let last_write = worker.last_write().await?;
                let generation = worker.generation_counter().await?;
                if generation > Self::defrag_limit(last_write) {
                    if let Ok(_token) = self.defrag_token.try_acquire() {
                        worker.defrag().await?;
                        continue
                    }
                }

                _ = tokio::time::timeout(tokio::time::Duration::from_secs(600), worker.notified()).await;
                continue
            } else if batch.len() < batch_size as usize {
                let wait_start = delay_from.get_or_insert_with(std::time::Instant::now);
                if let Some(wait_time) = batch_delay.checked_sub(wait_start.elapsed()) {
                    _ = tokio::time::timeout(wait_time, worker.notified()).await;
                    continue
                }
            }
            delay_from = None;

            // get the range of ids in the batch
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

            // Ingest the data to the journal
            let stamp = std::time::Instant::now();
            if !worker.write_batch(trigrams).await? {
                continue
            }
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
        // let mut ids = self.get_filters(&ExpiryGroup::min(), &ExpiryGroup::max()).await?;
        // let filters = self.filters.read().await;
        // ids.extend(filters.keys());

        // Make sure they are all ready
        // for id in ids {
        //     match filters.get(&id) {
        //         Some((filter, _, _)) => if !filter.is_ready() {
        //             return Ok(false);
        //         }
        //         None => return Ok(false)
        //     }
        // }
        return Ok(true)
    }

    pub async fn query_filter(self: Arc<Self>, id: FilterID, query: Query, access: HashSet<String>, respond: mpsc::Sender<FilterSearchResponse>) {
        if let Some(filter) = self.filters.read().await.get(&id) {
            match filter.query(query).await {
                Ok(file_indices) => {
                    match self.database.select_files(id, &file_indices, &access).await {
                        Ok(files) => { _ = respond.send(FilterSearchResponse::Candidates(id, files)).await; },
                        Err(err) => { _ = respond.send(FilterSearchResponse::Error(Some(id), err.to_string())).await; }
                    };
                }
                Err(err) => { _ = respond.send(FilterSearchResponse::Error(Some(id), err.to_string())).await; },
            };
        }
    }

    pub async fn run_yara(&self, yara_task: YaraTask) -> Result<(Vec<FileInfo>, Vec<String>)> {
        debug!("yara task {} starting", yara_task.id);
        let mut errors = vec![];
        let filter_handle = {
            let (file_send, mut file_recv) = mpsc::unbounded_channel::<(FileInfo, BlobHandle)>();

            // Run the interaction with yara in a blocking thread
            let filter_handle = tokio::task::spawn_blocking(move || -> Result<Vec<FileInfo>> {
                debug!("yara task {} launched yara worker", yara_task.id);
                // Compile the yara rules
                let compiler = yara::Compiler::new()?
                    .add_rules_str(&yara_task.yara_rule)?;
                let rules = compiler.compile_rules()?;
                debug!("yara task {} yara ready", yara_task.id);

                // Try
                let mut selected = vec![];
                while let Some((info, handle)) = file_recv.blocking_recv() {
                    debug!("yara task {} processing {}", yara_task.id, handle.id());
                    let result = rules.scan_file(handle.path(), 60 * 30)?;
                    if !result.is_empty() {
                        selected.push(info);
                    }
                }
                debug!("yara task {} yara finished", yara_task.id);
                return Ok(selected);
            });

            // Load the files and send them to the worker
            for info in yara_task.files {
                // download the file
                let hash_string = info.hash.hex();
                debug!("yara task {} waiting for {}", yara_task.id, hash_string);
                match self.file_cache.open(hash_string.clone()).await {
                    Ok(blob) => file_send.send((info, blob))?,
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

