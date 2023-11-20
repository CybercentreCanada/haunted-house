use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use anyhow::{Result, Context};
use chrono::DurationRound;
use log::{debug, info, error};
use tokio::sync::{mpsc, watch, oneshot};

use crate::blob_cache::{BlobHandle, BlobCache};
use crate::config::WorkerSettings;
use crate::error::ErrorKinds;
use crate::query::Query;
use crate::storage::BlobStorage;
use crate::types::{ExpiryGroup, Sha256, FilterID, FileInfo};

use super::YaraTask;
use super::database::{Database, IngestStatusBundle, IngestProgress};
use super::filter::ExtensibleTrigramFile;
use super::filter_worker::{FilterWorker, WriterCommand};
use super::interface::{FilterSearchResponse, UpdateFileInfoResponse, StorageStatus};
use super::trigrams::TrigramCache;

pub (crate) enum IngestFileResponse {
    Rejected,
    Finished
}

pub struct WorkerState {
    pub database: Database,
    pub running: watch::Receiver<bool>,
    pub filters: tokio::sync::RwLock<HashMap<FilterID, (FilterWorker, mpsc::Sender<WriterCommand>)>>,
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
                // let notify = Arc::new(tokio::sync::Notify::new());
                // let (send_running, recv_running) = watch::channel(true);
                // let (worker, channel) = ;
                // tokio::spawn(new.clone().ingest_feeder(id, worker.writer_connection.clone(), notify.clone(), recv_running));
                filters.insert(id, FilterWorker::open(new.config.clone(), id)?);
            }
        }

        for id in to_delete {
            new.delete_index(id).await.context("expiry")?;
        }
        tokio::spawn(new.clone().garbage_collector());

        Ok(new)
    }

    pub async fn stop(&self) {
        let mut filters = self.filters.write().await;
        let mut threads = vec![];
        // drop all the senders so the threads exit when they finish their current work
        for (_, (worker, _)) in filters.drain() {
            threads.push(worker);
        }
        // wait for them to finish that work
        for worker in threads {
            worker.join().await;
        }
    }

    pub async fn create_index(self: &Arc<Self>, id: FilterID, expiry: ExpiryGroup) -> Result<()> {
        self.database.create_filter(id, &expiry).await?;
        let mut filters = self.filters.write().await;
        if filters.contains_key(&id) { return Ok(()); }
        // let (send_running, recv_running) = watch::channel(true);
        // let worker = FilterWorker::open(self.config.clone(), id)?;
        // let notify = Arc::new(tokio::sync::Notify::new());
        // tokio::spawn(self.clone().ingest_feeder(id, worker.writer_connection.clone(), notify.clone(), recv_running));
        // filters.insert(id, (worker, notify, send_running));
        filters.insert(id, FilterWorker::open(self.config.clone(), id)?);
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
        if let Some((worker, channel)) = self.filters.write().await.remove(&id) {
            drop(channel);
            worker.join().await;
        }

        // Delete data behind the worker
        ExtensibleTrigramFile::delete(self.config.get_filter_directory(), id).context("Delete trigram index")?;

        // Delete the trigram cache data
        self.trigrams.expire(id).await.context("flush trigram cache")?;

        // Delete the database info
        self.database.delete_filter(id).await.context("Delete file info database")?;
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
        let total = self.get_used_storage().await?;
        return Ok(total >= self.config.data_limit - self.config.data_reserve)
    }

    pub async fn get_used_storage(&self) -> Result<u64> {
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

        return Ok(total)
    }

    pub (crate) async fn storage_status(&self) -> Result<StorageStatus> {
        Ok(StorageStatus{
            capacity: self.config.data_limit,
            high_water: self.config.data_limit - self.config.data_reserve,
            used: self.get_used_storage().await?,
        })
    }

    pub async fn ingest_file(self: &Arc<Self>, filter: FilterID, file: FileInfo) -> Result<IngestFileResponse, ErrorKinds> {
        // Check if we are working on old file data
        if file.expiry <= ExpiryGroup::today() {
            return Ok(IngestFileResponse::Rejected)
        }

        // Install the file in the database
        let id = match self.database.ingest_file(filter, file.clone()).await? {
            IngestProgress::Ready => return Ok(IngestFileResponse::Finished),
            IngestProgress::Pending(filter, number) => number,
        };
        
        // Load the file into the cache
        let guard = match self.trigrams.fetch(filter, file.hash.clone()).await? {
            Some(guard) => guard,
            None => {
                self.database.abandon_files(vec![(filter, file.hash.clone())]).await?; 
                return Ok(IngestFileResponse::Rejected);
            }
        };
        
        // Queue up the file 
        let (finished_send, finished_recv) = oneshot::channel();
        match self.filters.read().await.get(&filter) {
            Some((_, writer)) => {
                writer.send((guard, id as u64, finished_send)).await?;
            }
            None => return Err(ErrorKinds::ChannelError("lost connection to filter".to_owned()))
        }
        finished_recv.await?;

        // confirm file in database
        self.database.finished_ingest(filter, vec![(id as u64, file.hash)]).await?;
        Ok(IngestFileResponse::Finished)
    }

    pub async fn get_filters(&self, first: &ExpiryGroup, last: &ExpiryGroup) -> Result<Vec<FilterID>> {
        Ok(self.database.get_filters(first, last).await?)
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
                        Ok(files) => { _ = respond.send(FilterSearchResponse::Candidates(id, files)).await; },
                        Err(err) => { _ = respond.send(FilterSearchResponse::Error(Some(id), err.to_string())).await; }
                    };
                }
                Err(err) => { _ = respond.send(FilterSearchResponse::Error(Some(id), err.to_string())).await; },
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

