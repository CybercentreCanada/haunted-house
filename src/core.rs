use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::auth::Authenticator;
use crate::blob_cache::BlobCache;
use crate::database::{Database, IndexGroup};
use crate::interface::{SearchRequestResponse, SearchRequest, WorkRequest, WorkResult, WorkPackage, WorkResultValue, WorkError};
use crate::storage::BlobStorage;
use crate::access::AccessControl;
use crate::filter::TrigramFilter;
use crate::worker_watcher::worker_watcher;
use bitvec::vec::BitVec;
use futures::future::select_all;
use log::{error, debug, info};
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot};
use anyhow::{Result, Context};
use chrono::{DateTime, Utc, Duration};
use tokio::task::JoinHandle;
use weak_self::WeakSelf;


#[derive(Debug, Serialize, Deserialize)]
pub struct IngestStatus {
    pub active_batch: Option<u64>,
    pub queue_size: u64,
}

#[derive(Debug)]
pub enum IngestMessage {
    IngestMessage(Vec<u8>, AccessControl, Option<DateTime<Utc>>, oneshot::Sender<Result<()>>),
    Status(oneshot::Sender<IngestStatus>)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreConfig {
    #[serde(default="default_batch_limit_size")]
    pub batch_limit_size: u64,
    #[serde(default="default_batch_limit_seconds")]
    pub batch_limit_seconds: u64,
    #[serde(default="default_garbage_collection_interval")]
    pub garbage_collection_interval: std::time::Duration,
    #[serde(default="default_index_soft_entries_max")]
    pub index_soft_entries_max: u64,
    #[serde(default="default_index_soft_bytes_max")]
    pub index_soft_bytes_max: u64,
    #[serde(default="default_yara_job_size")]
    pub yara_job_size: u64,
    #[serde(default="default_max_result_set_size")]
    pub max_result_set_size: u64,
    #[serde(default="default_task_deadline")]
    pub task_deadline: std::time::Duration,
    #[serde(default="default_task_start_time")]
    pub task_start_time: std::time::Duration,
    #[serde(default="default_task_heartbeat_interval")]
    pub task_heartbeat_interval: std::time::Duration,
}

fn default_batch_limit_seconds() -> u64 { chrono::Duration::hours(1).num_seconds() as u64 }
fn default_batch_limit_size() -> u64 { 100 }
fn default_garbage_collection_interval() -> std::time::Duration { std::time::Duration::from_secs(60) }
fn default_index_soft_bytes_max() -> u64 { 50 << 30 }
fn default_index_soft_entries_max() -> u64 { 50 << 30 }
fn default_yara_job_size() -> u64 { 1000 }
fn default_max_result_set_size() -> u64 { 100_000 }
fn default_task_deadline() -> std::time::Duration { std::time::Duration::from_secs(60 * 60) }
fn default_task_start_time() -> std::time::Duration { std::time::Duration::from_secs(60) }
fn default_task_heartbeat_interval() -> std::time::Duration { std::time::Duration::from_secs(60 * 2) }

// impl Default for CoreConfig {
//     fn default() -> Self {
//         Self {
//             batch_limit_seconds: default_batch_limit_seconds(),
//             batch_limit_size: default_batch_limit_size(),
//             garbage_collection_interval: default_garbage_collection_interval(),
//             index_soft_bytes_max: default_index_soft_bytes_max(),
//             index_soft_entries_max: default_index_soft_entries_max(),
//             yara_job_size: default_yara_job_size(),
//             max_result_set_size: default_max_result_set_size(),
//         }
//     }
// }

pub struct HouseCore {
    weak_self: WeakSelf<Self>,
    pub database: Database,
    // pub quit_trigger: tokio::sync::watch::Sender<bool>,
    // pub quit_signal: tokio::sync::watch::Receiver<bool>,
    pub file_storage: BlobStorage,
    pub index_storage: BlobStorage,
    pub index_cache: BlobCache,
    pub authenticator: Authenticator,
    pub config: CoreConfig,
    pub ingest_queue: mpsc::UnboundedSender<IngestMessage>,
    search_watchers: mpsc::UnboundedSender<WorkResult>,
    garbage_collection_notification: tokio::sync::Notify,
}

impl HouseCore {
    pub fn new(index_storage: BlobStorage, file_storage: BlobStorage, database: Database, index_cache: BlobCache, authenticator: Authenticator, config: CoreConfig) -> Result<Arc<Self>> {
        let (send_ingest, receive_ingest) = mpsc::unbounded_channel();
        let (send_search, receive_search) = mpsc::unbounded_channel();

        // Stop flag
        // let (quit_trigger, quit_signal) = tokio::sync::watch::channel(false);

        let core = Arc::new(Self {
            weak_self: WeakSelf::new(),
            database,
            // quit_trigger,
            // quit_signal,
            file_storage,
            index_storage,
            index_cache,
            authenticator,
            ingest_queue: send_ingest,
            config,
            search_watchers: send_search,
            garbage_collection_notification: tokio::sync::Notify::new()
        });
        core.weak_self.init(&core);

        tokio::spawn(ingest_worker(core.clone(), receive_ingest));
        tokio::spawn(search_watcher(core.clone(), receive_search));
        tokio::spawn(worker_watcher(core.clone()));
        tokio::spawn(garbage_collector(core.clone()));

        return Ok(core)
    }

    pub async fn ingest_status(&self) -> Result<IngestStatus> {
        let (send, recv) = oneshot::channel();
        self.ingest_queue.send(IngestMessage::Status(send))?;
        return Ok(recv.await?);
    }

    pub async fn initialize_search(&self, req: SearchRequest) -> Result<SearchRequestResponse> {
        let res = self.database.initialize_search(req).await?;
        return Ok(res)
    }

    pub async fn search_status(&self, code: String) -> Result<SearchRequestResponse> {
        self.database.search_status(code).await
    }

    pub async fn get_work(&self, req: &WorkRequest) -> Result<WorkPackage> {
        self.database.get_work(req).await
    }

    pub async fn get_work_notification(&self) -> Result<()> {
        self.database.get_work_notification().await
    }

    pub fn finish_work(&self, req: WorkResult) -> Result<()> {
        match self.search_watchers.send(req){
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow::anyhow!("result processor error: {err}")),
        }
    }

    pub async fn work_error(&self, err: WorkError) -> Result<()> {
        self.database.work_error(err).await
    }
}

async fn ingest_worker(core: Arc<HouseCore>, mut input: mpsc::UnboundedReceiver<IngestMessage>) {
    loop {
        match _ingest_worker(core.clone(), &mut input).await {
            Err(err) => error!("Crash in ingestion system: {err}"),
            Ok(()) => {
                info!("Ingest worker stopped.");
                break;
            }
        }
    }
}

async fn wait_for_batch_or_ready(mut job: &mut Option<JoinHandle<()>>, instant: tokio::time::Instant) -> Result<()> {
    match &mut job {
        Some(task) => {
            task.await?;
        },
        None => {
            tokio::time::sleep_until(instant).await;
        },
    };
    Ok(())
}

async fn _ingest_worker(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<IngestMessage>) -> Result<()> {

    let mut pending_timers: HashMap<IndexGroup, tokio::time::Instant> = Default::default();
    let mut pending_batch: HashMap<IndexGroup, HashMap<Vec<u8>, (AccessControl, Vec<oneshot::Sender<Result<()>>>)>> = Default::default();

    let mut current_batch: Option<JoinHandle<()>> = None;
    let mut last_batch_size = 0;

    loop {
        debug!("Ingest loop");

        // Check if its time to launch an index building job
        if let Some(job) = &mut current_batch {
            if job.is_finished() {
                current_batch = None;
            }
        }

        //
        if current_batch.is_none() {
            for (key, values) in pending_batch.iter() {
                let at_size_limit = values.len() >= core.config.batch_limit_size as usize;
                let batch_start = match pending_timers.get(key) {
                    Some(start) => start.clone(),
                    None => tokio::time::Instant::now(),
                };
                if at_size_limit || batch_start.elapsed().as_secs() > core.config.batch_limit_seconds {
                    let key = key.clone();
                    if let Some(values) = pending_batch.remove(&key) {
                        let mut batch = HashMap::new();
                        let mut remains = HashMap::new();
                        for row in values.into_iter() {
                            if batch.len() < core.config.batch_limit_size as usize {
                                batch.insert(row.0, row.1);
                            } else {
                                remains.insert(row.0, row.1);
                            }
                        }

                        last_batch_size = batch.len();
                        current_batch = Some(tokio::spawn(run_batch_ingest(core.clone(), key.clone(), batch)));
                        if !remains.is_empty() {
                            pending_batch.insert(key, remains);
                        } else {
                            pending_timers.remove(&key);
                        }
                    }
                    break;
                }
            }
        }

        // get how long in the future the next pending batch is mature
        let batch_ready_time = pending_timers
            .iter()
            .map(|(_, time)| *time + tokio::time::Duration::from_secs(core.config.batch_limit_seconds))
            .fold(tokio::time::Instant::now() + tokio::time::Duration::from_secs(60), |a, b| a.min(b));

        if current_batch.is_none() {
            debug!("Batch ready time: {}", (batch_ready_time - tokio::time::Instant::now()).as_secs_f32());
        }

        // Wait for more ingest messages
        tokio::select!{
            message = input.recv() => {
                // Read an update command
                let message = match message {
                    Some(message) => message,
                    None => break
                };

                match message {
                    IngestMessage::IngestMessage(hash, access, expiry, respond) => {
                        // Make sure the hash value is correct
                        if hash.len() != 32 {
                            _ = respond.send(Err(anyhow::anyhow!("Expected hash to be binary encoded sha256")));
                            continue
                        }

                        // Make sure the expiry isn't already due
                        let index_group = IndexGroup::create(&expiry);
                        let today_group = IndexGroup::create(&Some(Utc::now()));
                        if index_group.as_str() <= today_group.as_str() {
                            continue
                        }

                        // Try to update in place without changing indices
                        debug!("Ingesting {}", hex::encode(&hash));
                        if core.database.update_file_access(&hash, &access, &index_group).await? {
                            debug!("Ingesting {} by updating file data", hex::encode(&hash));
                            _ = respond.send(Ok(()));
                            continue
                        }

                        // Merge into the next
                        match pending_batch.entry(index_group.clone()) {
                            std::collections::hash_map::Entry::Occupied(mut entry) => {
                                debug!("Ingesting {} merging into index group {}", hex::encode(&hash), index_group.as_str());
                                match entry.get_mut().entry(hash) {
                                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                                        entry.get_mut().0 = entry.get_mut().0.or(&access).simplify();
                                        entry.get_mut().1.push(respond);
                                    },
                                    std::collections::hash_map::Entry::Vacant(entry) => {
                                        entry.insert((access, vec![respond]));
                                    },
                                }
                            },
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                debug!("Ingesting {} creating index group {}", hex::encode(&hash), index_group.as_str());
                                let mut batch: HashMap<Vec<u8>, _> = Default::default();
                                batch.insert(hash, (access, vec![respond]));
                                entry.insert(batch);
                                pending_timers.insert(index_group, tokio::time::Instant::now());
                            },
                        }
                    },
                    IngestMessage::Status(response) => {
                        let mut total = 0;
                        for (_, buffered) in pending_batch.iter() {
                            total += buffered.len();
                        }
                        _ = response.send(IngestStatus {
                            active_batch: if current_batch.is_some() {
                                Some(last_batch_size as u64)
                            } else {
                                None
                            },
                            queue_size: total as u64
                        });
                    }
                }
            }

            // If the current batch finishes, or another batch should be checked
            _ = wait_for_batch_or_ready(&mut current_batch, batch_ready_time) => {
                debug!("Batch finish or ready");
                continue;
            }
        }
    }
    return Ok(())
}

async fn run_batch_ingest(core: Arc<HouseCore>, index_group: IndexGroup, new_items: HashMap<Vec<u8>, (AccessControl, Vec<oneshot::Sender<Result<()>>>)>) {
    if let Err(err) = _run_batch_ingest(core, index_group, new_items).await {
        error!("Error building index batch: {err}");
    }
}

async fn _run_batch_ingest(core: Arc<HouseCore>, index_group: IndexGroup, mut new_items: HashMap<Vec<u8>, (AccessControl, Vec<oneshot::Sender<Result<()>>>)>) -> Result<()> {
    debug!("Ingest {} batch {} items", index_group.as_str(), new_items.len());
    // Check if any of our new items are already handled
    {
        let mut to_remove = vec![];
        for (hash, (access, responses)) in new_items.iter_mut() {
            if core.database.update_file_access(hash, access, &index_group).await? {
                while let Some(res) = responses.pop() {
                    _ = res.send(Ok(()));
                }
                to_remove.push(hash.clone());
            }
        }
        for key in to_remove {
            if let Some((_, respond)) = new_items.remove(&key) {
                for resp in respond {
                    _ = resp.send(Ok(()));
                }
            }
        }
        if new_items.is_empty() {
            debug!("Ingest {} batch finished with updates", index_group.as_str());
            return Ok(())
        }
    }

    // Buildup the file data
    debug!("Ingest {} collecting file data", index_group.as_str());
    let files_being_ingested = new_items.len();
    let mut data = vec![];
    let mut meta = vec![];
    let mut remaining_responses = vec![];
    let mut outstanding = Vec::from_iter(new_items.keys().cloned());
    let mut active: Vec<JoinHandle<(Vec<u8>, Result<BitVec>)>> = Vec::new();
    while !new_items.is_empty() {
        while !outstanding.is_empty() && active.len() < 10 {
            if let Some(hash) = outstanding.pop(){
                debug!("Ingest {} requesting hash {}", index_group.as_str(), hex::encode(&hash));
                active.push(tokio::spawn(prepare_vector(core.clone(), hash)));
            }
        }

        let (result, _, remain) = select_all(active.into_iter()).await;
        active = remain;

        let (hash, result) = result?;
        debug!("Ingest {} finished hash {}", index_group.as_str(), hex::encode(&hash));
        if let Some((access, responses)) = new_items.remove(&hash) {
            match result {
                Ok(bits) => {
                    data.push(bits);
                    meta.push((hash, access));
                    remaining_responses.extend(responses);
                },
                Err(err) => {
                    error!("Error gathering file for {}: {err}", hex::encode(&hash));
                    for res in responses {
                        _ = res.send(Err(anyhow::format_err!("Error gathering file {err}")));
                    }
                }
            }
        }
    }

    debug!("Ingest {} batch finished collecting file data.", index_group.as_str());

    // Pick an index to merge into
    let selected_index = core.database.select_index_to_grow(&index_group).await?;

    // Merge into a new index file
    match selected_index {
        None => {
            debug!("Ingest {} building a new index", index_group.as_str());
            let final_size = TrigramFilter::guess_max_size(data.len());
            let new_blob = core.database.lease_blob().await
                .context("Leasing new blob id.")?;
            let new_index_file = core.index_cache.open_new(new_blob.to_string(), final_size).await
                .context("Opening a new file empty file in the cache space.")?;
            let out_handle = new_index_file.open()
                .context("Opening cache file to write new filter.")?;
            debug!("Ingest {} build filter file", index_group.as_str());
            tokio::task::spawn_blocking(move || {
                TrigramFilter::build_from_data(out_handle, data)?;
                anyhow::Ok(())
            }).await??;

            // Upload the new index file
            let size = new_index_file.size_to_fit().await?;
            debug!("Ingest {index_group} upload new index file as {new_blob} ({size} bytes)");
            core.index_storage.upload(new_blob.as_str(), new_index_file.path()).await?;

            // Add the new values into the database corresponding to this index
            debug!("Ingest {} update filter database", index_group.as_str());
            core.database.create_index_data(&index_group, new_blob, meta, size as u64).await?;
        }
        Some((index_id, old_blob, new_data_offset)) => {
            debug!("Ingest {} merging into filter {} {}", index_group.as_str(), index_id.as_str(), old_blob.as_str());
            // let data_size = core.index_storage.size(old_blob.as_str()).await?.ok_or(anyhow::anyhow!("Bad index id"))?;
            // let index_file = core.local_cache.open(data_size).await?;
            // let index_file_handle = index_file.open()?;

            debug!("Ingest {} downloading {}", index_group.as_str(), old_blob.as_str());
            let index_file = core.index_cache.open(old_blob.to_string()).await?;
            let index_file_handle = index_file.open()?;
            let old_data_size = index_file.size().await?;
            debug!("Ingest {index_group} downloaded {old_blob} ({} bytes)", old_data_size);

            debug!("Ingest {} creating local scratch space", index_group.as_str());
            let final_size = TrigramFilter::guess_max_size(data.len()) + old_data_size;
            let new_blob = core.database.lease_blob().await?;
            let new_index_file = core.index_cache.open_new(new_blob.to_string(), final_size).await?;
            let out_handle = new_index_file.open()?;
            debug!("Ingest {} merging data into index file", index_group.as_str());
            let _: Result<()> = tokio::task::spawn_blocking(move || {
                TrigramFilter::merge_in_data(out_handle, index_file_handle, data, new_data_offset)?;
                return Ok(())
            }).await?;

            // Upload the new index file
            let size = new_index_file.size_to_fit().await?;
            debug!("Ingest {index_group} upload new index file as {new_blob} ({size} bytes)");
            core.index_storage.upload(new_blob.as_str(), new_index_file.path()).await?;

            // Add the new values into the database corresponding to this index
            debug!("Ingest {} update filter database", index_group.as_str());
            core.database.update_index_data(&index_group, index_id, old_blob, new_blob, meta, new_data_offset, size as u64).await?;
        }
    };

    info!("Ingested batch of {} files into {}", files_being_ingested, index_group.as_str());
    for res in remaining_responses {
        _ = res.send(Ok(()));
    }
    core.garbage_collection_notification.notify_one();
    Ok(())
}

async fn prepare_vector(core: Arc<HouseCore>, hash: Vec<u8>) -> (Vec<u8>, Result<BitVec>) {
    let label = hex::encode(&hash);
    let stream = match core.file_storage.stream(&label).await {
        Ok(stream) => stream,
        Err(err) => return (hash, Err(err)),
    };

    let result = TrigramFilter::build_file(stream).await;

    return (hash, result)
}


async fn search_watcher(core: Arc<HouseCore>, mut results: mpsc::UnboundedReceiver<WorkResult>){

    let mut workers: HashMap<String, mpsc::UnboundedSender<WorkResult>> = Default::default();

    loop {
        workers.retain(|_, sender|!sender.is_closed());

        let message = match results.recv().await {
            Some(result) => result,
            None => return,
        };

        let message = if let Some(sock) = workers.get(&message.search) {
            match sock.send(message) {
                Ok(()) => continue,
                Err(err) => err.0,
            }
        } else {
            message
        };

        let (send, recv) = mpsc::unbounded_channel();
        let search = message.search.clone();
        _ = send.send(message);
        workers.insert(search.clone(), send);
        tokio::spawn(single_search_watcher(core.clone(), search, recv));
    }
}

async fn single_search_watcher(core: Arc<HouseCore>, code: String, results: mpsc::UnboundedReceiver<WorkResult>){
    if let Err(err) = _single_search_watcher(core, code, results).await {
        error!("{err}");
    }
}

pub struct SearchCache {
    pub seen: HashSet<Vec<u8>>,
    pub access: Option<HashSet<String>>,
}

async fn _single_search_watcher(core: Arc<HouseCore>, code: String, mut messages: mpsc::UnboundedReceiver<WorkResult>) -> Result<()> {

    let mut cache = SearchCache{
        seen: Default::default(),
        access: None
    };

    loop {
        tokio::select! {
            results = messages.recv() => {
                let results = match results {
                    Some(results) => results,
                    None => break,
                };

                match results.value {
                    WorkResultValue::Filter(index, _blob, file_ids) => {
                        core.database.finish_filter_work(results.id, &code, &mut cache, index, file_ids).await?;
                    }
                    WorkResultValue::Yara(files) => {
                        core.database.finish_yara_work(results.id, &results.search, files).await?;
                    }
                };
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
        }
    }
    return Ok(())
}


async fn garbage_collector(core: Arc<HouseCore>){
    loop {
        // Kick off a garbage collection job
        let result = tokio::spawn(_garbage_collector(core.clone())).await;

        match result {
            // Log errors
            Ok(result) => if let Err(err) = result {
                error!("Error in garbage collection: {err}");
            },
            // Log panics
            Err(err) => {
                error!("Panic in garbage collection: {err}");
            },
        };
    }
}

async fn _garbage_collector(core: Arc<HouseCore>) -> Result<()> {
    loop {
        // Delete any groups with expiry in the past.
        // Expiry is rounded to the day, so we give it yesterday.
        {
            let old = Some(Utc::now() - Duration::days(1));
            core.database.release_groups(IndexGroup::create(&old)).await?;
        }

        // Clean up unused blobs
        {
            let garbage_blobs = core.database.list_garbage_blobs().await?;
            for blob_id in garbage_blobs {
                core.index_storage.delete(blob_id.as_str()).await?;
                if core.index_storage.size(blob_id.as_str()).await?.is_none() {
                    core.database.release_blob(blob_id).await?;
                }
            }
        }

        // Wait a while before doing garbage collection again
        _ = tokio::time::timeout(
            core.config.garbage_collection_interval,
            core.garbage_collection_notification.notified()
        ).await;
    }
}
