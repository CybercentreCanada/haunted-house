use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::auth::Authenticator;
use crate::blob_cache::BlobCache;
use crate::bloom::{self, SimpleFilter};
use crate::database::{Database, IndexGroup};
use crate::interface::{InternalSearchStatus, SearchRequest, WorkRequest, WorkResult, WorkPackage, WorkError};
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
use tokio::task::{JoinHandle, JoinSet};
use weak_self::WeakSelf;


#[derive(Debug, Serialize, Deserialize)]
pub struct IngestStatus {
    pub active_workers: u64,
    pub max_workers: u64,
    pub build_queue_size: u64,
    pub insert_queues: HashMap<String, u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchStatus {
}

#[derive(Debug)]
pub enum IngestMessage {
    IngestMessage(Vec<u8>, AccessControl, Option<DateTime<Utc>>, oneshot::Sender<Result<()>>),
    Status(oneshot::Sender<IngestStatus>)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreConfig {
    #[serde(default="default_index_workers")]
    pub index_workers: u64,
    #[serde(default="default_search_workers")]
    pub search_workers: u64,
    #[serde(default="default_target_density")]
    pub target_density: f64,
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

fn default_target_density() -> f64 { 0.5 }
fn default_index_workers() -> u64 { 8 }
fn default_search_workers() -> u64 { 8 }
fn default_garbage_collection_interval() -> std::time::Duration { std::time::Duration::from_secs(60) }
fn default_index_soft_bytes_max() -> u64 { 50 << 30 }
fn default_index_soft_entries_max() -> u64 { 50 << 30 }
fn default_yara_job_size() -> u64 { 1000 }
fn default_max_result_set_size() -> u64 { 100_000 }
fn default_task_deadline() -> std::time::Duration { std::time::Duration::from_secs(60 * 60) }
fn default_task_start_time() -> std::time::Duration { std::time::Duration::from_secs(60) }
fn default_task_heartbeat_interval() -> std::time::Duration { std::time::Duration::from_secs(60 * 2) }

impl Default for CoreConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

pub struct HouseCore {
    weak_self: WeakSelf<Self>,
    pub database: Database,
    // pub quit_trigger: tokio::sync::watch::Sender<bool>,
    // pub quit_signal: tokio::sync::watch::Receiver<bool>,
    pub file_storage: BlobStorage,
    // pub index_storage: BlobStorage,
    // pub index_cache: BlobCache,
    pub authenticator: Authenticator,
    pub config: CoreConfig,
    pub ingest_queue: mpsc::UnboundedSender<IngestMessage>,
    insert_queue: mpsc::UnboundedSender<InsertMessage>,
    search_watchers: mpsc::UnboundedSender<WorkResult>,
    garbage_collection_notification: tokio::sync::Notify,
}

impl HouseCore {
    pub fn new(file_storage: BlobStorage, database: Database, authenticator: Authenticator, config: CoreConfig) -> Result<Arc<Self>> {
        let (send_ingest, receive_ingest) = mpsc::unbounded_channel();
        let (send_insert, receive_insert) = mpsc::unbounded_channel();
        let (send_search, receive_search) = mpsc::unbounded_channel();

        // Stop flag
        // let (quit_trigger, quit_signal) = tokio::sync::watch::channel(false);

        let core = Arc::new(Self {
            weak_self: WeakSelf::new(),
            database,
            // quit_trigger,
            // quit_signal,
            file_storage,
            // index_storage,
            // index_cache,
            authenticator,
            ingest_queue: send_ingest,
            insert_queue: send_insert,
            config,
            search_watchers: send_search,
            garbage_collection_notification: tokio::sync::Notify::new()
        });
        core.weak_self.init(&core);

        tokio::spawn(ingest_worker(core.clone(), receive_ingest));
        tokio::spawn(insert_worker(core.clone(), receive_insert));
        tokio::spawn(search_watcher(core.clone(), receive_search));
        tokio::spawn(worker_watcher(core.clone()));
        tokio::spawn(search_workers(core.clone()));
        tokio::spawn(garbage_collector(core.clone()));

        return Ok(core)
    }

    pub async fn ingest_status(&self) -> Result<IngestStatus> {
        let (send, ingest_recv) = oneshot::channel();
        self.ingest_queue.send(IngestMessage::Status(send))?;
        let (send, insert_recv) = oneshot::channel();
        if let Err(err) = self.insert_queue.send(InsertMessage::Status(send)){
            return Err(anyhow::anyhow!("Couldn't fetch insert status"));
        }
        let mut value = ingest_recv.await?;
        value.insert_queues = insert_recv.await?.0;
        Ok(value)
    }

    pub async fn initialize_search(&self, req: SearchRequest) -> Result<InternalSearchStatus> {
        let res = self.database.initialize_search(req).await?;
        return Ok(res)
    }

    pub async fn search_status(&self, code: String) -> Result<Option<InternalSearchStatus>> {
        self.database.search_status(code).await
    }

    // pub async fn get_work(&self, req: &WorkRequest) -> Result<WorkPackage> {
    //     self.database.get_work(req).await
    // }

    // pub async fn get_work_notification(&self) -> Result<()> {
    //     self.database.get_work_notification().await
    // }

    pub fn finish_work(&self, req: WorkResult) -> Result<()> {
        match self.search_watchers.send(req){
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow::anyhow!("result processor error: {err}")),
        }
    }

    // pub async fn work_error(&self, err: WorkError) -> Result<()> {
    //     self.database.work_error(err).await
    // }
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

#[derive(Clone, Debug)]
struct IngestData {
    hash: Vec<u8>,
    access: AccessControl,
    index: IndexGroup
}

#[derive(Debug)]
struct IngestTask {
    data: IngestData,
    response: oneshot::Sender<Result<()>>
}

async fn _ingest_worker(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<IngestMessage>) -> Result<()> {

    let mut buffer: VecDeque<IngestTask> = Default::default();
    let mut active: JoinSet<()> = JoinSet::new();

    loop {
        // Wait for more ingest messages
        tokio::select!{
            message = input.recv() => {
                debug!("Ingest message");
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
                            _ = respond.send(Ok(()));
                            continue
                        }

                        let task = IngestTask {
                            data: IngestData{hash, access, index: index_group},
                            response: respond
                        };

                        // Add to current running or buffer
                        if active.len() >= core.config.index_workers as usize {
                            buffer.push_back(task)
                        } else {
                            let core = core.clone();
                            active.spawn(run_ingest_build_filter(core, task));
                            debug!("Currently running: {}", active.len())
                        }
                    },
                    IngestMessage::Status(response) => {
                        _ = response.send(IngestStatus {
                            active_workers: active.len() as u64,
                            max_workers: core.config.index_workers,
                            build_queue_size: buffer.len() as u64,
                            insert_queues: Default::default()
                        });
                    }
                }
            }

            // If the current batch finishes, or another batch should be checked
            result = active.join_next(), if !active.is_empty() => {
                debug!("Ingest task done");

                // Check if there was an error
                if let Some(Err(err)) = result {
                    error!("Crash in ingest: {err}")
                }

                // Check if there is something new to add
                if active.len() < core.config.index_workers as usize {
                    if let Some(task) = buffer.pop_front() {
                        let core = core.clone();
                        active.spawn(run_ingest_build_filter(core, task));
                        debug!("Currently running: {}", active.len())
                    }
                }
            }
        }
    }
    return Ok(())
}

async fn run_ingest_build_filter(core: Arc<HouseCore>, task: IngestTask) {
    match tokio::spawn(_run_ingest_build_filter(core.clone(), task.data.clone())).await {
        Ok(Ok(Some(filter))) => {
            _ = core.insert_queue.send(InsertMessage::Insert{
                task,
                filter,
            })
        },
        Ok(Ok(None)) => _ = task.response.send(Ok(())),
        Ok(Err(err)) => _ = task.response.send(Err(anyhow::anyhow!("ingest error: {err:?}"))),
        Err(err) => _ = task.response.send(Err(anyhow::anyhow!("ingest task error: {err:?}"))),
    };
}

async fn _run_ingest_build_filter(core: Arc<HouseCore>, task: IngestData) -> Result<Option<SimpleFilter>> {

    let index_group: IndexGroup = task.index;
    let hash: Vec<u8> = task.hash;
    let access: AccessControl = task.access;

    // Try to update in place without changing indices
    debug!("Ingesting {}", hex::encode(&hash));
    if core.database.update_file_access(&hash, &access, &index_group).await.context("update_file_access")? {
        debug!("Ingesting {} by updating file data", hex::encode(&hash));
        return Ok(None)
    }

    // Collect the file and build its trigrams
    debug!("Collect data for {}", hex::encode(&hash));
    let mut trigrams = BitVec::repeat(false, 1 << 24);
    {
        // Get file content
        let label = hex::encode(&hash);
        let mut stream = core.file_storage.stream(&label).await.context("streaming ingested file")?;

        // Read the initial block
        let mut buffer = vec![];
        while buffer.len() <= 2 {
            let sub_buffer = match stream.recv().await {
                Some(sub_buffer) => sub_buffer.context("connecion error streaming ingested file")?,
                None => break,
            };

            buffer.extend(sub_buffer);
        }

        // Initialize trigram
        if buffer.len() >= 2 {
            let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
            let mut index_start = 2;

            loop {
                for byte in &buffer[index_start..] {
                    trigram = (trigram & 0x00FFFF) << 8 | (*byte as u32);
                    trigrams.set(trigram as usize, true);
                }

                buffer = match stream.recv().await {
                    Some(buffer) => buffer.context("connecion error streaming ingested file")?,
                    None => break,
                };
                if buffer.is_empty() {
                    break;
                }
                index_start = 0;
            }
        }
    }

    // Try filter configurations until we find one that matches
    for power in bloom::START_POWER..=bloom::END_POWER {
        let size = 1 << power;
        debug!("Attempting {size} {}", hex::encode(&hash));
        let filter = SimpleFilter::build(size, &trigrams);
        if power == bloom::END_POWER || filter.density() < core.config.target_density {
            return Ok(Some(filter));
        }
    }
    return Err(anyhow::anyhow!("Fallthrough on ingest loop: {}", hex::encode(&hash)))
}

enum InsertMessage {
    Insert {
        task: IngestTask,
        filter: SimpleFilter,
    },
    Status(oneshot::Sender<(HashMap<String, u64>, )>),
}

async fn insert_worker(core: Arc<HouseCore>, mut input: mpsc::UnboundedReceiver<InsertMessage>) {
    loop {
        match _insert_worker(core.clone(), &mut input).await {
            Err(err) => error!("Crash in insertion system: {err}"),
            Ok(()) => {
                info!("Insert worker stopped.");
                break;
            }
        }
    }
}

async fn _insert_worker(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<InsertMessage>) -> Result<()> {

    let mut buffer: HashMap<(IndexGroup, String), VecDeque<(IngestTask, SimpleFilter)>> = Default::default();
    let mut active: JoinSet<()> = JoinSet::new();
    let mut busy: HashMap<(IndexGroup, String), tokio::task::AbortHandle> = Default::default();

    loop {
        // Wait for more ingest messages
        tokio::select!{
            message = input.recv() => {
                debug!("Ingest message");
                // Read an update command
                let message = match message {
                    Some(message) => message,
                    None => break
                };

                match message {
                    InsertMessage::Insert{task, filter} => {
                        let key = (task.data.index.clone(), filter.kind());

                        if !busy.contains_key(&key) {
                            busy.insert(key, active.spawn(run_insert(core.clone(), task, filter)));
                        } else {
                            match buffer.entry(key) {
                                std::collections::hash_map::Entry::Occupied(mut entry) => {
                                    entry.get_mut().push_back((task, filter));
                                },
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    entry.insert([(task, filter)].into());
                                },
                            }
                        }
                    },
                    InsertMessage::Status(response) => {

                        let counts = buffer.iter().map(|(k, v)|{
                            (k.0.to_string() + ":" + &k.1, v.len() as u64)
                        }).collect();

                        _ = response.send((counts, ));
                    }
                }
            }

            // If the current batch finishes, or another batch should be checked
            _ = active.join_next(), if !active.is_empty() => {
                // Clear out finished jobs
                busy.retain(|_, v| !v.is_finished());

                // kick off new jobs
                for (key, queue) in buffer.iter_mut() {
                    if !busy.contains_key(key) {
                        if let Some((task, filter)) = queue.pop_front() {
                            busy.insert(key.clone(), active.spawn(run_insert(core.clone(), task, filter)));
                        }
                    }
                }

                // Clear out empty queues
                buffer.retain(|_, v| !v.is_empty())
            }
        }
    }
    return Ok(())
}

async fn run_insert(core: Arc<HouseCore>, task: IngestTask, filter: SimpleFilter) {
    // Make sure the expiry isn't already due
    let today_group = IndexGroup::create(&Some(Utc::now()));
    if task.data.index <= today_group {
        _ = task.response.send(Ok(()));
        return
    }

    match core.database.update_file_access(&task.data.hash, &task.data.access, &task.data.index).await.context("update_file_access") {
        Err(err) => {
            error!("error updating file access in insert: {err}");
        }
        Ok(false) => {},
        Ok(true) => {
            debug!("Ingesting {} by updating file data", hex::encode(&task.data.hash));
            _ = task.response.send(Ok(()));
            return
        }
    }

    // insert the data
    _ = match core.database.insert_file(&task.data.hash, &task.data.access, &task.data.index, &filter).await.context("insert_file") {
        Ok(()) => task.response.send(Ok(())),
        Err(err) => task.response.send(Err(err)),
    };
    debug!("Ingested {}", hex::encode(&task.data.hash));
}

async fn search_workers(core: Arc<HouseCore>){
    todo!("search not implemented");
    let mut workers = tokio::task::JoinSet::new();

    loop {
        while workers.len() < core.config.search_workers as usize {
            workers.spawn(_search_worker(core.clone()));
        }

        if let Some(res) = workers.join_next().await {
            match res {
                Ok(res) => if let Err(err) = res {
                    error!("Error in search worker: {err}")
                },
                Err(err) => {
                    error!("Error in search worker: {err}")
                },
            }
        }
    }
}

async fn _search_worker(core: Arc<HouseCore>) -> Result<()> {
    todo!()
}


async fn search_watcher(core: Arc<HouseCore>, mut results: mpsc::UnboundedReceiver<WorkResult>){
    loop {
        let message = match results.recv().await {
            Some(result) => result,
            None => break,
        };
        if let Err(err) = core.database.finish_yara_work(message.id, &message.search, message.yara_hits).await {
            error!("Error in search progress: {err}");
        };
    }
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
        // {
        //     let garbage_blobs = core.database.list_garbage_blobs().await?;
        //     for blob_id in garbage_blobs {
        //         core.index_storage.delete(blob_id.as_str()).await?;
        //         if core.index_storage.size(blob_id.as_str()).await?.is_none() {
        //             core.database.release_blob(blob_id).await?;
        //         }
        //     }
        // }

        // Wait a while before doing garbage collection again
        _ = tokio::time::timeout(
            core.config.garbage_collection_interval,
            core.garbage_collection_notification.notified()
        ).await;
    }
}
