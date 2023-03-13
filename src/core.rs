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
    pub queue_size: u64,
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
        tokio::spawn(search_workers(core.clone()));
        tokio::spawn(garbage_collector(core.clone()));

        return Ok(core)
    }

    pub async fn ingest_status(&self) -> Result<IngestStatus> {
        let (send, recv) = oneshot::channel();
        self.ingest_queue.send(IngestMessage::Status(send))?;
        return Ok(recv.await?);
    }

    pub async fn initialize_search(&self, req: SearchRequest) -> Result<InternalSearchStatus> {
        let res = self.database.initialize_search(req).await?;
        return Ok(res)
    }

    pub async fn search_status(&self, code: String) -> Result<Option<InternalSearchStatus>> {
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

// async fn wait_for_batch_or_ready(mut job: &mut Option<JoinHandle<()>>, instant: tokio::time::Instant) -> Result<()> {
//     match &mut job {
//         Some(task) => {
//             task.await?;
//         },
//         None => {
//             tokio::time::sleep_until(instant).await;
//         },
//     };
//     Ok(())
// }

async fn _ingest_worker(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<IngestMessage>) -> Result<()> {

    let mut buffer: VecDeque<(Vec<u8>, AccessControl, IndexGroup, oneshot::Sender<Result<()>>)> = Default::default();
    let mut active: JoinSet<()> = JoinSet::new();

    loop {
        debug!("Ingest loop");

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
                            _ = respond.send(Ok(()));
                            continue
                        }

                        // Add to current running or buffer
                        if active.len() >= core.config.index_workers as usize {
                            buffer.push_back((hash, access, index_group, respond))
                        } else {
                            let core = core.clone();
                            active.spawn(async move {
                                match tokio::spawn(_run_ingest(core, index_group, hash, access)).await {
                                    Ok(res) => respond.send(res),
                                    Err(err) => respond.send(Err(anyhow::anyhow!("{err}"))),
                                };
                            });
                        }
                    },
                    IngestMessage::Status(response) => {
                        _ = response.send(IngestStatus {
                            active_workers: active.len() as u64,
                            queue_size: buffer.len() as u64
                        });
                    }
                }
            }

            // If the current batch finishes, or another batch should be checked
            _ = active.join_next(), if !active.is_empty() => {
                if active.len() < core.config.index_workers as usize {
                    if let Some((hash, access, index_group, respond)) = buffer.pop_front() {
                        let core = core.clone();
                        active.spawn(async move {
                            match tokio::spawn(_run_ingest(core, index_group, hash, access)).await {
                                Ok(res) => respond.send(res),
                                Err(err) => respond.send(Err(anyhow::anyhow!("{err}"))),
                            };
                        });
                    }
                }
            }
        }
    }
    return Ok(())
}


async fn _run_ingest(core: Arc<HouseCore>, index_group: IndexGroup, hash: Vec<u8>, access: AccessControl) -> Result<()> {
    // Try to update in place without changing indices
    debug!("Ingesting {}", hex::encode(&hash));
    if core.database.update_file_access(&hash, &access, &index_group).await? {
        debug!("Ingesting {} by updating file data", hex::encode(&hash));
        return Ok(())
    }

    // Collect the file and build its trigrams
    let mut trigrams = BitVec::repeat(false, 1 << 24);
    {
        // Get file content
        let label = hex::encode(&hash);
        let mut stream = core.file_storage.stream(&label).await?;

        // Read the initial block
        let mut buffer = vec![];
        while buffer.len() <= 2 {
            let sub_buffer = match stream.recv().await {
                Some(sub_buffer) => sub_buffer?,
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
                    Some(buffer) => buffer?,
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
        let filter = SimpleFilter::build(size, &trigrams)?;
        if power == bloom::END_POWER || filter.density() < core.config.target_density {
            let size = core.database.insert_file(&hash, &access, &index_group, &filter).await?;
            todo!("build hash block");
            return Ok(());
        }
    }

    return Ok(())
}


async fn search_workers(core: Arc<HouseCore>){
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

        let result = tokio::spawn(async move {
            loop {
                // workers.retain(|_, sender|!sender.is_closed());

                let message = match results.recv().await {
                    Some(result) => result,
                    None => return Ok(()),
                };
                core.database.finish_yara_work(message.id, &message.search, message.yara_hits).await?;
            }
        });

        match result.await {
            Ok(res) => match res {
                Ok(()) => return,
                Err(err) => {
                    error!("Error handling worker results: {err}")
                }
            },
            Err(err) => {
                error!("Error handling worker results: {err}")
            }
        }
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
