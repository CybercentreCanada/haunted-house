pub mod interface;
mod auth;
mod database;
mod database_sqlite;

use std::collections::{HashSet, VecDeque, HashMap, hash_map};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use log::{error, info};
use reqwest_middleware::ClientWithMiddleware;
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot, RwLock};
use anyhow::Result;
use tokio::task::JoinHandle;

use crate::access::AccessControl;
use crate::types::{Sha256, ExpiryGroup, FileInfo};
use crate::worker::interface::{UpdateFileInfoRequest, UpdateFileInfoResponse};

use self::auth::Authenticator;
use self::database::Database;
use self::interface::{InternalSearchStatus, SearchRequest};


pub struct HouseCore {
    pub database: Database,
    pub client: ClientWithMiddleware,
//     // pub quit_trigger: tokio::sync::watch::Sender<bool>,
//     // pub quit_signal: tokio::sync::watch::Receiver<bool>,
//     pub file_storage: BlobStorage,
//     pub index_storage: BlobStorage,
//     pub index_cache: BlobCache,
    pub authenticator: Authenticator,
//     pub config: CoreConfig,
    pub ingest_queue: mpsc::UnboundedSender<IngestMessage>,
    pub worker_ingest: RwLock<HashMap<String, (JoinHandle<()>, mpsc::UnboundedSender<WorkerIngestMessage>)>>,
    pub running_searches: RwLock<HashMap<String, (JoinHandle<()>, mpsc::Sender<SearcherMessage>)>>
//     garbage_collection_notification: tokio::sync::Notify,
}

impl HouseCore {
    pub fn new(database: Database, authenticator: Authenticator, config: CoreConfig) -> Result<Arc<Self>> {
        let (send_ingest, receive_ingest) = mpsc::unbounded_channel();

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

        todo!("Revive search workers for ongoing searches.");

        tokio::spawn(ingest_worker(core.clone(), receive_ingest));
        // tokio::spawn(search_watcher(core.clone(), receive_search));
        // tokio::spawn(worker_watcher(core.clone()));
        // tokio::spawn(garbage_collector(core.clone()));

        return Ok(core)
    }

    pub async fn initialize_search(self: &Arc<Self>, req: SearchRequest) -> Result<InternalSearchStatus> {
        // Create a record for the search
        let code = hex::encode(uuid::Uuid::new_v4().as_bytes());
        let res = self.database.initialize_search(&code, &req).await?;

        // Start the search worker
        let searches = self.running_searches.write().await;
        let (send, recv) = mpsc::channel(64);
        let handle = tokio::task::spawn(search_worker(self.clone(), recv, code));
        searches.insert(code, (handle, send));
        return Ok(res)
    }

    pub async fn get_workers(self: &Arc<Self>) -> Vec<String> {
        todo!();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestStatus {
    pub active_batch: Option<u64>,
    pub queue_size: u64,
}

#[derive(Debug)]
struct IngestTask {
    pub info: FileInfo,
    pub response: Vec<oneshot::Sender<Result<()>>>
}

impl IngestTask {
    pub fn merge(&mut self, task: IngestTask) {
        self.info.expiry = self.info.expiry.max(task.info.expiry);
        self.info.access = self.info.access.or(&task.info.access).simplify();
        self.response.extend(task.response.into_iter());
    }
}

#[derive(Debug)]
pub enum IngestMessage {
    IngestMessage(IngestTask),
    Status(oneshot::Sender<IngestStatus>)
}

#[derive(Debug)]
pub enum WorkerIngestMessage {
    IngestMessage(IngestTask),
    Status(oneshot::Sender<IngestStatus>)
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

async fn _ingest_worker(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<IngestMessage>) -> Result<()> {
    let unchecked_buffer = HashMap::<Sha256, IngestTask>::new();
    let check_worker: Option<JoinHandle<Result<()>>> = None;

    loop {
        // Restart the check worker
        if check_worker.is_none() && !unchecked_buffer.is_empty() {
            let core = core.clone();
            // let tasks = ;
            check_worker = Some(tokio::spawn(_ingest_check(core, unchecked_buffer)));
            unchecked_buffer = Default::default();
        }

        //
        tokio::select!{
            // Watch for command messages
            message = input.recv() => {
                let message = match message {
                    Some(message) => message,
                    None => break
                };

                match message {
                    IngestMessage::IngestMessage(task) => {
                        match unchecked_buffer.entry(task.info.hash) {
                            hash_map::Entry::Occupied(entry) => { entry.get_mut().merge(task); },
                            hash_map::Entry::Vacant(entry) => { entry.insert(task); },
                        };
                    },
                    IngestMessage::Status(_) => todo!(),
                }
            }

            // If a check worker is running watch for it
            res = check_worker.unwrap(), if check_worker.is_some() => {
                check_worker = None;
                match res {
                    Ok(Ok(())) => {},
                    Ok(Err(err)) => {
                        error!("ingest checker error: {err}");
                    }
                    Err(err) => {
                        error!("ingest checker error: {err}");
                    },
                }
            }
        }
    }
    return Ok(())
}

async fn _ingest_check(core: Arc<HouseCore>, mut tasks: HashMap<Sha256, IngestTask>) -> Result<()> {
    // Turn the update tasks into a format for the worker
    let files: Vec<FileInfo> = tasks.values().map(|f|f.info.clone()).collect();
    let payload = UpdateFileInfoRequest {files};
    let payload = serde_json::to_string(&payload)?;

    // Send off the update requests
    let mut requests = tokio::task::JoinSet::new();
    for worker in core.get_workers().await {
        let core = core.clone();
        let payload = payload.clone();
        requests.spawn(async move {
            // Ask the worker to update information for this batch of files
            let resp = core.client.post(worker + "/file/update")
            .header("Content-Type", "application/json")
            .body(payload)
            .send().await?;

            // Decode the result
            let resp: UpdateFileInfoResponse = resp.json().await?;
            anyhow::Ok(resp)
        });
    }

    // Collect tasks that could be processed fully already
    while let Some(res) = requests.join_next().await {
        let res = match res {
            Ok(Ok(data)) => data,
            Ok(Err(err)) => {
                error!("{err}");
                continue
            }
            Err(err) => {
                error!("{err}");
                continue
            },
        };

        for sha in res.proccessed {
            if let Some(task) = tasks.remove(&sha) {
                for response in task.response {
                    response.send(Ok(()));
                }
            }
        }
    }

    // Forward the remaining tasks for ingestion
    let workers = core.worker_ingest.write().await;
    for (_, task) in tasks {
        workers.entry(key)
    }
    Ok(())
}



//
pub enum SearcherMessage {
    Status(oneshot::Sender<InternalSearchStatus>)
}

async fn search_worker(core: Arc<HouseCore>, mut input: mpsc::Receiver<SearcherMessage>, code: String) {
    loop {
        match _search_worker(core.clone(), &mut input, &code).await {
            Err(err) => error!("Crash in ingestion system: {err}"),
            Ok(()) => break,
        }
    }
}

async fn _search_worker(core: Arc<HouseCore>, input: &mut mpsc::Receiver<SearcherMessage>, code: &str) -> Result<()> {
    // Load the search status
    todo!();

    // Process the search
    loop {
        tokio::select!{
            message = input.recv() => {
                // Read an update command
                let message = match message {
                    Some(message) => message,
                    None => break
                };

                match message {
                    SearcherMessage::Status(status) => {



                        status.send(InternalSearchStatus {
                            view: ,
                            resp:
                        })
                    },
                }
            }
        }
    }
}