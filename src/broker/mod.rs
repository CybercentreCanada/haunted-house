pub mod interface;
mod auth;
mod database;
mod database_sqlite;

use std::collections::{HashSet, HashMap, hash_map};
use std::sync::Arc;

use itertools::Itertools;
use log::{error, info};
use reqwest_middleware::{ClientWithMiddleware, ClientBuilder};
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot, RwLock};
use anyhow::Result;
use tokio::task::{JoinHandle, JoinSet};

use crate::types::{Sha256, ExpiryGroup, FileInfo, FilterID, WorkerID};
use crate::worker::interface::{UpdateFileInfoRequest, UpdateFileInfoResponse, CreateIndexRequest, IngestFilesRequest, IngestFilesResponse};

use self::auth::Authenticator;
use self::database::Database;
use self::interface::{InternalSearchStatus, SearchRequest};


pub struct CoreConfig {
    pub workers: HashMap<WorkerID, String>,
    pub per_filter_pending_limit: u64,
    pub per_worker_group_duplication: u32,
}

pub struct HouseCore {
    pub database: Database,
    pub client: ClientWithMiddleware,
//     // pub quit_trigger: tokio::sync::watch::Sender<bool>,
//     // pub quit_signal: tokio::sync::watch::Receiver<bool>,
    pub authenticator: Authenticator,
    pub config: CoreConfig,
    pub ingest_queue: mpsc::UnboundedSender<IngestMessage>,
    pub worker_ingest: HashMap<WorkerID, mpsc::UnboundedSender<WorkerIngestMessage>>,
    pub running_searches: RwLock<HashMap<String, (JoinHandle<()>, mpsc::Sender<SearcherMessage>)>>
}

impl HouseCore {
    pub async fn new(database: Database, authenticator: Authenticator, config: CoreConfig) -> Result<Arc<Self>> {
        let (send_ingest, receive_ingest) = mpsc::unbounded_channel();

        // Stop flag
        // let (quit_trigger, quit_signal) = tokio::sync::watch::channel(false);

        // Prepare our http client
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let core = Arc::new(Self {
            database,
            // quit_trigger,
            // quit_signal,
            authenticator,
            ingest_queue: send_ingest,
            config,
            client,
            worker_ingest: Default::default(),
            running_searches: RwLock::new(Default::default()),
            // search_watchers: send_search,
            // garbage_collection_notification: tokio::sync::Notify::new()
        });

        // Revive search workers for ongoing searches
        let mut searches = core.running_searches.write().await;
        for code in core.database.list_active_searches().await? {
            let (send, recv) = mpsc::channel(64);
            let handle = tokio::task::spawn(search_worker(core.clone(), recv, code.clone()));
            searches.insert(code, (handle, send));
        }

        // Launch worker inget watchers.
        for (worker, address) in core.config.workers.iter() {
            let (send, recv) = mpsc::unbounded_channel();
            let handle = tokio::task::spawn(ingest_watcher(core.clone(), recv, worker.clone(), address.clone()));
            core.worker_ingest.insert(worker.clone(), send);
        }

        tokio::spawn(ingest_worker(core.clone(), receive_ingest));
        // tokio::spawn(worker_watcher(core.clone()));
        // tokio::spawn(garbage_collector(core.clone()));

        return Ok(core)
    }

    pub async fn initialize_search(self: &Arc<Self>, req: SearchRequest) -> Result<InternalSearchStatus> {
        // Create a record for the search
        let code = hex::encode(uuid::Uuid::new_v4().as_bytes());
        let res = self.database.initialize_search(&code, &req).await?;

        // Start the search worker
        let mut searches = self.running_searches.write().await;
        let (send, recv) = mpsc::channel(64);
        let handle = tokio::task::spawn(search_worker(self.clone(), recv, code.clone()));
        searches.insert(code, (handle, send));
        return Ok(res)
    }

    pub async fn create_new_filter(self: &Arc<Self>, worker: WorkerID, expiry: ExpiryGroup) -> Result<FilterID> {
        let filter_id = self.database.create_filter(&worker, &expiry).await?;
        self.install_filter(worker, expiry, filter_id).await?;
        return Ok(filter_id)
    }

    pub async fn install_filter(self: &Arc<Self>, worker: WorkerID, expiry: ExpiryGroup, filter_id: FilterID) -> Result<()> {
        self.client.put(self.config.workers.get(&worker).unwrap().clone() + "/index/create")
        .json(&CreateIndexRequest {
            filter_id,
            expiry,
        })
        .send().await?;
        return Ok(())
    }

    pub async fn send_to_ingest_watcher(self: &Arc<Self>, worker: &WorkerID, task: IngestTask, filter_id: FilterID) -> Result<()> {
        let channel = self.worker_ingest.get(worker).ok_or_else(|| anyhow::anyhow!("Worker list out of sync"))?;
        channel.send(WorkerIngestMessage::IngestMessage((task, filter_id)));
        return Ok(())
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
    IngestMessage((IngestTask, FilterID)),
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
    let check_worker: Option<JoinHandle<Result<Vec<IngestTask>>>> = None;

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
                    None => break Ok(())
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
                    Ok(Ok(delayed)) => {
                        for task in delayed {
                            match unchecked_buffer.entry(task.info.hash) {
                                hash_map::Entry::Occupied(entry) => { entry.get_mut().merge(task); },
                                hash_map::Entry::Vacant(entry) => { entry.insert(task); },
                            };
                        }
                    },
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
}

async fn _ingest_check(core: Arc<HouseCore>, mut tasks: HashMap<Sha256, IngestTask>) -> Result<Vec<IngestTask>> {
    // Turn the update tasks into a format for the worker
    let files: Vec<FileInfo> = tasks.values().map(|f|f.info.clone()).collect();
    let payload = UpdateFileInfoRequest {files};
    let payload = serde_json::to_string(&payload)?;

    // Send off the update requests
    let mut requests = tokio::task::JoinSet::new();
    for (worker, worker_address) in core.config.workers.clone() {
        let core = core.clone();
        let payload = payload.clone();
        requests.spawn(async move {
            // Ask the worker to update information for this batch of files
            let resp = core.client.post(worker_address + "/files/update")
            .header("Content-Type", "application/json")
            .body(payload)
            .send().await?;

            // Decode the result
            let resp: UpdateFileInfoResponse = resp.json().await?;
            anyhow::Ok((worker, resp))
        });
    }

    // Collect results from each worker, dismissing tasks which we can consider processed
    let mut filter_pending = HashMap::new();
    let mut workers_without_pressure = HashSet::new();
    let mut suggestions: HashMap<Sha256, Vec<(WorkerID, FilterID)>> = HashMap::new();
    while let Some(res) = requests.join_next().await {
        let (worker, res) = match res {
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

        // If an ingestion is totally processed by the worker, we can respond to the caller
        for sha in res.proccessed {
            if let Some(task) = tasks.remove(&sha) {
                for response in task.response {
                    response.send(Ok(()));
                }
            }
        }

        // If the ingestion is in progress we can forward this task to the relivant watcher
        for (sha, filter) in res.in_progress {
            if let Some(task) = tasks.remove(&sha) {
                core.send_to_ingest_watcher(&worker, task, filter).await?;
            }
        }

        // Collate the information across workers
        filter_pending.extend(res.filter_pending.into_iter());
        if !res.storage_pressure {
            workers_without_pressure.insert(worker.clone());
        }
        for (sha, filters) in res.assignments {
            match suggestions.entry(sha) {
                hash_map::Entry::Occupied(mut entry) => { entry.get_mut().extend(filters.into_iter().map(|fid|(worker.clone(), fid))); },
                hash_map::Entry::Vacant(entry) => { entry.insert(filters.into_iter().map(|fid|(worker.clone(), fid)).collect_vec()); },
            }
        }
    }

    // Accept assignments from workers that are uncontroversial
    for sha in tasks.keys().cloned().collect_vec() {
        let task = tasks.get(&sha).unwrap();
        let mut best = None;
        let mut best_pending = u64::MAX;

        for (worker, suggested) in suggestions.get(&sha).unwrap_or(&vec![]) {
            let pending = *filter_pending.get(suggested).unwrap_or(&0);
            if pending >= core.config.per_filter_pending_limit {
                continue
            }
            if pending < best_pending {
                best_pending = pending;
                best = Some((worker.clone(), *suggested))
            }
        }

        if let Some((worker, filter)) = best {
            if let Some(task) = tasks.remove(&sha) {
                core.send_to_ingest_watcher(&worker, task, filter).await?;
            }
        }
    }

    // Check if we need to do more work or not
    if tasks.is_empty() {
        return Ok(vec![])
    }

    // Pull down the list of all filters
    let mut filter_assignment: HashMap<ExpiryGroup, Vec<(WorkerID, FilterID)>> = Default::default();
    for (worker, filter, expiry) in core.database.list_filters().await? {
        // if !storage_pressure.get(&worker).unwrap_or(&true) {
            match filter_assignment.entry(expiry) {
                hash_map::Entry::Occupied(mut entry) => { entry.get_mut().push((worker, filter)); },
                hash_map::Entry::Vacant(entry) => { entry.insert(vec![(worker, filter)]); },
            };
        // }
    }

    // Since none of the workers recommended an OK filter for this we need to
    // look at creating new filters to contain these files
    let mut delayed = vec![];
    let mut new_filters = HashMap::new();
    'tasks: for (_, task) in tasks {
        // If we already made a filter for this group use it
        if let Some((worker, filter)) = new_filters.get(&task.info.expiry) {
            core.send_to_ingest_watcher(worker, task, *filter).await?;
            continue
        }

        // count related filters on each worker
        let mut filter_count = HashMap::<WorkerID, u32>::new();
        for worker in &workers_without_pressure {
            let mut count = 0;
            for filter_set in filter_assignment.get(&task.info.expiry) {
                for (ww, ff) in filter_set {
                    if ww == worker {
                        count += 1;
                    }
                }
            }
            filter_count.insert(worker.clone(), count);
        }

        // try to find a worker without a related filter to create one on
        // Then start doubling up filters until the limit
        for limit in 0..core.config.per_worker_group_duplication {
            for (worker, count) in &filter_count {
                if *count <= limit {
                    let filter_id = core.create_new_filter(worker.clone(), task.info.expiry.clone()).await?;
                    new_filters.insert(task.info.expiry.clone(), (worker.clone(), filter_id));
                    core.send_to_ingest_watcher(worker, task, filter_id).await?;
                    continue 'tasks
                }
            }
        }

        // Reject the ingestion command
        delayed.push(task);
    }

    Ok(delayed)
}

//
async fn ingest_watcher(core: Arc<HouseCore>, mut input: mpsc::UnboundedReceiver<WorkerIngestMessage>, worker: WorkerID, address: String) {
    loop {
        match _ingest_watcher(core.clone(), &mut input, &worker, &address).await {
            Err(err) => error!("Crash in ingestion system: {err}"),
            Ok(()) => {
                info!("Ingest worker stopped.");
                break;
            }
        }
    }
}

async fn _ingest_watcher(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<WorkerIngestMessage>, worker: &WorkerID, address: &String) -> Result<()> {
    let mut active: HashMap<Sha256, (FilterID, IngestTask)> = Default::default();
    let mut query: Option<JoinHandle<Result<reqwest::Response>>> = None;

    loop {
        //
        if query.is_none() && !active.is_empty() {
            let request = core.client.post(address.clone() + "/files/ingest")
            .json(&IngestFilesRequest{
                files: active.values().map(|(filter, task)|{
                    (*filter, task.info.clone())
                }).collect_vec()
            });
            query = Some(tokio::spawn(async move {
                let result = request.send().await?;
                anyhow::Ok(result)
            }))
        }

        // Wait until something changes
        tokio::select!{
            // Watch for command messages
            message = input.recv() => {
                let message = match message {
                    Some(message) => message,
                    None => break Ok(())
                };

                match message {
                    WorkerIngestMessage::IngestMessage((task, filter)) => {
                        match active.entry(task.info.hash.clone()) {
                            hash_map::Entry::Occupied(mut entry) => {
                                let (old_filter, old_task) = entry.get_mut();
                                if old_task.info.expiry < task.info.expiry {
                                    *old_filter = filter;
                                } 
                                old_task.merge(task);
                            },
                            hash_map::Entry::Vacant(entry) => { entry.insert((filter, task)); },
                        }
                    },
                    WorkerIngestMessage::Status(_) => todo!(),
                }
            },

            response = query.as_mut().unwrap(), if query.is_some() => {
                query = None;
                let response: IngestFilesResponse = match response {
                    Ok(Ok(resp)) => resp.json().await?,
                    Ok(Err(err)) => {
                        error!("Ingest error: {err}");
                        continue;
                    },
                    Err(err) => {
                        error!("Ingest error: {err}");
                        continue;
                    },
                };

                // Pull out the tasks that have been finished
                for hash in response.completed {
                    if let Some((_, task)) = active.remove(&hash) {
                        for response in task.response {
                            response.send(Ok(()));
                        }
                    }
                }

                // refresh any missing filters
                todo!();
            }
        }
    }
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
    let status = match core.database.search_record(code).await? {
        Some(status) => status,
        None => return Ok(()),
    };
    if status.finished {
        return Ok(())
    }

    // Run through filters
    let mut requests: JoinSet<Result<reqwest::Response>> = JoinSet::new();
    for (worker, address) in &core.config.workers {
        requests.spawn(async move {
            
        })
    }

    while !requests.is_empty() {
        tokio::select!{
            message = input.recv() => {
                // Read an update command
                let message = match message {
                    Some(message) => message,
                    None => break
                };

                todo!();
                // match message {                    
                //     // SearcherMessage::Status(status) => {
                //     //     status.send(InternalSearchStatus {
                //     //         view: ,
                //     //         resp:
                //     //     })
                //     // },
                // }
            },
            response = requests.join_next() => {
                let response: IngestFilesResponse = match response {
                    Ok(Ok(resp)) => resp.json().await?,
                    Ok(Err(err)) => {
                        error!("Ingest error: {err}");
                        continue;
                    },
                    Err(err) => {
                        error!("Ingest error: {err}");
                        continue;
                    },
                };
                todo!();
            }
        }
    }

    // Run through yara jobs
    todo!();


    // Save results
    todo!()

}