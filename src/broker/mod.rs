pub mod interface;
pub mod auth;
mod database;
mod database_sqlite;

use std::collections::{HashSet, HashMap, hash_map, BTreeSet, VecDeque};
use std::sync::Arc;

use futures::{StreamExt, SinkExt};
use itertools::Itertools;
use log::{error, info, debug};
use rand::{thread_rng, Rng};
use reqwest_middleware::{ClientWithMiddleware, ClientBuilder};
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot, RwLock};
use anyhow::{Result, Context};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::Message;

use crate::broker::interface::SearchRequestResponse;
use crate::config::{CoreConfig, WorkerAddress, WorkerTLSConfig};
use crate::sqlite_set::SqliteSet;
use crate::types::{Sha256, ExpiryGroup, FileInfo, FilterID, WorkerID};
use crate::worker::YaraTask;
use crate::worker::interface::{UpdateFileInfoRequest, UpdateFileInfoResponse, CreateIndexRequest, IngestFilesRequest, IngestFilesResponse, FilterSearchRequest, FilterSearchResponse, YaraSearchResponse};

use self::auth::Authenticator;
use self::database::Database;
use self::interface::{InternalSearchStatus, SearchRequest, StatusReport};

pub async fn main(config: crate::config::Config) -> Result<()> {
    // Initialize authenticator
    info!("Initializing Authenticator");
    let auth = Authenticator::from_config(config.authentication)?;

    // Initialize database
    info!("Connecting to database.");
    let database = match config.database {
        crate::config::Database::SQLite{path} => Database::new_sqlite(&path).await?,
        crate::config::Database::SQLiteTemp{..} => Database::new_sqlite_temp().await?,
    };

    // Start server core
    info!("Starting server core.");
    let core = HouseCore::new(database,auth, config.core).await
        .context("Error launching core.")?;

    // Start http interface
    let bind_address = match config.bind_address {
        None => "localhost:8080".to_owned(),
        Some(address) => address,
    };
    info!("Starting server interface on {bind_address}");
    let api_job = tokio::task::spawn(interface::serve(bind_address, config.tls, core.clone()));

    // Wait for server to stop
    api_job.await.context("Error in HTTP interface.")?;
    return Ok(())
}



pub struct HouseCore {
    pub database: Database,
    pub client: ClientWithMiddleware,
//     // pub quit_trigger: tokio::sync::watch::Sender<bool>,
//     // pub quit_signal: tokio::sync::watch::Receiver<bool>,
    pub authenticator: Authenticator,
    pub config: CoreConfig,
    pub ingest_queue: mpsc::UnboundedSender<IngestMessage>,
    pub worker_ingest: RwLock<HashMap<WorkerID, mpsc::UnboundedSender<WorkerIngestMessage>>>,
    pub running_searches: RwLock<HashMap<String, (JoinHandle<()>, mpsc::Sender<SearcherMessage>)>>,
    pub yara_permits: deadpool::unmanaged::Pool<(WorkerID, WorkerAddress)>,
}

impl HouseCore {
    pub async fn new(database: Database, authenticator: Authenticator, config: CoreConfig) -> Result<Arc<Self>> {
        let (send_ingest, receive_ingest) = mpsc::unbounded_channel();

        // setup pool for yara assignments
        let yara_permits: deadpool::unmanaged::Pool<(WorkerID, WorkerAddress)> = Default::default();
        for _ in 0..(config.yara_jobs_per_worker.max(1)) {
            for row in config.workers.clone() {
                if let Err((_, err)) = yara_permits.add(row).await {
                    return Err(err.into())
                }
            }
        }

        // Stop flag
        // let (quit_trigger, quit_signal) = tokio::sync::watch::channel(false);

        // Prepare our http client
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(std::time::Duration::from_millis(50), std::time::Duration::from_secs(30))
            .build_with_total_retry_duration(chrono::Duration::days(1).to_std()?);
        let client = match &config.worker_certificate {
            WorkerTLSConfig::AllowAll => {
                reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()?
            },
            WorkerTLSConfig::Certificate(cert) => {
                reqwest::Client::builder()
                .add_root_certificate(reqwest::Certificate::from_pem(cert.as_bytes())?)
                .build()?
            }
        };
        let client = ClientBuilder::new(client)
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
            worker_ingest: RwLock::new(Default::default()),
            running_searches: RwLock::new(Default::default()),
            yara_permits
            // search_watchers: send_search,
            // garbage_collection_notification: tokio::sync::Notify::new()
        });

        // Revive search workers for ongoing searches
        {
            let mut searches = core.running_searches.write().await;
            for code in core.database.list_active_searches().await? {
                let (send, recv) = mpsc::channel(64);
                let handle = tokio::task::spawn(search_worker(core.clone(), recv, code.clone()));
                searches.insert(code, (handle, send));
            }
        }

        // Launch worker inget watchers.
        {
            let mut worker_ingest = core.worker_ingest.write().await;
            for (worker, address) in core.config.workers.iter() {
                let (send, recv) = mpsc::unbounded_channel();
                tokio::task::spawn(ingest_watcher(core.clone(), recv, worker.clone(), address.clone()));
                worker_ingest.insert(worker.clone(), send);
            }
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

    // pub async fn create_new_filter(self: &Arc<Self>, worker: WorkerID, expiry: ExpiryGroup, filter_id: ) -> Result<FilterID> {
    //     info!("Create new filter for worker {worker} in expiry group {expiry}");
    //     // let filter_id = self.database.create_filter(&worker, &expiry).await?;
    //     self.install_filter(worker, expiry, filter_id).await?;
    //     return Ok(filter_id)
    // }

    pub async fn install_filter(self: &Arc<Self>, worker: WorkerID, expiry: ExpiryGroup, filter_id: FilterID) -> Result<()> {
        info!("Installing filter {filter_id} into worker {worker}");
        self.client.put(self.config.workers.get(&worker).unwrap().http("/index/create")?)
        .json(&CreateIndexRequest {
            filter_id,
            expiry,
        })
        .send().await?;
        return Ok(())
    }

    pub async fn send_to_ingest_watcher(self: &Arc<Self>, worker: &WorkerID, task: IngestTask, filter_id: FilterID) -> Result<()> {
        let workers = self.worker_ingest.read().await;
        let channel = workers.get(worker).ok_or_else(|| anyhow::anyhow!("Worker list out of sync"))?;
        channel.send(WorkerIngestMessage::IngestMessage((task, filter_id)))?;
        return Ok(())
    }

    pub async fn get_ingest_pending(self: &Arc<Self>) -> HashMap<FilterID, Vec<Sha256>> {
        let mut requests = vec![];
        {
            let workers = self.worker_ingest.read().await;
            for (_worker, channel) in workers.iter() {
                let (send, recv) = oneshot::channel();
                _ = channel.send(WorkerIngestMessage::ListPending(send));
                requests.push(recv);
            }
        }
        let mut output = HashMap::<FilterID, Vec<Sha256>>::new();
        for resp in requests {
            if let Ok(resp) = resp.await {
                output.extend(resp);
            }
        }
        return output
    }

    pub async fn search_status(&self, code: String) -> Result<Option<InternalSearchStatus>> {
        let channel = {
            let searches = self.running_searches.read().await;
            if let Some((_, search)) = searches.get(&code) {
                let (send, recv) = oneshot::channel();
                _ = search.send(SearcherMessage::Status(send)).await;
                Some(recv)
            } else {
                None
            }
        };

        match channel {
            Some(recv) => match recv.await {
                Ok(status) => return Ok(Some(status)),
                Err(err) => { error!("{err}"); },
            },
            None => {}
        }
        self.database.search_status(&code).await
    }

    pub async fn status(self: &Arc<Self>) -> Result<StatusReport> {
        let (ingest_send, ingest_recv) = oneshot::channel();
        self.ingest_queue.send(IngestMessage::Status(ingest_send))?;

        let mut watchers = HashMap::<WorkerID, oneshot::Receiver<HashMap<FilterID, IngestWatchStatus>>>::new();
        let workers = self.worker_ingest.read().await;
        for (id, channel) in workers.iter() {
            let (send, recv) = oneshot::channel();
            channel.send(WorkerIngestMessage::Status(send))?;
            watchers.insert(id.clone(), recv);
        }

        let mut ingest_watchers: HashMap<WorkerID, HashMap<FilterID, IngestWatchStatus>> = Default::default();
        for (id, sock) in watchers.into_iter() {
            if let Ok(value) = sock.await {
                ingest_watchers.insert(id, value);
            }
        }

        let active_searches = {
            self.running_searches.read().await.len() as u32
        };

        Ok(StatusReport{
            ingest_check: ingest_recv.await?,
            active_searches,
            ingest_watchers
        })
    }

}

#[derive(Debug)]
pub struct IngestTask {
    pub info: FileInfo,
    pub response: Vec<oneshot::Sender<Result<()>>>
}

impl IngestTask {
    pub fn merge(&mut self, task: IngestTask) {
        self.info.expiry = self.info.expiry.clone().max(task.info.expiry);
        self.info.access = self.info.access.or(&task.info.access).simplify();
        self.response.extend(task.response.into_iter());
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestCheckStatus {
    delayed: usize,
    queue: usize,
    active: usize,
    last_minute: usize
}

#[derive(Debug)]
pub enum IngestMessage {
    IngestMessage(IngestTask),
    Status(oneshot::Sender<IngestCheckStatus>)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestWatchStatus {
    queue: usize,
    per_minute: f64
}

#[derive(Debug)]
pub enum WorkerIngestMessage {
    IngestMessage((IngestTask, FilterID)),
    Status(oneshot::Sender<HashMap<FilterID, IngestWatchStatus>>),
    ListPending(oneshot::Sender<HashMap<FilterID, Vec<Sha256>>>),
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
    let mut buffer_hashes: HashSet<Sha256> = Default::default();
    let mut unchecked_buffer = VecDeque::<IngestTask>::new();
    let mut delay_buffer = tokio_util::time::DelayQueue::new();
    // This must only ever have a single task launched in order to be sure that it 
    // can create new filters with unique IDs.
    let mut check_worker: JoinSet<Result<Vec<IngestTask>>> = JoinSet::new(); 
    let mut active_batch_size = 0;
    let mut counter = crate::counters::RateCounter::new(60);

    loop {
        // Restart the check worker
        if check_worker.is_empty() && !unchecked_buffer.is_empty() {
            let today = ExpiryGroup::today();
            let core = core.clone();
            let mut tasks: HashMap<_, _> = Default::default();
            while let Some(task) = unchecked_buffer.pop_front() {
                // Drop stale tasks
                if task.info.expiry <= today {
                    for resp in task.response {
                        _ = resp.send(Ok(()));
                    }
                    continue
                }

                buffer_hashes.remove(&task.info.hash);
                tasks.insert(task.info.hash.clone(), task);
                if tasks.len() >= 200 {
                    break;
                }
            }
            active_batch_size = tasks.len();
            check_worker.spawn(_ingest_check(core, tasks));
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
                        if buffer_hashes.contains(&task.info.hash) {
                            for other in &mut unchecked_buffer {
                                if other.info.hash == task.info.hash {
                                    other.merge(task);
                                    break
                                }
                            }
                        } else {
                            buffer_hashes.insert(task.info.hash.clone());
                            unchecked_buffer.push_back(task);
                        }
                    },
                    IngestMessage::Status(resp) => {
                        _ = resp.send(IngestCheckStatus {
                            delayed: delay_buffer.len(),
                            queue: unchecked_buffer.len(),
                            active: active_batch_size,
                            last_minute: counter.average()
                        });
                    },
                }
            }

            // If a check worker is running watch for it
            res = check_worker.join_next(), if !check_worker.is_empty() => {
                let res = match res {
                    Some(res) => res,
                    None => continue
                };

                let before = unchecked_buffer.len();

                match res {
                    Ok(Ok(delayed)) => {
                        for task in delayed {
                            delay_buffer.insert(task, tokio::time::Duration::from_secs(60));
                        }
                    },
                    Ok(Err(err)) => {
                        error!("ingest checker error: {err}");
                    }
                    Err(err) => {
                        error!("ingest checker error: {err}");
                    },
                }

                counter.increment(active_batch_size - (unchecked_buffer.len() - before));
                active_batch_size = 0;
            }

            //
            task = delay_buffer.next(), if !delay_buffer.is_empty() => {
                let task = match task {
                    Some(x) => x.into_inner(),
                    None => continue,
                };

                if buffer_hashes.contains(&task.info.hash) {
                    for other in &mut unchecked_buffer {
                        if other.info.hash == task.info.hash {
                            other.merge(task);
                            break
                        }
                    }
                } else {
                    buffer_hashes.insert(task.info.hash.clone());
                    unchecked_buffer.push_back(task);
                }
            }
        }
    }
}

async fn _ingest_check(core: Arc<HouseCore>, mut tasks: HashMap<Sha256, IngestTask>) -> Result<Vec<IngestTask>> {
    debug!("Ingest Check batch {}", tasks.len());
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
            let resp = core.client.post(worker_address.http("/files/update")?)
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
    let mut filter_assignment = HashMap::<(WorkerID, ExpiryGroup), u32>::new();
    let mut last_filter: FilterID = FilterID::NULL;
    let mut suggestions: HashMap<Sha256, Vec<(WorkerID, FilterID)>> = HashMap::new();
    while let Some(res) = requests.join_next().await {
        let (worker, res) = res??;
        debug!("Ingest Check result from {worker}, {} processed, {} pending", res.processed.len(), res.pending.len());

        // If an ingestion is totally processed by the worker, we can respond to the caller
        for sha in res.processed {
            if let Some(task) = tasks.remove(&sha) {
                for response in task.response {
                    _ = response.send(Ok(()));
                }
            }
        }

        // If the ingestion is in progress we can forward this task to the relivant watcher
        for (sha, filter) in res.pending {
            if let Some(task) = tasks.remove(&sha) {
                core.send_to_ingest_watcher(&worker, task, filter).await?;
            }
        }

        // Collate the information across workers
        filter_pending.extend(res.filter_pending.into_iter());
        if !res.storage_pressure {
            workers_without_pressure.insert(worker.clone());
        }
        for (filter, expiry) in res.filters {
            last_filter = last_filter.max(filter);
            match filter_assignment.entry((worker.clone(), expiry)) {
                hash_map::Entry::Occupied(mut entry) => { *entry.get_mut() += 1; },
                hash_map::Entry::Vacant(entry) => { entry.insert(1); },
            }
        }
        for (sha, filters) in res.assignments {
            match suggestions.entry(sha) {
                hash_map::Entry::Occupied(mut entry) => { entry.get_mut().extend(filters.into_iter().map(|fid|(worker.clone(), fid))); },
                hash_map::Entry::Vacant(entry) => { entry.insert(filters.into_iter().map(|fid|(worker.clone(), fid)).collect_vec()); },
            }
        }
    }

    for (filter, files) in core.get_ingest_pending().await {
        match filter_pending.entry(filter) {
            hash_map::Entry::Occupied(mut entry) => { entry.get_mut().extend(files);},
            hash_map::Entry::Vacant(entry) => {entry.insert(files.into_iter().collect());},
        }
    }

    debug!("Ingest Check simple assignments, {} task remaining, {} nodes without pressure.", tasks.len(), workers_without_pressure.len());
    // debug!("Pending {filter_pending:?}");
    // Accept assignments from workers that are uncontroversial
    for sha in tasks.keys().cloned().collect_vec() {
        // let task = tasks.get(&sha).unwrap();
        let mut best = None;
        let mut best_pending = u64::MAX;

        for (worker, suggested) in suggestions.get(&sha).unwrap_or(&vec![]) {
            let pending = filter_pending.get(suggested).unwrap_or(&[].into()).len() as u64;
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
        debug!("Ingest Check all tasks handled by simple assignments.");
        return Ok(vec![])
    }

    // Pull down the list of all filters
    // let mut filter_assignment: HashMap<ExpiryGroup, Vec<(WorkerID, FilterID)>> = Default::default();
    // for (worker, filter, expiry) in core.database.list_filters().await? {
    //     match filter_assignment.entry(expiry) {
    //         hash_map::Entry::Occupied(mut entry) => { entry.get_mut().push((worker, filter)); },
    //         hash_map::Entry::Vacant(entry) => { entry.insert(vec![(worker, filter)]); },
    //     };
    // }

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
            if let Some(count) = filter_assignment.get(&(worker.clone(), task.info.expiry.clone())) {
                filter_count.insert(worker.clone(), *count);
            }
        }

        // try to find a worker without a related filter to create one on
        // Then start doubling up filters until the limit
        for limit in 0..core.config.per_worker_group_duplication {
            for (worker, count) in &filter_count {
                if *count <= limit {
                    last_filter = last_filter.next();
                    core.install_filter(worker.clone(), task.info.expiry.clone(), last_filter).await?;
                    new_filters.insert(task.info.expiry.clone(), (worker.clone(), last_filter));
                    core.send_to_ingest_watcher(worker, task, last_filter).await?;
                    continue 'tasks
                }
            }
        }

        // Reject the ingestion command
        delayed.push(task);
    }

    debug!("Ingest Check delaying {} tasks.", delayed.len());
    Ok(delayed)
}

//
async fn ingest_watcher(core: Arc<HouseCore>, mut input: mpsc::UnboundedReceiver<WorkerIngestMessage>, worker: WorkerID, address: WorkerAddress) {
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

async fn _ingest_watcher(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<WorkerIngestMessage>, _worker: &WorkerID, address: &WorkerAddress) -> Result<()> {
    let mut active: HashMap<Sha256, (FilterID, IngestTask)> = Default::default();
    let mut query: JoinSet<Result<reqwest::Response>> = JoinSet::new();
    let mut query_interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
    query_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut counters = HashMap::<FilterID, crate::counters::RateCounter>::new();
    let start = std::time::Instant::now();

    {
        let request = core.client.get(address.http("/files/ingest-queues")?);
        let result = request.send().await?;
        let response: HashMap<FilterID, Vec<FileInfo>> = result.json().await?;
        for (filter, files) in response {
            for file in files {
                active.insert(file.hash.clone(), (filter, IngestTask{info: file, response:vec![]}));
            }
        }
    }

    loop {
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
                    WorkerIngestMessage::Status(resp) => {
                        let mut count = HashMap::<FilterID, usize>::new();
                        for (id, _) in active.values() {
                            count.insert(*id, 1 + count.get(id).unwrap_or(&0));
                        }
                        let mut status = HashMap::<FilterID, IngestWatchStatus>::new();
                        for (id, num) in count {
                            status.insert(id, IngestWatchStatus {
                                queue: num,
                                per_minute: match counters.get_mut(&id){
                                    Some(count) => count.average() as f64/((start.elapsed().as_secs_f64()/60.0).clamp(1.0, 60.0)),
                                    None => 0.0,
                                }
                            });
                        }
                        _ = resp.send(status);
                    },
                    WorkerIngestMessage::ListPending(resp) => {
                        let mut count = HashMap::<FilterID, Vec<Sha256>>::new();
                        for (id, task) in active.values() {
                            match count.entry(*id) {
                                hash_map::Entry::Occupied(mut entry) => { entry.get_mut().push(task.info.hash.clone()); },
                                hash_map::Entry::Vacant(entry) => { entry.insert(vec![task.info.hash.clone()]); },
                            }
                        }
                        _ = resp.send(count);
                    },
                }
            },

            response = query.join_next(), if !query.is_empty() => {
                let response = match response {
                    Some(response) => response,
                    None => continue,
                };

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
                    if let Some((filter, task)) = active.remove(&hash) {
                        for response in task.response {
                            _ = response.send(Ok(()));
                        }
                        match counters.entry(filter) {
                            hash_map::Entry::Occupied(mut entry) => entry.get_mut().increment(1),
                            hash_map::Entry::Vacant(entry) => {
                                let mut counter = crate::counters::RateCounter::new(60 * 60);
                                counter.increment(1);
                                entry.insert(counter);
                            },
                        }
                    }
                }

                // refresh any missing filters
                for hash in active.keys().cloned().collect_vec() {
                    if let hash_map::Entry::Occupied(entry) = active.entry(hash) {
                        if response.unknown_filters.contains(&entry.get().0) {
                            let (_, task) = entry.remove();
                            core.ingest_queue.send(IngestMessage::IngestMessage(task))?;
                        }
                    }
                }
            }

            _ = query_interval.tick() => {
                // Pull out expired tasks
                let today = ExpiryGroup::today();
                for hash in active.keys().cloned().collect_vec() {
                    if let hash_map::Entry::Occupied(entry) = active.entry(hash) {
                        if entry.get().1.info.expiry <= today {
                            let (_, task) = entry.remove();
                            for resp in task.response {
                                _ = resp.send(Ok(()));
                            }    
                        }
                    }
                }

                //
                if query.is_empty() && !active.is_empty() {
                    let request = core.client.post(address.http("/files/ingest")?)
                    .json(&IngestFilesRequest{
                        files: active.values().map(|(filter, task)|{
                            (*filter, task.info.clone())
                        }).collect_vec()
                    });
                    query.spawn(async move {
                        let result = request.send().await?;
                        anyhow::Ok(result)
                    });
                }
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

// fn websocket_address(address: &str) -> String {
//     address.replace("https://", "wss://").replace("http://", "ws://")
// }

async fn _search_worker(core: Arc<HouseCore>, input: &mut mpsc::Receiver<SearcherMessage>, code: &str) -> Result<()> {
    // Load the search status
    let status = match core.database.search_record(code).await? {
        Some(status) => status,
        None => return Ok(()),
    };
    if status.finished {
        return Ok(())
    }

    // Open a connection for each worker
    let request_body = serde_json::to_string(&FilterSearchRequest{
        expiry_group_range: (status.start_date.clone(), status.end_date.clone()),
        query: status.query.clone(),
        access: status.access.clone(),
    })?;

    let (client_sender, mut result_stream) = mpsc::channel(128);
    for (_worker, address) in &core.config.workers {
        let address = address.websocket("/search/filter")?;
        let (mut socket, _) = tokio_tungstenite::connect_async(address).await?;
        let client_sender = client_sender.clone();
        let request_body = request_body.clone();
        tokio::spawn(async move {
            if let Err(err) = socket.send(tokio_tungstenite::tungstenite::Message::Text(request_body)).await {
                _ = client_sender.send(FilterSearchResponse::Error(format!("connection error: {err}")));
            }

            while let Some(message) = socket.next().await {
                let message: FilterSearchResponse = match message {
                    Ok(message) => if let Message::Text(text) = message {
                        match serde_json::from_str(&text) {
                            Ok(message) => message,
                            Err(err) => FilterSearchResponse::Error(format!("decode error: {err}")),
                        }
                    } else {
                        continue
                    },
                    Err(err) => FilterSearchResponse::Error(format!("message error: {err}")),
                };
                _ = client_sender.send(message);
            }
        });
    }

    // Absorb messages from the workers until we have them all
    let mut errors = vec![];
    let candidates = SqliteSet::<Sha256>::new_temp().await?;
    loop {
        tokio::select!{
            message = input.recv() => {
                // Read an update command
                let message = match message {
                    Some(message) => message,
                    None => break
                };

                match message {
                    SearcherMessage::Status(response) => {
                        _ = response.send(InternalSearchStatus {
                            view: status.view.clone(),
                            resp: SearchRequestResponse {
                                code: code.to_owned(),
                                finished: false,
                                errors: errors.clone(),
                                hits: vec![],
                                truncated: false
                            }
                        });
                    },
                }
            },
            message = result_stream.recv() => {
                let message = match message {
                    Some(message) => message,
                    None => break,
                };

                match message {
                    FilterSearchResponse::Candidates(hashes) => { candidates.insert_batch(&hashes).await?; },
                    FilterSearchResponse::Error(error) => { errors.push(error); },
                }
            }

        }
    }

    // Run through yara jobs
    let mut hits: BTreeSet<Sha256> = Default::default();
    let mut requests: JoinSet<Result<YaraSearchResponse>> = JoinSet::new();
    let mut next_batch = None;
    let truncated = loop {

        if next_batch.is_none() {
            let batch = candidates.pop_batch(core.config.yara_batch_size).await?;
            if !batch.is_empty() {
                next_batch = Some(batch)
            }
        }

        if next_batch.is_none() && requests.is_empty() {
            break false;
        }

        if hits.len() >= core.config.search_hit_limit {
            break true;
        }

        tokio::select!{
            message = input.recv() => {
                // Read an update command
                let message = match message {
                    Some(message) => message,
                    None => return Ok(())
                };

                match message {
                    SearcherMessage::Status(response) => {
                        _ = response.send(InternalSearchStatus {
                            view: status.view.clone(),
                            resp: SearchRequestResponse {
                                code: code.to_owned(),
                                finished: false,
                                errors: errors.clone(),
                                hits: hits.iter().cloned().map(|x|x.hex()).collect(),
                                truncated: false
                            }
                        });
                    },
                }
            },
            result = requests.join_next(), if !requests.is_empty() => {
                let result = match result {
                    Some(Ok(Ok(message))) => message,
                    Some(Ok(Err(err))) => { errors.push(err.to_string()); continue }
                    Some(Err(err)) => { errors.push(err.to_string()); continue }
                    None => continue,
                };

                hits.extend(result.hits);
                errors.extend(result.errors);
            }
            permit = core.yara_permits.get(), if next_batch.is_some() => {
                let id = thread_rng().gen();
                let yara_rule = status.yara_signature.clone();
                let core = core.clone();
                let hashes = next_batch.take().unwrap();
                requests.spawn(async move {
                    let permit = permit?;
                    let response = core.client.get(permit.1.http("/search/yara")?)
                    .json(&YaraTask{
                        id,
                        yara_rule,
                        hashes
                    }).send().await?;
                    let result: YaraSearchResponse = response.json().await?;
                    return anyhow::Ok(result);
                });
            }
        }
    };

    // Save results
    core.database.finalize_search(&status.code, hits, errors, truncated).await?;
    let mut searches = core.running_searches.write().await;
    searches.remove(code);
    return Ok(())
}