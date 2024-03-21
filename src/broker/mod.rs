//!
//! This module implements the broker that acts as the entry point to the system.
//!
pub mod interface;
pub mod auth;
mod fetcher;
mod yara_query;
mod elastic;

use std::collections::{HashSet, HashMap, hash_map, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::{ClassificationString, ExpandingClassification};
use assemblyline_models::datastore::retrohunt as models;
use chrono::{DateTime, Utc};
use futures::{StreamExt, SinkExt};
use itertools::Itertools;
use log::{error, info, debug};
use native_tls::Certificate;
use rand::{thread_rng, Rng};
use reqwest_middleware::{ClientWithMiddleware, ClientBuilder};
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot, RwLock, watch};
use anyhow::{Result, Context};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::Message;

use crate::access::AccessControl;
use crate::broker::elastic::Datastore;
use crate::config::{BrokerSettings, WorkerAddress, WorkerTLSConfig};
use crate::query::Query;
use crate::sqlite_set::SqliteSet;
use crate::types::{Sha256, ExpiryGroup, FileInfo, FilterID, WorkerID};
use crate::worker::YaraTask;
use crate::worker::interface::{UpdateFileInfoRequest, UpdateFileInfoResponse, CreateIndexRequest, IngestFilesRequest, IngestFilesResponse, FilterSearchRequest, FilterSearchResponse, YaraSearchResponse};

use self::auth::Authenticator;
use self::interface::{SearchRequest, StatusReport, FilterStatus, SearchProgress};
use self::yara_query::parse_yara_signature;

/// Entry point function to the broker
pub (crate) async fn main(config: crate::config::BrokerSettings) -> Result<()> {
    // Initialize authenticator
    info!("Initializing Authenticator");
    let auth = Authenticator::from_config(config.authentication.clone())?;

    // Initialize database
    info!("Connecting to database.");
    let ec = &config.datastore;
    let client = if let Some(url) = ec.url.first() {
        Datastore::new(url, ec.ca_cert.as_deref(), ec.connect_unsafe, ec.archive_access)?
    } else {
        return Err(anyhow::anyhow!("Datastore URL must be configured."));
    };

    // Start server core
    info!("Starting server core.");
    let core = HouseCore::new(client,auth, config.clone()).await
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

/// Bundle of values tracking a search
/// The handle for the search task
/// the channel for communicating with that task
type SearchInfo = (JoinHandle<()>, watch::Receiver<SearchProgress>);

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchStatus {
    last_minute_searches: i64,
    last_minute_throughput: i64,
    last_minute_retries: i64,
    checkpoint_data: chrono::DateTime<chrono::Utc>,
    last_fetch_rows: i64,
}

/// A message to the fetch worker
#[derive(Debug)]
pub enum FetchControlMessage {
    /// A message askning the check worker for a status request
    Status(oneshot::Sender<FetchStatus>)
}

/// Information encapsulating the broker state
pub struct HouseCore {
    /// Connection to the database where searches are stored
    pub database: Datastore,
    /// HTTP client for talking to workers
    pub client: ClientWithMiddleware,
    /// Websocket connector info for connecting to workers
    pub ws_connector: tokio_tungstenite::Connector,
    /// Authentication information controlling which api tokens have what roles
    pub authenticator: Authenticator,
    /// configuration information tuning the system behaviour
    pub config: BrokerSettings,
    /// Classification engine if that configuration was available
    pub access_engine: Arc<ClassificationParser>,
    /// Queue of files waiting to be ingested
    pub ingest_queue: mpsc::UnboundedSender<IngestMessage>,
    /// Queue of files waiting to be ingested
    pub fetcher_control_queue: mpsc::Sender<FetchControlMessage>,
    /// Set of files that couldn't be quickly accepted by any worker
    pub pending_assignments: RwLock<HashMap<ExpiryGroup, VecDeque<IngestTask>>>,
    /// tasks pushing new files to the corresponding worker
    pub worker_ingest: RwLock<HashMap<WorkerID, mpsc::UnboundedSender<WorkerIngestMessage>>>,
    /// Set of running searches
    pub running_searches: RwLock<HashMap<String, SearchInfo>>,
    /// Pool of permits limiting the assignment of yara tasks to a fixed number per worker
    pub yara_permits: deadpool::unmanaged::Pool<(WorkerID, WorkerAddress)>,

    pub resource_tracker: crate::timing::ResourceTracker,
}

impl HouseCore {
    /// Start the broker server
    pub (crate) async fn new(database: Datastore, authenticator: Authenticator, config: BrokerSettings) -> Result<Arc<Self>> {
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

        // Prepare tls settings for Websockets and HTTP
        let connector = match &config.worker_certificate {
            WorkerTLSConfig::AllowAll => {
                native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()?
            },
            WorkerTLSConfig::Certificate(cert) => {
                native_tls::TlsConnector::builder()
                .add_root_certificate(Certificate::from_pem(cert.as_bytes())?)
                .danger_accept_invalid_hostnames(true)
                .build()?
            },
        };

        // Prepare our http client
        let client = reqwest::Client::builder().use_preconfigured_tls(connector.clone()).build()?;
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(std::time::Duration::from_millis(50), std::time::Duration::from_secs(30))
            .build_with_total_retry_duration(chrono::Duration::days(1).to_std()?);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        // Load classification system
        let access_engine = config.classification.init().context("loading classification configuration")?;

        // Build a classification engine
        let (fetch_send, fetch_recv) = mpsc::channel(64);

        // Create core object
        let core = Arc::new(Self {
            database,
            authenticator,
            ingest_queue: send_ingest,
            config,
            client,
            ws_connector: Connector::NativeTls(connector),
            worker_ingest: RwLock::new(Default::default()),
            pending_assignments: RwLock::new(Default::default()),
            running_searches: RwLock::new(Default::default()),
            yara_permits,
            access_engine,
            fetcher_control_queue: fetch_send,
            resource_tracker: crate::timing::ResourceTracker::start(),
        });

        // Revive search workers for ongoing searches
        {
            info!("Starting search workers");
            let mut searches = core.running_searches.write().await;
            for mut search in core.database.list_active_searches().await? {
                let (send, recv) = watch::channel(SearchProgress::Starting { key: search.key.clone() });
                let code = search.key.clone();
                search.started_time = chrono::Utc::now();
                core.database.retrohunt.save(&search.key, &search, None).await?;
                let handle = tokio::task::spawn(search_worker(core.clone(), send, search));
                searches.insert(code, (handle, recv));
            }
        }

        // Launch worker ingest watchers.
        {
            info!("Starting worker watchers");
            let mut worker_ingest = core.worker_ingest.write().await;
            for (worker, address) in core.config.workers.iter() {
                let (send, recv) = mpsc::unbounded_channel();
                tokio::task::spawn(ingest_watcher(core.clone(), recv, worker.clone(), address.clone()));
                worker_ingest.insert(worker.clone(), send);
            }
        }

        info!("Starting ingest worker");
        tokio::spawn(ingest_worker(core.clone(), receive_ingest));
        // tokio::spawn(worker_watcher(core.clone()));
        // tokio::spawn(garbage_collector(core.clone()));

        // Start the file fetcher
        info!("Starting file fetcher");
        tokio::spawn(fetcher::fetch_agent(core.clone(), fetch_recv));

        return Ok(core)
    }

    /// Initialize a search in the database and start the process
    pub (crate) async fn initialize_search(self: &Arc<Self>, req: SearchRequest) -> Result<String> {
        let (query, warnings) = parse_yara_signature(&req.yara_signature)?;
        self.prepare_access(req.search_classification.as_str())?;

        let (start_group, end_group) = match req.indices {
            models::IndexCatagory::Hot => (ExpiryGroup::min(), ExpiryGroup::before_archive()),
            models::IndexCatagory::Archive => (ExpiryGroup::before_archive(), ExpiryGroup::max()),
            models::IndexCatagory::HotAndArchive => (ExpiryGroup::min(), ExpiryGroup::max()),
        };

        // Create a record for the search
        let code = hex::encode(uuid::Uuid::new_v4().as_bytes());
        let search = models::Retrohunt {
            indices: req.indices,
            classification: ExpandingClassification::new(req.classification.as_str().to_owned())?,
            search_classification: req.search_classification,
            creator: req.creator,
            description: assemblyline_models::Text(req.description),
            expiry_ts: req.expiry_ts,
            start_group: start_group.as_u32(),
            end_group: end_group.as_u32(),
            created_time: chrono::Utc::now(),
            started_time: chrono::Utc::now(),
            completed_time: None,
            key: code.clone(),
            raw_query: serde_json::to_string(&query)?,
            yara_signature: req.yara_signature,
            errors: vec![],
            warnings,
            finished: false,
            truncated: false,
        };
        self.database.retrohunt.save(&search.key, &search, None).await?;

        // Start the search worker
        let mut searches = self.running_searches.write().await;
        let (send, recv) = watch::channel(SearchProgress::Starting { key: search.key.clone() });
        let handle = tokio::task::spawn(search_worker(self.clone(), send, search));
        searches.insert(code.clone(), (handle, recv));
        return Ok(code)
    }

    /// Parse a classification string as user access flags
    fn prepare_access(&self, access: &str) -> Result<HashSet<String>> {
        let ce = &self.access_engine;


        let mut terms = vec![];

        if ce.original_definition.enforce {
            let parts = ce.get_classification_parts(access, false, true, true)?;
            terms.push(ce.get_classification_level_text(parts.level, false)?);

            /// classification levels that aren't real
            const FALSE_LEVELS: [&str; 2] = ["NULL", "INV"];

            let levels: Vec<String> = ce.levels()
                .iter()
                .filter(|(lvl, data)| **lvl <= parts.level && !FALSE_LEVELS.contains(&data.short_name.as_str()))
                .map(|(_, data)|data.short_name.to_string())
                .collect();

            terms.extend(levels);
            terms.extend(parts.required);
            terms.extend(parts.groups);
            terms.extend(parts.subgroups);
        }

        return Ok(terms.into_iter().collect())
    }

    /// Parse a classification string as data classification
    #[allow(clippy::comparison_chain)]
    fn prepare_classification(&self, classification: &str) -> Result<AccessControl> {
        let ce = &self.access_engine;
        let mut top = vec![];

        if ce.original_definition.enforce {
            let parts = ce.get_classification_parts(classification, false, true, true)?;


            top.push(AccessControl::Token(ce.get_classification_level_text(parts.level, false)?));

            for item in parts.required {
                top.push(AccessControl::Token(item))
            }

            if parts.groups.len() > 1 {
                top.push(AccessControl::Or(parts.groups.into_iter().map(AccessControl::Token).collect()));
            } else if parts.groups.len() == 1 {
                top.push(AccessControl::Token(parts.groups[0].clone()));
            }

            if parts.subgroups.len() > 1 {
                top.push(AccessControl::Or(parts.subgroups.into_iter().map(AccessControl::Token).collect()));
            } else if parts.subgroups.len() == 1 {
                top.push(AccessControl::Token(parts.subgroups[0].clone()));
            }
        }

        Ok(if top.is_empty() {
            AccessControl::Always
        } else if top.len() == 1 {
            top.pop().unwrap()
        } else {
            AccessControl::And(top)
        })
    }

    /// Kick off the ingestion of a file and wait for its completion
    pub (crate) async fn start_ingest(&self, file: &fetcher::FetchedFile) -> Result<bool> {
        let (send, recv) = oneshot::channel();
        self.ingest_queue.send(IngestMessage::IngestMessage(IngestTask{
            info: FileInfo{
                hash: Sha256::from_str(&file.sha256)?,
                access: self.prepare_classification(&file.classification)?,
                access_string: file.classification.clone(),
                expiry: ExpiryGroup::create(&file.expiry)
            },
            response: vec![send]
        }))?;
        return recv.await?;
    }

    /// Get the date to resume reading file records from
    pub (crate) async fn get_checkpoint(&self) -> Result<DateTime<Utc>> {
        Ok(match tokio::fs::read_to_string(self.config.runtime_config_file()).await {
            Ok(value) => DateTime::parse_from_rfc3339(&value)?.into(),
            Err(err) => {
                if let std::io::ErrorKind::NotFound = err.kind() {
                    DateTime::<Utc>::MIN_UTC
                } else {
                    return Err(err.into())
                }
            },
        })
    }

    /// Set the date checkpoint for reading files
    pub (crate) async fn set_checkpoint(&self, checkpoint: DateTime<Utc>) -> Result<()> {
        tokio::fs::write(self.config.runtime_config_file(), checkpoint.to_rfc3339()).await?;
        Ok(())
    }

    /// Send a task directly to a watcher for a particular worker.
    ///
    /// This isn't the default way to assign a task to a worker.
    /// Worker watchers are responsible for selecting their own tasks, this is used
    /// when a file is seen repeatedly and a watcher needs to be notified about a second task
    /// pointing at the same file
    pub (crate) async fn send_to_ingest_watcher(self: &Arc<Self>, worker: &WorkerID, task: IngestTask, filter_id: FilterID) -> Result<()> {
        let workers = self.worker_ingest.read().await;
        let channel = workers.get(worker).ok_or_else(|| anyhow::anyhow!("Worker list out of sync"))?;
        channel.send(WorkerIngestMessage::IngestMessage((task, filter_id)))?;
        return Ok(())
    }

    /// Read the status of the system including all workers
    pub (crate) async fn status(self: &Arc<Self>) -> Result<StatusReport> {
        /// How long to wait for elements of the status report to load
        const STATUS_TIMEOUT: Duration = Duration::from_secs(5);

        // Send requests to workers for details we want from them
        let mut queries = JoinSet::new();
        let mut timeouts = vec![];
        for (worker, worker_address) in self.config.workers.clone() {
            timeouts.push(format!("worker-{worker}"));
            let request = self.client.get(worker_address.http("/status/detail")?)
            .header("Content-Type", "application/json")
            .send();
            queries.spawn(async move {
                (worker, request.await)
            });
        }

        // Request data from internal components
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
            match tokio::time::timeout(STATUS_TIMEOUT, sock).await {
                Ok(Ok(value)) => {
                    ingest_watchers.insert(id, value);
                }
                _ => {
                    timeouts.push(format!("watcher-{id}"));
                }
            }
        }

        // Read out data from the directly reachable data tables
        let active_searches = {
            self.running_searches.read().await.len() as u32
        };

        let mut pending_tasks = HashMap::<String, u32>::new();
        for (group, queue) in self.pending_assignments.read().await.iter() {
            if !queue.is_empty() {
                pending_tasks.insert(group.to_string(), queue.len() as u32);
            }
        }

        // gather responses from the workers
        let mut filters = vec![];
        let mut storage = HashMap::new();
        let mut resources = HashMap::new();
        resources.insert("broker".to_owned(), self.resource_tracker.read().await);

        loop {
            let response = match tokio::time::timeout(STATUS_TIMEOUT, queries.join_next()).await {
                Ok(Some(result)) => result,
                _ => break,
            };
            
            let (worker, response) = response?;
            let response = response?;
            let body: crate::worker::interface::DetailedStatus = match tokio::time::timeout(STATUS_TIMEOUT, response.json()).await {
                Ok(result) => result?,
                Err(_) => continue,
            };
            timeouts.retain(|x|x != &format!("worker-{worker}"));
            storage.insert(worker.clone(), body.storage);
            resources.insert(worker.to_string(), body.resources);
            for (expiry, filter, size) in body.filters {
                filters.push(FilterStatus{
                    expiry: expiry.to_string(),
                    worker: worker.clone(),
                    id: filter,
                    size
                });
            }
        }
        filters.sort_unstable();

        // get status from fetcher
        let fetcher = {
            let (send, recv) = oneshot::channel();
            _ = self.fetcher_control_queue.send(FetchControlMessage::Status(send)).await;
            recv
        };

        // combine info
        Ok(StatusReport{
            pending_tasks,
            ingest_check: ingest_recv.await?,
            active_searches,
            ingest_watchers,
            fetcher: fetcher.await?,
            filters,
            storage,
            timeouts,
            resources,
        })
    }

    /// Rerun a new search, possibly with changes to parameters, without creating a new search record
    pub async fn repeat_search(self: &Arc<Self>, key: &str, classification: ClassificationString, expiry: Option<DateTime<Utc>>) -> Result<RepeatOutcome> {
        loop {
            // fetch the old value
            let (mut search, version) = match self.database.retrohunt.get(key).await? {
                Some(result) => result,
                None => return Ok(RepeatOutcome::NotFound),
            };

            if !search.finished {
                return Ok(RepeatOutcome::AlreadyRunning)
            }

            // Update search access level
            let new_classification = ClassificationString::new(self.access_engine.max_classification(search.search_classification.as_str(), classification.as_str(), false)?)?;

            // Update expiry
            search.expiry_ts = match (search.expiry_ts, expiry) {
                (Some(a), Some(b)) => Some(a.max(b)),
                _ => None
            };

            // update search doc to run again
            search.completed_time = None;
            search.finished = false;
            search.errors.clear();
            search.warnings.clear();
            search.search_classification = new_classification;
            search.started_time = Utc::now();
            let key = search.key.clone();

            // save it with version    
            if self.database.retrohunt.save(&key, &search, version).await? {
                let mut searches = self.running_searches.write().await;
                let (send, recv) = watch::channel(SearchProgress::Starting { key: search.key.clone() });
                let handle = tokio::task::spawn(search_worker(self.clone(), send, search));
                searches.insert(key, (handle, recv));    
                return Ok(RepeatOutcome::Started)
            }
        }
    }

}

/// Outcome of trying to repeat a search
pub enum RepeatOutcome {
    /// When a search has been started again
    Started,
    /// When the search targeted can't be found
    NotFound,
    /// When the search in question is currently running
    AlreadyRunning
}

/// A data struct encapsulate the ingestion of a file
#[derive(Debug)]
pub struct IngestTask {
    /// Information about the file being ingested
    pub info: FileInfo,
    /// A collection of channels waiting for the completion (or error) of this ingestion
    pub response: Vec<oneshot::Sender<Result<bool>>>
}

impl IngestTask {
    /// Merge two tasks that refer to the same file
    pub fn merge(&mut self, task: IngestTask) {
        // merge the metadata about the file
        self.info.expiry = self.info.expiry.max(task.info.expiry);
        self.info.access = self.info.access.or(&task.info.access).simplify();
        // collect all the response channels into one list
        self.response.extend(task.response);
    }
}

/// A status snapshot for the check worker
#[derive(Debug, Serialize, Deserialize)]
pub struct IngestCheckStatus {
    /// How many tasks are in the input queue
    queue: usize,
    /// How many tasks are currently being checked
    active: usize,
    /// How many tasks have been processed in the last minute
    last_minute: usize
}

/// A message to the check worker
#[derive(Debug)]
pub enum IngestMessage {
    /// A message telling the check worker about a new ingest task
    IngestMessage(IngestTask),
    /// A message askning the check worker for a status request
    Status(oneshot::Sender<IngestCheckStatus>)
}

/// Entry point for worker that runs the first stage (check) of ingest
///
/// This worker checks with the workers if they have seen this file before.
/// For every file they will reply:
///  - They have fully processed the ingestion task by merging the metadata
///    into their existing entry for that file.
///  - They are working on this file already, the task should be assigned to them.
///  - The file should move on to normal ingestion.
/// The results are collated and each task in the batch is routed appropriately.
async fn ingest_worker(core: Arc<HouseCore>, mut input: mpsc::UnboundedReceiver<IngestMessage>) {
    loop {
        // restart the worker every time it crashes.
        match _ingest_worker(core.clone(), &mut input).await {
            Err(err) => error!("Crash in ingestion system: {err}"),
            Ok(()) => {
                info!("Ingest worker stopped.");
                break;
            }
        }
    }
}

/// The implementation for the above worker
async fn _ingest_worker(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<IngestMessage>) -> Result<()> {
    // A buffer of tasks to be processed and a set of hashes in that buffer
    let mut buffer_hashes: HashSet<Sha256> = Default::default();
    let mut unchecked_buffer = VecDeque::<IngestTask>::new();

    // The task running the current batch of tasks. This leaves this task
    // free to take in new tasks and respond to status queries.
    // We are using a join set to manage the task, but only launching a single batch at a time
    let mut check_worker: JoinSet<Result<()>> = JoinSet::new();
    // How many tasks are in the current batch
    let mut active_batch_size = 0;
    // A counter to track how many tasks we completed in the last minute
    let mut counter = crate::counters::WindowCounter::new(60);

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
                        _ = resp.send(Ok(false));
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
                            queue: unchecked_buffer.len(),
                            active: active_batch_size,
                            last_minute: counter.value()
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

                match res {
                    Ok(Ok(())) => {},
                    Ok(Err(err)) => {
                        error!("ingest checker error: {err}");
                    }
                    Err(err) => {
                        error!("ingest checker error: {err}");
                    },
                };

                counter.increment(active_batch_size);
                active_batch_size = 0;
            }
        }
    }
}

/// Check a batch of hashes to see if they are already (or quickly) accommodated by a worker
async fn _ingest_check(core: Arc<HouseCore>, mut tasks: HashMap<Sha256, IngestTask>) -> Result<()> {
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
    while let Some(res) = requests.join_next().await {
        let (worker, res) = res??;
        debug!("Ingest Check result from {worker}, {} processed, {} pending", res.processed.len(), res.pending.len());

        // If an ingestion is totally processed by the worker, we can respond to the caller
        for sha in res.processed {
            if let Some(task) = tasks.remove(&sha) {
                for response in task.response {
                    _ = response.send(Ok(true));
                }
            }
        }

        // If the ingestion is in progress we can forward this task to the relivant watcher
        for (sha, filter) in res.pending {
            if let Some(task) = tasks.remove(&sha) {
                core.send_to_ingest_watcher(&worker, task, filter).await?;
            }
        }
    }

    // Put the tasks not marked as processed or pending by any working into the queue
    let mut unassigned = core.pending_assignments.write().await;
    'next_task: for task in tasks.into_values() {
        match unassigned.entry(task.info.expiry) {
            hash_map::Entry::Occupied(mut entry) => {
                for existing in entry.get_mut().iter_mut() {
                    if existing.info.hash == task.info.hash {
                        existing.merge(task);
                        continue 'next_task;
                    }
                }
                entry.get_mut().push_back(task)
            },
            hash_map::Entry::Vacant(entry) => { entry.insert(VecDeque::from_iter([task])); },
        }
    }
    return Ok(())
}


/// status message for task monitoring a worker node
#[derive(Debug, Serialize, Deserialize)]
pub struct IngestWatchStatus {
    /// length of queue for files being inserted
    queue: usize,
    /// how many files were ingested per minute for the last hour
    per_minute: f64
}

/// A message to a worker monitor task
#[derive(Debug)]
pub enum WorkerIngestMessage {
    /// A message assigning the task directly to the given worker
    IngestMessage((IngestTask, FilterID)),
    /// Request a status update
    Status(oneshot::Sender<HashMap<FilterID, IngestWatchStatus>>),
    // ListPending(oneshot::Sender<HashMap<FilterID, Vec<Sha256>>>),
}


/// A task that feeds files to a given worker node
async fn ingest_watcher(core: Arc<HouseCore>, mut input: mpsc::UnboundedReceiver<WorkerIngestMessage>, worker: WorkerID, address: WorkerAddress) {
    loop {
        // Keep running until the task willingly exits
        match _ingest_watcher(core.clone(), &mut input, &worker, &address).await {
            Err(err) => error!("Crash in ingestion system: {err}"),
            Ok(()) => {
                info!("Ingest worker stopped.");
                break;
            }
        }
    }
}

/// Implementation for above task
async fn _ingest_watcher(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<WorkerIngestMessage>, id: &WorkerID, address: &WorkerAddress) -> Result<()> {
    info!("Starting ingest watcher for {id}");
    let mut active: HashMap<Sha256, (FilterID, IngestTask)> = Default::default();
    let mut query: JoinSet<Result<reqwest::Response>> = JoinSet::new();
    let mut query_interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
    query_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut counters = HashMap::<FilterID, crate::counters::WindowCounter>::new();
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

    info!("Ingest watcher for {id} initialized");
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
                                    Some(count) => count.value() as f64/((start.elapsed().as_secs_f64()/60.0).clamp(1.0, 60.0)),
                                    None => 0.0,
                                }
                            });
                        }
                        _ = resp.send(status);
                    },
                    // WorkerIngestMessage::ListPending(resp) => {
                    //     let mut count = HashMap::<FilterID, Vec<Sha256>>::new();
                    //     for (id, task) in active.values() {
                    //         match count.entry(*id) {
                    //             hash_map::Entry::Occupied(mut entry) => { entry.get_mut().push(task.info.hash.clone()); },
                    //             hash_map::Entry::Vacant(entry) => { entry.insert(vec![task.info.hash.clone()]); },
                    //         }
                    //     }
                    //     _ = resp.send(count);
                    // },
                }
            },

            response = query.join_next(), if !query.is_empty() => {
                debug!("response from {id}");
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
                debug!("response from {id}: process {} completed", response.completed.len());
                for (targets, response_value) in [(response.completed, true), (response.rejected, false)] {
                    for (targeted_filter, hash) in targets {
                        if let Some((filter, task)) = active.remove(&hash) {
                            // its possible for a file to be queued for multiple filters when the expiry date
                            // moves, so if the filter id doesn't match, we have sent it twice for different days
                            // and this isn't the one we are waiting for
                            if filter != targeted_filter {
                                continue
                            }
                            // Otherwise this is the filter we are waiting for send a response to all waiters
                            for response in task.response {
                                _ = response.send(Ok(response_value));
                            }
                            match counters.entry(filter) {
                                hash_map::Entry::Occupied(mut entry) => entry.get_mut().increment(1),
                                hash_map::Entry::Vacant(entry) => {
                                    let mut counter = crate::counters::WindowCounter::new(60 * 60);
                                    counter.increment(1);
                                    entry.insert(counter);
                                },
                            }
                        }
                    }
                }

                // for (targeted_filter, hash) in response.rejected {
                //     if let Some((filter, task)) = active.remove(&hash) {
                //         // its possible for a file to be queued for multiple filters when the expiry date
                //         // moves, so if the filter id doesn't match, we have sent it twice for different days
                //         // and this isn't the one we are waiting for
                //         if filter != targeted_filter {
                //             continue
                //         }
                //         // Otherwise this is the filter we are waiting for send a response to all waiters
                //         for response in task.response {
                //             _ = response.send(Ok(false));
                //         }
                //         match counters.entry(filter) {
                //             hash_map::Entry::Occupied(mut entry) => entry.get_mut().increment(1),
                //             hash_map::Entry::Vacant(entry) => {
                //                 let mut counter = crate::counters::WindowCounter::new(60 * 60);
                //                 counter.increment(1);
                //                 entry.insert(counter);
                //             },
                //         }
                //     }
                // }


                if response.storage_pressure {
                    continue
                }

                // Check if there is room in existing filters
                debug!("response from {id}: check existing");
                let mut backlocked_groups = vec![];
                {
                    let mut filter_pending = response.filter_pending;
                    let mut unassigned = core.pending_assignments.write().await;
                    for (group, queue) in unassigned.iter_mut() {
                        if queue.is_empty() {
                            continue
                        }
                        for filter in response.expiry_groups.get(group).unwrap_or(&vec![]) {
                            if response.filter_size.get(filter).unwrap_or(&u64::MAX) >= &core.config.filter_item_limit {
                                continue
                            }

                            if let Some(pending) = filter_pending.get_mut(filter) {
                                while pending.len() < core.config.per_filter_pending_limit as usize {
                                    match queue.pop_front() {
                                        Some(task) => {
                                            pending.insert(task.info.hash.clone());
                                            active.insert(task.info.hash.clone(), (*filter, task));
                                        },
                                        None => break
                                    }
                                }
                            }
                        }
                        if !queue.is_empty() {
                            backlocked_groups.push(*group);
                        }
                    }
                }

                let mut last_id = FilterID::NULL;
                for ids in response.expiry_groups.values() {
                    for id in ids {
                        last_id = last_id.max(*id);
                    }
                }

                // Check if any of the expiry groups we couldn't fit in existing filters can take more filters
                debug!("response from {id}: check backlogs");
                'groups: for group in backlocked_groups {
                    for limit in 0..core.config.per_worker_group_duplication {
                        if response.expiry_groups.get(&group).unwrap_or(&vec![]).len() <= limit as usize {
                            last_id = last_id.next();
                            debug!("response from {id}: create filter {last_id}");
                            core.client.put(address.http("/index/create")?)
                            .json(&CreateIndexRequest {
                                filter_id: last_id,
                                expiry: group,
                            })
                            .send().await?;
                            continue 'groups
                        }
                    }
                }

                debug!("response from {id}: finished result");
            },
            _ = query_interval.tick() => {
                // Pull out expired tasks
                let today = ExpiryGroup::today();
                for hash in active.keys().cloned().collect_vec() {
                    if let hash_map::Entry::Occupied(entry) = active.entry(hash) {
                        if entry.get().1.info.expiry <= today {
                            let (_, task) = entry.remove();
                            for resp in task.response {
                                _ = resp.send(Ok(false));
                            }
                        }
                    }
                }

                // Only one query at a time
                if !query.is_empty() {
                    continue
                }

                // Check if there is work
                if active.is_empty() && core.pending_assignments.read().await.is_empty() {
                    continue
                }

                //
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

const MILLISECOND: Duration = Duration::from_millis(1);
const MAX_DELAY: Duration = Duration::from_secs(5 * 60);

/// Entrypoint for the search worker
async fn search_worker(core: Arc<HouseCore>, mut progress: watch::Sender<SearchProgress>, mut status: models::Retrohunt) {
    let mut timeout = MILLISECOND;
    // Keep restarting the search until it completes
    while let Err(err) = _search_worker(core.clone(), &mut progress, &mut status).await {
        error!("Crash in search: {err:?}");
        tokio::time::sleep(timeout).await;
        timeout = MAX_DELAY.min(timeout * 2);
    }
    _ = progress.send(SearchProgress::Finished { key: status.key.clone(), search: status });
}

/// impl for above
async fn _search_worker(core: Arc<HouseCore>, progress_sender: &mut watch::Sender<SearchProgress>, status: &mut models::Retrohunt) -> Result<()> {
    let code = status.key.clone();
    info!("Search {code}: Starting");
    if status.finished {
        return Ok(())
    }

    // Make sure the search is initialized
    let query: Query = match serde_json::from_str(&status.raw_query) {
        Ok(query) => query,
        Err(err) => {
            core.database.fatal_error(status, format!("Could not load query: {err}")).await?;
            return Ok(())
        }
    };
    let start_group = ExpiryGroup::from(status.start_group);
    let end_group = ExpiryGroup::from(status.end_group);
    let search_view = match core.prepare_access(status.search_classification.as_str()) {
        Ok(view) => view,
        Err(err) => {
            core.database.fatal_error(status, err.to_string()).await?;
            return Ok(())
        },
    };

    // Limit how often we update the progress watch, useless to do too often
    let mut last_progress = std::time::Instant::now();
    const PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

    // Open a connection for each worker
    info!("Search {code}: Filtering");
    progress_sender.send(SearchProgress::Filtering { key: code.clone(), progress: 0.0 })?;
    let request_body = serde_json::to_string(&FilterSearchRequest{
        expiry_group_range: (start_group, end_group),
        query,
        access: search_view,
    })?;

    let mut workers = vec![];
    let mut result_stream = {
        let (client_sender, result_stream) = mpsc::channel(128);
        for (worker, address) in &core.config.workers {
            workers.push(worker.clone());
            let address = address.websocket("/search/filter")?;
            let (mut socket, _) = tokio_tungstenite::connect_async_tls_with_config(
                address.clone(), None, false, Some(core.ws_connector.clone())).await
                .context(format!("Unable to connect to worker: {address}"))?;
            let client_sender = client_sender.clone();
            let request_body = request_body.clone();
            let worker = worker.clone();
            tokio::spawn(async move {
                if let Err(err) = socket.send(tokio_tungstenite::tungstenite::Message::Text(request_body)).await {
                    _ = client_sender.send((worker.clone(), FilterSearchResponse::Error(None, format!("connection error: {err}")))).await;
                }

                while let Some(message) = socket.next().await {
                    let message: FilterSearchResponse = match message {
                        Ok(message) => if let Message::Text(text) = message {
                            match serde_json::from_str(&text) {
                                Ok(message) => message,
                                Err(err) => FilterSearchResponse::Error(None, format!("decode error: {err}")),
                            }
                        } else {
                            continue
                        },
                        Err(err) => FilterSearchResponse::Error(None, format!("message error: {err}")),
                    };
                    if let Err(err) = client_sender.send((worker.clone(), message)).await {
                        error!("search worker connection error: {err}");
                    }
                }
                info!("Finished listening to: {worker}")
            });
        }
        result_stream
    };

    // Absorb messages from the workers until we have them all
    let candidates = SqliteSet::<FileInfo>::new_temp().await?;
    {
        let mut complete: HashMap<WorkerID, HashSet<FilterID>> = Default::default();
        let mut initial: HashMap<WorkerID, Vec<FilterID>> = Default::default();
        loop {
            let (worker, message) = match result_stream.recv().await {
                Some(message) => message,
                None => break,
            };

            info!("Search progress: {message:?}");
            match message {
                FilterSearchResponse::Filters(items) => { initial.insert(worker, items); },
                FilterSearchResponse::Candidates(filter, files) => {
                    candidates.insert_batch(&files).await?;
                    match complete.entry(worker) {
                        hash_map::Entry::Occupied(mut entry) => { entry.get_mut().insert(filter); },
                        hash_map::Entry::Vacant(entry) => { entry.insert(HashSet::from([filter])); },
                    }
                },
                FilterSearchResponse::Error(filter, error) => {
                    status.errors.push(error);
                    if let Some(filter) = filter {
                        match complete.entry(worker) {
                            hash_map::Entry::Occupied(mut entry) => { entry.get_mut().insert(filter); },
                            hash_map::Entry::Vacant(entry) => { entry.insert(HashSet::from([filter])); },
                        }
                    }
                },
            }

            if last_progress.elapsed() >= PROGRESS_INTERVAL {
                let mut progress = 0.0;
                for worker in &workers {
                    if let Some(total) = initial.get(worker).map(Vec::len) {
                        if let Some(current) = complete.get(worker).map(HashSet::len) {
                            let worker_progress = current as f64/total as f64;
                            progress += worker_progress / workers.len() as f64;
                        }
                    }
                }
                progress_sender.send(SearchProgress::Filtering { key: code.clone(), progress })?;
                last_progress = std::time::Instant::now();
            }
        }
    }

    // Run through yara jobs
    info!("Search {code}: Yara");
    progress_sender.send(SearchProgress::Yara { key: code.clone(), progress: 0.0 })?;
    last_progress = std::time::Instant::now();
    let initial_total: u64 = candidates.len().await?;
    let mut sent_hashes = 0;

    use super::broker::elastic::Result as ESResult;
    let mut elastic_writes: JoinSet<ESResult<()>> = JoinSet::new();
    let mut requests: JoinSet<Result<YaraSearchResponse>> = JoinSet::new();
    let mut next_batch = None;
    loop {

        if next_batch.is_none() {
            let batch = candidates.pop_batch(core.config.yara_batch_size).await?;
            if !batch.is_empty() {
                next_batch = Some(batch)
            }
        }

        if next_batch.is_none() && requests.is_empty() {
            break;
        }

        let hits = core.database.count_retrohunt_hits(&code, core.config.search_hit_limit).await?;
        if hits >= core.config.search_hit_limit {
            status.truncated = true;
            break;
        }

        tokio::select!{
            result = requests.join_next(), if !requests.is_empty() => {
                let result = match result {
                    Some(Ok(Ok(message))) => message,
                    Some(Ok(Err(err))) => { status.errors.push(err.to_string()); continue }
                    Some(Err(err)) => { status.errors.push(err.to_string()); continue }
                    None => continue,
                };

                let db = core.database.clone();
                let code = code.clone();
                elastic_writes.spawn(async move { db.save_hits(&code, result.hits).await });
                status.errors.extend(result.errors);
            }
            permit = core.yara_permits.get(), if next_batch.is_some() => {
                let id = thread_rng().gen();
                let yara_rule = status.yara_signature.clone();
                let core = core.clone();
                let files = next_batch.take().unwrap();
                sent_hashes += files.len();
                requests.spawn(async move {
                    let permit = permit?;
                    let response = core.client.get(permit.1.http("/search/yara")?)
                    .json(&YaraTask{
                        id,
                        yara_rule,
                        files
                    }).send().await?;
                    let result: YaraSearchResponse = response.json().await?;
                    return anyhow::Ok(result);
                });
            }
        }

        if last_progress.elapsed() >= PROGRESS_INTERVAL {
            progress_sender.send(SearchProgress::Yara { key: code.clone(), progress: sent_hashes as f64 / initial_total as f64 })?;
            last_progress = std::time::Instant::now();
        }
    };

    // wait for database activity to finish
    while let Some(result) = elastic_writes.join_next().await { result??; }

    // Save results
    let hits = core.database.count_retrohunt_hits(&code, core.config.search_hit_limit).await?;
    info!("Search {code}: Finishing, {hits} hits");
    core.database.finalize_search(status).await?;
    let mut searches = core.running_searches.write().await;
    searches.remove(&code);
    return Ok(())
}