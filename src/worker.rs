use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};


use log::{info, error, debug};
use pyo3::exceptions::PyValueError;
use pyo3::{pyclass, PyResult, pymethods, Python, PyObject};
use reqwest::StatusCode;
use tokio::sync::{mpsc};
use tokio::task::{JoinSet, JoinHandle};
use tokio::time::sleep;


use crate::blob_cache::{BlobCache, BlobHandle};
use crate::database::BlobID;
use crate::filter::TrigramFilter;
use crate::interface::{WorkPackage, FilterTask, YaraTask, WorkRequest, WorkResult, WorkResultValue, WorkError};
use crate::storage::BlobStorageConfig;


#[pyclass]
#[derive(Default)]
pub struct WorkerBuilder {
    index_storage: Option<BlobStorageConfig>,
    file_storage: Option<BlobStorageConfig>,
    api_token: Option<String>,
    server_address: Option<String>,
    cache_space: Option<(PathBuf, usize, usize)>,
}

#[pymethods]
impl WorkerBuilder {
    #[new]
    fn new() -> Self {
        Default::default()
    }

    fn index_storage(&mut self, config: BlobStorageConfig) -> PyResult<()> {
        self.index_storage = Some(config);
        Ok(())
    }

    fn file_storage(&mut self, config: BlobStorageConfig) -> PyResult<()> {
        self.file_storage = Some(config);
        Ok(())
    }

    fn cache_directory(&mut self, path: PathBuf, index_capacity: usize, file_capacity: usize) -> PyResult<()> {
        self.cache_space = Some((path, index_capacity, file_capacity));
        Ok(())
    }

    fn api_token(&mut self, token: String) -> PyResult<()> {
        self.api_token = Some(token);
        Ok(())
    }

    fn server_address(&mut self, token: String) -> PyResult<()> {
        self.server_address = Some(token);
        Ok(())
    }

    fn start(&mut self, py: Python) -> PyResult<PyObject> {
        // Initialize blob stores
        let index_storage_config = match self.index_storage.take() {
            Some(index) => index,
            None => return Err(PyValueError::new_err("index storage must be configured"))
        };
        let file_storage_config = match self.file_storage.take() {
            Some(index) => index,
            None => return Err(PyValueError::new_err("file storage must be configured"))
        };

        // Initialize authenticator
        let token = self.api_token.take().ok_or(anyhow::format_err!("An api token must be configured."))?;

        // Get cache
        let (cache_dir, index_cache_size, file_cache_size) = self.cache_space.take().ok_or(anyhow::format_err!("A cache directory must be configured."))?;

        let address = match &self.server_address {
            Some(address) => address.clone(),
            None => "http://localhost:8080".to_owned()
        };

        Ok(pyo3_asyncio::tokio::future_into_py(py, async move {
            // Define cache directory
            let index_storage = crate::storage::connect(index_storage_config).await?;
            let file_storage = crate::storage::connect(file_storage_config).await?;
            let index_cache = BlobCache::new(index_storage, index_cache_size, cache_dir.clone());
            let file_cache = BlobCache::new(file_storage, file_cache_size, cache_dir);


            let (sender, recv) = mpsc::unbounded_channel();
            let weak = sender.downgrade();
            let data = Arc::new(WorkerData::new(weak, file_cache, index_cache, address, token)?);

            tokio::spawn(async {
                if let Err(err) = worker_manager(data, recv).await {
                    error!("{err}");
                }
            });

            // return internal interface to core
            Ok(WorkerHandle{
                connection: sender
            })
        })?.into())
    }
}

enum WorkerMessage {
    Stop,
    NoWork,
    Work(WorkPackage)
}

#[pyclass]
struct WorkerHandle {
    connection: mpsc::UnboundedSender<WorkerMessage>,
}

#[pymethods]
impl WorkerHandle {
    pub fn stop(&self) {
        _ = self.connection.send(WorkerMessage::Stop);
    }
}

struct WorkerData {
    connection: mpsc::WeakUnboundedSender<WorkerMessage>,
    hostname: String,
    file_cache: BlobCache,
    index_cache: BlobCache,
    address: String,
    client: reqwest::Client,
}


enum TaskId {
    Yara(i64),
    Filter(i64)
}

impl WorkerData {

    fn new(connection: mpsc::WeakUnboundedSender<WorkerMessage>, file_cache: BlobCache, index_cache: BlobCache, address: String, token: String) -> Result<Self> {
        // Build our default header list
        let mut headers = reqwest::header::HeaderMap::new();
        let mut auth_value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))?;
        auth_value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, auth_value);

        // Configure http client
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        Ok(Self {
            connection,
            hostname: match gethostname::gethostname().into_string() {
                Ok(host) => host,
                Err(_) => return Err(anyhow::anyhow!("Hostname has bad characters?")),
            },
            file_cache,
            index_cache,
            address,
            client,
        })
    }

    async fn get_work_package(&self) -> Result<WorkPackage> {
        let work_request = WorkRequest {
            worker: self.hostname.clone(),
            cached_filters: self.index_cache.current_open().await?.into_iter().map(BlobID::from).collect(),
        };

        // Try to get a work package
        let resp = self.client.get(format!("{}/work/", self.address))
            .json(&work_request)
            .send().await?;

        // Check if we got a response we can use
        let status_code = resp.status();
        if status_code != StatusCode::OK {
            let error_content = resp.text().await.context("Reading response error body")?;
            return Err(anyhow::anyhow!("Failed to reach control server {} {}", status_code, error_content));
        }

        // Decode work package
        let body = resp.bytes().await?;
        let work: WorkPackage = serde_json::from_slice(&body)?;
        return Ok(work)
    }

    async fn report_error(&self, id: TaskId, error: String) -> Result<()> {
        error!("{error}");
        let error = match id {
            TaskId::Filter(id) => WorkError::Filter(id, error),
            TaskId::Yara(id) => WorkError::Yara(id, error),
        };
        let resp = self.client.post(format!("{}/work/error/", self.address))
            .json(&error)
            .send().await?;

        if let Err(err) = resp.error_for_status() {
            error!("Error reporting error: {err}");
        }

        return Ok(())
    }

    async fn report_result(&self, result: WorkResult) -> Result<()> {
        let resp = self.client.post(format!("{}/work/finished/", self.address))
            .json(&result)
            .send().await?;
        resp.error_for_status().context("Error reporting result")?;
        Ok(())
    }
}


async fn worker_manager(data: Arc<WorkerData>, mut messages: mpsc::UnboundedReceiver<WorkerMessage>) -> Result<()> {

    let mut connection_good = true;
    let mut still_running = true;
    let mut active_tasks: JoinSet<(TaskId, Result<()>)> = Default::default();
    let mut fetch_work: Option<JoinHandle<()>> = None;

    while still_running || active_tasks.len() > 0 {

        if let Some(job) = &fetch_work {
            if job.is_finished() {
                fetch_work = None;
            }
        }

        if still_running && fetch_work.is_none() && active_tasks.len() < 5 {
            let data = data.clone();
            fetch_work = Some(tokio::spawn(async move {
                let conn = match data.connection.upgrade() {
                    Some(conn) => conn,
                    None => return,
                };

                // Get some work
                let work = match data.get_work_package().await {
                    Ok(work) => work,
                    Err(err) => {
                        error!("Couldn't get work package: {err}");
                        sleep(Duration::from_secs(5)).await;
                        _ = conn.send(WorkerMessage::NoWork);
                        return
                    }
                };

                if !work.filter.is_empty() || !work.yara.is_empty() {
                    info!("Got {} filter and {} yara tasks", work.filter.len(), work.yara.len());
                }

                _ = conn.send(WorkerMessage::Work(work));
            }))
        }

        tokio::select! {
            message = messages.recv(), if connection_good => {
                let message = match message {
                    Some(message) => message,
                    None => {
                        info!("Lost connection to host process starting clean stop.");
                        sleep(Duration::from_millis(100)).await;
                        still_running = false;
                        connection_good = false;
                        continue
                    },
                };

                match message {
                    WorkerMessage::Stop=>still_running=false,
                    WorkerMessage::NoWork => continue,
                    WorkerMessage::Work(work) => {
                        // dispatch all the tasks in the package
                        for filter_task in work.filter {
                            active_tasks.spawn(do_filter_task(data.clone(), filter_task));
                        }

                        for yara_task in work.yara {
                            active_tasks.spawn(do_yara_task(data.clone(), yara_task));
                        }
                    },
                }
            },

            finished_task = active_tasks.join_next(), if active_tasks.len() > 0 => {
                if let Some(task_result) = finished_task {
                    match task_result {
                        Ok((id, value)) => {
                            // active_task_id_map.remove(&id);
                            if let Err(err) = value {
                                data.report_error(id, format!("task error: {err}")).await?;
                            }
                        },
                        Err(err) => {
                            error!("task panic: {err}");
                        },
                    };
                }
            },
        }
    }

    error!("Hard shutdown not handled");
    return Ok(())
}


async fn do_filter_task(data: Arc<WorkerData>, filter_task: FilterTask) -> (TaskId, Result<()>) {
    let id = TaskId::Filter(filter_task.id);
    let result = match tokio::spawn(_do_filter_task(data, filter_task)).await {
        Ok(result) => result,
        Err(err) => return (id, Err(err.into())),
    };
    match result {
        Ok(()) => (id, Ok(())),
        Err(err) => (id, Err(err)),
    }
}

async fn _do_filter_task(data: Arc<WorkerData>, filter_task: FilterTask) -> Result<()> {
    // Download filter file
    let index_blob = data.index_cache.open(filter_task.filter_blob.to_string()).await?;
    let index_file = index_blob.open()?;

    // Run query
    let file_ids = tokio::task::spawn_blocking(move || -> Result<Vec<u64>> {
        let index = TrigramFilter::open(index_file)?;
        Ok(index.run_query(&filter_task.query)?.into_iter().collect())
    }).await??;

    // Report finding
    let result = WorkResult {
        id: filter_task.id,
        search: filter_task.search,
        value: WorkResultValue::Filter(filter_task.filter_id, filter_task.filter_blob, file_ids),
    };

    data.report_result(result).await.context("Failed to report results")?;
    return Ok(())
}

async fn do_yara_task(data: Arc<WorkerData>, yara_task: YaraTask) -> (TaskId, Result<()>) {
    let id = TaskId::Yara(yara_task.id);
    let result = match tokio::spawn(_do_yara_task(data, yara_task)).await {
        Ok(result) => result,
        Err(err) => return (id, Err(err.into())),
    };
    match result {
        Ok(()) => (id, Ok(())),
        Err(err) => (id, Err(err)),
    }
}



async fn _do_yara_task(data: Arc<WorkerData>, yara_task: YaraTask) -> Result<()> {
    debug!("yara task {} starting", yara_task.id);
    let filter_handle = {
        let (file_send, mut file_recv) = mpsc::unbounded_channel::<(Vec<u8>, BlobHandle)>();

        // Run the interaction with yara in a blocking thread
        let filter_handle = tokio::task::spawn_blocking(move || -> Result<Vec<Vec<u8>>> {
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
            let hash_string = hex::encode(&hash);
            debug!("yara task {} waiting for {}", yara_task.id, hash_string);
            match data.file_cache.open(hash_string.clone()).await {
                Ok(blob) => file_send.send((hash, blob))?,
                Err(err) => info!("File not available: {hash_string} {err}"),
            }
        }

        filter_handle
    };

    // Wait for worker to finish
    let filtered = filter_handle.await??;

    // Report the result to the broker
    debug!("yara task {} finished", yara_task.id);
    data.report_result(WorkResult {
        id: yara_task.id,
        search: yara_task.search,
        value: WorkResultValue::Yara(filtered),
    }).await
}