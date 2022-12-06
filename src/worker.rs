use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};


use log::{info, error, warn};
use pyo3::exceptions::PyRuntimeError;
use pyo3::{pyclass, PyResult, pymethods, Py, PyAny, Python, PyObject};
use reqwest::StatusCode;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::sleep;


use crate::blob_cache::BlobCache;
use crate::filter::TrigramFilter;
use crate::interface::{WorkPackage, FilterTask, YaraTask, WorkRequest, WorkResult, WorkResultValue, WorkError};
use crate::storage::{BlobStorage, LocalDirectory, PythonBlobStore};
use crate::cache::LocalCache;


#[pyclass]
#[derive(Default)]
pub struct WorkerBuilder {
    index_storage: Option<BlobStorage>,
    file_storage: Option<BlobStorage>,
    api_token: Option<String>,
    server_address: Option<String>,
    cache_space: Option<(PathBuf, usize)>,
}

#[pymethods]
impl WorkerBuilder {
    #[new]
    fn new() -> Self {
        Default::default()
    }

    fn index_storage_path(&mut self, path: PathBuf) -> PyResult<()> {
        self.index_storage = Some(BlobStorage::Local(LocalDirectory::new(path)));
        Ok(())
    }

    fn index_storage_object(&mut self, object: Py<PyAny>) -> PyResult<()> {
        self.index_storage = Some(BlobStorage::Python(PythonBlobStore::new(object)));
        Ok(())
    }

    fn file_storage_path(&mut self, path: PathBuf) -> PyResult<()> {
        self.file_storage = Some(BlobStorage::Local(LocalDirectory::new(path)));
        Ok(())
    }

    fn file_storage_object(&mut self, object: Py<PyAny>) -> PyResult<()> {
        self.file_storage = Some(BlobStorage::Python(PythonBlobStore::new(object)));
        Ok(())
    }

    fn cache_directory(&mut self, path: PathBuf, capacity: usize) -> PyResult<()> {
        self.cache_space = Some((path, capacity));
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
        let index_storage = match self.index_storage.take() {
            Some(index) => index,
            None => LocalDirectory::new_temp().context("Error setting up local blob store")?
        };
        let file_storage = match self.file_storage.take() {
            Some(index) => index,
            None => LocalDirectory::new_temp().context("Error setting up local blob store")?
        };

        // Initialize authenticator
        let token = self.api_token.take().ok_or(anyhow::format_err!("An api token must be configured."))?;

        // Get cache
        let cache = self.cache_space.take().ok_or(anyhow::format_err!("A cache directory must be configured."))?;

        let address = match &self.server_address {
            Some(address) => address.clone(),
            None => "http://localhost:8080".to_owned()
        };

        Ok(pyo3_asyncio::tokio::future_into_py(py, async move {
            // Define cache directory
            let index_cache = BlobCache::new(index_storage, cache.1, cache.0);


            let (sender, recv) = mpsc::unbounded_channel();
            let data = Arc::new(WorkerData::new(sender.clone(), file_storage, index_cache, address, token)?);

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

}

#[pyclass]
struct WorkerHandle {
    connection: mpsc::UnboundedSender<WorkerMessage>,
}

struct WorkerData {
    // connection: mpsc::UnboundedSender<WorkerMessage>,
    hostname: String,
    file_storage: BlobStorage,
    index_cache: BlobCache,
    address: String,
    token: String,
    client: reqwest::Client,
}


enum TaskId {
    Yara(i64),
    Filter(i64)
}

impl WorkerData {

    fn new(connection: mpsc::UnboundedSender<WorkerMessage>, file_storage: BlobStorage, index_cache: BlobCache, address: String, token: String) -> Result<Self> {
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
            // connection,
            hostname: match gethostname::gethostname().into_string() {
                Ok(host) => host,
                Err(_) => return Err(anyhow::anyhow!("Hostname has bad characters?")),
            },
            file_storage,
            index_cache,
            address,
            token,
            client,
        })
    }

    async fn get_work_package(&self) -> Result<WorkPackage> {
        let work_request = WorkRequest {
            worker: self.hostname.clone(),
            cached_filters: Default::default(),
        };
        warn!("TODO track local index blob cache.");

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

    // async fn cleanup_tasks(&self, task_map: &mut IdMap, error: &String) -> Result<()> {
    //     let mut bad = vec![];
    //     for (id, handle) in task_map.iter() {
    //         if handle.is_finished() {
    //             bad.push(*id);
    //         }
    //     }
    //     for id in bad {
    //         task_map.remove(&id);
    //         self.report_error(id, error).await?;
    //     }
    //     Ok(())
    // }
}


async fn worker_manager(data: Arc<WorkerData>, mut messages: mpsc::UnboundedReceiver<WorkerMessage>) -> Result<()> {

    let mut connection_good = true;
    let mut still_running = true;
    let mut active_tasks: JoinSet<(TaskId, Result<()>)> = Default::default();
    // let mut active_task_id_map: IdMap = Default::default();

    while still_running || active_tasks.len() > 0 {

        if still_running && active_tasks.len() < 5 {
            // Get some work
            let work = match data.get_work_package().await {
                Ok(work) => work,
                Err(err) => {
                    error!("Couldn't get work package: {err}");
                    sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            // dispatch all the tasks in the package
            for filter_task in work.filter {
                active_tasks.spawn(do_filter_task(data.clone(), filter_task));
            }

            for yara_task in work.yara {
                active_tasks.spawn(do_yara_task(data.clone(), yara_task));
            }
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

            _ = sleep(Duration::from_secs(30)) => {}
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

    data.report_result(result).await?;
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
    todo!("yara task not implemented");
}