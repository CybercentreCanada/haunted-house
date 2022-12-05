use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};


use log::{info, error};
use pyo3::exceptions::PyRuntimeError;
use pyo3::{pyclass, PyResult, pymethods, Py, PyAny, Python, PyObject};
use reqwest::StatusCode;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::sleep;


use crate::interface::{WorkPackage, FilterTask, YaraTask, WorkRequest};
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
            let cache = LocalCache::new(cache.1, cache.0).await;

            // let sender = match WorkerImpl::start(index_storage, file_storage, cache, address, token).await {
            //     Ok(sender) => sender,
            //     Err(err) => return Err(PyRuntimeError::new_err(format!("{err}")))
            // };

            let (sender, recv) = mpsc::unbounded_channel();
            let data = Arc::new(WorkerData::new(sender.clone(), index_storage, file_storage, cache, address, token)?);

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
    index_storage: BlobStorage, 
    file_storage: BlobStorage, 
    cache: LocalCache, 
    address: String, 
    token: String,
    client: reqwest::Client,
}

impl WorkerData {
    
    fn new(connection: mpsc::UnboundedSender<WorkerMessage>, index_storage: BlobStorage, file_storage: BlobStorage, cache: LocalCache, address: String, token: String) -> Result<Self> {

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
            index_storage,
            file_storage,
            cache,
            address,
            token,
            client,
        })
    }

    async fn get_work_package(&self) -> Result<WorkPackage> {
        let work_request = WorkRequest {
            worker: self.hostname.clone(),
            cached_filters: Default::default(), // TODO
        };

        // Try to get a work package
        let resp = self.client.get(format!("{}/work/", self.address))
            .body(serde_json::to_string(&work_request)?)
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

}

async fn worker_manager(data: Arc<WorkerData>, mut messages: mpsc::UnboundedReceiver<WorkerMessage>) -> Result<()> {

    let mut connection_good = true;
    let mut still_running = true;
    let mut active_tasks: JoinSet<Result<i64>> = Default::default();
    let mut active_task_id_map: HashMap<i64, tokio::task::AbortHandle> = Default::default();

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
                let id = filter_task.id;
                let handle = active_tasks.spawn(do_filter_task(data.clone(), filter_task));
                active_task_id_map.insert(id, handle);
            }

            for yara_task in work.yara {
                let id = yara_task.id;
                let handle = active_tasks.spawn(do_yara_task(data.clone(), yara_task));
                active_task_id_map.insert(id, handle);
            }
        }

        tokio::select! {
            finished_task = active_tasks.join_next(), if active_tasks.len() > 0 => {
                todo!()
            },

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

            _ = sleep(Duration::from_secs(30)) => {}
        }
    }

    error!("Hard shutdown not handled");
    return Ok(())
}


async fn do_filter_task(data: Arc<WorkerData>, filter_task: FilterTask) -> Result<i64> {
    todo!();
}

async fn do_yara_task(data: Arc<WorkerData>, yara_task: YaraTask) -> Result<i64> {
    todo!();
}
