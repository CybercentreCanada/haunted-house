use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};


use itertools::Itertools;
use log::{info, error, debug};
use rand::{thread_rng, Rng};
use reqwest::StatusCode;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep;


use crate::blob_cache::{BlobCache, BlobHandle};
use crate::config::WorkerTLSConfig;
use crate::filter::TrigramFilter;
use crate::interface::{WorkPackage, YaraTask, WorkRequest, WorkResult, WorkError};

use super::StatusReport;



pub enum WorkerMessage {
    Stop,
    NoWork,
    Work(WorkPackage),
    WorkDone,
    Status(oneshot::Sender<StatusReport>)
}

pub struct WorkerData {
    connection: mpsc::UnboundedSender<WorkerMessage>,
    hostname: String,
    file_cache: BlobCache,
    // index_cache: BlobCache,
    server_address: String,
    client: reqwest::Client,
}

#[derive(PartialEq, Eq, Hash, Copy, Clone)]
enum TaskId {
    Yara(i64),
    Filter(i64)
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskId::Yara(id) => f.write_fmt(format_args!("yara({id})")),
            TaskId::Filter(id) => f.write_fmt(format_args!("filter({id})")),
        }
    }
}

impl WorkerData {

    pub fn new(connection: mpsc::UnboundedSender<WorkerMessage>, file_cache: BlobCache, server_address: String, tls: WorkerTLSConfig, token: String, port: u16) -> Result<Self> {
        // Build our default header list
        let mut headers = reqwest::header::HeaderMap::new();
        let mut auth_value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))?;
        auth_value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, auth_value);

        // Configure http client
        let client = {
            let builder = reqwest::Client::builder();
            let builder = match tls {
                WorkerTLSConfig::AllowAll => builder.danger_accept_invalid_certs(true),
                WorkerTLSConfig::Certificate(_) => todo!(),
            };
            builder.default_headers(headers).build()?
        };

        // Get the name we can be reached back at
        let hostname = match gethostname::gethostname().into_string() {
            Ok(host) => host,
            Err(_) => return Err(anyhow::anyhow!("Hostname has bad characters?")),
        };
        let hostname = format!("{hostname}:{port}");

        Ok(Self {
            connection,
            hostname,
            file_cache,
            server_address,
            client,
        })
    }

    pub fn stop(&self) {
        _ = self.connection.send(WorkerMessage::Stop);
    }

    async fn get_work_package(&self) -> Result<WorkPackage> {
        let work_request = WorkRequest {
            worker: self.hostname.clone(),
            // cached_filters: self.index_cache.current_open().await?.into_iter().map(BlobID::from).collect(),
        };

        // Try to get a work package
        let resp = self.client.get(format!("{}/work/", self.server_address))
            .timeout(Duration::from_secs(120))
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

    async fn _post<B: serde::Serialize>(&self, url: String, body: B) {
        loop {
            let res = self.client.post(&url)
                .json(&body)
                .send().await;

            match res {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return
                    }
                    if status.is_client_error() {
                        error!("HTTP Error reaching server (will not retry): {status}");
                        return
                    }
                    error!("HTTP Error reaching server (will retry): {status}");
                },
                Err(err) => {
                    error!("Error reaching server (will retry): {err}");
                },
            };

            let delay: f64 = thread_rng().gen();
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(1.0 + 5.0 * delay)).await;
        }
    }

    async fn report_error(&self, id: TaskId, error: String) {
        error!("{error}");
        let error = match id {
            TaskId::Filter(id) => WorkError::Filter(id, error),
            TaskId::Yara(id) => WorkError::Yara(id, error),
        };

        self._post(format!("{}/work/error/", self.server_address), error).await;
    }

    async fn report_result(&self, result: WorkResult) {
        self._post(format!("{}/work/finished/", self.server_address), result).await;
        _ = self.connection.send(WorkerMessage::WorkDone);
    }
}


pub async fn worker_manager(data: Arc<WorkerData>, mut messages: mpsc::UnboundedReceiver<WorkerMessage>) -> Result<()> {

    let mut still_running = true;
    let mut active_tasks: HashMap<TaskId, JoinHandle<Result<()>>> = Default::default();
    let mut fetch_work: Option<JoinHandle<()>> = None;

    while still_running || active_tasks.len() > 0 {
        // Clear the handle if our previous fetch work job is finished
        if let Some(job) = &fetch_work {
            if job.is_finished() {
                fetch_work = None;
            }
        }

        // If its time to, get more work
        if still_running && fetch_work.is_none() && active_tasks.len() < 5 {
            let data = data.clone();
            fetch_work = Some(tokio::spawn(async move {
                // Get some work
                let work = match data.get_work_package().await {
                    Ok(work) => work,
                    Err(err) => {
                        error!("Couldn't get work package: {err}");
                        sleep(Duration::from_secs(5)).await;
                        _ = data.connection.send(WorkerMessage::NoWork);
                        return
                    }
                };

                if !work.yara.is_empty() {
                    info!("Got {} yara tasks", work.yara.len());
                }

                _ = data.connection.send(WorkerMessage::Work(work));
            }))
        }

        // Clear out any finished tasks
        {
            let ids: Vec<TaskId> = active_tasks.keys().cloned().collect_vec();
            for id in ids {
                if let Some(job) = active_tasks.get(&id) {
                    if !job.is_finished() {
                        continue;
                    }

                    let job = match active_tasks.remove(&id) {
                        Some(job) => job,
                        None => continue,
                    };
                    match job.await {
                        Ok(Ok(_)) => {},
                        Ok(Err(err)) => {
                            data.report_error(id, format!("worker error in {id}: {err}")).await;
                        },
                        Err(err) => {
                            data.report_error(id, format!("task error in {id}: {err}")).await;
                        },
                    };
                }
            }
        }

        let time_limit = tokio::time::Duration::from_secs(60);

        tokio::select! {
            message = tokio::time::timeout(time_limit, messages.recv()) => {
                let message = match message {
                    Ok(message) => message,
                    Err(_) => continue,
                };

                let message = match message {
                    Some(message) => message,
                    None => {
                        error!("Lost connection to host process starting clean stop.");
                        break;
                    },
                };

                match message {
                    WorkerMessage::Stop=> {
                        info!("Processing shutdown");
                        still_running=false
                    },
                    WorkerMessage::NoWork => continue,
                    WorkerMessage::WorkDone => continue,
                    WorkerMessage::Status(response) => {
                        let mut active_filter = vec![];
                        let mut active_yara = vec![];

                        for id in active_tasks.keys() {
                            match id {
                                TaskId::Yara(id) => active_yara.push(*id),
                                TaskId::Filter(id) => active_filter.push(*id),
                            }
                        }

                        _ = response.send(StatusReport {
                            active_filter,
                            active_yara
                        });
                    },
                    WorkerMessage::Work(work) => {
                        // dispatch all the tasks in the package
                        for yara_task in work.yara {
                            active_tasks.insert(TaskId::Yara(yara_task.id), tokio::spawn(do_yara_task(data.clone(), yara_task)));
                        }
                    },
                }
            }
        }
    }

    info!("Worker stopping.");
    return Ok(())
}


// async fn do_filter_task(data: Arc<WorkerData>, filter_task: FilterTask) -> Result<()> {
//     // Download filter file
//     let index_blob = data.index_cache.open(filter_task.filter_blob.to_string()).await.context("Error downloading filter.")?;
//     let index_file = index_blob.open().context("Error opening filter.")?;

//     // Run query
//     let file_ids = tokio::task::spawn_blocking(move || -> Result<Vec<u64>> {
//         let index = TrigramFilter::open(index_file)?;
//         Ok(index.run_query(&filter_task.query)?.into_iter().collect())
//     }).await??;

//     // Report finding
//     info!("filter task {} finished ({} hits)", filter_task.id, file_ids.len());
//     let result = WorkResult {
//         id: filter_task.id,
//         search: filter_task.search,
//         value: WorkResultValue::Filter(filter_task.filter_id, filter_task.filter_blob, file_ids),
//     };

//     data.report_result(result).await;
//     return Ok(())
// }

async fn do_yara_task(data: Arc<WorkerData>, yara_task: YaraTask) -> Result<()> {
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
    info!("yara task {} finished ({} hits)", yara_task.id, filtered.len());
    data.report_result(WorkResult {
        id: yara_task.id,
        search: yara_task.search,
        yara_hits: filtered
    }).await;
    return Ok(())
}