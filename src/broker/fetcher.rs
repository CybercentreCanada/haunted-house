//!
//! Given an Assemlyline client pull files from its file record and ingest them into this system.
//!
//! Track the oldest record processed so that that when restarted it has
//! a safe place to resume from.
//!

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use log::{error, info};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Duration;
use anyhow::Result;

use crate::error::ErrorKinds;
use crate::types::JsonMap;
use super::{HouseCore, FetchStatus, FetchControlMessage};

/// Pull files from an assemblyline system
pub (crate) async fn fetch_agent(core: Arc<HouseCore>, recv: mpsc::Receiver<FetchControlMessage>) {
    let recv = Arc::new(Mutex::new(recv));
    loop {
        match tokio::spawn(_fetch_agent(core.clone(), recv.clone())).await {
            Ok(Ok(())) => break,
            Ok(Err(err)) => error!("Error in the fetch agent: {err}"),
            Err(err) => error!("Error in the fetch agent: {err}"),
        }
        tokio::time::sleep(Duration::from_secs(60)).await
    }
}

/// Raw file details from assemblyline.
/// Not yet parsed into formats that we will use internally.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub (crate) struct FetchedFile {
    /// When was this file last seen by the assemblyline system
    pub seen: DateTime<Utc>,
    /// Hash of the file in question
    pub sha256: String,
    /// Classification of the file for this sighting
    pub classification: String,
    /// Time of file expiry for this sighting
    pub expiry: Option<DateTime<Utc>>,
}


impl FetchedFile {
    /// Build a fetched file object from the json data
    fn extract(data: &JsonMap) -> Option<Self> {
        Some(Self{
            sha256: data.get("sha256")?.as_str()?.to_owned(),
            classification: data.get("classification")?.as_str()?.to_owned(),
            expiry: extract_date(data, "expiry_ts"),
            seen: extract_date(data.get("seen")?.as_object()?, "last")?,
        })
    }
}

/// Extract a date value from a json field
fn extract_date(item: &JsonMap, key: &str) -> Option<DateTime<Utc>> {
    match item.get(key) {
        Some(item) => match DateTime::parse_from_rfc3339(item.as_str()?) {
            Ok(expiry) => Some(expiry.into()),
            Err(_) => {
                return None
            },
        },
        None => None,
    }
}

/// How many times should ingesting a file be retried
const RETRY_LIMIT: usize = 10;

struct PendingInfo {
    finished: bool,
    retries: usize,
}


/// implementation loop to fetch files from assemblyline
async fn _fetch_agent(core: Arc<HouseCore>, control: Arc<Mutex<mpsc::Receiver<FetchControlMessage>>>) -> Result<()> {
    let mut control = control.lock().await;
    info!("Fetch agent starting");

    // connect to elasticsearch
    let config = core.config.datastore.clone();
    let client = Elastic::new(&config.url, config.ca_cert.as_deref(), config.connect_unsafe)?;
    
    //
    let mut running = tokio::task::JoinSet::<(FetchedFile, Result<bool>)>::new();
    let mut pending: BTreeMap<FetchedFile, PendingInfo> = Default::default();
    let mut recent: BTreeSet<FetchedFile> = Default::default();
    let mut poll_interval = tokio::time::interval(Duration::from_secs_f64(config.poll_interval));

    // metrics collectors
    let mut search_counter = crate::counters::WindowCounter::new(60);
    let mut throughput_counter = crate::counters::WindowCounter::new(60);
    let mut retry_counter = crate::counters::WindowCounter::new(60);
    let mut last_fetch_rows: i64 = 0;

    // The checkpoint represents the earliest value for the seen.last field where
    // ALL unprocessed file entries must occur after this point. Many PROCESSED
    // entries may also occur after this point.
    let mut checkpoint: DateTime<Utc> = core.get_checkpoint().await?;

    loop {
        // Update the checkpoint if needed
        let old_checkpoint = checkpoint;
        while let Some(first) = pending.first_entry() {
            if first.get().finished {
                let file = first.remove_entry().0;
                checkpoint = file.seen;
                recent.insert(file);
            } else {
                break
            }
        }
        if old_checkpoint != checkpoint {
            core.set_checkpoint(checkpoint).await?;
        }
        while recent.len() > 100_000 {
            recent.pop_first();
        }

        // If there is free space get extra jobs
        if running.len() < config.concurrent_tasks {
            // Rather than searching at the current checkpoint we can search from the last seen at the tail
            // of items being processed. If nothing is being processed then the checkpoint is that value by default.
            let seek_point = match pending.last_key_value() {
                Some((file, _)) => file.seen,
                None => checkpoint,
            };

            search_counter.increment(1);
            let result = client.fetch_files(seek_point, config.batch_size).await?;
            last_fetch_rows = result.len() as i64;

            for file in result {
                if recent.contains(&file) {
                    continue
                }

                // insert into the set of jobs
                match pending.entry(file.clone()) {
                    std::collections::btree_map::Entry::Occupied(_) => {}
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        println!("{} {}", file.sha256, file.seen);
                        entry.insert(PendingInfo{finished: false, retries: 0});
                        let core = core.clone();
                        running.spawn(async move {
                            let resp = core.start_ingest(&file).await;
                            return (file, resp)
                        });
                    }
                }
            }
        }

        // wait for running jobs to finish
        tokio::select! {
            complete = running.join_next(), if !running.is_empty() => {
                let (file, result) = match complete {
                    Some(Ok(value)) => value,
                    Some(Err(err)) => { error!("Error in fetcher: {err}"); continue },
                    None => continue,
                };

                throughput_counter.increment(1);
                if let std::collections::btree_map::Entry::Occupied(mut entry) = pending.entry(file.clone()) {
                    entry.get_mut().retries += 1;
                    if entry.get().retries <= RETRY_LIMIT {
                        if let Err(err) = result {
                            retry_counter.increment(1);
                            error!("Error in fetch: {err}");
                            let core = core.clone();
                            running.spawn(async move {
                                let resp = core.start_ingest(&file).await;
                                return (file, resp)
                            });
                            continue;
                        }
                    }
                    entry.get_mut().finished = true;
                }

            }
            message = control.recv() => {
                match message {
                    Some(FetchControlMessage::Status(respond)) => {
                        _ = respond.send(FetchStatus {
                            last_minute_searches: search_counter.value() as i64,
                            last_minute_throughput: throughput_counter.value() as i64,
                            last_minute_retries: retry_counter.value() as i64,
                            checkpoint_data: checkpoint,
                            last_fetch_rows,
                        });
                    },
                    None => return Ok(())
                };
            }
            _timeout = poll_interval.tick() => {
                continue
            }
        }
    }
}


#[derive(Deserialize)]
struct SearchResult {
    // took: u64,
    timed_out: bool,
    hits: SearchResultHits
}

#[derive(Deserialize)]
struct SearchResultHits {
    // total: SearchResultHitTotals,
    // max_score: f64,
    hits: Vec<SearchResultHitItem>,
}

// #[derive(Deserialize)]
// struct SearchResultHitTotals {
//     value: i64,
//     relation: String,
// }

#[derive(Deserialize)]
struct SearchResultHitItem {
    _index: String,
    _id: String,
    _score: f64,
    fields: JsonMap,
}

struct Elastic {
    client: reqwest::Client,
    host: reqwest::Url,
}


impl Elastic {

    fn new(host: &str, ca_cert: Option<&str>, connect_unsafe: bool) -> Result<Self> {

        let mut builder = reqwest::Client::builder();

        if let Some(ca_cert) = ca_cert {
            let cert = reqwest::Certificate::from_pem(ca_cert.as_bytes())?;
            builder = builder.add_root_certificate(cert);
        }

        if connect_unsafe {
            builder = builder.danger_accept_invalid_certs(true);
        }

        Ok(Self {
            client: builder.build()?,
            host: host.parse()?,
        })
    }

    async fn fetch_files(&self, seek_point: chrono::DateTime<chrono::Utc>, batch_size: usize) -> Result<Vec<FetchedFile>> {
        let path = "file,file-hot/_search";
        let mut attempt: u64 = 0;
        const MAX_DELAY: std::time::Duration = Duration::from_secs(60);

        loop {
            attempt += 1;

            // Build and dispatch the request
            let result = self.client.request(reqwest::Method::GET, self.host.join(path)?)
            .json(&json!({
                "query": {
                    "bool": {
                        "must": {
                            "query_string": {
                                "query": format!("seen.last: [{} TO *]", seek_point.to_rfc3339())
                            }
                        },
                        // 'filter': filter_queries
                    }
                },
                "size": batch_size,
                "sort": "seen.last:asc",
                "fields": "classification,expiry_ts,sha256,seen.last"
            })).send().await;

            // Handle connection errors with a retry, let other non http errors bubble up
            let response = match result {
                Ok(response) => response,
                Err(err) => {
                    // always retry for connect and timeout errors
                    if err.is_connect() || err.is_timeout() {
                        error!("Error connecting to datastore: {err}");
                        let delay = MAX_DELAY.min(Duration::from_secs_f64((attempt as f64).powf(2.0)/5.0));
                        tokio::time::sleep(delay).await;
                        continue
                    }

                    return Err(err.into())
                },
            };

            // Handle server side http status errors with retry, let other error codes bubble up, decode successful bodies
            let status = response.status();
            let body: SearchResult = match response.error_for_status() {
                Ok(response) => response.json().await?,
                Err(err) => {
                    if status.is_server_error() {
                        error!("Server error in datastore: {err}");
                        let delay = MAX_DELAY.min(Duration::from_secs_f64((attempt as f64).powf(2.0)/5.0));
                        tokio::time::sleep(delay).await;
                        continue                        
                    }

                    return Err(err.into())
                },
            };

            // read the body of our response
            let mut out = vec![];
            if body.timed_out {
                continue
            }
            for row in body.hits.hits {
                out.push(FetchedFile::extract(&row.fields).ok_or(ErrorKinds::MalformedResponse)?)
            }
            return Ok(out)
        }
        
        
    }
}

