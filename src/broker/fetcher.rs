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
use tokio::sync::{mpsc, Mutex};
use tokio::time::Duration;
use anyhow::Result;
use assemblyline_client::{Client, JsonMap};

use crate::config::AssemblylineConfig;
use super::{HouseCore, FetchStatus, FetchControlMessage};

/// Pull files from an assemblyline system
pub (crate) async fn fetch_agent(core: Arc<HouseCore>, client: Arc<Client>, config: AssemblylineConfig, recv: mpsc::Receiver<FetchControlMessage>) {
    let recv = Arc::new(Mutex::new(recv));
    loop {
        match tokio::spawn(_fetch_agent(core.clone(), client.clone(), config.clone(), recv.clone())).await {
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
async fn _fetch_agent(core: Arc<HouseCore>, client: Arc<Client>, config: AssemblylineConfig, control: Arc<Mutex<mpsc::Receiver<FetchControlMessage>>>) -> Result<()> {
    let mut control = control.lock().await;
    info!("Fetch agent starting");
    //
    let mut running = tokio::task::JoinSet::<(FetchedFile, Result<bool>)>::new();
    let mut pending: BTreeMap<FetchedFile, PendingInfo> = Default::default();
    let mut recent: BTreeSet<FetchedFile> = Default::default();
    let mut poll_interval = tokio::time::interval(Duration::from_secs_f64(config.poll_interval));

    // metrics collectors
    let mut search_counter = crate::counters::WindowCounter::new(60);
    let mut throughput_counter = crate::counters::WindowCounter::new(60);
    let mut retry_counter = crate::counters::WindowCounter::new(60);
    let mut last_fetch_rows = 0;

    // Get checkpoint for this source.
    // The checkpoint represents the earliest value for the seen.last field where
    // ALL unprocessed file entries must occur after this point. Many PROCESSED
    // entries may also occur after this point.
    let mut checkpoint: DateTime<Utc> = core.get_checkpoint(&config).await?;

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
            core.set_checkpoint(&config, checkpoint).await?;
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
            let result = client.search
                .file(format!("seen.last: [{} TO *]", seek_point.to_rfc3339()))
                .sort("seen.last asc".to_owned())
                .rows(config.batch_size)
                .use_archive(true)
                .field_list("classification,expiry_ts,sha256,seen.last".to_owned())
                .search().await?;
            last_fetch_rows = result.rows;

            for item in result.items {
                let file = match FetchedFile::extract(&item) {
                    Some(file) => file,
                    None => continue,
                };
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
