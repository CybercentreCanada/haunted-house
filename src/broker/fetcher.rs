//!
//! Given an Assemlyline client pull files from its file record and ingest them into this system.
//!
//! Track the oldest record processed so that that when restarted it has
//! a safe place to resume from.
//!

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use log::{debug, error, info};
use tokio::sync::{mpsc, Mutex};
use tokio::time::Duration;
use anyhow::Result;

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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
    let client = core.database.clone();
    
    //
    let mut running = tokio::task::JoinSet::<(FetchedFile, Result<bool>)>::new();
    let mut pending: BTreeMap<FetchedFile, PendingInfo> = Default::default();
    let mut recent: BTreeSet<FetchedFile> = Default::default();
    let poll_interval_time = Duration::from_secs_f64(config.poll_interval);
    let mut poll_interval = tokio::time::interval(poll_interval_time);

    // metrics collectors
    let mut search_counter = crate::counters::WindowCounter::new(60);
    let mut throughput_counter = crate::counters::WindowCounter::new(60);
    let mut retry_counter = crate::counters::WindowCounter::new(60);
    let mut last_fetch_time = std::time::Instant::now();
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
        if running.len() < config.concurrent_tasks && last_fetch_time.elapsed() >= poll_interval_time {
            last_fetch_time = std::time::Instant::now();

            // Rather than searching at the current checkpoint we can search from the last seen at the tail
            // of items being processed. If nothing is being processed then the checkpoint is that value by default.
            let seek_point = match pending.last_key_value() {
                Some((file, _)) => file.seen,
                None => checkpoint,
            };

            search_counter.increment(1);
            let result = client.fetch_files(seek_point, config.batch_size).await?;
            last_fetch_rows = result.len() as i64;
            let mut launched = 0;

            for file in result {
                if recent.contains(&file) {
                    continue
                }

                // insert into the set of jobs
                match pending.entry(file.clone()) {
                    std::collections::btree_map::Entry::Occupied(_) => {}
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(PendingInfo{finished: false, retries: 0});
                        let core = core.clone();
                        launched += 1;
                        running.spawn(async move {
                            let resp = core.start_ingest(&file).await;
                            return (file, resp)
                        });
                    }
                }
            }

            debug!("Fetched {last_fetch_rows} rows, launched {launched} ingestions");
        }

        // wait for running jobs to finish
        tokio::select! {
            complete = running.join_next(), if !running.is_empty() => {
                let (file, result) = match complete {
                    Some(Ok(value)) => value,
                    Some(Err(err)) => { error!("Error in fetcher: {err}"); continue },
                    None => continue,
                };

                debug!("File ingest attempt: {file:?} {result:?}");
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
                        let pending = client.count_files(&format!("seen.last: {{{} TO *]", checkpoint.to_rfc3339()), 1_000_000).await?;

                        _ = respond.send(FetchStatus {
                            last_minute_searches: search_counter.value() as i64,
                            last_minute_throughput: throughput_counter.value() as i64,
                            last_minute_retries: retry_counter.value() as i64,
                            checkpoint_data: checkpoint,
                            pending_files: pending,
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


