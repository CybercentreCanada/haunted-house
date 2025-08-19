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

use crate::broker::elastic::Datastore;
use crate::broker::PendingStatus;
use crate::counters::WindowCounter;

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

fn _search_stream(
    client: Datastore, 
    mut seek_point: DateTime<Utc>, 
    poll_interval: Duration, 
    batch_size: usize, 
    search_counter: Arc<parking_lot::Mutex<WindowCounter>>,
    last_fetch_rows: Arc<std::sync::atomic::AtomicI64>,
) -> mpsc::Receiver<FetchedFile> {
    let (channel, output) = mpsc::channel(batch_size * 10);
    tokio::spawn(async move {
        loop {
            // get a batch from elasticsearch
            search_counter.lock().increment(1);
            let result = match client.fetch_files(seek_point, batch_size).await {
                Ok(result) => result,
                Err(err) => {
                    error!("Error polling elasticsearch: {err}");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue
                },
            };
            let result_size = result.len();
            last_fetch_rows.store(result_size as i64, std::sync::atomic::Ordering::Relaxed);

            // send the data to the angent
            for file in result {
                seek_point = seek_point.max(file.seen);
                if channel.send(file).await.is_err() {
                    break
                }
            }

            // wait if we aren't getting full batches
            if result_size < batch_size {
                tokio::time::sleep(poll_interval).await;
            }
        }
    });
    output
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
    let maximum_recent = config.concurrent_tasks;
    let poll_interval_time = Duration::from_secs_f64(config.poll_interval);
    let mut poll_interval = tokio::time::interval(poll_interval_time);

    // metrics collectors
    let search_counter = Arc::new(parking_lot::Mutex::new(WindowCounter::new(60)));
    let mut throughput_counter = WindowCounter::new(60);
    let mut retry_counter = WindowCounter::new(60);
    // let mut last_fetch_time = std::time::Instant::now();
    let last_fetch_rows = Arc::new(std::sync::atomic::AtomicI64::new(0));

    // The checkpoint represents the earliest value for the seen.last field where
    // ALL unprocessed file entries must occur after this point. Many PROCESSED
    // entries may also occur after this point.
    let mut checkpoint: DateTime<Utc> = core.get_checkpoint().await?;
    let mut last_checkpoint_print = checkpoint;
    let mut seek_point = checkpoint;
    info!("Initial file fetcher checkpoint: {checkpoint}");

    let mut file_stream = _search_stream(client.clone(), checkpoint, poll_interval_time, config.batch_size, search_counter.clone(), last_fetch_rows.clone());

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
            if (checkpoint - last_checkpoint_print).num_days() > 0 {
                last_checkpoint_print = checkpoint;
                info!("File fetcher checkpoint reached: {checkpoint}");
            }
        }
        while recent.len() > maximum_recent {
            recent.pop_first();
        }

        // If there is free space get extra jobs
        while running.len() < config.concurrent_tasks {
            
            let file = match file_stream.try_recv() {
                Ok(file) => file,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => anyhow::bail!("File stream disconnect")
            };

            if recent.contains(&file) {
                continue
            }
            seek_point = seek_point.max(file.seen);

            // insert into the set of jobs
            match pending.entry(file.clone()) {
                std::collections::btree_map::Entry::Occupied(_) => {}
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(PendingInfo{finished: false, retries: 0});
                    let core = core.clone();
                    running.spawn(async move {
                        let resp = core.start_ingest(&file).await;
                        return (file, resp)
                    });
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

                debug!("File ingest attempt: {file:?} {result:?}");
                throughput_counter.increment(1);
                if let std::collections::btree_map::Entry::Occupied(mut entry) = pending.entry(file.clone()) {
                    entry.get_mut().retries += 1;
                    if entry.get().retries <= RETRY_LIMIT {

                        let err = match result {
                            Ok(true) => None,
                            Ok(false) => Some("File rejected".to_string()),
                            Err(err) => Some(format!("{err:?}")),
                        };

                        if let Some(err) = err {
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
                } else {
                    error!("Unexpected file completed");
                }
            }
            message = control.recv() => {
                match message {
                    Some(FetchControlMessage::Status(respond)) => {
                        let oldest_pending_file = pending.first_entry()
                            .map(|entry| PendingStatus {
                                file: entry.key().sha256.clone(),
                                finished: entry.get().finished,
                                retries: entry.get().retries,
                            });
                        let pending = client.count_files(&format!("seen.last: {{{} TO *]", checkpoint.to_rfc3339()), 1_000_000).await?;

                        _ = respond.send(FetchStatus {
                            last_minute_searches: search_counter.lock().value() as i64,
                            last_minute_throughput: throughput_counter.value() as i64,
                            last_minute_retries: retry_counter.value() as i64,
                            checkpoint_data: checkpoint,
                            read_cursor: seek_point,
                            pending_files: pending,
                            inflight: running.len() as u64,
                            last_fetch_rows: last_fetch_rows.load(std::sync::atomic::Ordering::Relaxed),
                            oldest_pending_file,
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


