use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::Authenticator;
use crate::database::Database;
use crate::filter_input_batch::FilterInputBatch;
use crate::storage::BlobStorage;
use crate::cache::LocalCache;
use crate::access::AccessControl;
use log::error;
use tokio::sync::{mpsc, oneshot};
use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;

type IngestMessage = (String, AccessControl, Option<DateTime<Utc>>, oneshot::Sender<Result<()>>);

pub struct Config {
    pub batch_limit_size: usize,
    pub batch_limit_seconds: i64,
}

pub struct HouseCore {
    pub database: Database,
    pub file_storage: BlobStorage,
    pub index_storage: BlobStorage,
    pub local_cache: LocalCache,
    pub authenticator: Authenticator,
    pub config: Config,
    pub ingest_queue: mpsc::UnboundedSender<IngestMessage>,
}

impl HouseCore {
    pub fn new(index_storage: BlobStorage, file_storage: BlobStorage, database: Database, local_cache: LocalCache, authenticator: Authenticator) -> Result<Arc<Self>> {
        let (send_ingest, receive_ingest) = mpsc::unbounded_channel();

        let core = Arc::new(Self {
            database,
            file_storage,
            index_storage,
            local_cache,
            authenticator,
            ingest_queue: send_ingest,
            config: Config {
                batch_limit_seconds: chrono::Duration::hours(1).num_seconds(),
                batch_limit_size: 100,
            }
        });

        tokio::spawn(ingest_worker(core.clone(), receive_ingest));

        return Ok(core)
    }

    pub async fn update_file_access(&self, hash: &String, access: &AccessControl, index_group: &String) -> Result<bool> {
        todo!()
    }
}

pub fn generate_index_group(expiry: &Option<DateTime<Utc>>) -> String {
    match expiry {
        Some(date) => format!("{}", date.format("%Y-%U")),
        None => format!("9999-53"),
    }
}

async fn ingest_worker(core: Arc<HouseCore>, mut input: mpsc::UnboundedReceiver<IngestMessage>) {
    loop {
        match _ingest_worker(core.clone(), &mut input).await {
            Err(err) => error!("Crash in ingestion system: {err}"),
            Ok(()) => break
        }
    }
}

async fn _ingest_worker(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<IngestMessage>) -> Result<()> {

    let mut pending_timers: HashMap<String, DateTime<Utc>> = Default::default();
    let mut pending_batch: HashMap<String, HashMap<String, (AccessControl, Vec<oneshot::Sender<Result<()>>>)>> = Default::default();

    let mut current_batch: Option<JoinHandle<Result<()>>> = None;
    let mut batch_check_timer = tokio::time::interval(std::time::Duration::from_secs(60));

    loop {
        // Check if its time to launch an index building job
        if let Some(job) = &mut current_batch {
            if job.is_finished() {
                if let Err(err) = job.await? {
                    error!("Error building index batch: {err}");
                }
                current_batch = None;
            }
        }

        //
        if current_batch.is_none() {
            for (key, values) in pending_batch.iter() {
                let at_size_limit = values.len() >= core.config.batch_limit_size;
                let batch_start = match pending_timers.get(key) {
                    Some(start) => start.clone(),
                    None => Utc::now(),
                };
                if at_size_limit || (Utc::now() - batch_start).num_seconds() > core.config.batch_limit_seconds {
                    let key = key.clone();
                    if let Some(values) = pending_batch.remove(&key) {
                        current_batch = Some(tokio::spawn(run_batch_ingest(core.clone(), key, values)));
                    }
                    break;
                }
            }
        }

        // Wait for more ingest messages
        tokio::select!{
            message = input.recv() => {
                // Read an update command
                let message = match message {
                    Some(message) => message,
                    None => break
                };
                let (hash, access, expiry, respond) = message;

                // Try to update in place without changing indices
                let index_group = generate_index_group(&expiry);
                if core.update_file_access(&hash, &access, &index_group).await? {
                    _ = respond.send(Ok(()));
                    continue
                }

                // Merge into the next
                match pending_batch.entry(index_group.clone()) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        match entry.get_mut().entry(hash) {
                            std::collections::hash_map::Entry::Occupied(mut entry) => {
                                entry.get_mut().0 = entry.get_mut().0.or(&access).simplify();
                                entry.get_mut().1.push(respond);
                            },
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert((access, vec![respond]));
                            },
                        }
                    },
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let mut batch: HashMap<String, _> = Default::default();
                        batch.insert(hash, (access, vec![respond]));
                        entry.insert(batch);
                        pending_timers.insert(index_group, Utc::now());
                    },
                }
            }
            // At the interval break this wait and go check the batches again
            _ = batch_check_timer.tick() => { continue; }
        }
    }
    return Ok(())
}


async fn run_batch_ingest(core: Arc<HouseCore>, index_group: String, new_items: HashMap<String, (AccessControl, Vec<oneshot::Sender<Result<()>>>)>) -> Result<()> {
    // Check if any of our new items are already handled
    todo!();

    // Buildup the file data on disk
    todo!();

    // Pick an index to merge into
    todo!();

    // Merge into a new index file
    todo!();

    // Add the new values into the database corresponding to this index
    todo!();
}
