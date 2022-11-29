use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::Authenticator;
use crate::database::{Database, IndexGroup, BlobID};
use crate::storage::BlobStorage;
use crate::cache::LocalCache;
use crate::access::AccessControl;
use crate::ursadb::{self, UrsaDBTrigramFilter};
use bitvec::vec::BitVec;
use futures::future::select_all;
use log::error;
use tokio::sync::{mpsc, oneshot};
use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;

type IngestMessage = (Vec<u8>, AccessControl, Option<DateTime<Utc>>, oneshot::Sender<Result<()>>);

pub struct Config {
    pub batch_limit_size: usize,
    pub batch_limit_seconds: i64,
}

pub struct HouseCore {
    pub runtime: tokio::runtime::Runtime,
    pub database: Database,
    pub file_storage: BlobStorage,
    pub index_storage: BlobStorage,
    pub local_cache: LocalCache,
    pub authenticator: Authenticator,
    pub config: Config,
    pub ingest_queue: mpsc::UnboundedSender<IngestMessage>,
}

impl HouseCore {
    pub fn new(runtime: tokio::runtime::Runtime, index_storage: BlobStorage, file_storage: BlobStorage, database: Database, local_cache: LocalCache, authenticator: Authenticator) -> Result<Arc<Self>> {
        let (send_ingest, receive_ingest) = mpsc::unbounded_channel();

        let core = Arc::new(Self {
            runtime,
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

    let mut pending_timers: HashMap<IndexGroup, DateTime<Utc>> = Default::default();
    let mut pending_batch: HashMap<IndexGroup, HashMap<Vec<u8>, (AccessControl, Vec<oneshot::Sender<Result<()>>>)>> = Default::default();

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

                // Make sure the hash value is correct
                if hash.len() != 32 {
                    _ = respond.send(Err(anyhow::anyhow!("Expected hash to be binary encoded sha256")));
                    continue
                }

                // Try to update in place without changing indices
                let index_group = IndexGroup::create(&expiry);
                if core.database.update_file_access(&hash, &access, &index_group).await? {
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
                        let mut batch: HashMap<Vec<u8>, _> = Default::default();
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


async fn run_batch_ingest(core: Arc<HouseCore>, index_group: IndexGroup, mut new_items: HashMap<Vec<u8>, (AccessControl, Vec<oneshot::Sender<Result<()>>>)>) -> Result<()> {
    // Check if any of our new items are already handled
    {
        let mut to_remove = vec![];
        for (hash, (access, responses)) in new_items.iter_mut() {
            if core.database.update_file_access(hash, access, &index_group).await? {
                while let Some(res) = responses.pop() {
                    _ = res.send(Ok(()));
                }
                to_remove.push(hash.clone());
            }
        }
        for key in to_remove {
            new_items.remove(&key);
        }
        if new_items.is_empty() {
            return Ok(())
        }
    }

    // Buildup the file data
    let mut data = vec![];
    let mut meta = vec![];
    let mut remaining_responses = vec![];
    let mut outstanding = Vec::from_iter(new_items.keys().cloned());
    let mut active: Vec<JoinHandle<(Vec<u8>, Result<BitVec>)>> = Vec::new();
    while !new_items.is_empty() {
        while !outstanding.is_empty() && active.len() < 10 {
            if let Some(hash) = outstanding.pop(){
                active.push(tokio::spawn(prepare_vector(core.clone(), hash)));
            }
        }

        let (result, _, remain) = select_all(active.into_iter()).await;
        active = remain;

        let (hash, result) = result?;
        if let Some((access, responses)) = new_items.remove(&hash) {
            match result {
                Ok(bits) => {
                    data.push(bits);
                    meta.push((hash, access));
                    remaining_responses.extend(responses);
                },
                Err(err) => {
                    error!("Error gathering file for {}: {err}", base64::encode(&hash));
                    for res in responses {
                        _ = res.send(Err(anyhow::format_err!("Error gathering file {err}")));
                    }
                }
            }
        }
    }

    // Pick an index to merge into
    let selected_index = core.database.select_index_to_grow(&index_group).await?;

    // Merge into a new index file
    match selected_index {
        None => {
            let final_size = ursadb::UrsaDBTrigramFilter::guess_max_size(data.len());
            let new_index_file = core.local_cache.open(final_size).await?;
            let out_handle = new_index_file.open()?;
            let ok: Result<()> = tokio::task::spawn_blocking(move || {
                ursadb::UrsaDBTrigramFilter::build_from_data(out_handle, data)?;
                Ok(())
            }).await?;
            ok?;

            // Upload the new index file
            let size = new_index_file.size_to_fit().await?;
            let new_blob = BlobID::new();
            core.index_storage.upload(new_blob.as_str(), new_index_file.path()).await?;

            // Add the new values into the database corresponding to this index
            core.database.create_index_data(&index_group, new_blob, meta, size).await?;

        }
        Some((index_id, old_blob)) => {
            let data_size = core.index_storage.size(old_blob.as_str()).await?.ok_or(anyhow::anyhow!("Bad index id"))?;
            let index_file = core.local_cache.open(data_size).await?;
            core.index_storage.download(old_blob.as_str(), index_file.path()).await?;

            let final_size = ursadb::UrsaDBTrigramFilter::guess_max_size(data.len()) + data_size;
            let new_index_file = core.local_cache.open(final_size).await?;
            let out_handle = new_index_file.open()?;
            let ok: Result<usize> = tokio::task::spawn_blocking(move || {
                let (_, offset) = ursadb::UrsaDBTrigramFilter::merge_in_data(out_handle, index_file.open()?, data)?;
                Ok(offset)
            }).await?;
            let offset = ok?;

            // Upload the new index file
            let new_blob = BlobID::new();
            let size = new_index_file.size_to_fit().await?;
            core.index_storage.upload(new_blob.as_str(), new_index_file.path()).await?;

            // Add the new values into the database corresponding to this index
            core.database.update_index_data(&index_group, index_id, old_blob, new_blob, meta, offset, size).await?;
        }
    };

    for res in remaining_responses {
        _ = res.send(Ok(()));
    }
    Ok(())
}

async fn prepare_vector(core: Arc<HouseCore>, hash: Vec<u8>) -> (Vec<u8>, Result<BitVec>) {
    let label = base64::encode(&hash);
    let stream = match core.file_storage.stream(&label).await {
        Ok(stream) => stream,
        Err(err) => return (hash, Err(err)),
    };

    let result = tokio::task::spawn_blocking(|| {
        let result = UrsaDBTrigramFilter::build_file(stream);
        return result
    }).await;

    let result = match result {
        Ok(stream) => stream,
        Err(err) => return (hash, Err(err.into())),
    };

    return (hash, result)
}