use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use log::error;
use tokio::time::Duration;
use anyhow::Result;
use assemblyline_client::{Client, JsonMap};

use crate::config::AssemblylineConfig;
use super::HouseCore;

/// Pull files from an assemblyline system
pub (crate) async fn fetch_agent(core: Arc<HouseCore>, client: Arc<Client>, config: AssemblylineConfig) {
    loop {
        match tokio::spawn(_fetch_agent(core.clone(), client.clone(), config.clone())).await {
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
    pub seen: DateTime<Utc>,
    pub sha256: String,
    pub classification: String,
    pub expiry: Option<DateTime<Utc>>,
}


impl FetchedFile {
    fn extract(data: &JsonMap) -> Option<Self> {
        Some(Self{
            sha256: data.get("sha256")?.as_str()?.to_owned(),
            classification: data.get("classification")?.as_str()?.to_owned(),
            expiry: extract_date(data, "expiry_ts"),
            seen: extract_date(data.get("seen")?.as_object()?, "last")?,
        })
    }
}

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

const RETRY_LIMIT: usize = 10;

/// implementation loop to fetch files from assemblyline
async fn _fetch_agent(core: Arc<HouseCore>, client: Arc<Client>, config: AssemblylineConfig) -> Result<()> {
    //
    let mut running = tokio::task::JoinSet::<(FetchedFile, Result<()>)>::new();
    let mut pending: BTreeMap<FetchedFile, (bool, usize)> = Default::default();
    let mut poll_interval = tokio::time::interval(Duration::from_secs_f64(config.poll_interval));

    // Get checkpoint for this source.
    // The checkpoint represents the earliest value for the seen.last field where
    // ALL unprocessed file entries must occur after this point. Many PROCESSED
    // entries may also occur after this point.
    let mut checkpoint: DateTime<Utc> = core.get_checkpoint(&config).await?;

    loop {
        // Update the checkpoint if needed
        let old_checkpoint = checkpoint;
        while let Some(first) = pending.first_entry() {
            if first.get().0 {
                checkpoint = first.remove_entry().0.seen;
            } else {
                break
            }
        }
        if old_checkpoint != checkpoint {
            core.set_checkpoint(&config, checkpoint).await?;
        }

        // If there is free space get extra jobs
        if running.len() < config.concurrent_tasks {
            // Rather than searching at the current checkpoint we can search from the last seen at the tail
            // of items being processed. If nothing is being processed then the checkpoint is that value by default.
            let seek_point = match pending.last_key_value() {
                Some((file, _)) => file.seen,
                None => checkpoint,
            };

            let result = client.search
                .file(format!("seen.last: [{} TO *]", seek_point.to_rfc3339()))
                .sort("seen.last asc".to_owned())
                .rows(config.batch_size)
                .use_archive(true)
                .field_list("classification,expiry_ts,sha256,seen.last".to_owned())
                .search().await?;
            for item in result.items {
                let file = match FetchedFile::extract(&item) {
                    Some(file) => file,
                    None => continue,
                };

                // insert into the set of jobs
                match pending.entry(file.clone()) {
                    std::collections::btree_map::Entry::Occupied(_) => {}
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert((false, 0));
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

                if let std::collections::btree_map::Entry::Occupied(mut entry) = pending.entry(file.clone()) {
                    entry.get_mut().1 += 1;
                    if entry.get().1 <= RETRY_LIMIT {
                        if let Err(err) = result {
                            error!("Error in fetch: {err}");
                            let core = core.clone();
                            running.spawn(async move {
                                let resp = core.start_ingest(&file).await;
                                return (file, resp)
                            });
                            continue;
                        }
                    }
                    entry.get_mut().0 = true;
                }

            }
            _timeout = poll_interval.tick() => {
                continue
            }
        }
    }
}
