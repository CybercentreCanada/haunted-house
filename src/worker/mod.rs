use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};
use log::{info, error};
use crate::blob_cache::BlobCache;
use crate::types::FileInfo;
use crate::worker::database::Database;
use crate::worker::manager::WorkerState;

pub mod interface;
mod database;
mod database_sqlite;
mod trigrams;
mod encoding;
pub mod journal;
mod manager;
mod filter;
mod filter_worker;


#[derive(Serialize, Deserialize)]
pub struct YaraTask {
    pub id: i64,
    pub yara_rule: String,
    pub files: Vec<FileInfo>,
}

pub async fn main(config: crate::config::WorkerSettings) -> Result<()> {
    // Setup the storage
    info!("Connect to file storage");
    let file_storage = crate::storage::connect(&config.files).await?;

    // Get cache
    info!("Setup caches");
    let (file_cache, _file_temp) = match &config.file_cache {
        crate::config::CacheConfig::TempDir { size } => {
            let temp_dir = tempfile::tempdir()?;
            (BlobCache::new(file_storage.clone(), *size, temp_dir.path().to_owned())?, Some(temp_dir))
        }
        crate::config::CacheConfig::Directory { path, size } => {
            (BlobCache::new(file_storage.clone(), *size, PathBuf::from(path))?, None)
        }
    };

    // Figure out where the worker status interface will be hosted
    info!("Determine bind address");
    let bind_address = config.bind_address.clone().unwrap_or("localhost:8080".to_owned());
    let mut addresses = tokio::net::lookup_host(&bind_address).await?.collect_vec();
    let bind_address = match addresses.pop() {
        Some(x) => x,
        None => {
            return Err(anyhow::anyhow!("Couldn't resolve bind address: {}", bind_address));
        }
    };
    info!("Status interface will bind on: {bind_address}");

    info!("Loading classification");
    let ce = config.classification.init()?;

    info!("Setting up database.");
    let database = Database::new_sqlite(config.get_database_directory(), ce).await.context("setting up database")?;

    info!("Spawing processing daemons.");
    let (set_running, running) = tokio::sync::watch::channel(true);
    let data = WorkerState::new(database, file_storage, file_cache, config.clone(), running).await.context("spawning")?;

    // Watch for exit signal
    let exit_notice = Arc::new(tokio::sync::Notify::new());
    tokio::spawn({
        let exit_notice = exit_notice.clone();
        async move {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    _ = set_running.send(false);
                    exit_notice.notify_waiters();
                },
                Err(err) => {
                    error!("Error waiting for exit signal: {err}");
                },
            }
        }
    });

    // Run the worker
    info!("Starting HTTP interface.");
    let api = tokio::spawn(interface::serve(bind_address, config.tls, data.clone(), exit_notice.clone()));
    // let manager = tokio::spawn(worker_manager(data.clone(), recv));

    // Run the worker
    match api.await {
        Ok(Ok(())) => {},
        Ok(Err(err)) => error!("Server crashed: {err}"),
        Err(err) => error!("Server crashed: {err}")
    }

    info!("Waiting for data flush...");
    data.stop().await;
    // let result = tokio::select! {
    //     res = api => res,
    //     res = manager => res,
    // };

    // match result {
    //     Ok(Err(err)) => error!("Server crashed: {err} {}", err.root_cause()),
    //     Err(err) => error!("Server crashed: {err}"),
    //     _ => {}
    // };
    return Ok(())
}

/// Helper function to remove a file and succeed when missing
pub fn remove_file(path: &Path) -> Result<()> {
    if let Err(err) = std::fs::remove_file(path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            return Err(err.into());
        }
    };
    return Ok(())
}


fn into_trigrams(bytes: &[u8]) -> Vec<u32> {
    if bytes.len() < 3 {
        return vec![];
    }
    let mut trigrams = vec![];
    let mut trigram: u32 = (bytes[0] as u32) << 8 | (bytes[1] as u32);

    for byte in bytes.iter().skip(2) {
        trigram = (trigram & 0x00FFFF) << 8 | (*byte as u32);
        trigrams.push(trigram);
    }

    return trigrams;
}


/// Calculate the union of two sets of numbers into the the first set
///
/// list need not be ordered
fn union(base: &mut Vec<u64>, other: &Vec<u64>) {
    base.extend(other);
    base.sort_unstable();
    base.dedup();
}

/// Calculate the intersection of two sets of numbers into the the first set
///
/// list must be ordered
fn intersection(base: &mut Vec<u64>, other: &[u64]) {

    let mut base_read_index = 0;
    let mut base_write_index = 0;
    let mut other_index = 0;

    while base_read_index < base.len() && other_index < other.len() {
        match base[base_read_index].cmp(&other[other_index]) {
            std::cmp::Ordering::Less => { base_read_index += 1},
            std::cmp::Ordering::Equal => {
                base[base_write_index] = base[base_read_index];
                base_write_index += 1;
                base_read_index += 1;
                other_index += 1;
            },
            std::cmp::Ordering::Greater => {other_index += 1},
        }
    }

    base.truncate(base_write_index);
}


#[test]
fn test_union() {
    {
        let mut base = vec![];
        union(&mut base, &vec![]);
        assert!(base.is_empty());
    }
    {
        let mut base = vec![0x0];
        union(&mut base, &vec![]);
        assert_eq!(base, vec![0x0]);
    }
    {
        let mut base = vec![];
        union(&mut base, &vec![0x0]);
        assert_eq!(base, vec![0x0]);
    }
    {
        let mut base = vec![0x0, 0x1];
        union(&mut base, &vec![]);
        assert_eq!(base, vec![0x0, 0x1]);
    }
    {
        let mut base = vec![];
        union(&mut base, &vec![0x0, 0x1]);
        assert_eq!(base, vec![0x0, 0x1]);
    }
    {
        let mut base = vec![0xff];
        union(&mut base, &vec![0x0, 0xff]);
        assert_eq!(base, vec![0x0, 0xff]);
    }
    {
        let mut base = vec![0xff];
        union(&mut base, &vec![0xff, 0x0]);
        assert_eq!(base, vec![0x0, 0xff]);
    }
    {
        let mut base = vec![0x0, 0x1];
        union(&mut base, &vec![0x0, 0x1, 0xff]);
        assert_eq!(base, vec![0x0, 0x1, 0xff]);
    }
    {
        let mut base = vec![0xff, 0x0, 0xff];
        union(&mut base, &vec![0x1, 0x0]);
        assert_eq!(base, vec![0x0, 0x1, 0xff]);
    }
}

#[test]
fn test_intersection() {
    {
        let mut base = vec![];
        intersection(&mut base, &[]);
        assert!(base.is_empty());
    }
    {
        let mut base = vec![0x0];
        intersection(&mut base, &[]);
        assert!(base.is_empty());
    }
    {
        let mut base = vec![];
        intersection(&mut base, &[0x0]);
        assert!(base.is_empty());
    }
    {
        let mut base = vec![0x0, 0x1];
        intersection(&mut base, &[]);
        assert!(base.is_empty());
    }
    {
        let mut base = vec![];
        intersection(&mut base, &[0x0, 0x1]);
        assert!(base.is_empty());
    }
    {
        let mut base = vec![0x0];
        intersection(&mut base, &[0x0]);
        assert_eq!(base, vec![0x0]);
    }
    {
        let mut base = vec![0xff];
        intersection(&mut base, &[0x0, 0xff]);
        assert_eq!(base, vec![0xff]);
    }
    {
        let mut base = vec![0xff];
        intersection(&mut base, &[0xff, 0x0]);
        assert_eq!(base, vec![0xff]);
    }
    {
        let mut base = vec![0x0, 0x1];
        intersection(&mut base, &[0x0, 0x1, 0xff]);
        assert_eq!(base, vec![0x0, 0x1]);
    }
    {
        let mut base = vec![0x1, 0xff];
        intersection(&mut base, &[0x0, 0x1]);
        assert_eq!(base, vec![0x1]);
    }
}

#[test]
fn test_into_trigrams() {
    assert_eq!(into_trigrams(&[]), Vec::<u32>::new());
    assert_eq!(into_trigrams(&[0x10, 0xff]), Vec::<u32>::new());
    assert_eq!(into_trigrams(&[0x10, 0xff, 0x44]), vec![0x10ff44]);
    assert_eq!(into_trigrams(&[0x10, 0xff, 0x44, 0x22]), vec![0x10ff44, 0xff4422]);
}

