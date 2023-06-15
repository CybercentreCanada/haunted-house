use std::path::PathBuf;
use std::sync::Arc;

use itertools::Itertools;
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};
use log::{info, error};
use crate::blob_cache::BlobCache;
use crate::types::Sha256;
use crate::worker::database::Database;
use crate::worker::manager::WorkerState;

pub mod interface;
mod database;
mod database_sqlite;
mod sparse;
mod trigram_cache;
mod encoding;
mod manager;
mod filter;
mod filter_worker;


#[derive(Serialize, Deserialize)]
pub struct YaraTask {
    pub id: i64,
    pub yara_rule: String,
    pub hashes: Vec<Sha256>,
}

pub async fn main(config: crate::config::WorkerConfig) -> Result<()> {
    // Setup the storage
    info!("Connect to file storage");
    let file_storage = crate::storage::connect(config.files).await?;

    // Get cache
    info!("Setup caches");
    let (file_cache, _file_temp) = match config.file_cache {
        crate::config::CacheConfig::TempDir { size } => {
            let temp_dir = tempfile::tempdir()?;
            (BlobCache::new(file_storage.clone(), size, temp_dir.path().to_owned())?, Some(temp_dir))
        }
        crate::config::CacheConfig::Directory { path, size } => {
            (BlobCache::new(file_storage.clone(), size, PathBuf::from(path))?, None)
        }
    };

    // Figure out where the worker status interface will be hosted
    info!("Determine bind address");
    let bind_address = config.bind_address.unwrap_or("localhost:8080".to_owned());
    let mut addresses = tokio::net::lookup_host(&bind_address).await?.collect_vec();
    let bind_address = match addresses.pop() {
        Some(x) => x,
        None => {
            return Err(anyhow::anyhow!("Couldn't resolve bind address: {}", bind_address));
        }
    };
    info!("Status interface binding on: {bind_address}");

    info!("Setting up database.");
    let database = Database::new_sqlite(config.settings.get_database_directory()).await.context("setting up database")?;

    info!("Spawing processing daemons.");
    let (set_running, running) = tokio::sync::watch::channel(true);
    let data = WorkerState::new(database, file_storage, file_cache, config.settings, running).await.context("spawning")?;

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