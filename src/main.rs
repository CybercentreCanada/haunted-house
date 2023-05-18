mod config;
mod core;
mod storage;
mod size_type;
mod query;
mod error;
mod access;
mod varint;
mod types;
mod logging;
mod sqlite_set;
mod broker;
mod blob_cache;
mod worker;
mod worker_watcher;

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, Context};
use auth::Authenticator;
use clap::{Parser, Subcommand};
use itertools::Itertools;
use log::{info, error};
use tokio::sync::mpsc;

use crate::blob_cache::BlobCache;
use crate::core::HouseCore;
use crate::database::Database;


#[derive(Parser, Debug)]
struct Args {
    #[command(subcommand)]
    cmd: Commands
}

#[derive(Debug, Clone)]
enum ConfigMode {
    Server,
    Worker
}

impl FromStr for ConfigMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.to_lowercase();
        if s == "server" {
            return Ok(ConfigMode::Server);
        }
        if s == "worker" {
            return Ok(ConfigMode::Worker);
        }
        return Err(anyhow::anyhow!("unknown config type: {s}"))
    }
}


#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Server {
        #[arg(short, long)]
        config: Option<PathBuf>
    },
    Worker {
        #[arg(short, long)]
        config: Option<PathBuf>
    },
    LintConfig {
        mode: ConfigMode,
        #[arg(short, long)]
        config: Option<PathBuf>,
        #[arg(short, long)]
        default: bool,
    }
}


fn load_config(path: Option<PathBuf>) -> Result<crate::config::Config> {
    let config = path.unwrap_or(PathBuf::from("./config.json"));
    let config_body = std::fs::read_to_string(config)?;
    Ok(serde_json::from_str(&config_body)?)
}

fn load_worker_config(path: Option<PathBuf>) -> Result<crate::config::WorkerConfig> {
    let config = path.unwrap_or(PathBuf::from("./config.json"));
    let config_body = std::fs::read_to_string(config)?;
    Ok(serde_json::from_str(&config_body)?)
}


#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "haunted_house=info")
    }
    env_logger::init();

    let args = Args::parse();
    match args.cmd {
        Commands::LintConfig { mode, config, default } => {
            match mode {
                ConfigMode::Server => {
                    let config = if default {
                        Default::default()
                    } else {
                        load_config(config)?
                    };
                    let config_body = serde_json::to_string_pretty(&config)?;
                    println!("{}", config_body);
                },
                ConfigMode::Worker => {
                    let config = if default {
                        Default::default()
                    } else {
                        load_worker_config(config)?
                    };
                    let config_body = serde_json::to_string_pretty(&config)?;
                    println!("{}", config_body);
                },
            }
        },
        Commands::Server { config } => {
            // Load the config file
            info!("Loading configuration");
            let config = load_config(config)?;

            // Initialize authenticator
            info!("Initializing Authenticator");
            let auth = Authenticator::from_config(config.authentication)?;

            // Setup the storage
            info!("Connect to index storage");
            let index_storage = crate::storage::connect(config.blobs).await?;
            info!("Connect to file storage");
            let file_storage = crate::storage::connect(config.files).await?;

            info!("Setup cache");
            let (cache, _temp) = match config.cache {
                config::CacheConfig::TempDir { size } => {
                    let temp_dir = tempfile::tempdir()?;
                    (BlobCache::new(index_storage.clone(), size, temp_dir.path().to_owned())?, Some(temp_dir))
                }
                config::CacheConfig::Directory { path, size } => {
                    (BlobCache::new(index_storage.clone(), size, PathBuf::from(path))?, None)
                }
            };

            // Initialize database
            info!("Connecting to database.");
            let database = match config.database {
                config::Database::SQLite{path} => Database::new_sqlite(config.core.clone(), &path).await?,
                config::Database::SQLiteTemp{..} => Database::new_sqlite_temp(config.core.clone()).await?,
            };

            // Start server core
            info!("Starting server core.");
            let core = HouseCore::new(index_storage, file_storage, database, cache, auth, config.core)
                .context("Error launching core.")?;

            // Start http interface
            let bind_address = match config.bind_address {
                None => "localhost:8080".to_owned(),
                Some(address) => address,
            };
            info!("Starting server interface on {bind_address}");
            let api_job = tokio::task::spawn(crate::interface::serve(bind_address, config.tls, core.clone()));

            // Wait for server to stop
            api_job.await.context("Error in HTTP interface.")?;
        },
        Commands::Worker { config } => {
            // Load the config file
            info!("Loading config from: {config:?}");
            let config = load_worker_config(config)?;

            // Setup the storage
            info!("Connect to blob storage");
            let index_storage = crate::storage::connect(config.blobs).await?;
            info!("Connect to file storage");
            let file_storage = crate::storage::connect(config.files).await?;

            // Initialize authenticator
            let token = config.api_token;

            // Get cache
            info!("Setup caches");
            let (file_cache, _file_temp) = match config.file_cache {
                config::CacheConfig::TempDir { size } => {
                    let temp_dir = tempfile::tempdir()?;
                    (BlobCache::new(file_storage.clone(), size, temp_dir.path().to_owned())?, Some(temp_dir))
                }
                config::CacheConfig::Directory { path, size } => {
                    (BlobCache::new(file_storage.clone(), size, PathBuf::from(path))?, None)
                }
            };
            let (index_cache, _index_temp) = match config.blob_cache {
                config::CacheConfig::TempDir { size } => {
                    let temp_dir = tempfile::tempdir()?;
                    (BlobCache::new(index_storage.clone(), size, temp_dir.path().to_owned())?, Some(temp_dir))
                }
                config::CacheConfig::Directory { path, size } => {
                    (BlobCache::new(index_storage.clone(), size, PathBuf::from(path))?, None)
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

            //
            let address = config.server_address;
            let verify = config.server_tls;
            let (sender, recv) = mpsc::unbounded_channel();
            let data = Arc::new(WorkerData::new(sender.clone(), file_cache, index_cache, address, verify, token, bind_address.port())?);

            // Watch for exit signal
            tokio::spawn({
                let data = data.clone();
                async move {
                    match tokio::signal::ctrl_c().await {
                        Ok(()) => {
                            data.stop();
                        },
                        Err(err) => {
                            error!("Error waiting for exit signal: {err}");
                        },
                    }
                }
            });

            // Run the worker
            let exit_notice = Arc::new(tokio::sync::Notify::new());
            let api = tokio::spawn(worker::interface::serve(bind_address, config.tls, sender, exit_notice.clone()));
            let manager = tokio::spawn(worker_manager(data.clone(), recv));

            // Run the worker
            let result = tokio::select! {
                res = api => res,
                res = manager => res,
            };

            match result {
                Ok(Err(err)) => error!("Server crashed: {err} {}", err.root_cause()),
                Err(err) => error!("Server crashed: {err}"),
                _ => {}
            };
        }
    }

    return Ok(())
}
