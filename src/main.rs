mod api;
mod auth;
mod config;
mod jobs;
mod core;
mod storage;
mod size_type;
mod database;
mod database_sqlite;
mod query;
mod error;
mod filter;
mod access;
mod interface;
mod ursadb;
mod varint;
mod blob_cache;

use std::path::PathBuf;

use anyhow::{Result, Context};
use auth::Authenticator;
use clap::Parser;
use log::info;

use crate::blob_cache::BlobCache;
use crate::core::HouseCore;
use crate::database::Database;


#[derive(Parser, Debug)]
struct Args {
    #[command(subcommand)]
    cmd: Commands
}

#[derive(clap::Subcommand, Debug, Clone)]
enum Commands {
    Server {
        #[arg(short, long)]
        config: Option<PathBuf>
    },
    LintConfig {
        #[arg(short, long)]
        config: Option<PathBuf>
    }
}


fn load_config(path: Option<PathBuf>) -> Result<crate::config::Config> {
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
        Commands::LintConfig { config } => {
            let config = load_config(config)?;
            let config_body = serde_json::to_string_pretty(&config)?;
            println!("{}", config_body);
        },
        Commands::Server { config } => {
            // Load the config file
            let config = load_config(config)?;

            // Initialize authenticator
            let auth = Authenticator::from_config(config.authentication)?;

            // Setup the storage
            let index_storage = crate::storage::connect(config.blobs).await?;
            let file_storage = crate::storage::connect(config.files).await?;

            let (cache, _temp) = match config.cache {
                config::CacheConfig::TempDir { size } => {
                    let temp_dir = tempfile::tempdir()?;
                    (BlobCache::new(index_storage.clone(), size, temp_dir.path().to_owned()), Some(temp_dir))
                }
                config::CacheConfig::Directory { path, size } => {
                    (BlobCache::new(index_storage.clone(), size, PathBuf::from(path)), None)
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
            info!("Starting server interface.");
            let bind_address = match config.bind_address {
                None => "localhost:8080".to_owned(),
                Some(address) => address,
            };
            let api_job = tokio::task::spawn(crate::interface::serve(bind_address, core.clone()));

            // Wait for server to stop
            api_job.await.context("Error in HTTP interface.")?;
        }
    }

    return Ok(())
}
