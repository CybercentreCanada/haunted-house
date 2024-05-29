#![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
    unused_crate_dependencies, noop_method_call, single_use_lifetimes, trivial_casts,
    unused_lifetimes, nonstandard_style, variant_size_differences)]
#![deny(keyword_idents)]
// #![warn(clippy::missing_docs_in_private_items)]
#![allow(clippy::needless_return, clippy::while_let_on_iterator)]

//!
//! Haunted house is a microservice designed to sit behind Assemblyline malware triage systems
//! to provide retrohunting capabilities.
//!

mod config;
mod storage;
mod timing;
mod query;
mod error;
mod access;
mod types;
mod logging;
mod sqlite_set;
mod counters;
mod broker;
mod blob_cache;
mod worker;
mod pool;

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use clap::{Parser, Subcommand};
use log::info;
use types::FilterID;
use worker::journal::JournalFilter;


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
    },
    Defrag {
        path: PathBuf
    }
}


fn load_config(path: Option<PathBuf>) -> Result<crate::config::BrokerSettings> {
    let config = path.unwrap_or(PathBuf::from("./config.json"));
    let config_body = std::fs::read_to_string(config)?;
    let config_body = config::apply_env(&config_body)?;
    Ok(serde_json::from_str(&config_body)?)
}

fn load_worker_config(path: Option<PathBuf>) -> Result<crate::config::WorkerSettings> {
    let config = path.unwrap_or(PathBuf::from("./config.json"));
    let config_body = std::fs::read_to_string(config)?;
    let config_body = config::apply_env(&config_body)?;
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
            crate::broker::main(config).await?;
        },
        Commands::Worker { config } => {
            // Load the config file
            info!("Loading config from: {config:?}");
            let config = load_worker_config(config)?;
            config.init_directories()?;
            crate::worker::main(config).await?;
        },
        Commands::Defrag { path } => {
            let file = path.file_name().unwrap().to_string_lossy();
            let path = path.parent().unwrap();
            let id: FilterID = file.parse()?;
            let journal = JournalFilter::open(path.to_owned(), id).await?;
            journal.defrag().await?;
        }
    }

    return Ok(())
}
