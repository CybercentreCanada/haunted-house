mod auth;
mod config;
mod core;
mod storage;
mod size_type;
mod database;
mod database_sqlite;
mod query;
mod error;
mod access;
mod interface;
mod blob_cache;
mod worker;
mod bloom;
mod worker_watcher;
mod counters;

use std::ops::BitXor;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, Context};
use auth::Authenticator;
use bitvec::macros::internal::funty::Numeric;
use bloom::Filter;
use clap::{Parser, Subcommand};
use itertools::Itertools;
use log::{info, error};
use rand::{thread_rng, Rng};
use tokio::sync::mpsc;
use worker::{worker_manager, WorkerData};

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
    },
    Test {
        #[arg(short, long)]
        config: Option<PathBuf>,
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
            // info!("Connect to index storage");
            // let index_storage = crate::storage::connect(config.blobs).await?;
            info!("Connect to file storage");
            let file_storage = crate::storage::connect(config.files).await?;

            // info!("Setup cache");
            // let (cache, _temp) = match config.cache {
            //     config::CacheConfig::TempDir { size } => {
            //         let temp_dir = tempfile::tempdir()?;
            //         (BlobCache::new(index_storage.clone(), size, temp_dir.path().to_owned())?, Some(temp_dir))
            //     }
            //     config::CacheConfig::Directory { path, size } => {
            //         (BlobCache::new(index_storage.clone(), size, PathBuf::from(path))?, None)
            //     }
            // };

            // Initialize database
            info!("Connecting to database.");
            let database = match config.database {
                config::Database::SQLite{path} => Database::new_sqlite(config.core.clone(), &path).await?,
                config::Database::SQLiteTemp{..} => Database::new_sqlite_temp(config.core.clone()).await?,
            };

            // Start server core
            info!("Starting server core.");
            let core = HouseCore::new(file_storage, database, auth, config.core)
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
            // info!("Connect to blob storage");
            // let index_storage = crate::storage::connect(config.blobs).await?;
            info!("Connect to file storage");
            let file_storage = crate::storage::connect(config.files).await?;

            // Initialize authenticator
            let token = config.api_token;

            // Get cache
            info!("Setup caches");
            let (file_cache, _file_temp) = match config.cache {
                config::CacheConfig::TempDir { size } => {
                    let temp_dir = tempfile::tempdir()?;
                    (BlobCache::new(file_storage.clone(), size, temp_dir.path().to_owned())?, Some(temp_dir))
                }
                config::CacheConfig::Directory { path, size } => {
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

            //
            let address = config.server_address;
            let verify = config.server_tls;
            let (sender, recv) = mpsc::unbounded_channel();
            let data = Arc::new(WorkerData::new(sender.clone(), file_cache, address, verify, token, bind_address.port())?);

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
        },
        Commands::Test { config } => {
            // Load the config file
            info!("Loading config from: {config:?}");
            let config = load_config(config)?;

            // Initialize database
            info!("Connecting to database.");
            let database = match config.database {
                config::Database::SQLite{path} => Database::new_sqlite(config.core.clone(), &path).await?,
                config::Database::SQLiteTemp{..} => Database::new_sqlite_temp(config.core.clone()).await?,
            };

            // database.partition_test().await?;

            let data = database.list_filters("simple:4096", 1000).await?;

            let mut tree = TestTree::new(4096);

            for filter in data {
                if tree.insert(filter) {
                    let mask = tree.mask().clone();
                    let other = tree.split();
                    tree = TestTree::Internal(mask, vec![other, tree]);
                }
            }

            tree.describe("".to_owned());

            // for item in data {
            //     let original = {
            //         let buffer = item.to_buffer()?;
            //         buffer.len()
            //     };

            //     let raw = {
            //         let mut buffer = vec![];
            //         let mut data = item.data.clone();
            //         data.force_align();
            //         for num in data.into_vec() {
            //             for byte in num.to_le_bytes() {
            //                 buffer.push(byte);
            //             }
            //         }
            //         while let Some(&last) = buffer.last() {
            //             if last == 0 {
            //                 buffer.pop();
            //             } else {
            //                 break
            //             }
            //         }

            //         buffer.len()
            //     };

            //     let sparse = {
            //         let buffer = encode_sparse(&item.data);
            //         buffer.len()
            //     };

            //     println!("{original}\t{raw}\t{sparse}");
            // }
        }
    }

    return Ok(())
}



enum TestTree {
    Internal(Filter, Vec<TestTree>),
    Leaves(Filter, Vec<Filter>)
}

const SIZE: usize = 64;

impl TestTree {

    fn add_score(a: &Filter, b: &Filter) -> i64 {
        // (a.data.clone().bitxor(&b.data).count_ones() as i64)
        a.overlap(&b).unwrap().count_zeros() as i64
    }

    pub fn new(size: u64) -> Self {
        TestTree::Leaves(Filter::empty(size, 1, 1), vec![])
    }

    pub fn mask(&self) -> &Filter {
        match self {
            TestTree::Internal(mask, _) => mask,
            TestTree::Leaves(mask, _) => mask,
        }
    }

    pub fn insert(&mut self, filter: Filter) -> bool {
        match self {
            TestTree::Internal(mask, children) => {
                *mask = mask.overlap(&filter).unwrap();

                let mut best_score = i64::MIN;
                let mut best: Option<&mut TestTree> = None;
                for child in children.iter_mut() {
                    let score = Self::add_score(child.mask(), &filter);
                    if score > best_score {
                        best_score = score;
                        best = Some(child);
                    }
                }

                if let Some(best) = best {
                    if best.insert(filter) {
                        let new = best.split();
                        children.push(new);
                    }
                } else {
                    todo!();
                }

                children.len() >= SIZE
            },
            TestTree::Leaves(mask, children) => {
                *mask = mask.overlap(&filter).unwrap();
                children.push(filter);
                children.len() >= SIZE
            },
        }
    }

    pub fn split(&mut self) -> TestTree {
        let children = self.children_masks();
        let (a_seed, b_seed) = choose_opposed(&children);

        let mut a = vec![a_seed];
        let mut b = vec![b_seed];
        let mut a_cover = children[a_seed].clone();
        let mut b_cover = children[b_seed].clone();

        for (index, filter) in children.iter().enumerate() {
            if index == a_seed || index == b_seed {
                continue
            }

            if Self::add_score(&a_cover, filter) > Self::add_score(&b_cover, filter) {
                a.push(index);
                a_cover = a_cover.overlap(filter).unwrap();
            } else {
                b.push(index);
                b_cover = b_cover.overlap(filter).unwrap();
            }
        }

        println!("Split {} {} {}", self.mask().count_zeros(), a_cover.count_zeros(), b_cover.count_zeros());

        b.sort();
        match self {
            TestTree::Internal(mask, children) => {
                *mask = a_cover;

                let mut b_children = vec![];
                while let Some(index) = b.pop() {
                    b_children.push(children.swap_remove(index));
                }

                TestTree::Internal(b_cover, b_children)
            },
            TestTree::Leaves(mask, children) => {
                *mask = a_cover;

                let mut b_children = vec![];
                while let Some(index) = b.pop() {
                    b_children.push(children.swap_remove(index));
                }

                TestTree::Leaves(b_cover, b_children)
            }
        }
    }

    fn children_masks(&self) -> Vec<&Filter> {
        let mut out = vec![];
        match self {
            TestTree::Internal(_, children) => {
                for child in children {
                    out.push(child.mask());
                }
            },
            TestTree::Leaves(_, children) => {
                for child in children {
                    out.push(child);
                }
            },
        }
        return out;
    }

    pub fn describe(&self, pad: String) {
        println!("{pad}{}", self.mask().count_zeros());
        match self {
            TestTree::Internal(_, children) => {
                for child in children {
                    child.describe(format!("{pad}  "))
                }
            },
            TestTree::Leaves(mask, children) => {
                let mut slack = 0;
                for item in children {
                    slack += item.data.clone().bitxor(&mask.data).count_ones();
                }
                println!("{pad}  {}", slack/children.len());
            },
        }
    }
}


fn choose_opposed_once(values: &Vec<&Filter>) -> (usize, usize, usize) {
    let a = thread_rng().gen_range(0..values.len());
    let mut b = 0;
    let mut best_score = values[a].data.clone().bitxor(&values[b].data).count_ones();
    // let mut best_score = values[a].overlap(&values[b]).unwrap().count_zeros();

    for index in 1..values.len() {
        if index == a {
            continue;
        }

        let score = values[a].data.clone().bitxor(&values[index].data).count_ones();
        // let score = values[a].overlap(&values[index]).unwrap().count_zeros();
        if score < best_score {
            b = index;
            best_score = score;
        }
    }

    (a, b, best_score)
}


fn choose_opposed(values: &Vec<&Filter>) -> (usize, usize) {
    let (a, b, mut best_score) = choose_opposed_once(values);

    let mut best_pair: (usize, usize) = (a, b);

    for _ in 0..10 {
        let (a, b, score) = choose_opposed_once(values);
        if score > best_score {
            best_pair = (a, b);
            best_score = score;
        }
    }

    return best_pair
}

// fn encode_zeros(encoded: &mut bitvec::vec::BitVec, zeros: usize) {
//     if zeros == 0 {
//         encoded.push(false);
//     } else if zeros <= 1 << 4 {
//         let zeros = zeros - 1; // counting from 1 because zero isn't an option
//         encoded.push(true);
//         encoded.push(false);
//         encoded.push(zeros & 0b0001 > 0);
//         encoded.push(zeros & 0b0010 > 0);
//         encoded.push(zeros & 0b0100 > 0);
//         encoded.push(zeros & 0b1000 > 0);
//     } else {
//         let zeros = zeros - 1; // counting from 1 because zero isn't an option
//         encoded.push(true);
//         encoded.push(true);
//         encoded.push(zeros & 0b0001 > 0);
//         encoded.push(zeros & 0b0010 > 0);
//         encoded.push(zeros & 0b0100 > 0);
//         encoded.push(zeros & 0b1000 > 0);
//         let zeros = zeros >> 4;

//         loop {
//             let bits = [
//                 zeros & 0b0001 > 0,
//                 zeros & 0b0010 > 0,
//                 zeros & 0b0100 > 0,
//                 zeros & 0b1000 > 0
//             ];
//             let zeros = zeros >> 4;

//             if zeros > 0 {
//                 encoded.push(true);
//                 encoded.extend(bits);
//             } else {
//                 encoded.push(false);
//                 encoded.extend(bits);
//                 break;
//             }
//         }
//     }
// }

// fn encode_sparse(data: &bitvec::vec::BitVec) -> Vec<u8> {
//     let mut encoded: bitvec::vec::BitVec = bitvec::vec::BitVec::new();
//     let mut last: usize = 0;

//     for (current, next) in data.iter_ones().tuple_windows() {
//         last = next;
//         let zeros = next - current - 1;

//         // One step
//         encode_zeros(&mut encoded, zeros);
//     }
//     encode_zeros(&mut encoded, data.len() - last);

//     let mut buffer = vec![];
//     encoded.force_align();
//     for num in encoded.into_vec() {
//         for byte in num.to_le_bytes() {
//             buffer.push(byte);
//         }
//     }
//     while let Some(&last) = buffer.last() {
//         if last == 0 {
//             buffer.pop();
//         } else {
//             break
//         }
//     }

//     return buffer;
// }

// 0 one followed by a one
// 10xxxx one followed by zeros encoded by x
// 11xxxx(1xxxx)*0xxxx one followed
