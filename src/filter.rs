use std::collections::HashMap;
use std::path::{Path, PathBuf};
use anyhow::Result;
use log::error;
use serde::{Serialize, Deserialize};
use tokio::io::{BufReader, AsyncReadExt};
use sha2::{Sha256, Digest};

use crate::access::AccessControl;

// type Expiry = chrono::DateTime<chrono::Utc>;

struct FilterMetadata {
    path: PathBuf,
    handle: tokio::fs::File,
}

// struct FilterMetadataData {
//     hashes_offset: u64,
//     access_offset: u64
//     files: (u64, u64)[]
//     hashes: [u8; 32][]
//     access: AccessControl[]
// }

impl FilterMetadata {
    pub async fn write(path: PathBuf, data: HashMap<Box<[u8; 32]>, (u64, AccessControl)>) -> Result<Self> {
        // Build data tables
        let mut hashes: Vec<&Box<[u8; 32]>> = Default::default();
        let mut access_lookup: HashMap<AccessControl, usize> = Default::default();
        let mut order: Vec<(u64, usize, &Box<[u8; 32]>)> = Default::default();

        for (sha, (item_index, item_access)) in &data {
            hashes.push(sha);
            let access_size = access_lookup.len();
            let access_index = match access_lookup.entry(item_access.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(access_size);
                    access_size
                },
            };
            order.push((*item_index, access_index, sha))
        }
        hashes.sort();
        let hashes_lookup: HashMap<&Box<[u8; 32]>, usize> = hashes.iter().enumerate().map(|(index, hash)|{
            (*hash, index)
        }).collect();

        // Build a lookup into the tables
        let mut files: Vec<(u64, u64)> = Default::default();
        order.sort();
        for index in 1..=files.len() {
            let (_, access_index, sha) = order[index];
            let hash_index = hashes_lookup.get(sha).unwrap();
            files.push((access_index as u64, *hash_index as u64));
        }

        // Serialize and write the data
        let handle = tokio::fs::File::open(&path).await?;


        Ok(FilterMetadata {
            path,
            handle,
        })
    }
}


struct Filter {
    path: PathBuf,
    handle: tokio::fs::File,
    meta: FilterMetadata
}

impl Filter {

    async fn build_file(path: &Path) -> Result<(Vec<bool>, Vec<u8>)> {
        // Prepare to read
        let mut handle = tokio::fs::File::open(path).await?;
        let mut buffer: Vec<u8> = vec![0; 1 << 20];

        // Prepare accumulators
        let mut hasher = Sha256::new();
        let mut mask: Vec<bool> = vec![false; 1 << 24];

        // Read the initial block
        let read = handle.read(&mut buffer).await?;
        hasher.update(&buffer[0..read]);

        // Terminate if file too short
        if read <= 2 {
            return Ok((mask, hasher.finalize().to_vec()))
        }

        // Initialize trigram
        let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
        let mut index_start = 2;

        loop {
            for index in index_start..read {
                trigram = (trigram & 0x00FFFF) << 8 | (buffer[index] as u32);
                mask[trigram as usize] = true;
            }

            let read = handle.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[0..read]);
            index_start = 0;
        }

        return Ok((mask, hasher.finalize().to_vec()))
    }

    pub async fn build(output: &Path, files: Vec<(PathBuf, AccessControl)>) -> Result<Self> {
        let mut accum: Vec<Vec<u64>> = Default::default();
        let mut meta: HashMap<Vec<u8>, (u64, AccessControl)> = Default::default();
        let mut next_index: u64 = 1;

        for (input_file_path, access) in files {
            let (trigrams, sha256) = match Filter::build_file(&input_file_path).await {
                Ok(handle) => handle,
                Err(err) => {
                    error!("Couldn't read input file [{input_file_path:?}]: {err}");
                    continue;
                },
            };

            let file_index = match meta.entry(sha256) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let (_, existing_access) = entry.get_mut();
                    *existing_access = existing_access.or(&access);
                    continue;
                },
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let index = next_index;
                    next_index += 1;
                    entry.insert((index, access));
                    index
                },
            };

            for (index, value) in trigrams.iter().enumerate() {
                if *value {
                    accum[index].push(file_index);
                }
            }
        }

        let data = FilterMetadataData::from(meta);
        BlobID::random();

        todo!();
    }

    pub fn open(filter: &Path, meta: &Path) -> Result<Self> {
        todo!()
    }

    pub fn merge(a: &Filter, b: &Filter) -> Result<Self> {
        todo!()
    }
}

