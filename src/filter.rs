use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use anyhow::{Result, Context};
use log::error;
use serde::{Serialize, Deserialize};
use sha2::digest::generic_array::{GenericArray, ArrayLength};
use tokio::io::{BufReader, AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use sha2::{Sha256, Digest};

use crate::access::AccessControl;



struct Filter {
    handle: tokio::fs::File,
}

fn to_array(value: Vec<u8>) -> Result<Box<[u8; 32]>> {
    Ok(Box::new(match value.try_into() {
        Ok(hash) => hash,
        Err(_) => return Err(crate::error::ErrorKinds::InvalidHashProduced.into()),
    }))
}

impl Filter {

    async fn build_file(path: &Path) -> Result<(Vec<bool>, Box<[u8; 32]>)> {
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
            let hash: Box<[u8; 32]> = to_array(hasher.finalize().to_vec())?;
            return Ok((mask, hash))
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

        return Ok((mask, to_array(hasher.finalize().to_vec())?))
    }

    pub async fn build(filter_file: &tokio::fs::File, meta_file: &tokio::fs::File, files: Vec<(PathBuf, AccessControl)>) -> Result<Self> {
        let mut accum: Vec<Vec<u64>> = Default::default();
        let mut meta: HashMap<Box<[u8; 32]>, (u64, AccessControl)> = Default::default();
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

        // let data = FilterMetadata::write(output + meta_file, meta);

        todo!();
    }

    pub async fn open(filter: &Path, meta: &Path) -> Result<Self> {
        // let meta = FilterMetadata::open(meta).await?;
        Ok(Filter {
            handle: tokio::fs::File::open(filter).await?,
            // meta,
        })
    }

    pub fn merge(a: &Filter, b: &Filter) -> Result<Self> {
        todo!()
    }
}


#[cfg(test)]
mod test {

}