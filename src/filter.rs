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

// type Expiry = chrono::DateTime<chrono::Utc>;

struct FilterMetadata {
    handle: tokio::fs::File,
    hashes_offset: u64,
    access_offset: u64,
    access: Vec<AccessControl>,
}

// struct FilterMetadataData {
//     hashes_offset: u64,
//     access_offset: u64
//     files: (u64, u64)[]
//     hashes: ([u8; 32], u64)[]
//     access: AccessControl[]
// }

impl FilterMetadata {
    pub async fn write(path: &Path, data: HashMap<Box<[u8; 32]>, (u64, AccessControl)>) -> Result<Self> {
        // Build data tables
        let mut hashes: Vec<(&Box<[u8; 32]>, u64)> = Default::default();
        let mut access_lookup: HashMap<AccessControl, usize> = Default::default();
        let mut order: Vec<(u64, usize, &Box<[u8; 32]>)> = Default::default();

        for (sha, (item_index, item_access)) in &data {
            let access_size = access_lookup.len();
            let access_index = match access_lookup.entry(item_access.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(access_size);
                    access_size
                },
            };
            hashes.push((sha, access_index as u64));
            order.push((*item_index, access_index, sha))
        }
        hashes.sort();
        let hashes_lookup: HashMap<&Box<[u8; 32]>, usize> = hashes.iter().enumerate().map(|(index, (hash, _))|{
            (*hash, index)
        }).collect();

        // Build a lookup into the tables
        let mut files: Vec<(u64, u64)> = Default::default();
        order.sort();
        for (_, access_index, sha) in order {            
            let hash_index = hashes_lookup.get(sha).unwrap();
            files.push((*hash_index as u64, access_index as u64));
        }

        // Serialize and write the data
        let mut handle = tokio::fs::OpenOptions::new().read(true).write(true).open(path).await
            .context("Could not open meta file to write")?;

        let hashes_offset: u64 = (files.len() as u64 + 1) * (8 + 8);
        let access_offset: u64 = hashes_offset + files.len() as u64 * (32 + 8);

        handle.write_u64(hashes_offset).await?;
        handle.write_u64(access_offset).await?;
        for (hash_index, access_index) in files {
            handle.write_u64(hash_index).await?;
            handle.write_u64(access_index).await?;
        }

        // Write hashes
        for (hash, access_index) in hashes {
            handle.write_all(hash.as_ref()).await?;
            handle.write_u64(access_index).await?;
        }

        // write access
        let mut access_values: Vec<(usize, AccessControl)> = access_lookup.into_iter().map(|(a, b)|(b, a)).collect();
        access_values.sort_by(|(a, _), (b, _)| a.cmp(b));
        let access_values: Vec<AccessControl> = access_values.into_iter().map(|(_, b)|b).collect();
        let buffer = postcard::to_allocvec::<Vec<AccessControl>>(&access_values)?;
        handle.write_all(&buffer).await?;
        handle.flush().await?;

        Ok(FilterMetadata {
            handle,
            hashes_offset,
            access_offset,
            access: access_values
        })
    }

    pub async fn open(path: &Path) -> Result<Self> {
        let mut handle = tokio::fs::File::open(path).await?;
        let hashes_offset = handle.read_u64().await?;
        let access_offset = handle.read_u64().await?;
        let file_size = tokio::fs::metadata(path).await?.len();
        let mut buffer: Vec<u8> = vec![0; (file_size - access_offset) as usize];
        handle.seek(SeekFrom::Start(access_offset)).await?;
        handle.read_exact(&mut buffer).await?;
        return Ok(Self {
            handle,
            hashes_offset,
            access_offset,
            access: postcard::from_bytes(&buffer)?,
        })
    }

    pub async fn get_sha(&mut self, index: u64) -> Result<(Box<[u8; 32]>, u64)> {
        self.handle.seek(SeekFrom::Start(self.hashes_offset + index * (32 + 8))).await?;
        let mut buffer: Vec<u8> = vec![0; 32];
        self.handle.read_exact(&mut buffer).await?;
        let access_index = self.handle.read_u64().await?;
        return Ok((to_array(buffer)?, access_index))
    }

    pub async fn get_sha256_access(&mut self, sha: &Box<[u8; 32]>) -> Result<Option<AccessControl>> {

        let sha_count = (self.access_offset - self.hashes_offset) / (32 + 8);
        if sha_count == 0 {
            return Ok(None)
        }

        let mut first = 0;
        let mut last = sha_count - 1;

        loop {
            let center = (last + first)/2;
            let (center_sha, access) = self.get_sha(center).await?;
            if first == last {
                if &center_sha == sha {
                    return Ok(Some(self.access[access as usize].clone()))
                } else {
                    return Ok(None)
                }
            }
            if sha < &center_sha {
                last = center - 1;
            } else if sha > &center_sha {
                first = center + 1;
            } else {
                return Ok(Some(self.access[access as usize].clone()))
            }
        }
    }

    pub async fn get_file_info(&mut self, index: u64) -> Result<(Box<[u8; 32]>, AccessControl)> {
        self.handle.seek(SeekFrom::Start(index * (8 + 8))).await?;
        let hash_index = self.handle.read_u64().await?;
        let (hash, access_index) = self.get_sha(hash_index).await?;
        return Ok((hash, self.access[access_index as usize].clone()));
    }

    pub async fn read_all(&mut self) -> Result<HashMap<Box<[u8; 32]>, (u64, AccessControl)>> {
        let mut out: HashMap<Box<[u8; 32]>, (u64, AccessControl)> = Default::default();
        let entry_count = self.hashes_offset / (8 + 8);
        for index in 1..entry_count {
            let (sha, access) = self.get_file_info(index).await?;
            out.insert(sha, (index, access));
        }
        return Ok(out);
    }

}


struct Filter {
    handle: tokio::fs::File,
    meta: FilterMetadata
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
        let meta = FilterMetadata::open(meta).await?;
        Ok(Filter {
            handle: tokio::fs::File::open(filter).await?,
            meta,
        })
    }

    pub fn merge(a: &Filter, b: &Filter) -> Result<Self> {
        todo!()
    }
}


#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use anyhow::{Result, Context};
    use rand::Rng;
    use rand::seq::IteratorRandom;

    use crate::access::AccessControl;

    use super::FilterMetadata;

    #[tokio::test]
    async fn metadata_save_load() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let mut prng = rand::thread_rng();

        let access_options = [
            AccessControl::parse("A//B//C/Rel:X", "/", ","),
            AccessControl::parse("A//C/Rel:X", "/", ","),
            AccessControl::parse("A//B/Rel:X,Rel:Y", "/", ",")
        ];

        let mut data: HashMap<Box<[u8; 32]>, (u64, AccessControl)> = Default::default();
        let mut other_data: Vec<(Box<[u8; 32]>, AccessControl)> = Default::default();
        for index in 1..500 {
            let sha: Box<[u8; 32]> = Box::new(prng.gen());
            let access = access_options.iter().choose(&mut prng).unwrap().clone();
            data.insert(
                sha.clone(),
                (index, access.clone())
            );
            other_data.push((sha, access));
        }
        println!("data rows {}", data.len());

        let views = {
            let written = FilterMetadata::write(file.path(), data.clone()).await
                .context("Writing filter failed")?;
            let read = FilterMetadata::open(file.path()).await
                .context("Opening filter failed")?;
            vec![written, read]
        };

        for mut filter in views {
            println!("test filter");
            for (sha, (_, access)) in data.iter() {
                assert_eq!(filter.get_sha256_access(sha).await.unwrap(), Some(access.clone()));
            }
            for index in 1..500 {
                assert_eq!(filter.get_file_info(index).await.unwrap(), other_data[(index-1) as usize])
            }
            assert_eq!(filter.read_all().await.unwrap(), data);
        }

        return Ok(())
    }
}