use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use log::error;
use tokio::sync::{Semaphore, RwLock};
use tokio::task::JoinHandle;

use crate::config::WorkerSettings;
use crate::storage::BlobStorage;
use crate::types::{Sha256, FilterID, FileInfo};

use super::sparse::SparseBits;


pub struct TrigramCache {
    cache_dir: PathBuf,
    temp_dir: PathBuf,
    permits: Semaphore,
    pending: RwLock<HashMap<(FilterID, Sha256), JoinHandle<()>>>,
    files: BlobStorage
}


impl TrigramCache {

    pub async fn new(config: &WorkerSettings, files: BlobStorage) -> Result<Arc<Self>> {
        let temp_dir = config.get_trigram_cache_directory().join("temp");
        if temp_dir.exists() {
            tokio::fs::remove_dir_all(&temp_dir).await?;
        }
        tokio::fs::create_dir_all(&temp_dir).await?;
        Ok(Arc::new(Self {
            cache_dir: config.get_trigram_cache_directory(),
            temp_dir,
            permits: Semaphore::new(config.parallel_file_downloads),
            pending: RwLock::new(Default::default()),
            files,
        }))
    }

    pub async fn expire(&self, filter: FilterID) -> Result<()> {
        // Flush out pending tasks
        {
            let mut pending = self.pending.write().await;
            let mut remove = vec![];
            for (key, task) in pending.iter() {
                if key.0 == filter {
                    remove.push(key.clone());
                    task.abort();
                }
            }
            for key in remove {
                pending.remove(&key);
            }
        }

        // Erase directory
        return Ok(tokio::fs::remove_dir_all(self._filter_path(filter)).await?);
    }

    pub async fn start_fetch(self: &Arc<Self>, filter: FilterID, hash: Sha256) -> Result<()> {
        if self.is_ready(filter, &hash).await? {
            return Ok(())
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = self.pending.write().await.entry((filter, hash.clone())) {
            let core = self.clone();
            entry.insert(tokio::spawn(async move {
                if let Err(err) = core._fetch_file(filter, &hash).await {
                    error!("Fetch file error: {err}");
                }
                let mut pending = core.pending.write().await;
                pending.remove(&(filter, hash));
            }));
        }
        return Ok(())
    }

    async fn _fetch_file(&self, filter: FilterID, hash: &Sha256) -> Result<()> {
        let _permit = self.permits.acquire().await?;

        // Gather the file content
        let stream = self.files.stream(&hash.hex()).await?;
        let trigrams = build_file(stream).await?;

        // Store the trigrams
        let cache_path = self._path(filter, hash);
        let temp_dir = self.temp_dir.clone();
        let filter_dir = self._filter_path(filter);
        tokio::task::spawn_blocking(move ||{
            let mut temp = tempfile::NamedTempFile::new_in(&temp_dir)?;
            temp.write_all(&postcard::to_allocvec(&trigrams)?)?;
            temp.flush()?;
            std::fs::create_dir_all(filter_dir)?;
            temp.persist(cache_path)?;
            anyhow::Ok(())
        }).await??;
        return Ok(())
    }

    pub async fn strip_pending(&self, collection: &mut Vec<(FilterID, FileInfo)>) {
        let workers = self.pending.read().await;
        collection.retain(|(filter, info)|{
            !workers.contains_key(&(*filter, info.hash.clone()))
        });
    }

    pub async fn add_pending(&self, collection: &mut HashMap<FilterID, HashSet<Sha256>>) {
        let workers = self.pending.read().await;
        for (filter, sha) in workers.keys() {
            match collection.entry(*filter) {
                std::collections::hash_map::Entry::Occupied(mut entry) => { entry.get_mut().insert(sha.clone()); },
                std::collections::hash_map::Entry::Vacant(entry) => { entry.insert([sha.clone()].into()); },
            }
        }
    }

    pub async fn is_ready(&self, filter: FilterID, hash: &Sha256) -> Result<bool> {
        Ok(tokio::fs::try_exists(self._path(filter, hash)).await?)
    }

    pub async fn get(&self, filter: FilterID, hash: &Sha256) -> Result<SparseBits> {
        let data = tokio::fs::read(self._path(filter, hash)).await?;
        Ok(postcard::from_bytes(&data)?)
    }

    pub async fn release(&self, filter: FilterID, hash: &Sha256) -> Result<()> {
        let path = self._path(filter, hash);
        match tokio::fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(err) => {
                if !tokio::fs::try_exists(path).await? {
                    Ok(())
                } else {
                    Err(err.into())
                }
            },
        }
    }

    pub fn _filter_path(&self, filter: FilterID) -> PathBuf {
        self.cache_dir.join(filter.to_string())
    }

    pub fn _path(&self, filter: FilterID, hash: &Sha256) -> PathBuf {
        self._filter_path(filter).join(hash.to_string())
    }
}


async fn build_file(mut input: tokio::sync::mpsc::Receiver<Result<Vec<u8>>>) -> Result<SparseBits> {
    // Prepare accumulators
    let mut output = SparseBits::new();

    // Read the initial block
    let mut buffer = vec![];
    while buffer.len() <= 2 {
        let sub_buffer = match input.recv().await {
            Some(sub_buffer) => sub_buffer?,
            None => return Ok(output),
        };

        buffer.extend(sub_buffer);
    }

    // Initialize trigram
    let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
    let mut index_start = 2;

    'outer: loop {
        for _ in 0..1<<16 {
            for byte in &buffer[index_start..] {
                trigram = (trigram & 0x00FFFF) << 8 | (*byte as u32);
                output.insert(trigram);
            }

            buffer = match input.recv().await {
                Some(buffer) => buffer?,
                None => break 'outer,
            };
            if buffer.is_empty() {
                break 'outer;
            }
            index_start = 0;
        }
        output.compact();
    }
    output.compact();

    return Ok(output)
}
