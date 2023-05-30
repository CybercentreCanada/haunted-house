use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use log::error;
use tokio::sync::{Semaphore, RwLock};
use tokio::task::JoinHandle;

use crate::config::WorkerSettings;
use crate::types::{Sha256, FilterID};

use super::sparse::SparseBits;



pub struct TrigramCache {
    cache_dir: PathBuf,
    temp_dir: PathBuf,
    permits: Semaphore,
    pending: RwLock<HashMap<(FilterID, Sha256), JoinHandle<()>>>,
}


impl TrigramCache {

    pub async fn new(config: WorkerSettings) -> Result<Arc<Self>> {
        todo!()
    }

    pub async fn expire(&self, filter: FilterID) -> Result<()> {
        todo!()
    }

    pub async fn start_fetch(self: Arc<Self>, filter: FilterID, hash: Sha256) -> Result<()> {
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
        todo!();
    }

    pub async fn is_pending(&self, entries: Vec<(FilterID, Sha256)>) -> Result<HashMap<(FilterID, Sha256), bool>> {
        let workers = self.pending.read().await;
        let mut output = HashMap::new();
        for job_parameters in entries {
            if let Some(task) = workers.get(&job_parameters) {
                if !task.is_finished() {
                    output.insert(job_parameters, true);
                    continue
                }
            }
            output.insert(job_parameters, false);
        }
        return Ok(output)
    }

    pub async fn is_ready(&self, filter: FilterID, hash: &Sha256) -> Result<bool> {
        Ok(tokio::fs::try_exists(self._path(filter, hash)).await?)
    }

    pub async fn get(&mut self, filter: FilterID, hash: &Sha256) -> Result<SparseBits> {
        let data = tokio::fs::read(self._path(filter, hash)).await?;
        Ok(postcard::from_bytes(&data)?)
    }

    pub async fn release(&mut self, filter: FilterID, hash: &Sha256) -> Result<()> {
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

    pub fn _path(&self, filter: FilterID, hash: &Sha256) -> PathBuf {
        self.cache_dir.join(filter.to_string()).join(hash.to_string())
    }
}

