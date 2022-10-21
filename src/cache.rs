use anyhow::Result;
use futures::stream::FuturesUnordered;
use tempfile::NamedTempFile;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::{BlobStorage, BlobID};


pub struct BlobHandle {
    file: NamedTempFile
}

impl BlobHandle {
    pub fn open(&self) -> Result<std::fs::File> {
        Ok(self.file.reopen()?)
    }
}

struct CacheEntry {
    access_time: chrono::DateTime<chrono::Utc>,
    handle: Arc<BlobHandle>
}

struct PendingBlobs {

}

enum BlobCacheCommand {
    ListBlobs(oneshot::Sender<Vec<BlobID>>),
    Open(BlobID, oneshot::Sender<Arc<BlobHandle>>)
}

#[derive(Clone)]
pub struct BlobCache {
    connection: mpsc::Sender<BlobCacheCommand>
}

impl BlobCache {

    fn new<Storage: BlobStorage>(storage: Storage, capacity: usize, path: PathBuf) -> Self {
        let (send, recv) = mpsc::channel(32);

        tokio::spawn(Inner::new(storage, capacity, path, recv).run());

        Self {
            connection: send
        }
    }

    pub async fn open(&self, id: BlobID) -> Result<Arc<BlobHandle>> {
        let (send, recv) = oneshot::channel();
        self.connection.send(BlobCacheCommand::Open(id, send)).await;
        Ok(recv.await?)
    }

    pub async fn current_open(&self) -> Result<Vec<BlobID>> {
        let (send, recv) = oneshot::channel();
        self.connection.send(BlobCacheCommand::ListBlobs(send)).await;
        Ok(recv.await?)
    }

}



struct Inner<Storage: BlobStorage> {
    connection: mpsc::Receiver<BlobCacheCommand>,
    committed_capacity: usize,
    capacity: usize,
    open: HashMap<BlobID, CacheEntry>,
    pending: FuturesUnordered::<JoinHandle<Result<()>>>,
    pending_handles: HashMap<BlobID, NamedTempFile>,
    waiting_for_pending: HashMap<BlobID, Vec<oneshot::Sender<Arc<BlobHandle>>>>,
    waiting_for_space: HashMap<BlobID, Vec<oneshot::Sender<Arc<BlobHandle>>>>,
    storage: Storage,
    path: PathBuf
}

impl<Storage: BlobStorage + 'static> Inner<Storage> {

    fn new(storage: Storage, capacity: usize, path: PathBuf, connection: mpsc::Receiver<BlobCacheCommand>) -> Self {
        Self {
            connection,
            committed_capacity: 0,
            capacity,
            open: Default::default(),
            pending: Default::default(),
            pending_handles: Default::default(),
            waiting_for_pending: Default::default(),
            waiting_for_space: Default::default(),
            storage,
            path,
        }
    }

    async fn run(mut self) {
        loop {
            assert!(self.waiting_for_space.is_empty());
            assert!(self.waiting_for_pending.is_empty());

            tokio::select! {
                message = self.connection.recv() => {
                    let message = match message {
                        Some(message) => message,
                        None => break,
                    };

                    self.handle_process(message).await;
                }
            }
        }
    }

    async fn handle_process(&mut self, message: BlobCacheCommand) -> Result<()> {
        match message {
            BlobCacheCommand::ListBlobs(respond) => {
                let mut found: Vec<BlobID> = self.open.keys().cloned().collect();
                found.extend(self.waiting_for_pending.keys().cloned());
                respond.send(found);
            },
            BlobCacheCommand::Open(id, respond) => {
                // If the blob is already open, attach to that
                if let Some(entry) = self.open.get_mut(&id) {
                    entry.access_time = chrono::Utc::now();
                    respond.send(entry.handle.clone());
                    return Ok(())
                }

                // If we are already loading it, join the list of connections waiting for it
                if let Some(entry) = self.waiting_for_pending.get_mut(&id) {
                    entry.push(respond);
                    return Ok(())
                }

                // Find out how large the object is
                let new_size = match self.storage.size(&id).await? {
                    Some(size) => size,
                    None => return Ok(()),
                };

                // if we don't have enough room try to free it up
                if self.free_space(new_size) {
                    // Start downloading the blob
                    let temp = NamedTempFile::new()?;
                    self.pending.push(tokio::spawn({
                        let storage = self.storage.clone();
                        let id = id.clone();
                        let path = temp.path().to_path_buf();
                        async move {
                            storage.download(id, path).await
                        }
                    }));
                    self.pending_handles.insert(id.clone(), temp);
                    self.waiting_for_pending.insert(id, vec![respond]);

                } else {
                    // Mark this as something we will need to download later
                    match self.waiting_for_space.entry(id) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            entry.get_mut().push(respond);
                        },
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(vec![respond]);
                        },
                    }
                }
            },
        }
        return Ok(())
    }

    fn free_space(&mut self, size: usize) -> bool {
        todo!()
    }
}


#[cfg(test)]
mod test {
    use crate::storage::{connect, BlobStorageConfig, BlobID};
    use super::BlobCache;

    #[tokio::test]
    async fn access_files() {
        let storage_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let storage = connect(BlobStorageConfig::Directory { path: storage_dir.path().to_str().unwrap().to_string() }).await.unwrap();
        let cache = BlobCache::new(storage, 1024, cache_dir.path().to_owned());

        // Add some data
        let id = BlobID(uuid::Uuid::new_v4());
        

        {

            cache.open(id)
        }
    }

    #[tokio::test]
    async fn size_limit() {
        todo!()
    }

    #[tokio::test]
    async fn cycling() {
        todo!()
    }
}