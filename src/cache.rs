use anyhow::Result;
use log::{error, info};
use tempfile::NamedTempFile;
use tokio::sync::{mpsc, oneshot, watch};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::{BlobStorage, BlobID};

#[derive(Debug)]
pub struct BlobHandle {
    file: NamedTempFile
}

impl BlobHandle {
    pub fn open(&self) -> Result<std::fs::File> {
        Ok(self.file.reopen()?)
    }

    pub async fn read_all(&self) -> Result<Vec<u8>> {
        Ok(tokio::fs::read(self.file.path()).await?)
    }
}

#[derive(Clone)]
struct CacheEntry {
    access_time: chrono::DateTime<chrono::Utc>,
    handle: Arc<BlobHandle>,
    size: usize,
}

struct PendingBlobs {

}

enum BlobCacheCommand {
    ListBlobs(oneshot::Sender<Vec<BlobID>>),
    Open(BlobID, oneshot::Sender<Result<Arc<BlobHandle>>>),
    Capacity(oneshot::Sender<(usize, usize)>),
    Flush(oneshot::Sender<()>),
    LoadFinished(BlobID),
}

#[derive(Clone)]
pub struct BlobCache {
    connection: mpsc::Sender<BlobCacheCommand>
}

impl BlobCache {

    fn new<Storage: BlobStorage>(storage: Storage, capacity: usize, path: PathBuf) -> Self {
        let (daemon, send) = Inner::new(storage, capacity, path);

        tokio::spawn(daemon.run());

        Self {
            connection: send
        }
    }

    pub async fn open(&self, id: BlobID) -> Result<Arc<BlobHandle>> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(BlobCacheCommand::Open(id, send)).await;
        Ok(recv.await??)
    }

    pub async fn current_open(&self) -> Result<Vec<BlobID>> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(BlobCacheCommand::ListBlobs(send)).await;
        Ok(recv.await?)
    }

    pub async fn capacity(&self) -> Result<(usize, usize)> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(BlobCacheCommand::Capacity(send)).await;
        Ok(recv.await?)
    }

    pub async fn flush(&self) -> Result<()> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(BlobCacheCommand::Flush(send)).await;
        Ok(recv.await?)
    }
}


struct Inner<Storage: BlobStorage> {
    sender: mpsc::WeakSender<BlobCacheCommand>,
    connection: mpsc::Receiver<BlobCacheCommand>,
    committed_capacity: usize,
    capacity: usize,
    open: HashMap<BlobID, CacheEntry>,
    pending: HashMap<BlobID, watch::Receiver<Option<Result<CacheEntry>>>>,
    // pending_handles: HashMap<BlobID, (usize, NamedTempFile)>,
    // waiting_for_pending: HashMap<BlobID, Vec<oneshot::Sender<Result<Arc<BlobHandle>>>>>,
    waiting_for_space: HashMap<BlobID, Vec<oneshot::Sender<Result<Arc<BlobHandle>>>>>,
    storage: Storage,
    path: PathBuf
}

impl<Storage: BlobStorage + 'static> Inner<Storage> {

    fn new(storage: Storage, capacity: usize, path: PathBuf) -> (Self, mpsc::Sender<BlobCacheCommand>) {
        let (sender, connection) = mpsc::channel(32);

        (Self {
            sender: sender.downgrade(),
            connection,
            committed_capacity: 0,
            capacity,
            open: Default::default(),
            pending: Default::default(),
            // pending_handles: Default::default(),
            // waiting_for_pending: Default::default(),
            waiting_for_space: Default::default(),
            storage,
            path,
        }, sender)
    }

    async fn run(mut self) {
        loop {
            assert!(self.waiting_for_space.is_empty());

            tokio::select! {
                message = self.connection.recv() => {
                    let message = match message {
                        Some(message) => message,
                        None => break,
                    };

                    if let Err(err) = self.handle_process(message).await {
                        error!("Error in BlobCache: {err}")
                    }
                }
            }
        }
    }

    async fn handle_process(&mut self, message: BlobCacheCommand) -> Result<()> {
        match message {
            BlobCacheCommand::ListBlobs(respond) => {
                info!("blob list {:?} {:?}", self.open.keys(), self.pending.keys());
                let mut found: Vec<BlobID> = self.open.keys().cloned().collect();
                found.extend(self.pending.keys().cloned());
                _ = respond.send(found);
            },
            BlobCacheCommand::Capacity(respond) => {
                _ = respond.send((self.capacity - self.committed_capacity, self.capacity));
            },
            BlobCacheCommand::Flush(respond) => {
                self.free_space(self.capacity);
                _ = respond.send(());
            },
            BlobCacheCommand::Open(id, respond) => {
                // If the blob is already open, attach to that
                if let Some(entry) = self.open.get_mut(&id) {
                    entry.access_time = chrono::Utc::now();
                    _ = respond.send(Ok(entry.handle.clone()));
                    return Ok(())
                }

                // If we are already loading it, join the list of connections waiting for it
                if let Some(loaded) = self.pending.get(&id) {
                    let mut loaded = loaded.clone();
                    tokio::spawn(async move {
                        while let Ok(_) = loaded.changed().await {
                            let entry = loaded.borrow();
                            if let Some(entry) = &*entry {
                                if let Ok(entry) = entry {
                                    _ = respond.send(Ok(entry.handle.clone()));
                                }
                                break
                            }
                        }
                    });
                    return Ok(())
                }

                // Find out how large the object is
                let new_size = match self.storage.size(&id).await? {
                    Some(size) => size,
                    None => return Ok(()),
                };

                if new_size > self.capacity {
                    _ = respond.send(Err(crate::error::ErrorKinds::BlobTooLargeForCache.into()));
                    return Ok(())
                }

                // if we don't have enough room try to free it up
                if self.free_space(new_size) {
                    // Start downloading the blob
                    let (finished, load_result) = watch::channel(None);
                    tokio::spawn({
                        let temp = NamedTempFile::new_in(&self.path)?;
                        let command = self.sender.upgrade().unwrap();
                        let storage = self.storage.clone();
                        let id = id.clone();
                        let path = temp.path().to_path_buf();

                        async move {
                            let result = storage.download(&id, path).await;
                            info!("Load Done");
                            match result {
                                Ok(_) => {
                                    let entry = CacheEntry{
                                        access_time: chrono::Utc::now(),
                                        handle: Arc::new(BlobHandle { file: temp }),
                                        size: new_size
                                    };
                                    _ = respond.send(Ok(entry.handle.clone()));
                                    _ = finished.send(Some(Ok(entry)));
                                },
                                Err(err) => {
                                    _ = finished.send(Some(Err(err.into())));
                                },
                            }
                            info!("Send cache notice");
                            _ = command.send(BlobCacheCommand::LoadFinished(id)).await;
                            finished.closed().await
                        }
                    });

                    self.pending.insert(id, load_result);
                    self.committed_capacity += new_size;

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
            BlobCacheCommand::LoadFinished(id) => {
                info!("LoadFinished");
                if let Some(entry) = self.pending.remove(&id) {
                    if let Some(entry) = &*entry.borrow() {
                        match entry {
                            Ok(entry) => {self.open.insert(id, entry.clone());},
                            Err(err) => {
                                error!("Error in blob load: {err}");
                            }
                        }
                    }
                }
            },
        }
        return Ok(())
    }

    fn free_space(&mut self, size: usize) -> bool {
        while self.capacity - self.committed_capacity < size {
            let mut candidate: Option<&BlobID> = None;
            let mut candidate_changed = chrono::Utc::now();
            for (id, value) in self.open.iter() {
                if Arc::<BlobHandle>::strong_count(&value.handle) > 1 {
                    continue;
                }
                if candidate.is_none() {
                    candidate = Some(id);
                    candidate_changed = value.access_time;
                } else if value.access_time < candidate_changed {
                    candidate = Some(id);
                    candidate_changed = value.access_time;
                }
            }

            if let Some(id) = candidate {
                if let Some(entry) = self.open.remove(&id.clone()) {
                    self.committed_capacity -= entry.size;
                }
            } else {
                return false;
            }
        }
        return true;
    }
}


#[cfg(test)]
mod test {
    use rand::Rng;

    use crate::storage::{connect, BlobStorageConfig, BlobID, BlobStorage};
    use super::BlobCache;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn access_files() {
        init();
        let storage_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let storage = connect(BlobStorageConfig::Directory { path: storage_dir.path().to_str().unwrap().to_string() }).await.unwrap();
        let cache = BlobCache::new(storage.clone(), cache_size, cache_dir.path().to_owned());

        let mut rng = rand::thread_rng();
        let sample_size = 128;
        let data: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();

        // Add some data
        let id = BlobID(uuid::Uuid::new_v4());
        storage.put(id.clone(), &data).await.unwrap();

        {
            let handle = cache.open(id.clone()).await.unwrap();
            assert_eq!(handle.read_all().await.unwrap(), data);
            assert_eq!(cache.capacity().await.unwrap(), (cache_size - sample_size, cache_size));
            cache.flush().await.unwrap();
        }

        assert_eq!(cache.current_open().await.unwrap(), vec![id]);
        assert_eq!(cache.capacity().await.unwrap(), (cache_size - sample_size, cache_size));
        cache.flush().await.unwrap();
        assert_eq!(cache.current_open().await.unwrap(), vec![]);
        assert_eq!(cache.capacity().await.unwrap(), (cache_size, cache_size));
    }

    #[tokio::test]
    async fn size_limit() {
        init();

        let storage_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let storage = connect(BlobStorageConfig::Directory { path: storage_dir.path().to_str().unwrap().to_string() }).await.unwrap();
        let cache = BlobCache::new(storage.clone(), cache_size, cache_dir.path().to_owned());

        {
            let mut rng = rand::thread_rng();
            let sample_size = cache_size + 1;
            let data: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();
            let id = BlobID(uuid::Uuid::new_v4());
            storage.put(id.clone(), &data).await.unwrap();
            assert_eq!(cache.open(id).await.unwrap_err().downcast::<crate::error::ErrorKinds>().unwrap(), crate::error::ErrorKinds::BlobTooLargeForCache)
        }


    }

    #[tokio::test]
    async fn space_reuse() {
        init();
        todo!()
    }
}