use anyhow::Result;
use log::{error, info};
use tempfile::NamedTempFile;
use tokio::sync::{mpsc, oneshot, watch};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::BlobStorage;


#[derive(Debug)]
struct BlobHandleInner {
    id: String,
    file: NamedTempFile,
    cleanup_notification: mpsc::Sender<BlobCacheCommand>,
}


#[derive(Debug, Clone)]
pub struct BlobHandle {
    data: Arc<BlobHandleInner>
}

impl BlobHandle {
    fn new(id: String, file: NamedTempFile, cleanup_notification: mpsc::Sender<BlobCacheCommand>) -> Self {
        Self {
            data: Arc::new(BlobHandleInner { id, file, cleanup_notification })
        }
    }

    pub fn open(&self) -> Result<std::fs::File> {
        Ok(self.data.file.reopen()?)
    }

    pub async fn read_all(&self) -> Result<Vec<u8>> {
        Ok(tokio::fs::read(self.data.file.path()).await?)
    }

    pub fn count(&self) -> usize {
        Arc::strong_count(&self.data)
    }
}

impl Drop for BlobHandle {
    fn drop(&mut self) {
        if self.count() <= 2 {
            _ = self.data.cleanup_notification.try_send(BlobCacheCommand::HandleDropped(self.data.id.clone()));
        }
    }
}


#[derive(Clone)]
struct CacheEntry {
    access_time: chrono::DateTime<chrono::Utc>,
    handle: BlobHandle,
    size: usize,
}


enum BlobCacheCommand {
    ListBlobs(oneshot::Sender<Vec<String>>),
    Open(String, oneshot::Sender<Result<BlobHandle>>),
    Capacity(oneshot::Sender<(usize, usize)>),
    Flush(oneshot::Sender<()>),
    LoadFinished(String),
    HandleDropped(String),
}

#[derive(Clone)]
pub struct BlobCache {
    connection: mpsc::Sender<BlobCacheCommand>
}

impl BlobCache {

    pub fn new(storage: BlobStorage, capacity: usize, path: PathBuf) -> Self {
        let (daemon, send) = Inner::new(storage, capacity, path);

        tokio::spawn(daemon.run());

        Self {
            connection: send
        }
    }

    pub async fn open(&self, id: String) -> Result<BlobHandle> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(BlobCacheCommand::Open(id, send)).await;
        Ok(recv.await??)
    }

    pub async fn current_open(&self) -> Result<Vec<String>> {
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


struct Inner {
    sender: mpsc::WeakSender<BlobCacheCommand>,
    connection: mpsc::Receiver<BlobCacheCommand>,
    committed_capacity: usize,
    capacity: usize,
    open: HashMap<String, CacheEntry>,
    pending: HashMap<String, watch::Receiver<Option<Result<CacheEntry>>>>,
    waiting_for_space: Vec<(String, Vec<oneshot::Sender<Result<BlobHandle>>>)>,
    storage: BlobStorage,
    path: PathBuf,
}

impl Inner {

    fn new(storage: BlobStorage, capacity: usize, path: PathBuf) -> (Self, mpsc::Sender<BlobCacheCommand>) {
        let (sender, connection) = mpsc::channel(32);

        (Self {
            sender: sender.downgrade(),
            connection,
            committed_capacity: 0,
            capacity,
            open: Default::default(),
            pending: Default::default(),
            waiting_for_space: Default::default(),
            storage,
            path,
        }, sender)
    }

    async fn run(mut self) {
        loop {
            let message = match self.connection.recv().await {
                Some(message) => message,
                None => break,
            };

            if let Err(err) = self.handle_process(message).await {
                error!("Error in BlobCache: {err}")
            }
        }
    }

    async fn handle_process(&mut self, message: BlobCacheCommand) -> Result<()> {
        match message {
            BlobCacheCommand::ListBlobs(respond) => {
                info!("blob list {:?} {:?}", self.open.keys(), self.pending.keys());
                let mut found: Vec<String> = self.open.keys().cloned().collect();
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
                let new_size = match self.storage.size(id.as_str()).await? {
                    Some(size) => size,
                    None => return Ok(()),
                };

                if new_size > self.capacity {
                    _ = respond.send(Err(crate::error::ErrorKinds::BlobTooLargeForCache.into()));
                    return Ok(())
                }

                // if we don't have enough room try to free it up
                if self.free_space(new_size) {
                    self.insert(id, vec![respond], new_size)?;
                } else {
                    // Mark this as something we will need to download later
                    for (entry_id, responders) in self.waiting_for_space.iter_mut() {
                        if entry_id == &id {
                            responders.push(respond);
                            return Ok(())
                        }
                    }
                    self.waiting_for_space.push((id, vec![respond]));
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
            BlobCacheCommand::HandleDropped(id) => {
                info!("handle dropped");
                if self.waiting_for_space.len() > 0 {
                    if let Entry::Occupied(entry) = self.open.entry(id) {
                        if entry.get().handle.count() == 1 {
                            self.committed_capacity -= entry.get().size;
                            entry.remove();
                            self.check_blocked().await?;
                        }
                    }
                }
            },
        }
        return Ok(())
    }

    async fn check_blocked(&mut self) -> Result<()> {
        while !self.waiting_for_space.is_empty() {
            if let Some((id, responders)) = self.waiting_for_space.first_mut() {
                for index in (0..responders.len()).rev() {
                    if responders[index].is_closed() {
                        responders.swap_remove(index);
                    }
                }
                if responders.is_empty() {
                    info!("Abandoning load of {id:?}, no one is waiting for it");
                    self.waiting_for_space.remove(0);
                    continue;
                }
                let new_size = match self.storage.size(id.as_str()).await? {
                    Some(size) => size,
                    None => {
                        info!("Abandoning load of {id:?}, no one is waiting for it");
                        self.waiting_for_space.remove(0);
                        continue;
                    },
                };
                if self.free_space(new_size) {
                    let (id, responders) = self.waiting_for_space.remove(0);
                    self.insert(id, responders, new_size)?;
                    continue;
                } else {
                    return Ok(())
                }
            }
        }
        return Ok(())
    }

    fn insert(&mut self, id: String, responders: Vec<oneshot::Sender<Result<BlobHandle>>>, new_size: usize) -> Result<()> {
        // Start downloading the blob
        let (finished, load_result) = watch::channel(None);
        tokio::spawn({
            let temp = NamedTempFile::new_in(&self.path)?;
            let command = self.sender.upgrade().unwrap();
            let storage = self.storage.clone();
            let id = id.clone();
            let path = temp.path().to_path_buf();

            async move {
                let result = storage.download(id.as_str(), path).await;
                info!("Load Done");
                match result {
                    Ok(_) => {
                        let entry = CacheEntry{
                            access_time: chrono::Utc::now(),
                            handle: BlobHandle::new(id.clone(), temp, command.clone()),
                            size: new_size
                        };
                        for respond in responders {
                            _ = respond.send(Ok(entry.handle.clone()));
                        }
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
        return Ok(())
    }

    fn free_space(&mut self, size: usize) -> bool {
        while self.capacity - self.committed_capacity < size {
            let mut candidate: Option<&String> = None;
            let mut candidate_changed = chrono::Utc::now();
            for (id, value) in self.open.iter() {
                if value.handle.count() > 1 {
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
    use std::collections::HashSet;
    use std::time::Duration;

    use assertor::{assert_that, VecAssertion};
    use rand::Rng;

    use crate::storage::{connect, BlobStorageConfig, BlobStorage};
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
        let id = uuid::Uuid::new_v4().to_string();
        storage.put(id.as_str(), &data).await.unwrap();

        {
            let handle = cache.open(id.clone()).await.unwrap();
            assert_eq!(handle.read_all().await.unwrap(), data);
            assert_eq!(cache.capacity().await.unwrap(), (cache_size - sample_size, cache_size));
            cache.flush().await.unwrap();
            assert_eq!(handle.read_all().await.unwrap(), data);
            assert_eq!(cache.capacity().await.unwrap(), (cache_size - sample_size, cache_size));
        }

        assert_eq!(cache.current_open().await.unwrap(), vec![id]);
        assert_eq!(cache.capacity().await.unwrap(), (cache_size - sample_size, cache_size));
        cache.flush().await.unwrap();
        assert!(cache.current_open().await.unwrap().is_empty());
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
            let id = uuid::Uuid::new_v4().to_string();
            storage.put(id.as_str(), &data).await.unwrap();
            assert_eq!(cache.open(id).await.unwrap_err().downcast::<crate::error::ErrorKinds>().unwrap(), crate::error::ErrorKinds::BlobTooLargeForCache)
        }


    }

    #[tokio::test]
    async fn space_reuse() {
        init();
        let storage_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let storage = connect(BlobStorageConfig::Directory { path: storage_dir.path().to_str().unwrap().to_string() }).await.unwrap();
        let cache = BlobCache::new(storage.clone(), cache_size, cache_dir.path().to_owned());

        let mut rng = rand::thread_rng();
        let sample_size = 512;


        // Add some data
        let id1 = uuid::Uuid::new_v4().to_string();
        let data1: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();
        storage.put(id1.as_str(), &data1).await.unwrap();
        let id2 = uuid::Uuid::new_v4().to_string();
        let data2: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();
        storage.put(id2.as_str(), &data2).await.unwrap();
        let id3 = uuid::Uuid::new_v4().to_string();
        let data3: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();
        storage.put(id3.as_str(), &data3).await.unwrap();

        //
        let get1 = tokio::time::timeout(Duration::from_secs(1), cache.open(id1.clone())).await.unwrap().unwrap();
        assert_eq!(get1.read_all().await.unwrap(), data1);

        let get3 = {
            let get2 = tokio::time::timeout(Duration::from_secs(1), cache.open(id2.clone())).await.unwrap().unwrap();
            assert_eq!(get2.read_all().await.unwrap(), data2);

            let get3 = tokio::spawn({
                let cache = cache.clone();
                let id3 = id3.clone();
                async move {
                    cache.open(id3.clone()).await
                }
            });
            assert!(tokio::time::timeout(Duration::from_millis(100), cache.open(id3.clone())).await.is_err());

            assert_that!(cache.current_open().await.unwrap()).contains_exactly(vec![id1.clone(), id2]);
            assert_eq!(cache.capacity().await.unwrap(), (0, cache_size));
            get3
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        let get3 = get3.await.unwrap().unwrap();
        assert_eq!(get3.read_all().await.unwrap(), data3);
        assert_that!(cache.current_open().await.unwrap()).contains_exactly(vec![id1, id3]);
        assert_eq!(cache.capacity().await.unwrap(), (0, cache_size));
    }
}