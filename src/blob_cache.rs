use anyhow::Result;
use log::{error, info};
use tempfile::NamedTempFile;
use tokio::sync::{mpsc, oneshot, watch};
use std::collections::{HashMap, VecDeque};
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::BlobStorage;


#[derive(Debug)]
struct BlobHandleInner {
    id: String,
    file: NamedTempFile,
    connection: mpsc::Sender<BlobCacheCommand>,
}

#[clippy::has_significant_drop]
#[derive(Debug, Clone)]
pub struct BlobHandle {
    data: Arc<BlobHandleInner>
}

impl BlobHandle {
    fn new(id: String, file: NamedTempFile, connection: mpsc::Sender<BlobCacheCommand>) -> Self {
        Self {
            data: Arc::new(BlobHandleInner { id, file, connection })
        }
    }

    pub fn path(&self) -> PathBuf {
        self.data.file.path().to_owned()
    }

    pub fn id(&self) -> &str {
        &self.data.id
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

    pub async fn resize(&self, new_size: u64) -> Result<u64> {
        let (send, recv) = oneshot::channel();
        _ = self.data.connection.send(BlobCacheCommand::Resize(self.data.id.clone(), new_size, send)).await;
        return Ok(recv.await??)
    }

    pub async fn size_to_fit(&self) -> Result<u64> {
        self.resize(self.size().await?).await
    }

    pub async fn size(&self) -> Result<u64> {
        let handle = self.open()?;
        let meta = tokio::task::spawn_blocking(move ||{
            handle.metadata()
        }).await??;
        return Ok(meta.len())
    }
}

impl Drop for BlobHandle {
    fn drop(&mut self) {
        if self.count() <= 2 {
            _ = self.data.connection.try_send(BlobCacheCommand::HandleDropped(self.data.id.clone()));
        }
    }
}


#[derive(Clone)]
struct CacheEntry {
    access_time: chrono::DateTime<chrono::Utc>,
    handle: BlobHandle,
    size: u64,
}


enum BlobCacheCommand {
    ListBlobs(oneshot::Sender<Vec<String>>),
    Open(String, oneshot::Sender<Result<BlobHandle>>),
    OpenNew(String, u64, oneshot::Sender<Result<BlobHandle>>),
    Resize(String, u64, oneshot::Sender<Result<u64>>),
    Capacity(oneshot::Sender<(u64, u64)>),
    Flush(oneshot::Sender<()>),
    LoadFinished(String),
    HandleDropped(String),
}

#[derive(Clone)]
pub struct BlobCache {
    connection: mpsc::Sender<BlobCacheCommand>
}

impl BlobCache {

    pub fn new(storage: BlobStorage, capacity: u64, path: PathBuf) -> Self {
        let (daemon, send) = Inner::new(storage, capacity, path);

        tokio::spawn(async {
            if let Err(err) = tokio::spawn(daemon.run()).await {
                error!("Panic in blob cache: {err}");
            }
        });

        Self {
            connection: send
        }
    }

    pub async fn open(&self, id: String) -> Result<BlobHandle> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(BlobCacheCommand::Open(id, send)).await;
        Ok(recv.await??)
    }

    pub async fn open_new(&self, id: String, size: u64) -> Result<BlobHandle> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(BlobCacheCommand::OpenNew(id, size, send)).await;
        Ok(recv.await??)
    }

    pub async fn current_open(&self) -> Result<Vec<String>> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(BlobCacheCommand::ListBlobs(send)).await;
        Ok(recv.await?)
    }

    pub async fn capacity(&self) -> Result<(u64, u64)> {
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
    committed_capacity: u64,
    capacity: u64,
    open: HashMap<String, CacheEntry>,
    pending: HashMap<String, watch::Receiver<Option<Result<CacheEntry>>>>,
    waiting_for_space: VecDeque<WaitingForSpace>,
    storage: BlobStorage,
    path: PathBuf,
}

struct WaitingForSpace {
    pub id: String,
    pub additional_local_bytes: u64,
    pub responders: Vec<oneshot::Sender<Result<BlobHandle>>>,
    pub act: WaitingOperation,
}

enum WaitingOperation {
    Open,
    OpenNew,
    Resize{
        new_size: u64,
        responders: oneshot::Sender<Result<u64>>
    }
}

impl Inner {

    fn new(storage: BlobStorage, capacity: u64, path: PathBuf) -> (Self, mpsc::Sender<BlobCacheCommand>) {
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
                    self.start_download(id, vec![respond], new_size)?;
                } else {
                    // Mark this as something we will need to download later
                    for wait in self.waiting_for_space.iter_mut() {
                        if wait.id == id {
                            wait.responders.push(respond);
                            return Ok(())
                        }
                    }
                    self.waiting_for_space.push_back(WaitingForSpace{
                        id,
                        responders: vec![respond],
                        additional_local_bytes: new_size,
                        act: WaitingOperation::Open,
                    });
                }
            },
            BlobCacheCommand::OpenNew(id, size, respond) => {
                // If the blob is already open, attach to that
                if let Some(entry) = self.open.get_mut(&id) {
                    entry.access_time = chrono::Utc::now();
                    _ = respond.send(Ok(entry.handle.clone()));
                    return Ok(())
                }

                if size > self.capacity {
                    _ = respond.send(Err(crate::error::ErrorKinds::BlobTooLargeForCache.into()));
                    return Ok(())
                }

                // if we don't have enough room try to free it up
                if self.free_space(size) {
                    self.open_empty(id, vec![respond], size)?;
                } else {
                    // Mark this as something we will need to download later
                    for wait in self.waiting_for_space.iter_mut() {
                        if wait.id == id {
                            wait.responders.push(respond);
                            return Ok(())
                        }
                    }
                    self.waiting_for_space.push_back(WaitingForSpace {
                        id,
                        additional_local_bytes: size,
                        responders: vec![respond],
                        act: WaitingOperation::OpenNew
                    });
                }
            },
            BlobCacheCommand::Resize(id, new_size, respond) => {
                // Make sure the requested size is at least possible to satisfy
                if new_size > self.capacity {
                    _ = respond.send(Err(crate::error::ErrorKinds::BlobTooLargeForCache.into()));
                    return Ok(())
                }

                let old_size = {
                    let handle = match self.open.get_mut(&id) {
                        Some(handle) => handle,
                        None => {
                            _ = respond.send(Err(anyhow::anyhow!("Bad handle")));
                            return Ok(())
                        },
                    };

                    if new_size <= handle.size {
                        let freed = handle.size - new_size;
                        self.committed_capacity -= freed;
                        handle.size = new_size;
                        _ = respond.send(Ok(new_size));
                        return Ok(())
                    }

                    handle.size
                };

                // if we don't have enough room try to free it up
                if self.free_space(new_size - old_size) {
                    self.do_resize(id, new_size, respond)?;
                } else {
                    // Mark this as something we will need to download later
                    self.waiting_for_space.push_back(WaitingForSpace {
                        id,
                        additional_local_bytes: new_size - old_size,
                        responders: vec![],
                        act: WaitingOperation::Resize {
                            new_size,
                            responders: respond
                        }
                    });
                }
            }
            BlobCacheCommand::LoadFinished(id) => {
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
            if let Some(mut waiting) = self.waiting_for_space.pop_front() {
                waiting.responders.retain(|resp|!resp.is_closed());
                if waiting.responders.is_empty() {
                    info!("Abandoning load of {}, no one is waiting for it", waiting.id);
                    continue;
                }
                if self.free_space(waiting.additional_local_bytes) {
                    match waiting.act {
                        WaitingOperation::Open => {
                            self.start_download(waiting.id, waiting.responders, waiting.additional_local_bytes)?;
                        },
                        WaitingOperation::OpenNew => {
                            self.open_empty(waiting.id, waiting.responders, waiting.additional_local_bytes)?;
                        },
                        WaitingOperation::Resize { new_size, responders } => {
                            self.do_resize(waiting.id, new_size, responders)?;
                        },
                    };
                    continue;
                } else {
                    self.waiting_for_space.push_front(waiting);
                    return Ok(())
                }
            }
        }
        return Ok(())
    }

    fn start_download(&mut self, id: String, responders: Vec<oneshot::Sender<Result<BlobHandle>>>, new_size: u64) -> Result<()> {
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
                _ = command.send(BlobCacheCommand::LoadFinished(id)).await;
                finished.closed().await
            }
        });

        self.pending.insert(id, load_result);
        self.committed_capacity += new_size;
        return Ok(())
    }

    fn do_resize(&mut self, id: String, new_size: u64, respond: oneshot::Sender<Result<u64>>) -> Result<()> {
        let handle = match self.open.get_mut(&id) {
            Some(handle) => handle,
            None => {
                _ = respond.send(Err(anyhow::anyhow!("Bad handle")));
                return Ok(())
            },
        };

        self.committed_capacity += new_size - handle.size;
        handle.size = new_size;
        _ = respond.send(Ok(new_size));
        return Ok(())
    }

    fn open_empty(&mut self, id: String, responses: Vec<oneshot::Sender<Result<BlobHandle>>>, new_size: u64) -> Result<()> {
        let sender = match self.sender.upgrade() {
            Some(sender) => sender,
            None => return Err(anyhow::format_err!("Cache disconnected.")),
        };

        let temp = NamedTempFile::new_in(&self.path)?;
        let handle = BlobHandle::new(id.clone(), temp, sender);
        let entry = CacheEntry{
            access_time: chrono::Utc::now(),
            handle: handle.clone(),
            size: new_size
        };
        self.open.insert(id, entry);
        self.committed_capacity += new_size;
        for respond in responses {
            _ = respond.send(Ok(handle.clone()));
        }
        return Ok(())
    }

    fn free_space(&mut self, size: u64) -> bool {
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
    use std::time::Duration;

    use assertor::{assert_that, VecAssertion};
    use rand::Rng;

    use crate::storage::{connect, BlobStorageConfig};
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
        let storage = connect(BlobStorageConfig::Directory {
            path: storage_dir.path().to_owned(),
            size: 100 << 30
        }).await.unwrap();
        let cache = BlobCache::new(storage.clone(), cache_size, cache_dir.path().to_owned());

        let mut rng = rand::thread_rng();
        let sample_size = 128;
        let data: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();

        // Add some data
        let id = uuid::Uuid::new_v4().to_string();
        storage.put(id.as_str(), data.clone()).await.unwrap();

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
        let storage = connect(BlobStorageConfig::Directory { path: storage_dir.path().to_owned(), size: 1 << 30 }).await.unwrap();
        let cache = BlobCache::new(storage.clone(), cache_size, cache_dir.path().to_owned());

        {
            let mut rng = rand::thread_rng();
            let sample_size = cache_size + 1;
            let data: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();
            let id = uuid::Uuid::new_v4().to_string();
            storage.put(id.as_str(), data).await.unwrap();
            assert_eq!(cache.open(id).await.unwrap_err().downcast::<crate::error::ErrorKinds>().unwrap(), crate::error::ErrorKinds::BlobTooLargeForCache)
        }


    }

    #[tokio::test]
    async fn space_reuse() {
        init();
        let storage_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let storage = connect(BlobStorageConfig::Directory { path: storage_dir.path().to_owned(), size: 1 << 30 }).await.unwrap();
        let cache = BlobCache::new(storage.clone(), cache_size, cache_dir.path().to_owned());

        let mut rng = rand::thread_rng();
        let sample_size = 512;


        // Add some data
        let id1 = uuid::Uuid::new_v4().to_string();
        let data1: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();
        storage.put(id1.as_str(), data1.clone()).await.unwrap();
        let id2 = uuid::Uuid::new_v4().to_string();
        let data2: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();
        storage.put(id2.as_str(), data2.clone()).await.unwrap();
        let id3 = uuid::Uuid::new_v4().to_string();
        let data3: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();
        storage.put(id3.as_str(), data3.clone()).await.unwrap();

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