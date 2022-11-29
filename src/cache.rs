

use uuid::Uuid;
use anyhow::Result;
use log::{error, info};
use tempfile::NamedTempFile;
use tokio::sync::{mpsc, oneshot};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::Arc;


type HandleId = Uuid;


#[derive(Debug)]
struct FileHandleInner {
    id: HandleId,
    file: NamedTempFile,
    connection: mpsc::Sender<LocalCacheCommand>,
}


#[derive(Debug, Clone)]
pub struct FileHandle {
    data: Arc<FileHandleInner>
}

impl FileHandle {
    fn new(id: Uuid, file: NamedTempFile, connection: mpsc::Sender<LocalCacheCommand>) -> Self {
        Self {
            data: Arc::new(FileHandleInner { id, file, connection })
        }
    }

    pub fn id(&self) -> Uuid {
        return self.data.id;
    }

    pub fn path(&self) -> PathBuf {
        self.data.file.path().to_path_buf()
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

    pub async fn resize(&self, new_size: usize) -> Result<usize> {
        let (send, recv) = oneshot::channel();
        _ = self.data.connection.send(LocalCacheCommand::Resize(self.data.id, new_size, send)).await;
        return Ok(recv.await??)
    }

    pub async fn size_to_fit(&self) -> Result<usize> {
        let handle = self.open()?;
        let meta = tokio::task::spawn_blocking(move ||{
            handle.metadata()
        }).await??;
        self.resize(meta.len() as usize).await
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        if self.count() <= 2 {
            _ = self.data.connection.try_send(LocalCacheCommand::HandleDropped(self.data.id.clone()));
        }
    }
}


#[derive(Clone)]
struct CacheEntry {
    access_time: chrono::DateTime<chrono::Utc>,
    handle: FileHandle,
    size: usize,
}


enum LocalCacheCommand {
    List(oneshot::Sender<Vec<(HandleId, usize)>>),
    Open(usize, oneshot::Sender<Result<FileHandle>>),
    Resize(HandleId, usize, oneshot::Sender<Result<usize>>),
    Capacity(oneshot::Sender<(usize, usize)>),
    Flush(oneshot::Sender<()>),
    HandleDropped(HandleId),
}

#[derive(Clone)]
pub struct LocalCache {
    connection: mpsc::Sender<LocalCacheCommand>
}

impl LocalCache {

    pub fn new(capacity: usize, path: PathBuf) -> Self {
        let (daemon, send) = Inner::new(capacity, path);

        tokio::spawn(daemon.run());

        Self {
            connection: send
        }
    }

    pub async fn open(&self, size: usize) -> Result<FileHandle> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(LocalCacheCommand::Open(size, send)).await;
        Ok(recv.await??)
    }

    pub async fn current_open(&self) -> Result<Vec<(HandleId, usize)>> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(LocalCacheCommand::List(send)).await;
        Ok(recv.await?)
    }

    pub async fn capacity(&self) -> Result<(usize, usize)> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(LocalCacheCommand::Capacity(send)).await;
        Ok(recv.await?)
    }

    pub async fn flush(&self) -> Result<()> {
        let (send, recv) = oneshot::channel();
        _ = self.connection.send(LocalCacheCommand::Flush(send)).await;
        Ok(recv.await?)
    }
}


enum WaitingFile {
    Open(usize, oneshot::Sender<Result<FileHandle>>),
    Resize(HandleId, usize, usize, oneshot::Sender<Result<usize>>)
}

struct Inner {
    sender: mpsc::WeakSender<LocalCacheCommand>,
    connection: mpsc::Receiver<LocalCacheCommand>,
    committed_capacity: usize,
    capacity: usize,
    open: HashMap<HandleId, CacheEntry>,
    waiting_for_space: Vec<WaitingFile>,
    path: PathBuf,
}

impl Inner {

    fn new(capacity: usize, path: PathBuf) -> (Self, mpsc::Sender<LocalCacheCommand>) {
        let (sender, connection) = mpsc::channel(32);

        (Self {
            sender: sender.downgrade(),
            connection,
            committed_capacity: 0,
            capacity,
            open: Default::default(),
            waiting_for_space: Default::default(),
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

    async fn handle_process(&mut self, message: LocalCacheCommand) -> Result<()> {
        match message {
            LocalCacheCommand::List(respond) => {
                // info!("blob list {:?} {:?}", self.open.keys(), self.waiting_for_space.keys());
                let found: Vec<(HandleId, usize)> = self.open.iter().map(|(k, v)| (k.clone(), v.size)).collect();
                _ = respond.send(found);
            },
            LocalCacheCommand::Resize(id, new_size, respond) => {
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
                    self.waiting_for_space.push(WaitingFile::Resize(id, new_size, new_size - old_size, respond));
                }
            },

            LocalCacheCommand::Capacity(respond) => {
                _ = respond.send((self.capacity - self.committed_capacity, self.capacity));
            },
            LocalCacheCommand::Flush(respond) => {
                self.free_space(self.capacity);
                _ = respond.send(());
            },
            LocalCacheCommand::Open(size, respond) => {
                // Make sure the requested size is at least possible to satisfy
                if size > self.capacity {
                    _ = respond.send(Err(crate::error::ErrorKinds::BlobTooLargeForCache.into()));
                    return Ok(())
                }

                // if we don't have enough room try to free it up
                if self.free_space(size) {
                    self.do_open(respond, size)?;
                } else {
                    // Mark this as something we will need to download later
                    self.waiting_for_space.push(WaitingFile::Open(size, respond));
                }
            },
            LocalCacheCommand::HandleDropped(id) => {
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
            if let Some(item) = self.waiting_for_space.first() {
                let required = match item {
                    WaitingFile::Open(size, responder) => {
                        if responder.is_closed() {
                            info!("Abandoning reserve of {size}, no one is waiting for it");
                            self.waiting_for_space.remove(0);
                            continue;
                        }
                        *size
                    },
                    WaitingFile::Resize(_, _, add, responder) => {
                        if responder.is_closed() {
                            info!("Abandoning reserve of {add}, no one is waiting for it");
                            self.waiting_for_space.remove(0);
                            continue;
                        }
                        *add
                    },
                };

                if self.free_space(required) {
                    let item = self.waiting_for_space.remove(0);
                    match item {
                        WaitingFile::Open(size, responder) => {
                            self.do_open(responder, size)?;
                        },
                        WaitingFile::Resize(id, new_size, _, responder) => {
                            self.do_resize(id, new_size, responder)?;
                        },
                    }
                    continue;
                } else {
                    return Ok(())
                }
            }
        }
        return Ok(())
    }

    fn do_open(&mut self, respond: oneshot::Sender<Result<FileHandle>>, new_size: usize) -> Result<()> {
        let id = Uuid::new_v4();
        let sender = match self.sender.upgrade() {
            Some(sender) => sender,
            None => return Err(anyhow::format_err!("Cache disconnected.")),
        };

        let temp = NamedTempFile::new_in(&self.path)?;
        let handle = FileHandle::new(id.clone(), temp, sender);
        let entry = CacheEntry{
            access_time: chrono::Utc::now(),
            handle: handle.clone(),
            size: new_size
        };
        self.open.insert(id, entry);
        self.committed_capacity += new_size;
        _ = respond.send(Ok(handle));
        return Ok(())
    }

    fn do_resize(&mut self, id: Uuid, new_size: usize, respond: oneshot::Sender<Result<usize>>) -> Result<()> {
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

    fn free_space(&mut self, size: usize) -> bool {
        while self.capacity - self.committed_capacity < size {
            let mut candidate: Option<&HandleId> = None;
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
    use super::LocalCache;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn simple_file_cleanup() {
        init();

        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let cache = LocalCache::new(cache_size, cache_dir.path().to_owned());
        let sample_size = 128;

        // Add some data
        let id = {
            let handle = cache.open(sample_size).await.unwrap();
            assert_eq!(cache.capacity().await.unwrap(), (cache_size - sample_size, cache_size));
            cache.flush().await.unwrap();
            assert_eq!(cache.capacity().await.unwrap(), (cache_size - sample_size, cache_size));
            handle.id()
        };

        assert_eq!(cache.current_open().await.unwrap(), vec![(id, sample_size)]);
        assert_eq!(cache.capacity().await.unwrap(), (cache_size - sample_size, cache_size));
        cache.flush().await.unwrap();
        assert_eq!(cache.current_open().await.unwrap(), vec![]);
        assert_eq!(cache.capacity().await.unwrap(), (cache_size, cache_size));
    }

    #[tokio::test]
    async fn size_limit() {
        init();

        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let cache = LocalCache::new(cache_size, cache_dir.path().to_owned());

        let oversize = cache_size + 1;
        assert_eq!(cache.open(oversize).await.unwrap_err().downcast::<crate::error::ErrorKinds>().unwrap(), crate::error::ErrorKinds::BlobTooLargeForCache);
        assert_eq!(cache.capacity().await.unwrap(), (cache_size, cache_size));

        let handle = cache.open(cache_size/2).await.unwrap();
        assert_eq!(cache.capacity().await.unwrap(), (cache_size - cache_size/2, cache_size));

        handle.resize(cache_size).await.unwrap();
        assert_eq!(cache.capacity().await.unwrap(), (0, cache_size));
        assert_eq!(handle.resize(oversize).await.unwrap_err().downcast::<crate::error::ErrorKinds>().unwrap(), crate::error::ErrorKinds::BlobTooLargeForCache);
    }

    #[tokio::test]
    async fn space_reuse() {
        init();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let cache = LocalCache::new(cache_size, cache_dir.path().to_owned());
        let sample_size = 512;

        //
        let get1 = tokio::time::timeout(Duration::from_secs(1), cache.open(sample_size)).await.unwrap().unwrap();

        let get3 = {
            let get2 = tokio::time::timeout(Duration::from_secs(1), cache.open(sample_size)).await.unwrap().unwrap();

            let get3 = tokio::spawn({
                let cache = cache.clone();
                async move {
                    cache.open(sample_size).await
                }
            });

            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!get3.is_finished());

            assert_that!(cache.current_open().await.unwrap()).contains_exactly(vec![(get1.id(), sample_size), (get2.id(), sample_size)]);
            assert_eq!(cache.capacity().await.unwrap(), (0, cache_size));
            get3
        };

        // tokio::time::sleep(Duration::from_millis(50)).await;
        let get3 = get3.await.unwrap().unwrap();
        // assert_eq!(get3.read_all().await.unwrap(), data3);
        assert_that!(cache.current_open().await.unwrap()).contains_exactly(vec![(get1.id(), sample_size), (get3.id(), sample_size)]);
        assert_eq!(cache.capacity().await.unwrap(), (0, cache_size));
    }
}