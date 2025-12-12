use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tempfile::NamedTempFile;

use tokio::sync::Notify;
use parking_lot::Mutex as BlockingMutex;
use tokio::sync::Mutex as AsyncMutex;

use crate::storage::BlobStorage;

#[derive(Clone)]
pub struct BlobCache {
    data: Arc<BlockingMutex<BlobCacheInner>>,
}

struct BlobCacheInner {
    open_files: HashMap<String, Arc<BlobHandle>>,
    loading_files: HashMap<String, Arc<AsyncMutex<LoadingHandle>>>,
    errors: HashMap<String, String>,
    total_space: u64,
    free_space: u64,
    storage_change: Arc<tokio::sync::Notify>,
    requesting_space: Arc<tokio::sync::Mutex<()>>,
    storage: BlobStorage,
    directory: PathBuf,
}

#[derive(Debug)]
pub struct BlobHandle {
    label: String,
    file: NamedTempFile,
    space_token: StorageToken,
    loaded: chrono::DateTime<chrono::Utc>
}

impl BlobHandle {
    #[cfg(test)]
    pub async fn read_all(&self) -> Result<Vec<u8>> {
        Ok(tokio::fs::read(self.file.path()).await?)
    }

    pub fn id(&self) -> &str {
        &self.label
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }
}

struct LoadingHandle {
    task: Option<tokio::task::JoinHandle<Result<Arc<BlobHandle>>>>,
}

impl BlobCache {
    pub fn new(storage: BlobStorage, capacity: u64, path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&path).context(format!("Failed to create cache directory: {path:?}"))?;

        // Leave some slack in capacity for delay on operations
        let capacity = capacity - (1 << 30).min(capacity / 10);

        Ok(Self {
            data: Arc::new(BlockingMutex::new(BlobCacheInner { 
                open_files: Default::default(), 
                loading_files: Default::default(),
                errors: Default::default(),
                total_space: capacity,
                free_space: capacity,
                storage_change: Arc::new(Notify::new()),
                requesting_space: Arc::new(tokio::sync::Mutex::new(())),
                directory: path,
                storage,
            })),
        })
    }

    #[cfg(test)]
    pub fn capacity(&self) -> (u64, u64) {
        let inner = self.data.lock();
        (inner.free_space, inner.total_space)
    }   

    pub async fn open(&self, label: String) -> Result<Arc<BlobHandle>> {
        loop {
            let loading = {
                // check if the file has already been opened
                let mut inner = self.data.lock();
                if let Some(err) = inner.errors.get(&label) {
                    return Err(anyhow::anyhow!("Failed to load file: {err}"))
                }
                if let Some(handle) = inner.open_files.get(&label) {
                    return Ok(handle.clone())
                }
                match inner.loading_files.entry(label.clone()) {
                    // join waiting if it is already being opened
                    std::collections::hash_map::Entry::Occupied(occupied_entry) => {
                        occupied_entry.get().clone()
                    },
                    // if the file hasn't already been opened start opening it
                    std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                        let handle = LoadingHandle::start(self.clone(), label.clone());
                        vacant_entry.insert(handle).clone()
                    },
                }
            };

            // check if the file is already done
            let mut locked_loading: tokio::sync::MutexGuard<'_, LoadingHandle> = loading.lock().await;
            match &mut locked_loading.task {
                // seems to be already done, loop back and look again
                None => {
                    tokio::task::yield_now().await;
                    continue
                }, 
                // task hasn't been collected yet, wait for it to finish.
                Some(task) => {
                    match task.await {
                        Ok(Ok(blob)) => {
                            let mut inner = self.data.lock();
                            inner.loading_files.remove(&label);
                            inner.open_files.insert(label, blob.clone());
                            return Ok(blob)
                        },
                        Ok(Err(err)) => {
                            let mut inner = self.data.lock();
                            inner.loading_files.remove(&label);
                            inner.errors.insert(label, format!("{err:?}"));
                            return Err(anyhow::anyhow!("Failed to load file: {err:?}"))
                        }
                        Err(err) => {
                            let mut inner = self.data.lock();
                            inner.loading_files.remove(&label);
                            inner.errors.insert(label, format!("TaskError: {err:?}"));
                            return Err(anyhow::anyhow!("Failed to load file: {err:?}"))
                        },
                    }
                },
            }
        }
    }

    async fn release_space(&self, size: u64) -> bool {
        let mut inner = self.data.lock();
        
        // loop through held files collecting any that aren't in use, release them starting from the oldest
        let mut can_free = vec![];
        for (label, blob) in inner.open_files.iter() {
            if Arc::strong_count(blob) == 1 {
                can_free.push((blob.loaded, label.clone()));
            }
        }
        can_free.sort_unstable_by(|a, b| b.0.cmp(&a.0));

        let mut free_space = inner.free_space;
        let mut dropped = vec![];
        while free_space < size {
            match can_free.pop() {
                Some((_, label)) => {
                    if let Some(blob) = inner.open_files.remove(&label) {
                        free_space += blob.space_token.value;
                        dropped.push(blob);
                    }
                },
                None => {
                    drop(inner);
                    return false
                },
            }
        }
        // force the lock to be released before we drop other variables (here and the return false before)
        // This is because the drop in the blob handles will try and take this lock and deadlock
        // if we are still holding it. 
        drop(inner); 
        return true
    }

    #[cfg(test)]
    pub async fn flush(&self) {
        self.release_space(u64::MAX).await;
    }

    #[cfg(test)]
    pub fn current_open(&self) -> Vec<String> {
        let inner = self.data.lock();
        inner.open_files.keys().cloned().collect()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum OpenError {
    #[error("Requested file too large for local cache ({size} > {total_storage}): {label}")]
    TooLarge {
        size: u64,
        total_storage: u64,
        label: String
    }
}

impl LoadingHandle {
    fn start(host: BlobCache, label: String) -> Arc<AsyncMutex<Self>> {
        let handle = tokio::spawn(async move {
            Self::load_file(host, label).await
        });

        Arc::new(AsyncMutex::new(Self { task: Some(handle) }))
    }

    async fn load_file(host: BlobCache, label: String) -> Result<Arc<BlobHandle>> {
        // get some resources from the cache 
        let (storage, total_storage, storage_path) = {
            let inner = host.data.lock();
            (inner.storage.clone(), inner.total_space, inner.directory.clone())
        };
        
        // figure out how much space we need
        let size = storage.size(&label).await?.ok_or_else(|| anyhow::anyhow!("File does not exist: {label}"))?;
        if size > total_storage {
            return Err(OpenError::TooLarge { size, total_storage, label }.into())
        }

        // reserve space for the file
        let token = Self::reserve_space(&host, size).await;
        
        // start downloading the file
        let file = NamedTempFile::new_in(storage_path).context("During file creation")?;
        storage.download(&label, file.path().to_path_buf()).await.context("During download")?;

        Ok(Arc::new(BlobHandle {
            label,
            file,
            space_token: token,
            loaded: chrono::Utc::now()
        }))
    }

    async fn reserve_space(host: &BlobCache, size: u64) -> StorageToken {
        let (space_lock, storage_notice) = {
            let inner = host.data.lock();
            (inner.requesting_space.clone(), inner.storage_change.clone())
        };

        // only one loading process should be _taking_ space at a time, get the lock on it
        let _guard = space_lock.lock().await;

        // wait until the storage space we need is free
        loop {
            // check if it is already free
            {
                let mut inner = host.data.lock();
                if inner.free_space >= size {
                    inner.free_space -= size;
                    break StorageToken {
                        value: size,
                        host: host.clone()
                    }
                }
            }

            // try to find space
            if !host.release_space(size).await {
                // wait for a change in storage condition
                _ = tokio::time::timeout(Duration::from_secs(5), storage_notice.notified()).await;
            }
        }        
    }
}

struct StorageToken {
    value: u64,
    host: BlobCache
}

impl std::fmt::Debug for StorageToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageToken").field("value", &self.value).finish()
    }
}

impl Drop for StorageToken {
    fn drop(&mut self) {
        let mut inner = self.host.data.lock();
        inner.free_space += self.value;
        inner.storage_change.notify_one();
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
        let _ = env_logger::builder().filter_level(log::LevelFilter::Debug).is_test(true).try_init();
    }

    #[tokio::test]
    async fn access_files() {
        init();
        let storage_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let storage = connect(&BlobStorageConfig::Directory {
            path: storage_dir.path().to_owned(),
        }).await.unwrap();
        let cache = BlobCache::new(storage.clone(), cache_size, cache_dir.path().to_owned()).unwrap();

        let mut rng = rand::thread_rng();
        let sample_size = 128;
        let data: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();

        // Add some data
        let id = uuid::Uuid::new_v4().to_string();
        storage.put(id.as_str(), data.clone()).await.unwrap();
        assert_eq!(storage.get(id.as_str()).await.unwrap(), data);

        {
            let handle = cache.open(id.clone()).await.unwrap();
            assert_eq!(handle.read_all().await.unwrap(), data);
            assert_eq!(cache.capacity(), (cache_size - sample_size, cache_size));
            cache.flush().await;
            assert_eq!(handle.read_all().await.unwrap(), data);
            assert_eq!(cache.capacity(), (cache_size - sample_size, cache_size));
        }

        assert_eq!(cache.current_open(), vec![id]);
        assert_eq!(cache.capacity(), (cache_size - sample_size, cache_size));
        cache.flush().await;
        assert!(cache.current_open().is_empty());
        assert_eq!(cache.capacity(), (cache_size, cache_size));
    }

    #[tokio::test]
    async fn size_limit() {
        init();

        let storage_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let storage = connect(&BlobStorageConfig::Directory { path: storage_dir.path().to_owned() }).await.unwrap();
        let cache = BlobCache::new(storage.clone(), cache_size, cache_dir.path().to_owned()).unwrap();

        {
            let mut rng = rand::thread_rng();
            let sample_size = cache_size + 1;
            let data: Vec<u8> = (0..sample_size).map(|_| rng.gen()).collect();
            let id = uuid::Uuid::new_v4().to_string();
            storage.put(id.as_str(), data).await.unwrap();
            assert!(cache.open(id.clone()).await.unwrap_err().to_string().contains("Requested file too large"))
        }


    }

    #[tokio::test]
    async fn space_reuse() {
        init();
        println!("setup environment");
        let storage_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache_size = 1024;
        let storage = connect(&BlobStorageConfig::Directory { path: storage_dir.path().to_owned() }).await.unwrap();
        let cache = BlobCache::new(storage.clone(), cache_size, cache_dir.path().to_owned()).unwrap();

        let mut rng = rand::thread_rng();
        let sample_size = 512;

        // Add some data
        println!("insert data");
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
        println!("open {id1}");
        let get1 = tokio::time::timeout(Duration::from_secs(1), cache.open(id1.clone())).await.unwrap().unwrap();
        assert_eq!(get1.read_all().await.unwrap(), data1);
        println!("open {id1} finished");

        let get3 = {
            println!("open {id2}");
            let get2 = tokio::time::timeout(Duration::from_secs(1), cache.open(id2.clone())).await.unwrap().unwrap();
            assert_eq!(get2.read_all().await.unwrap(), data2);
            println!("open {id2} finished");

            let get3 = tokio::spawn({
                let cache = cache.clone();
                let id3 = id3.clone();
                async move {
                    println!("open {id3}");
                    cache.open(id3.clone()).await
                }
            });
            println!("open {id3} again expecting timeout");
            assert!(tokio::time::timeout(Duration::from_millis(100), cache.open(id3.clone())).await.is_err());
            println!("open {id3} again did timeout");

            assert_that!(cache.current_open()).contains_exactly(vec![id1.clone(), id2]);
            assert_eq!(cache.capacity(), (0, cache_size));
            get3
        };
        println!("other opens finished");

        tokio::time::sleep(Duration::from_millis(50)).await;
        let get3 = get3.await.unwrap().unwrap();
        println!("open {id3} finish");
        assert_eq!(get3.read_all().await.unwrap(), data3);
        assert_that!(cache.current_open()).contains_exactly(vec![id1, id3]);
        assert_eq!(cache.capacity(), (0, cache_size));
    }
}