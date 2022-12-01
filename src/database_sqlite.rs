use anyhow::Result;
use log::{error, info, debug};
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;
use crate::database::{IndexGroup, BlobID, IndexID};
use crate::sqlite_kv::{SKV, Collection};


pub struct SQLiteInterface {
    kv: SKV,
    index_soft_max: u64,
    _temp_dir: Option<tempfile::TempDir>,
}

#[derive(Serialize, Deserialize)]
struct IndexEntry {
    group: IndexGroup,
    label: IndexID,
    current_blob: BlobID,
    size_bytes: u64,
    size_entries: u64
}

impl IndexEntry {
    fn prepare_key(group: &IndexGroup, label: &IndexID) -> String {
        format!("{}:{}", group.as_str(), label.as_str())
    }

    fn key(&self) -> String {
        IndexEntry::prepare_key(&self.group, &self.label)
    }
}

#[derive(Serialize, Deserialize)]
struct FileEntry<'a> {
    access: AccessControl,
    hash: &'a [u8]
}


impl SQLiteInterface {
    pub async fn new(index_soft_max: u64, path: &str) -> Result<Self> {
        Ok(Self {
            kv: SKV::open(path).await?,
            index_soft_max,
            _temp_dir: None,
        })
    }

    pub async fn new_temp(index_soft_max: u64) -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let path = tempdir.path().join("house.db");

        let path = if path.is_absolute() {
            format!("//{}?mode=rwc", path.to_str().unwrap())
        } else {
            format!("{}?mode=rwc", path.to_str().unwrap())
        };

        Ok(Self {
            kv: SKV::open(&path).await?,
            index_soft_max,
            _temp_dir: Some(tempdir),
        })
    }

    pub async fn open_garbage_column(&self) -> Result<Collection> {
        Ok(self.kv.create_collection("garbage_blobs").await?)
    }

    pub async fn open_directory_column(&self) -> Result<Collection> {
        Ok(self.kv.create_collection("index_index").await?)
    }

    pub async fn open_index_column(&self, name: &IndexID) -> Result<Option<Collection>> {
        Ok(self.kv.open_collection(&format!("index_{}", name.as_str())).await?)
    }

    pub async fn create_index_column(&self, name: &IndexID) -> Result<Collection> {
        Ok(self.kv.create_collection(&format!("index_{}", name.as_str())).await?)
    }

    pub async fn update_file_access(&self, hash: &[u8], access: &AccessControl, new_index_group: &IndexGroup) -> Result<bool> {
        // Get all the groups that expire later than this one
        let mut index_index = self.open_directory_column().await?;
        let mut list = index_index.list_inclusive_range(new_index_group.as_bytes(), IndexGroup::max().as_bytes()).await?;

        // Run from oldest to newest that fit
        list.sort_by(|a, b|b.cmp(a));

        for (key, _value) in list {
            let key = std::str::from_utf8(&key)?;

            // Check if the index has the hash being update
            let (_index_group, index_name) = key.split_once(":").unwrap();
            if let Some(mut col) = self.open_index_column(&IndexID::from(index_name)).await? {
                let hash_index = if let Some(slice) = col.get(hash).await? {
                    slice
                } else {
                    continue;
                };

                loop {
                    // Check if the access is correct
                    if let Some(buffer) = col.get(&hash_index).await? {
                        let old: FileEntry = postcard::from_bytes(&buffer)?;

                        let entry = FileEntry{
                            access: access.or(&old.access).simplify(),
                            hash,
                        };

                        if col.cas(&hash_index, Some(&buffer), &postcard::to_allocvec(&entry)?).await? {
                            return Ok(true)
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        return Ok(false)
    }

    pub async fn select_index_to_grow(&self, index_group: &IndexGroup) -> Result<Option<(IndexID, BlobID, u64)>> {
        let mut index_index = self.open_directory_column().await?;
        let mut best: Option<IndexEntry> = None;

        let list = index_index.list_inclusive_range(index_group.as_bytes(), index_group.as_bytes()).await?;
        debug!("selecting index to expand from {} options", list.len());

        for (_key, value) in list {
            let value: IndexEntry = match postcard::from_bytes(&value) {
                Ok(value) => value,
                Err(err) => {
                    error!("Corrupted index entry: {err}");
                    continue
                },
            };

            if value.size_bytes >= self.index_soft_max {
                debug!("index {} chunk over size {}/{} skipping", value.label, value.size_bytes, self.index_soft_max);
                continue;
            }

            match best {
                Some(old_best) => {
                    if old_best.size_bytes > value.size_bytes {
                        best = Some(value)
                    } else {
                        best = Some(old_best)
                    }
                },
                None => best = Some(value),
            }
        }

        match best {
            Some(best) => Ok(Some((best.label, best.current_blob, best.size_entries))),
            None => Ok(None),
        }
    }

    pub async fn create_index_data(&self, index_group: &IndexGroup, blob_id: BlobID, meta: Vec<(Vec<u8>, AccessControl)>, new_size: u64) -> Result<()> {
        let index_id = IndexID::new();
        debug!("create collection for new index {index_group} {index_id}");
        self.create_index_column(&index_id).await?;
        self.update_index_data(index_group, index_id, &blob_id, &blob_id, meta, 0, new_size).await
    }

    pub async fn update_index_data(&self, index_group: &IndexGroup, index_id: IndexID, old_blob_id: &BlobID, blob_id: &BlobID, meta: Vec<(Vec<u8>, AccessControl)>, index_offset: u64, new_size: u64) -> Result<()> {
        debug!("update collection for {index_group} {index_id}");
        // Open column family for the index meta data
        let mut garbage = self.open_garbage_column().await?;
        let mut collection = match self.open_index_column(&index_id).await? {
            Some(fam) => fam,
            None => return Err(anyhow::anyhow!("Update on missing index"))
        };

        // Add all the new file entries
        let new_entries = meta.len() as u64;
        for (index, (hash, access)) in meta.into_iter().enumerate() {
            let index = index as u64 + index_offset;
            let index = index.to_le_bytes();
            collection.set(&hash, &index).await?;
            collection.set(&index, &postcard::to_allocvec(&FileEntry {
                access,
                hash: &hash,
            })?).await?;
        }
        debug!("create {index_group} {index_id} file records updated");

        // Update size in index table
        let mut index_index = self.open_directory_column().await?;
        let key = IndexEntry::prepare_key(&index_group, &index_id);
        loop {
            debug!("update {index_group} {index_id} index directory {key}");
            // Get
            let buffer = index_index.get(key.as_bytes()).await?;

            // modify
            let (entry, buffer) = match &buffer {
                Some(buffer) => {
                    let mut entry: IndexEntry = postcard::from_bytes(&buffer)?;
                    if &entry.current_blob != old_blob_id {
                        return Err(anyhow::anyhow!("Blob replaced"));
                    }
                    entry.size_bytes = new_size as u64;
                    entry.size_entries += new_entries;
                    entry.current_blob = blob_id.clone();
                    (entry, Some(&buffer[..]))
                },
                None => (IndexEntry{ 
                    group: index_group.clone(), 
                    label: index_id.clone(), 
                    current_blob: blob_id.clone(), 
                    size_bytes: new_size as u64, 
                    size_entries: new_entries 
                }, None),
            };

            // write
            garbage.set(old_blob_id.as_bytes(), b"\x00").await?;
            if index_index.cas(entry.key().as_bytes(), buffer, &postcard::to_allocvec(&entry)?).await? {
                return Ok(())
            }
        }
    }

    pub async fn list_indices(&self) -> Result<Vec<(IndexGroup, IndexID)>> {
        let mut index_index = self.open_directory_column().await?;
        Ok(index_index.list_all().await?
            .iter()
            .filter_map(|(_, val)|postcard::from_bytes::<IndexEntry>(val).ok())
            .map(|entry|(entry.group, entry.label))
            .collect())
    }

}
