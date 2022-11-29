use std::sync::Arc;

use anyhow::Result;
use log::error;
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;
use crate::database::{IndexGroup, BlobID, IndexID};
use crate::sqlite_kv::{SKV, Collection};


pub struct SQLiteInterface {
    kv: SKV,
    index_soft_max: usize,
}

#[derive(Serialize, Deserialize)]
struct IndexEntry {
    group: IndexGroup,
    label: IndexID,
    current_blob: BlobID,
    size: usize,
}

impl IndexEntry {
    fn key(&self) -> String {
        format!("{}:{}", self.group.0, self.label.0)
    }
}

#[derive(Serialize, Deserialize)]
struct FileEntry<'a> {
    access: AccessControl,
    hash: &'a [u8]
}


impl SQLiteInterface {
    pub async fn new(index_soft_max: usize, path: &str) -> Result<Self> {
        Ok(Self {
            kv: SKV::open(path).await?,
            index_soft_max,
        })
    }

    pub async fn open_directory_column(&self) -> Result<Collection> {
        Ok(self.kv.create_collection("index-index").await?)
    }

    pub async fn open_index_column(&self, name: &str) -> Result<Option<Collection>> {
        Ok(self.kv.open_collection(&format!("index-{name}")).await?)
    }
    
    pub async fn create_index_column(&self, name: &str) -> Result<Collection> {
        Ok(self.kv.create_collection(&format!("index-{name}")).await?)
    }

    pub async fn update_file_access(&self, hash: &[u8], access: &AccessControl, new_index_group: &IndexGroup) -> Result<bool> {
        // Get all the groups that expire later than this one
        let mut index_index = self.open_directory_column().await?;
        let mut list = index_index.list_inclusive_range(new_index_group.0.as_bytes(), b"99999").await?;

        // Run from oldest to newest that fit
        list.sort_by(|a, b|b.cmp(a));

        for (key, value) in list {
            let key = std::str::from_utf8(&key)?;

            // Check if the index has the hash being update
            let (_index_group, index_name) = key.split_once(":").unwrap();
            if let Some(col) = self.open_index_column(index_name).await? {
                let hash_index = if let Some(slice) = col.get(hash).await? {
                    slice
                } else {
                    continue;
                };

                loop {
                    // Check if the access is correct
                    if let Some(buffer) = col.get(&hash_index)? {
                        let entry = FileEntry{
                            access: access.clone(),
                            hash,
                        };

                        if col.cas(hash_index, buffer, postcard::to_allocvec(&entry)?).await? {
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

    pub async fn select_index_to_grow(&self, index_group: &IndexGroup) -> Result<Option<(IndexID, BlobID, BlobID)>> {
        let mut index_index = self.open_directory_column().await?;
        let mut best: Option<IndexEntry> = None;

        let list = index_index.list_inclusive_range(index_group.0.as_bytes(), index_group.0.as_bytes()).await?;

        for (key, value) in list {
            let value: IndexEntry = match postcard::from_bytes(&value) {
                Ok(value) => value,
                Err(err) => {
                    error!("Corrupted index entry: {err}");
                    continue
                },
            };

            if value.size >= self.index_soft_max {
                continue;
            }

            match best {
                Some(old_best) => {
                    if old_best.size > value.size {
                        best = Some(value)
                    } else {
                        best = Some(old_best)
                    }
                },
                None => best = Some(value),
            }
        }

        match best {
            Some(best) => Ok(Some((best.label, best.current_blob, BlobID::new()))),
            None => Ok(None),
        }
    }

    pub async fn create_index_data(&self, index_group: &String, index_id: String, meta: Vec<(Vec<u8>, AccessControl)>, new_size: usize) -> Result<()> {
        todo!()
        // {
        //     let index_index = self.open_directory_column()?;
        //     let entry = IndexEntry {
        //         group: index_group.clone(),
        //         label: index_id.clone(),
        //         size: new_size,
        //     };
        //     self.db.put_cf(&index_index, entry.key(), &postcard::to_allocvec(&entry)?)?;
        // }
        // self.update_index_data(index_group, index_id, meta, 0, new_size).await
    }

    pub async fn update_index_data(&self, index_group: &String, index_id: String, meta: Vec<(Vec<u8>, AccessControl)>, index_offset: usize, new_size: usize) -> Result<()> {
        todo!();
        // // Open column family for the index meta data
        // let column_family = match self.open_index_column(&index_id)? {
        //     Some(fam) => fam,
        //     None => return Err(anyhow::anyhow!("Update on missing index"))
        // };

        // // Add all the new file entries
        // for (index, (hash, access)) in meta.into_iter().enumerate() {
        //     let index = index + index_offset;
        //     let index = index.to_le_bytes();
        //     let mut batch = WriteBatchWithTransaction::<false>::default();
        //     batch.put_cf(&column_family, &hash, index);
        //     batch.put_cf(&column_family, index, postcard::to_allocvec(&FileEntry {
        //         access,
        //         hash: &hash,
        //     })?);
        //     self.db.write(batch)?;
        // }

        // // Update size in index table
        // {
        //     let index_index = self.open_directory_column()?;
        //     let entry = IndexEntry {
        //         group: index_group.clone(),
        //         label: index_id,
        //         size: new_size,
        //     };
        //     self.db.merge(entry.key(), &postcard::to_allocvec(&entry)?)?;
        // }
        // return Ok(())
    }

}
