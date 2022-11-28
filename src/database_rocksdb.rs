// use std::sync::Arc;

// use anyhow::Result;
// use log::error;
// use rocksdb::{BoundColumnFamily, MergeOperands, WriteBatchWithTransaction};
// use serde::{Deserialize, Serialize};

// use crate::access::AccessControl;
// use crate::database::IndexGroup;







// pub struct RocksInterface {
//     db: rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>,
//     index_soft_max: usize,
// }

// impl RocksInterface {
//     pub fn new(index_soft_max: usize) -> Result<Self> {
//         Ok(Self {
//             db: todo!(),
//             index_soft_max,
//         })
//     }

//     pub fn open_directory_column(&self) -> Result<Arc<BoundColumnFamily>> {
//         let mut options: rocksdb::Options = Default::default();
//         options.create_if_missing(true);
//         options.set_merge_operator_associative("index_entry_merge", index_entry_merge);
//         self.db.create_cf("index-index", &options)?;
//         Ok(self.db.cf_handle("index-index").unwrap())
//     }

//     pub fn open_index_column(&self, name: &str) -> Result<Option<Arc<BoundColumnFamily>>> {
//         let mut options: rocksdb::Options = Default::default();
//         options.create_if_missing(true);
//         options.set_merge_operator_associative("file_entry_merge", file_entry_merge);
//         self.db.create_cf(name, &options)?;
//         Ok(self.db.cf_handle(name))
//     }

//     pub async fn update_file_access(&self, hash: &[u8], access: &AccessControl, new_index_group: &IndexGroup) -> Result<bool> {
//         // Iterate from the furthest from expiry backwards until we hit the limiting group
//         let index_index_cf = self.open_directory_column()?;
//         let indices = self.db.iterator_cf(&index_index_cf, rocksdb::IteratorMode::End);
//         for index_entry in indices {
//             let (key, _value) = index_entry?;
//             let key = std::str::from_utf8(&key)?;

//             // Check if we have gone past the group we were looking for
//             if key < new_index_group.as_str() {
//                 break
//             }

//             // Check if the index has the hash being update
//             let (_index_group, index_name) = key.split_once(":").unwrap();
//             if let Some(column_family) = self.open_index_column(index_name)? {
//                 let hash_index = if let Some(slice) = self.db.get_pinned_cf(&column_family, hash)? {
//                     slice
//                 } else {
//                     continue;
//                 };

//                 // Check if the access is correct
//                 if let Some(_) = self.db.get_pinned_cf(&column_family, &hash_index)? {
//                     let entry = FileEntry{
//                         access: access.clone(),
//                         hash,
//                     };
//                     self.db.merge_cf(&column_family, hash_index, postcard::to_allocvec(&entry)?)?;
//                     return Ok(true)
//                 }
//             }
//         }

//         return Ok(false)
//     }

//     pub async fn select_index_to_grow(&self, index_group: &String) -> Result<(Option<String>, String)> {
//         let index_index_cf = self.open_directory_column()?;
//         let mut best: Option<IndexEntry> = None;
//         for item in self.db.prefix_iterator_cf(&index_index_cf, index_group) {
//             let (key, value) = item?;
//             let value: IndexEntry = match postcard::from_bytes(&value) {
//                 Ok(value) => value,
//                 Err(err) => {
//                     error!("Corrupted index entry: {err}");
//                     continue
//                 },
//             };

//             if value.size >= self.index_soft_max {
//                 continue;
//             }

//             match best {
//                 Some(old_best) => {
//                     if old_best.size > value.size {
//                         best = Some(value)
//                     } else {
//                         best = Some(old_best)
//                     }
//                 },
//                 None => best = Some(value),
//             }
//         }

//         match best {
//             Some(best) => Ok((false, best.label.to_owned())),
//             None => Ok((true, uuid::Uuid::new_v4().to_string())),
//         }
//     }

//     pub async fn create_index_data(&self, index_group: &String, index_id: String, meta: Vec<(Vec<u8>, AccessControl)>, new_size: usize) -> Result<()> {
//         {
//             let index_index = self.open_directory_column()?;
//             let entry = IndexEntry {
//                 group: index_group.clone(),
//                 label: index_id.clone(),
//                 size: new_size,
//             };
//             self.db.put_cf(&index_index, entry.key(), &postcard::to_allocvec(&entry)?)?;
//         }
//         self.update_index_data(index_group, index_id, meta, 0, new_size).await
//     }

//     pub async fn update_index_data(&self, index_group: &String, index_id: String, meta: Vec<(Vec<u8>, AccessControl)>, index_offset: usize, new_size: usize) -> Result<()> {
//         // Open column family for the index meta data
//         let column_family = match self.open_index_column(&index_id)? {
//             Some(fam) => fam,
//             None => return Err(anyhow::anyhow!("Update on missing index"))
//         };

//         // Add all the new file entries
//         for (index, (hash, access)) in meta.into_iter().enumerate() {
//             let index = index + index_offset;
//             let index = index.to_le_bytes();
//             let mut batch = WriteBatchWithTransaction::<false>::default();
//             batch.put_cf(&column_family, &hash, index);
//             batch.put_cf(&column_family, index, postcard::to_allocvec(&FileEntry {
//                 access,
//                 hash: &hash,
//             })?);
//             self.db.write(batch)?;
//         }

//         // Update size in index table
//         {
//             let index_index = self.open_directory_column()?;
//             let entry = IndexEntry {
//                 group: index_group.clone(),
//                 label: index_id,
//                 size: new_size,
//             };
//             self.db.merge(entry.key(), &postcard::to_allocvec(&entry)?)?;
//         }
//         return Ok(())
//     }

// }

// #[derive(Serialize, Deserialize)]
// struct IndexEntry {
//     group: String,
//     label: String,
//     size: usize,
// }

// impl IndexEntry {
//     fn key(&self) -> String {
//         format!("{}:{}", self.group, self.label)
//     }
// }

// #[derive(Serialize, Deserialize)]
// struct FileEntry<'a> {
//     access: AccessControl,
//     hash: &'a [u8]
// }

// fn file_entry_merge(new_key: &[u8], existing_val: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
//     // If there was no value, don't change that
//     let existing_val = match existing_val {
//         Some(val) => val,
//         None => return None,
//     };

//     // If a value exists, merge in every valid file entry
//     let mut base: FileEntry = match postcard::from_bytes(existing_val) {
//         Ok(base) => base,
//         Err(err) => {
//             error!("Corrupted file entry: {err}");
//             return None
//         },
//     };

//     for op in operands {
//         let op: FileEntry = match postcard::from_bytes(op) {
//             Ok(entry) => entry,
//             Err(err) => {
//                 error!("Corrupted file operation: {err}");
//                 continue
//             },
//         };
//         if op.hash == base.hash {
//             base.access = base.access.or(&op.access).simplify();
//         }
//     }

//     match postcard::to_allocvec(&base) {
//         Ok(buf) => Some(buf),
//         Err(err) => {
//             error!("Corrupted file operation: {err}");
//             None
//         },
//     }
// }

// fn index_entry_merge(new_key: &[u8], existing_val: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
//     // If there was no value, don't change that
//     let existing_val = match existing_val {
//         Some(val) => val,
//         None => return None,
//     };

//     // If a value exists, merge in every valid file entry
//     let mut base: IndexEntry = match postcard::from_bytes(existing_val) {
//         Ok(base) => base,
//         Err(err) => {
//             error!("Corrupted index entry: {err}");
//             return None
//         },
//     };

//     for op in operands {
//         let op: IndexEntry = match postcard::from_bytes(op) {
//             Ok(entry) => entry,
//             Err(err) => {
//                 error!("Corrupted index operation: {err}");
//                 continue
//             },
//         };
//         if op.key() == base.key() {
//             base.size = base.size.max(op.size);
//         }
//     }

//     match postcard::to_allocvec(&base) {
//         Ok(buf) => Some(buf),
//         Err(err) => {
//             error!("Corrupted index operation: {err}");
//             None
//         },
//     }
// }