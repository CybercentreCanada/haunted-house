// use std::collections::{HashMap, HashSet};
// use std::io::{Read, Write, SeekFrom, Seek};
// use std::mem::size_of;
// use std::os::unix::prelude::FileExt;
// use std::path::{PathBuf, Path};

// use anyhow::Result;
// use bitvec::vec::BitVec;

// use crate::varint;

// const MAGIC_NUMBER: u32 = 0x5FD59C20;
// const VERSION_NUMBER: u32 = 0;

// const HEADER_SIZE: usize = size_of::<u32>() * 2 + size_of::<u64>();
// const OFFSET_TABLE_SIZE: u64 = (1 << 24 + 1) * 8;

// pub struct TrigramFilter {
//     file: std::fs::File,
//     table_offset: u64,
// }

// impl TrigramFilter {
//     pub fn build_file<IN: std::io::Read>(mut handle: IN) -> Result<BitVec> {
//         // Prepare to read
//         let mut buffer: Vec<u8> = vec![0; 1 << 20];

//         // Prepare accumulators
//         let mut mask = BitVec::repeat(false, 1 << 24);

//         // Read the initial block
//         let read = handle.read(&mut buffer)?;

//         // Terminate if file too short
//         if read <= 2 {
//             return Ok(mask)
//         }

//         // Initialize trigram
//         let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
//         let mut index_start = 2;

//         loop {
//             for index in index_start..read {
//                 trigram = (trigram & 0x00FFFF) << 8 | (buffer[index] as u32);
//                 mask.set(trigram as usize, true);
//             }

//             let read = handle.read(&mut buffer)?;
//             if read == 0 {
//                 break;
//             }
//             index_start = 0;
//         }

//         return Ok(mask)
//     }

//     pub fn encode_indices(mut indices: Vec<u64>) -> Vec<u8> {
//         indices.sort_by(|a, b| b.cmp(a));
//         let mut diff = Vec::new();
//         for index in 0..(indices.len()-1) {
//             diff.push(indices[index] - indices[index + 1]);
//         }
//         diff.push(indices[indices.len()-1]);
//         // println!("diff {:?}", diff);
//         let mut bytes = Vec::new();
//         for value in diff {
//             bytes.extend(varint::encode(value))
//         }
//         return bytes;
//     }

//     pub fn decode_indices(mut data: Vec<u8>) -> Result<Vec<u64>> {
//         let data = &mut &data[..];
//         let mut diff = Vec::new();
//         while !data.is_empty() {
//             diff.push(match varint::decode_from(data){
//                 Ok(value) => value,
//                 Err(error) => return Err(anyhow::anyhow!(error.to_string())),
//             });
//         }
//         let mut out = vec![diff[diff.len() - 1]];
//         for index in (0..(diff.len()-1)).rev() {
//             out.push(out[out.len() - 1] + diff[index]);
//         }
//         return Ok(out);
//     }

//     pub fn guess_max_size(files: usize) -> usize {
//         HEADER_SIZE + OFFSET_TABLE_SIZE as usize + files * 8
//     }

//     // pub fn merge_in_data(mut file: std::fs::File, old: std::fs::File, data: Vec<BitVec>) -> Result<(Self, usize)> {
//     //     let old = Self::open(old)?;

//     //     // Write header
//     //     file.seek(SeekFrom::Start(0))?;
//     //     file.write_all(&0xCA7DA7Au32.to_le_bytes())?;
//     //     file.write_all(&6u32.to_le_bytes())?;
//     //     file.write_all(&0u32.to_le_bytes())?;
//     //     file.write_all(&0u32.to_le_bytes())?;

//     //     // Write the bin data, tracking the offsets
//     //     let mut cursor_offset: u64 = HEADER_SIZE as u64;
//     //     let mut offsets: Vec<u64> = vec![cursor_offset];



//     //     todo!();
//     // }

//     pub fn build_from_data(mut file: std::fs::File, raw_data: Vec<BitVec>) -> Result<Self> {

//         let mut data: Vec<Vec<u64>> = vec![];
//         data.resize_with(1 << 24, Default::default);

//         for (index, matches) in raw_data.iter().enumerate() {
//             for (trigram_index, value) in matches.iter().enumerate() {
//                 if *value {
//                     data[trigram_index].push(index as u64);
//                 }
//             }
//         }

//         // Write header
//         file.seek(SeekFrom::Start(0))?;
//         file.write_all(&0xCA7DA7Au32.to_le_bytes())?;
//         file.write_all(&6u32.to_le_bytes())?;
//         file.write_all(&0u32.to_le_bytes())?;
//         file.write_all(&0u32.to_le_bytes())?;

//         // Write the bin data, tracking the offsets
//         let mut cursor_offset: u64 = HEADER_SIZE as u64;
//         let mut offsets: Vec<u64> = vec![cursor_offset];

//         for bin in data {
//             if bin.len() > 0 {
//                 let bytes: Vec<u8> = Self::encode_indices(bin);
//                 cursor_offset += bytes.len() as u64;
//                 file.write_all(&bytes)?;
//             }
//             offsets.push(cursor_offset);
//         }

//         // Write the offsets
//         for offset in offsets {
//             file.write_all(&offset.to_le_bytes())?
//         }

//         return Ok(Self{
//             file,
//             table_offset: cursor_offset
//         })
//     }

//     pub fn build(mut file: std::fs::File, input: Vec<PathBuf>) -> Result<Self> {
//         let mut data: Vec<BitVec> = vec![];

//         for (index, file) in input.iter().enumerate() {
//             data.push(Self::build_file(std::fs::File::open(file)?)?);
//         }

//         return Self::build_from_data(file, data)
//     }

//     fn open(file: std::fs::File) -> Result<Self> {
//         // Read header
//         let mut buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
//         file.read_exact_at(&mut buf, 0)?;
//         let magic = u32::from_le_bytes(buf[0..4].try_into()?);
//         let version = u32::from_le_bytes(buf[4..8].try_into()?);
//         let raw_type = u32::from_le_bytes(buf[8..12].try_into()?);
//         let _reserved = u32::from_le_bytes(buf[12..16].try_into()?);

//         if magic != 0xCA7DA7A {
//             return Err(crate::error::ErrorKinds::IndexHasInvalidMagic.into())
//         }
//         if version != 6 {
//             return Err(crate::error::ErrorKinds::IndexHasUnsupportedVersion.into())
//         }
//         if raw_type != 0 {
//             return Err(crate::error::ErrorKinds::IndexHasUnsupportedType.into())
//         }

//         // Get file length
//         let meta = file.metadata()?;
//         let table_offset = meta.len() - OFFSET_TABLE_SIZE;
//         let filter = Self{file, table_offset};

//         // Read the vector offsets and make sure they are in range
//         for index in 0..(1 << 24) {
//             let (start, end) = filter.get_bucket_range(index)?;
//             if start > end || (start as usize) < HEADER_SIZE || end > table_offset {
//                 return Err(crate::error::ErrorKinds::IndexCorruptTable.into())
//             }
//         }

//         // File checks out
//         return Ok(filter)
//     }

//     fn get_bucket_range(&self, index: u32) -> Result<(u64, u64)> {
//         let mut buf: [u8; 8 * 2] = [0; 8 * 2];
//         self.file.read_exact_at(&mut buf, self.table_offset + index as u64 * 8)?;
//         Ok((
//             u64::from_le_bytes(buf[0..8].try_into()?),
//             u64::from_le_bytes(buf[8..16].try_into()?),
//         ))
//     }

//     fn search(&self, targets: &Vec<Vec<u8>>) -> Result<Vec<u64>> {
//         // Get the trigrams we are interested in
//         let mut trigrams: Vec<u32> = Default::default();
//         for segment in targets {
//             if segment.len() < 3 {
//                 continue;
//             }
//             let mut trigram: u32 = (segment[0] as u32) << 8 | (segment[1] as u32);
//             for index in 2..segment.len() {
//                 trigram = (trigram & 0x00FFFF) << 8 | (segment[index] as u32);
//                 trigrams.push(trigram);
//             }
//         }
//         trigrams.sort();
//         trigrams.dedup();
//         if trigrams.len() == 0 {
//             return Ok(vec![]);
//         }

//         // Get the block ranges
//         let mut addresses: HashMap<u32, (u64, u64)> = Default::default();
//         for trigram in trigrams.iter() {
//             addresses.insert(*trigram, self.get_bucket_range(*trigram)?);
//         }

//         // sort by block size
//         trigrams.sort_by(|a, b| {
//             let a = addresses.get(a).unwrap();
//             let b = addresses.get(b).unwrap();
//             let a = a.1 - a.0;
//             let b = b.1 - b.0;
//             return a.cmp(&b);
//         });

//         // Fetch each set and merge in
//         let mut items: HashSet<u64> = self.get_indices(*addresses.get(&trigrams[0]).unwrap())?.into_iter().collect();
//         for trigram in trigrams[1..].iter() {
//             let net_items: HashSet<u64> = self.get_indices(*addresses.get(trigram).unwrap())?.into_iter().collect();
//             items = items.intersection(&net_items).cloned().collect();
//         }

//         return Ok(items.into_iter().collect())
//     }

//     fn get_indices(&self, (start, end): (u64, u64)) -> Result<Vec<u64>> {
//         let mut buffer = vec![0; (end-start).try_into()?];
//         self.file.read_exact_at(&mut buffer, start)?;
//         Ok(Self::decode_indices(buffer)?)
//     }
// }
