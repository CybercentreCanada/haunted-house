use std::collections::{HashMap, HashSet};
use std::io::{Write, SeekFrom, Seek};
use std::os::unix::prelude::FileExt;

use anyhow::Result;
use bitvec::vec::BitVec;
use tokio::sync::mpsc;

use crate::query::Query;
use crate::varint;


const HEADER_SIZE: usize = 4 * 4;
const OFFSET_TABLE_SIZE: u64 = ((1 << 24) + 1) * 8;

pub struct TrigramFilter {
    file: std::fs::File,
    table: Vec<u64>,
}

impl TrigramFilter {
    pub async fn build_file(mut input: mpsc::Receiver<Result<Vec<u8>>>) -> Result<BitVec> {
        // Prepare accumulators
        let mut mask = BitVec::repeat(false, 1 << 24);

        // Read the initial block
        let mut buffer = vec![];
        while buffer.len() <= 2 {
            let sub_buffer = match input.recv().await {
                Some(sub_buffer) => sub_buffer?,
                None => return Ok(mask),
            };

            buffer.extend(sub_buffer);
        }

        // Initialize trigram
        let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
        let mut index_start = 2;

        loop {
            for byte in &buffer[index_start..] {
                trigram = (trigram & 0x00FFFF) << 8 | (*byte as u32);
                mask.set(trigram as usize, true);
            }

            buffer = match input.recv().await {
                Some(buffer) => buffer?,
                None => return Ok(mask),
            };
            if buffer.is_empty() {
                break;
            }
            index_start = 0;
        }

        return Ok(mask)
    }

    pub fn encode_indices(mut indices: Vec<u64>) -> Vec<u8> {
        indices.sort_by(|a, b| b.cmp(a));
        let mut diff = Vec::new();
        for index in 0..(indices.len()-1) {
            diff.push(indices[index] - indices[index + 1]);
        }
        diff.push(indices[indices.len()-1]);
        // println!("diff {:?}", diff);
        let mut bytes = Vec::new();
        for value in diff {
            bytes.extend(varint::encode(value))
        }
        return bytes;
    }

    pub fn decode_indices(data: Vec<u8>) -> Result<Vec<u64>> {
        let data = &mut &data[..];
        let mut diff = Vec::new();
        while !data.is_empty() {
            diff.push(match varint::decode_from(data){
                Ok(value) => value,
                Err(error) => return Err(anyhow::anyhow!(error.to_string())),
            });
        }
        let mut out = vec![diff[diff.len() - 1]];
        for index in (0..(diff.len()-1)).rev() {
            out.push(out[out.len() - 1] + diff[index]);
        }
        return Ok(out);
    }

    pub fn guess_max_size(files: usize) -> u64 {
        HEADER_SIZE as u64 + OFFSET_TABLE_SIZE + files as u64 * 8
    }

    pub fn merge_in_data(mut file: std::fs::File, old: std::fs::File, data: Vec<BitVec>, index_offset: u64) -> Result<Self> {
        let old = Self::open(old)?;

        // Write header
        file.seek(SeekFrom::Start(0))?;
        let mut file = std::io::BufWriter::new(file);
        file.write_all(&0xCA7DA7Au32.to_le_bytes())?;
        file.write_all(&6u32.to_le_bytes())?;
        file.write_all(&0u32.to_le_bytes())?;
        file.write_all(&0u32.to_le_bytes())?;

        // Write the bin data, tracking the offsets
        let mut cursor_offset: u64 = HEADER_SIZE as u64;
        let mut offsets: Vec<u64> = vec![cursor_offset];

        for trigram in 0..(1 << 24) {
            let mut indices = old.get_bucket_indices(trigram)?;
            for (local_index, vec) in data.iter().enumerate() {
                if let Some(val) = vec.get(trigram as usize) {
                    if *val {
                        indices.push(index_offset + local_index as u64);
                    }
                }
            }
            if indices.len() > 0 {
                let bytes: Vec<u8> = Self::encode_indices(indices);
                cursor_offset += bytes.len() as u64;
                file.write_all(&bytes)?;
            }
            offsets.push(cursor_offset);
        }

        // Write the offsets
        for offset in offsets.iter() {
            file.write_all(&offset.to_le_bytes())?
        }

        file.flush()?;
        return Ok(Self{
            file: file.into_inner()?,
            table: offsets
        })
    }

    pub fn build_from_data(mut file: std::fs::File, raw_data: Vec<BitVec>) -> Result<Self> {

        let mut data: Vec<Vec<u64>> = vec![];
        data.resize_with(1 << 24, Default::default);

        for (index, matches) in raw_data.iter().enumerate() {
            for (trigram_index, value) in matches.iter().enumerate() {
                if *value {
                    data[trigram_index].push(index as u64);
                }
            }
        }

        // Write header
        file.seek(SeekFrom::Start(0))?;
        let mut file = std::io::BufWriter::new(file);
        file.write_all(&0xCA7DA7Au32.to_le_bytes())?;
        file.write_all(&6u32.to_le_bytes())?;
        file.write_all(&0u32.to_le_bytes())?;
        file.write_all(&0u32.to_le_bytes())?;

        // Write the bin data, tracking the offsets
        let mut cursor_offset: u64 = HEADER_SIZE as u64;
        let mut offsets: Vec<u64> = vec![cursor_offset];

        for bin in data {
            if bin.len() > 0 {
                let bytes: Vec<u8> = Self::encode_indices(bin);
                cursor_offset += bytes.len() as u64;
                file.write_all(&bytes)?;
            }
            offsets.push(cursor_offset);
        }

        // Write the offsets
        for offset in offsets.iter() {
            file.write_all(&offset.to_le_bytes())?
        }

        file.flush()?;
        return Ok(Self{
            file: file.into_inner()?,
            table: offsets
        })
    }

    #[cfg(test)]
    pub async fn build(file: std::fs::File, input: Vec<std::path::PathBuf>) -> Result<Self> {

        let mut data: Vec<BitVec> = vec![];

        for source_file in input.into_iter() {
            data.push(Self::build_file(crate::storage::read_chunks(source_file)).await?);
        }

        return Self::build_from_data(file, data)
    }

    pub fn open(file: std::fs::File) -> Result<Self> {
        // Read header
        let mut buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
        file.read_exact_at(&mut buf, 0)?;
        let magic = u32::from_le_bytes(buf[0..4].try_into()?);
        let version = u32::from_le_bytes(buf[4..8].try_into()?);
        let raw_type = u32::from_le_bytes(buf[8..12].try_into()?);
        let _reserved = u32::from_le_bytes(buf[12..16].try_into()?);

        if magic != 0xCA7DA7A {
            return Err(crate::error::ErrorKinds::IndexHasInvalidMagic.into())
        }
        if version != 6 {
            return Err(crate::error::ErrorKinds::IndexHasUnsupportedVersion.into())
        }
        if raw_type != 0 {
            return Err(crate::error::ErrorKinds::IndexHasUnsupportedType.into())
        }

        // Get file length
        let meta = file.metadata()?;

        // Read out the index table from the filter
        let table_offset = meta.len() - OFFSET_TABLE_SIZE;
        let mut table_buffer = vec![0u8; OFFSET_TABLE_SIZE as usize];
        file.read_exact_at(&mut table_buffer, table_offset)?;
        let mut table = vec![0u64; (1 << 24) + 1];
        for index in 0..(1 << 24) + 1 {
            table[index] = u64::from_le_bytes(table_buffer[index * 8..(index + 1)*8].try_into()?);
        }

        //
        let filter = Self{file, table};

        // Read the vector offsets and make sure they are in range
        for index in 0..(1 << 24) {
            let (start, end) = filter.get_bucket_range(index)?;
            if start > end || (start as usize) < HEADER_SIZE || end > table_offset {
                return Err(crate::error::ErrorKinds::IndexCorruptTable.into())
            }
        }

        // File checks out
        return Ok(filter)
    }

    pub fn run_query(&self, query: &Query) -> Result<HashSet<u64>> {
        match query {
            // Query::Not(query) => todo!(),
            Query::Or(parts) => {
                let (mut accumulated, remain) = match parts.split_first() {
                    Some((first, remain)) => {
                        (self.run_query(first)?, remain)
                    },
                    None => return Ok(Default::default()),
                };
                for sub_query in remain {
                    accumulated.extend(self.run_query(sub_query)?);
                }
                Ok(accumulated)
            },
            Query::And(parts) => {
                let (mut accumulated, remain) = match parts.split_first() {
                    Some((first, remain)) => {
                        (self.run_query(first)?, remain)
                    },
                    None => return Ok(Default::default()),
                };
                for sub_query in remain {
                    accumulated = accumulated.intersection(&self.run_query(sub_query)?).cloned().collect();
                }
                Ok(accumulated)
            },
            // Query::String(string) => self.buffer_query(string.as_bytes()),
            Query::Literal(data) => self.buffer_query(data),
        }
    }

    pub fn buffer_query(&self, target: &[u8]) -> Result<HashSet<u64>> {
        // Get the trigrams we are interested in
        let mut trigrams: Vec<u32> = Default::default();
        if target.len() < 3 {
            return Ok(Default::default());
        }
        let mut trigram: u32 = (target[0] as u32) << 8 | (target[1] as u32);
        for index in 2..target.len() {
            trigram = (trigram & 0x00FFFF) << 8 | (target[index] as u32);
            trigrams.push(trigram);
        }
        trigrams.sort();
        trigrams.dedup();
        self.trigram_query(&mut trigrams)
    }

    // fn multi_buffer_query(&self, targets: &Vec<Vec<u8>>) -> Result<HashSet<u64>> {
    //     // Get the trigrams we are interested in
    //     let mut trigrams: Vec<u32> = Default::default();
    //     for segment in targets {
    //         if segment.len() < 3 {
    //             continue;
    //         }
    //         let mut trigram: u32 = (segment[0] as u32) << 8 | (segment[1] as u32);
    //         for index in 2..segment.len() {
    //             trigram = (trigram & 0x00FFFF) << 8 | (segment[index] as u32);
    //             trigrams.push(trigram);
    //         }
    //     }
    //     trigrams.sort();
    //     trigrams.dedup();
    //     self.trigram_query(&mut trigrams)
    // }

    fn trigram_query(&self, trigrams: &mut Vec<u32>) -> Result<HashSet<u64>> {
        if trigrams.len() == 0 {
            return Ok(Default::default());
        }

        // Get the block ranges
        let mut addresses: HashMap<u32, (u64, u64)> = Default::default();
        for trigram in trigrams.iter() {
            addresses.insert(*trigram, self.get_bucket_range(*trigram)?);
        }

        // sort by block size
        trigrams.sort_by(|a, b| {
            let a = addresses.get(a).unwrap();
            let b = addresses.get(b).unwrap();
            let a = a.1 - a.0;
            let b = b.1 - b.0;
            return a.cmp(&b);
        });

        // Fetch each set and merge in
        let mut items: HashSet<u64> = self.get_indices(*addresses.get(&trigrams[0]).unwrap())?.into_iter().collect();
        for trigram in trigrams[1..].iter() {
            let net_items: HashSet<u64> = self.get_indices(*addresses.get(trigram).unwrap())?.into_iter().collect();
            items = items.intersection(&net_items).cloned().collect();
        }

        return Ok(items)
    }

    fn get_bucket_range(&self, index: u32) -> Result<(u64, u64)> {
        if index as usize + 1 < self.table.len() {
            Ok((self.table[index as usize], self.table[index as usize + 1]))
        } else {
            Err(anyhow::anyhow!("Bad bucket range"))
        }
    }

    fn get_indices(&self, (start, end): (u64, u64)) -> Result<Vec<u64>> {
        if start == end {
            return Ok(vec![])
        }
        let mut buffer = vec![0; (end-start).try_into()?];
        self.file.read_exact_at(&mut buffer, start)?;
        Ok(Self::decode_indices(buffer)?)
    }

    fn get_bucket_indices(&self, index: u32) -> Result<Vec<u64>> {
        let (start, end) = self.get_bucket_range(index)?;
        self.get_indices((start, end))
    }
}


#[cfg(test)]
mod test {
    use std::io::Write;

    use rand::{thread_rng, Rng};
    use anyhow::Result;

    use super::TrigramFilter;

    #[tokio::test]
    async fn build_and_search() -> Result<()> {
        let mut input_data: Vec<u8> = Default::default();
        let mut input = tempfile::NamedTempFile::new()?;
        for _ in 0..10000 {
            let numb: [u8; 8] = thread_rng().gen();
            input_data.extend(numb);
        }
        input.write_all(&input_data)?;
        input.flush()?;

        let filter = tempfile::tempfile()?;

        let filter = {
            let filter = TrigramFilter::build(filter, vec![input.path().to_path_buf()]).await?;
            for _ in 0..10 {
                let index = rand::thread_rng().gen_range(0..input_data.len()-50);
                let search = Vec::from(&input_data[index..index+50]);
                assert_eq!(filter.buffer_query(&search).unwrap(), [0u64].into());
            }
            filter.file
        };

        assert!(filter.metadata()?.len() > 0);
        println!("filter disk size: {}", filter.metadata()?.len());

        {
            let filter = TrigramFilter::open(filter).unwrap();
            for _ in 0..10 {
                let index = rand::thread_rng().gen_range(0..input_data.len()-50);
                let search = Vec::from(&input_data[index..index+50]);
                assert_eq!(filter.buffer_query(&search).unwrap(), [0u64].into());
            }
        }

        return Ok(())
    }
}