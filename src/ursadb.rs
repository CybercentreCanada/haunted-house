use std::io::{Read, Write, SeekFrom, Seek};
use std::os::unix::prelude::FileExt;
use std::path::{PathBuf, Path};

use anyhow::Result;


const HEADER_SIZE: usize = 4 * 4;
const OFFSET_TABLE_SIZE: u64 = (1 << 24 + 1) * 8;

pub struct UrsaDBTrigramFilter {
    file: std::fs::File,
    table_offset: u64,
}

impl UrsaDBTrigramFilter {
    fn build_file(path: &Path) -> Result<Vec<bool>> {
        // Prepare to read
        let mut handle = std::fs::File::open(path)?;
        let mut buffer: Vec<u8> = vec![0; 1 << 20];

        // Prepare accumulators
        let mut mask: Vec<bool> = vec![false; 1 << 24];

        // Read the initial block
        let read = handle.read(&mut buffer)?;

        // Terminate if file too short
        if read <= 2 {
            return Ok(mask)
        }

        // Initialize trigram
        let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
        let mut index_start = 2;

        loop {
            for index in index_start..read {
                trigram = (trigram & 0x00FFFF) << 8 | (buffer[index] as u32);
                mask[trigram as usize] = true;
            }

            let read = handle.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            index_start = 0;
        }

        return Ok(mask)
    }

    pub fn encode_indices(indices: Vec<u64>) -> Vec<u8> {
        todo!();
    }

    pub fn decode_indices(data: Vec<u8>) -> Vec<u64> {
        todo!();
    }

    pub fn build(mut file: std::fs::File, input: Vec<PathBuf>) -> Result<Self> {
        file.seek(SeekFrom::Start(0))?;
        let mut data: Vec<Vec<u64>> = vec![];
        data.resize_with(1 << 24, Default::default);

        for (index, file) in input.iter().enumerate() {
            let matches = Self::build_file(file)?;
            for (trigram_index, value) in matches.iter().enumerate() {
                if *value {
                    data[trigram_index].push(index as u64);
                }
            }
        }

        // Write header
        file.write_all(&0xCA7DA7Au32.to_le_bytes())?;
        file.write_all(&6u32.to_le_bytes())?;
        file.write_all(&0u32.to_le_bytes())?;
        file.write_all(&0u32.to_le_bytes())?;

        // Write the bin data, tracking the offsets
        let mut cursor_offset: u64 = HEADER_SIZE as u64;
        let mut offsets: Vec<u64> = vec![cursor_offset];

        for bin in data {
            let bytes: Vec<u8> = Self::encode_indices(bin);
            cursor_offset += bytes.len() as u64;
            offsets.push(cursor_offset);
            file.write_all(&bytes)?;
        }

        // Write the offsets
        for offset in offsets {
            file.write_all(&offset.to_le_bytes())?
        }

        return Ok(Self{
            file,
            table_offset: cursor_offset
        })
    }

    fn open(file: std::fs::File) -> Result<Self> {
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
        let table_offset = meta.len() - OFFSET_TABLE_SIZE;
        let filter = Self{file, table_offset};

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

    fn get_bucket_range(&self, index: u64) -> Result<(u64, u64)> {
        let mut buf: [u8; 8 * 2] = [0; 8 * 2];
        self.file.read_exact_at(&mut buf, self.table_offset + index * 8)?;
        Ok((
            u64::from_le_bytes(buf[0..8].try_into()?),
            u64::from_le_bytes(buf[8..16].try_into()?),
        ))
    }

    fn search(&self, mut targets: Vec<u32>) -> Result<Vec<u64>> {
        // Get the ranges for indices


        //

        todo!()
    }
}


