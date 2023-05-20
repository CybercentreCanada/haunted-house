use bitvec::vec::BitVec;
use anyhow::{Result, Context};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::{Write, Seek, SeekFrom, Read, BufRead};
use std::path::{Path, PathBuf};

use crate::query::Query;

use super::encoding::{cost_to_add, encode_into, decode_into};


const HEADER_SIZE: u64 = 4 + 4 + 4 + 4;
const HEADER_MAGIC: u32 = 0x0e3d9def;
const POINTER_SIZE: u64 = 4;
const TRIGRAM_RANGE: u64 = 1 << 24;


struct TrigramCursor<'a> {
    host: &'a ExtensibleTrigramFile,
    current: Vec<u64>,
    offset: usize,
    next: Option<u32>
}

impl<'a> TrigramCursor<'a> {
    pub fn new(host: &'a ExtensibleTrigramFile, trigram: u32) -> Result<Self> {
        let mut buffer = vec![];
        let segment = host.read_initial_segment(trigram)?;
        let next = segment.extension();
        segment.decode_into(&mut buffer);

        Ok(Self {
            host,
            current: buffer,
            offset: 0,
            next
        })
    }
}

impl<'a> Iterator for TrigramCursor<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset < self.current.len() {
            self.offset += 1;
            return Some(self.current[self.offset-1])
        }

        match self.next {
            Some(address) => {
                let segment = match self.host.read_extended_segment(address) {
                    Ok(segment) => segment,
                    Err(_) => return None
                };

                self.current.clear();
                segment.decode_into(&mut self.current);
                self.next = segment.extension();
                self.offset = 1;
                match self.current.get(0) {
                    Some(val) => Some(*val),
                    None => None,
                }
            },
            None => None,
        }
    }
}

fn into_trigrams(bytes: &Vec<u8>) -> Vec<u32> {
    if bytes.len() < 3 {
        return vec![];
    }
    let mut trigrams = vec![];
    let mut trigram: u32 = (bytes[0] as u32) << 8 | (bytes[1] as u32);

    for index in 2.. {
        trigram = (trigram & 0x00FFFF) << 8 | (bytes[index] as u32);
        trigrams.push(trigram);
    }

    return trigrams;
}

fn union(base: &mut Vec<u64>, other: &Vec<u64>) {
    base.extend(other);
    base.sort_unstable();
    base.dedup();
}

fn intersection(base: &mut Vec<u64>, other: &Vec<u64>) {

    let mut base_read_index = 0;
    let mut base_write_index = 0;
    let mut other_index = 0;

    while base_read_index < base.len() && other_index < other.len() {
        match base[base_read_index].cmp(&other[other_index]) {
            std::cmp::Ordering::Less => { base_read_index += 1},
            std::cmp::Ordering::Equal => {
                base[base_write_index] = base[base_read_index];
                base_write_index += 1;
                base_read_index += 1;
                other_index += 1;
            },
            std::cmp::Ordering::Greater => {other_index += 1},
        }
    }

    base.truncate(base_write_index);
}

pub struct ExtensibleTrigramFile {
    initial_segment_size: u32,
    extended_segment_size: u32,
    data_location: PathBuf,
    // data: std::fs::File,
    data: memmap2::MmapMut,
    edit_buffer_location: PathBuf,
    extended_segments: u32,
}

// impl std::fmt::Debug for ExtensibleTrigramFile {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("ExtensibleTrigramFile")
//             .field("initial_segment_size", &self.initial_segment_size)
//             .field("extended_segment_size", &self.extended_segment_size)
//             .field("data_location", &self.data_location)
//             .field("extended_segments", &self.extended_segments).finish()
//     }
// }


#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
enum SegmentAddress {
    Trigram(u32),
    Segment(u32),
}

#[derive(Serialize, Deserialize)]
enum UpdateOperations<'a> {
    WriteSegment{segment: SegmentAddress, data: &'a[u8]},
    ExtendSegment{segment: SegmentAddress, new_segment: u32},
}

// impl std::fmt::Debug for UpdateOperations {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Self::WriteSegment { segment, data } => f.debug_struct("WriteSegment").field("segment", segment).field("data_len", &data.len()).finish(),
//             Self::ExtendSegment { segment, new_segment } => f.debug_struct("ExtendSegment").field("segment", segment).field("new_segment", new_segment).finish(),
//         }
//     }
// }

struct RawSegmentInfo<'a> {
    data: &'a[u8],
}

impl<'a> RawSegmentInfo<'a> {
    fn new(data: &'a[u8]) -> Self {
        RawSegmentInfo { data }
    }

    fn payload_bytes(&self) -> u32 {
        self.data.len() as u32 - POINTER_SIZE as u32
    }

    fn decode_into(&self, buffer: &mut Vec<u64>) -> u32 {
        decode_into(&self.data[0..self.data.len() - POINTER_SIZE as usize], buffer)
    }

    fn extension(&self) -> Option<u32> {
        let bytes = self.data[self.data.len() - POINTER_SIZE as usize..self.data.len()].try_into().unwrap();
        let num = u32::from_le_bytes(bytes);
        if num == 0 {
            None
        } else {
            Some(num)
        }
    }
}

impl ExtensibleTrigramFile {

    pub fn new(location: &Path, initial_segment_size: u32, extended_segment_size: u32) -> Result<Self> {
        // prepare file
        let mut data = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(location).context("Creating data file")?;
        data.set_len(HEADER_SIZE + TRIGRAM_RANGE * initial_segment_size as u64).context("Adjusting file size")?;

        // write header
        data.write_u32::<LittleEndian>(HEADER_MAGIC).context("Writing header")?;
        data.write_u32::<LittleEndian>(HEADER_SIZE as u32).context("Writing header")?;
        data.write_u32::<LittleEndian>(initial_segment_size).context("Writing header")?;
        data.write_u32::<LittleEndian>(extended_segment_size).context("Writing header")?;
        data.sync_all()?;

        //
        let mut edit_buffer_location = location.to_owned();
        edit_buffer_location.set_extension("wb");

        // return object
        let data = unsafe { memmap2::MmapMut::map_mut(&data)? };
        data.advise(memmap2::Advice::Random)?;
        Ok(Self { initial_segment_size, extended_segment_size, data_location: location.to_owned(), data, edit_buffer_location, extended_segments: 0 })
    }

    pub fn open(location: &Path) -> Result<Self> {
        // Open the file
        let mut data = std::fs::OpenOptions::new().write(true).read(true).truncate(false).open(location).context("Creating data file")?;

        // Read the header
        let mut header_buffer = vec![0; HEADER_SIZE as usize];
        data.read_exact(&mut header_buffer)?;

        // parse the header
        let mut header = std::io::Cursor::new(header_buffer);
        if header.read_u32::<LittleEndian>()? != HEADER_MAGIC {
            return Err(anyhow::anyhow!("Corrupt data file: header magic wrong"));
        }
        if header.read_u32::<LittleEndian>()? != HEADER_SIZE as u32 {
            return Err(anyhow::anyhow!("Corrupt data file: header format wrong"));
        }
        let initial_segment_size = header.read_u32::<LittleEndian>()?;
        let extended_segment_size = header.read_u32::<LittleEndian>()?;

        // Use file size to infer number of extended segments
        let metadata = data.metadata()?;
        if metadata.len() < HEADER_SIZE + initial_segment_size as u64 * TRIGRAM_RANGE {
            return Err(anyhow::anyhow!("Corrupt data file: below minimum size for configuration"));
        }
        let extended_size = metadata.len() - HEADER_SIZE - initial_segment_size as u64 * TRIGRAM_RANGE;
        let extended_segments = (extended_size/extended_segment_size as u64) as u32;
        let mut edit_buffer_location = location.to_owned();
        edit_buffer_location.set_extension("wb");
        let data = unsafe { memmap2::MmapMut::map_mut(&data)? };
        data.advise(memmap2::Advice::Random)?;
        let mut filter = Self{ initial_segment_size, extended_segment_size, data_location: location.to_owned(), data, edit_buffer_location: edit_buffer_location.clone(), extended_segments };

        // Check for a edit buffer
        if edit_buffer_location.exists() {
            filter.check_and_apply_operations(File::open(edit_buffer_location).context("could not open edit buffer")?)?;
        }
        return Ok(filter);
    }

    pub fn read_trigram(&self, trigram: u32) -> Result<Vec<u64>> {
        let mut output = vec![];
        let mut segment = self.read_initial_segment(trigram).context("reading initial segment")?;
        segment.decode_into(&mut output);
        while let Some(extension) = segment.extension() {
            segment = self.read_extended_segment(extension).context("reading extended segment")?;
            segment.decode_into(&mut output);
        }
        return Ok(output);
    }

    pub fn read_trigrams(&self, trigrams: Vec<u32>) -> Result<Vec<u64>> {
        // Handle corner cases
        if trigrams.len() == 0 {
            return Ok(vec![])
        }
        if trigrams.len() == 1 {
            return self.read_trigram(trigrams[0])
        }

        let mut output = vec![];
        let mut sources = vec![];
        for trigram in trigrams {
            sources.push(TrigramCursor::new(self, trigram)?.peekable());
        }

        let mut candidate = match sources[0].peek() {
            Some(item) => *item,
            None => return Ok(vec![]),
        };

        'next_candidate: loop {
            'next_cursor: for cursor in &mut sources {
                loop {
                    let next = match cursor.peek() {
                        Some(next) => *next,
                        None => break 'next_candidate,
                    };

                    match next.cmp(&candidate) {
                        std::cmp::Ordering::Less => { cursor.next(); continue },
                        std::cmp::Ordering::Equal => { continue 'next_cursor },
                        std::cmp::Ordering::Greater => { candidate = next; continue 'next_candidate },
                    }
                }
            }

            output.push(candidate);
            candidate += 1;
        }
        return Ok(output)
    }

    pub fn query(&self, query: &Query) -> Result<Vec<u64>> {
        let mut cache = Default::default();
        self._query(query, &mut cache)
    }

    pub fn _query(&self, query: &Query, cache: &mut HashMap<Vec<u8>, Vec<u64>>) -> Result<Vec<u64>> {
        match query {
            Query::And(items) => {
                let mut base = self._query(&items[0], cache)?;
                for item in &items[1..] {
                    intersection(&mut base, &self._query(item, cache)?);
                    if base.is_empty() {
                        break
                    }
                }
                Ok(base)
            },
            Query::Or(items) => {
                let mut base = vec![];
                for item in items {
                    union(&mut base, &self._query(item, cache)?);
                }
                Ok(base)
            },
            Query::Literal(values) => {
                match cache.entry(values.clone()) {
                    Entry::Occupied(entry) => Ok(entry.get().clone()),
                    Entry::Vacant(entry) => {
                        let hits = self.read_trigrams(into_trigrams(values))?;
                        entry.insert(hits.clone());
                        Ok(hits)
                    },
                }
            },
            Query::MinOf(count, items) => {
                let mut hits = HashMap::<u64, u32>::new();
                for item in items {
                    for file in self._query(item, cache)? {
                        match hits.entry(file) {
                            Entry::Occupied(mut entry) => { *entry.get_mut() += 1; },
                            Entry::Vacant(entry) => { entry.insert(1); },
                        }
                    }
                }
                Ok(hits.into_iter().filter_map(|(file, hits)|{
                    if hits >= *count as u32 {
                        Some(file)
                    } else {
                        None
                    }
                }).collect_vec())
            },
        }
    }

    fn get_segment_offset(&self, address: SegmentAddress) -> u64 {
        match address {
            SegmentAddress::Trigram(address) => self.get_initial_segment_offset(address),
            SegmentAddress::Segment(address) => self.get_extended_segment_offset(address),
        }
    }

    fn get_initial_segment_offset(&self, address: u32) -> u64 {
        HEADER_SIZE + address as u64 * self.initial_segment_size as u64
    }

    fn get_extended_segment_offset(&self, address: u32) -> u64 {
        HEADER_SIZE + TRIGRAM_RANGE * self.initial_segment_size as u64 + (address - 1) as u64 * self.extended_segment_size as u64
    }

    // #[instrument]
    fn read_initial_segment(&self, trigram: u32) -> Result<RawSegmentInfo> {
        let location = self.get_initial_segment_offset(trigram) as usize;
        let data = &self.data[location..location+self.initial_segment_size as usize];
        return Ok(RawSegmentInfo::new(data))
    }

    // #[instrument]
    fn read_extended_segment(&self, segment: u32) -> Result<RawSegmentInfo> {
        let location = self.get_extended_segment_offset(segment) as usize;
        let data = &self.data[location..location+self.extended_segment_size as usize];
        return Ok(RawSegmentInfo::new(data))
    }

    // #[instrument]
    pub fn write_batch(&self, files: &mut [(u64, BitVec)]) -> Result<(File, u32)> {
        // prepare the buffer for operations
        let write_buffer = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(&self.edit_buffer_location)?;
        let mut skipped = HashSet::<u64>::new();

        // Leave room for the end offset at the start
        let mut writer = std::io::BufWriter::new(write_buffer);
        writer.write_u64::<LittleEndian>(0)?;

        // Timing information
        let start = std::time::Instant::now();
        let mut invert_time: f64 = 0.0;
        let mut build_ops_time: f64 = 0.0;
        let mut op_write_time: f64 = 0.0;
        let mut seg_read_time: f64 = 0.0;

        // Buffers reused in this loop
        let mut file_ids = vec![];
        let mut extend_data_buffer = vec![0u8; 128];
        let mut write_data_buffer = vec![];
        let mut encode_data_buffer = vec![];
        let mut content = vec![];

        // sort the files so that file ids always end up reversed below
        files.sort_unstable_by(|a, b| b.0.cmp(&a.0));

        // track how many segments are added in this batch
        let mut added_segments = 0u32;
        'trigrams: for trigram in 0u32..TRIGRAM_RANGE as u32 {
            let stamp = std::time::Instant::now();
            // Invert batch into REVERSED index lists
            file_ids.clear();
            for (id, grams) in files.iter() {
                if skipped.contains(id) {
                    continue
                }
                if *grams.get(trigram as usize).unwrap() {
                    file_ids.push(*id);
                }
            }
            if file_ids.is_empty() {
                continue
            }

            // file_ids.sort_unstable_by(|a, b| b.cmp(&a));
            invert_time += stamp.elapsed().as_secs_f64();
            let stamp = std::time::Instant::now();

            // move through segments until we get the last one
            let mut address = SegmentAddress::Trigram(trigram);
            let seg_stamp = std::time::Instant::now();
            let mut active_segment = self.read_initial_segment(trigram)?;
            seg_read_time += seg_stamp.elapsed().as_secs_f64();
            while let Some(extension) = active_segment.extension() {
                let seg_stamp = std::time::Instant::now();
                content.clear();
                active_segment.decode_into(&mut content);
                if let Some(&peak) = content.last() {
                    loop {
                        let next = match file_ids.last() {
                            Some(next) => *next,
                            None => continue 'trigrams,
                        };

                        if peak < next {
                            break
                        } else {
                            file_ids.pop();
                            skipped.insert(next);
                        }
                    }
                }

                active_segment = self.read_extended_segment(extension)?;
                seg_read_time += seg_stamp.elapsed().as_secs_f64();
                address = SegmentAddress::Segment(extension);
            }

            // Load the existing segment content
            content.clear();
            let mut encoded_size = active_segment.decode_into(&mut content);
            if let Some(&peak) = content.last() {
                loop {
                    let next = match file_ids.last() {
                        Some(next) => *next,
                        None => continue 'trigrams,
                    };

                    if peak < next {
                        break
                    } else {
                        file_ids.pop();
                        skipped.insert(next);
                    }
                }
            }

            // pack in as many more values as we can
            let mut changed = false;
            let limit = active_segment.payload_bytes();
            while let Some(index) = file_ids.pop() {
                let new_size = encoded_size + cost_to_add(&content, index);
                if new_size <= limit {
                    content.push(index);
                    encoded_size = new_size;
                    changed = true;
                    continue
                } else {
                    file_ids.push(index);
                    break;
                }
            }

            // if we changed the content of that segment add the write op
            if changed {
                let stamp = std::time::Instant::now();
                encode_data_buffer.clear();
                encode_into(&content, &mut encode_data_buffer);
                write_data_buffer.resize(encode_data_buffer.len() + 128, 0);
                writer.write_all(&postcard::to_slice_cobs(&UpdateOperations::WriteSegment { segment: address, data: &encode_data_buffer }, &mut write_data_buffer)?)?;
                op_write_time += stamp.elapsed().as_secs_f64();
            }

            // if there are more indices add extensions
            while !file_ids.is_empty() {
                // add a new segment per iteration of this loop
                added_segments += 1;
                let new_segment = self.extended_segments + added_segments;

                //
                content.clear();
                let mut encoded_size = 0;
                while let Some(index) = file_ids.pop() {
                    let new_size = encoded_size + cost_to_add(&content, index);
                    if new_size <= self.extended_segment_size - POINTER_SIZE as u32{
                        content.push(index);
                        encoded_size = new_size;
                        continue
                    } else {
                        file_ids.push(index);
                        break;
                    }
                }

                let new_address = SegmentAddress::Segment(new_segment);
                let stamp = std::time::Instant::now();
                writer.write_all(&postcard::to_slice_cobs(&UpdateOperations::ExtendSegment { segment: address, new_segment }, &mut extend_data_buffer)?)?;
                encode_data_buffer.clear();
                encode_into(&content, &mut encode_data_buffer);
                write_data_buffer.resize(encode_data_buffer.len() + 128, 0);
                writer.write_all(&postcard::to_slice_cobs(&UpdateOperations::WriteSegment { segment: new_address, data: &encode_data_buffer }, &mut write_data_buffer)?)?;
                op_write_time += stamp.elapsed().as_secs_f64();
                address = new_address;
            }
            build_ops_time += stamp.elapsed().as_secs_f64();
        }

        println!("Operation file built {:.2} [in {:.2}, bu {:.2}, bu>wr {:.2}, bu>sr {:.2}]", start.elapsed().as_secs_f64(), invert_time, build_ops_time, op_write_time, seg_read_time);
        let stamp = std::time::Instant::now();

        // Write where the finalization is
        let mut write_buffer = writer.into_inner()?;
        let ending_offset = write_buffer.seek(SeekFrom::Current(0))?;
        write_buffer.seek(SeekFrom::Start(0))?;
        write_buffer.write_u64::<LittleEndian>(ending_offset)?;

        // Write finalization info
        write_buffer.seek(SeekFrom::Start(ending_offset))?;
        write_buffer.write_u32::<LittleEndian>(added_segments)?;
        write_buffer.write_u8(1)?;

        // Sync
        write_buffer.sync_all()?;

        println!("Operation file synced {:.2}", stamp.elapsed().as_secs_f64());
        let stamp = std::time::Instant::now();

        // Apply operation set
        // self.apply_operations(write_buffer, self.extended_segments + added_segments).context("apply operations")?;
        // println!("Operation file applied {:.2}", stamp.elapsed().as_secs_f64());
        return Ok((write_buffer, self.extended_segments + added_segments))
    }

    fn check_and_apply_operations(&mut self, mut buffer: File) -> Result<()> {
        // Read the length
        let offset = buffer.read_u64::<LittleEndian>()?;
        if offset == 0 {
            std::fs::remove_file(&self.edit_buffer_location)?;
            return Ok(())
        }

        // read the resize and commit marker
        buffer.seek(SeekFrom::Start(offset))?;
        let added_segments = buffer.read_u32::<LittleEndian>()?;
        let commit = buffer.read_u8()?;
        if commit != 1 {
            std::fs::remove_file(&self.edit_buffer_location)?;
            return Ok(())
        }

        // apply
        self.apply_operations(buffer, self.extended_segments + added_segments)
    }

    // #[instrument]
    pub fn apply_operations(&mut self, mut source: File, extended_segments: u32) -> Result<()> {
        let stamp = std::time::Instant::now();
        source.seek(SeekFrom::Start(0)).context("reseting the operation source")?;

        // resize first
        let new_size = HEADER_SIZE + TRIGRAM_RANGE * self.initial_segment_size as u64 + extended_segments as u64 * self.extended_segment_size as u64;
        let data = std::fs::OpenOptions::new().write(true).read(true).truncate(false).open(&self.data_location)?;
        data.set_len(new_size).context("Resizing data file")?;
        self.data = unsafe { memmap2::MmapMut::map_mut(&data)? };
        self.data.advise(memmap2::Advice::Random)?;
        self.extended_segments = extended_segments;

        let resize_time = stamp.elapsed().as_secs_f64();
        let apply_stamp = std::time::Instant::now();

        // apply operations
        let mut reader = std::io::BufReader::new(source);
        let offset = reader.read_u64::<LittleEndian>().context("reading change offset value")?;
        let mut bytes_read = 8;
        let mut buffer = vec![];
        while bytes_read < offset {
            buffer.clear();
            bytes_read += reader.read_until(0, &mut buffer)? as u64;
            if !buffer.is_empty() {
                let operation: UpdateOperations = postcard::from_bytes_cobs(&mut buffer).context("parsing operation")?;
                self.apply_operation(operation).context("applying operation")?;
            }
        }

        let apply_time = apply_stamp.elapsed().as_secs_f64();
        let flush_stamp = std::time::Instant::now();

        // Commit operations
        self.data.flush()?;

        let flush_time = flush_stamp.elapsed().as_secs_f64();
        println!("time to apply {:.2} [rs {:.2}, ap {:.2}, fl {:.2}]", stamp.elapsed().as_secs_f64(), resize_time, apply_time, flush_time);

        // Clear edit buffer
        std::fs::remove_file(&self.edit_buffer_location)?;
        return Ok(())
    }

    // #[instrument]
    fn apply_operation(&mut self, operation: UpdateOperations) -> Result<()> {
        match operation {
            UpdateOperations::WriteSegment { segment, data } => {
                let segment_offset = self.get_segment_offset(segment) as usize;
                self.data[segment_offset..segment_offset + data.len()].copy_from_slice(&data[..]);
                // self.data.seek(SeekFrom::Start(segment_offset))?;
                // self.data.write(&data)?;
            },
            UpdateOperations::ExtendSegment { segment, new_segment } => {
                let segment_offset = self.get_segment_offset(segment);
                let segment_length = match segment {
                    SegmentAddress::Trigram(_) => self.initial_segment_size as u64,
                    SegmentAddress::Segment(_) => self.extended_segment_size as u64,
                };
                let write_location = segment_offset + segment_length - POINTER_SIZE;
                // self.data.seek(SeekFrom::Start(write_location))?;
                // self.data.write(&new_segment.to_le_bytes())?;
                self.data[write_location as usize..write_location as usize+4].copy_from_slice(&new_segment.to_le_bytes());
            },
        }
        Ok(())
    }

}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use anyhow::{Result, Context};
    use bitvec::vec::BitVec;
    use itertools::Itertools;
    use rand::{Rng, SeedableRng};

    use crate::worker::filter::ExtensibleTrigramFile;

    use super::TRIGRAM_RANGE;

    #[test]
    fn simple_save_and_load() -> Result<()> {
        // build test data
        let mut trigrams = vec![];
        for ii in 1..102 {
            let mut data = BitVec::repeat(false, TRIGRAM_RANGE as usize);
            data.set(500, true);
            if ii < 50 {
                data.set(0, true);
            } else {
                data.set(TRIGRAM_RANGE as usize - 1, true);
            }
            trigrams.push((ii, data));
        }


        // write it
        let tempdir = tempfile::tempdir()?;
        let location = tempdir.path().join("test");
        {
            let mut file = ExtensibleTrigramFile::new(&location, 128, 128)?;
            let x = file.write_batch(&mut trigrams).context("write batch")?;
            file.apply_operations(x.0, x.1);
            assert_eq!(file.extended_segments, 0)
        }

        // Read it again
        {
            let mut file = ExtensibleTrigramFile::open(&location)?;
            for trigram in 0..TRIGRAM_RANGE as u32 {
                let values = file.read_trigram(trigram)?;
                if trigram == 0 {
                    assert_eq!(values, (1..50).collect_vec());
                } else if trigram == 500 {
                    assert_eq!(values, (1..102).collect_vec());
                } else if trigram == TRIGRAM_RANGE as u32 - 1 {
                    assert_eq!(values, (50..102).collect_vec());
                } else {
                    assert!(values.is_empty());
                }
            }
        }
        Ok(())
    }

    #[test]
    fn extended_save_and_load() -> Result<()> {
        // build test data
        let mut trigrams = vec![];
        for ii in 1..102 {
            let mut data = BitVec::repeat(false, TRIGRAM_RANGE as usize);
            data.set(500, true);
            if ii < 50 {
                data.set(0, true);
            } else {
                data.set(TRIGRAM_RANGE as usize - 1, true);
            }
            trigrams.push((ii, data));
        }

        // write it
        let tempdir = tempfile::tempdir()?;
        let location = tempdir.path().join("test");
        {
            let mut file = ExtensibleTrigramFile::new(&location, 16, 16)?;
            let x = file.write_batch(&mut trigrams)?;
            file.apply_operations(x.0, x.1);
            assert!(file.extended_segments > 0)
        }

        // Read it again
        {
            let mut file = ExtensibleTrigramFile::open(&location)?;
            for trigram in 0..(1<<24) {
                let values = file.read_trigram(trigram)?;
                if trigram == 0 {
                    assert_eq!(values, (1..50).collect_vec());
                } else if trigram == 500 {
                    assert_eq!(values, (1..102).collect_vec());
                } else if trigram == (1 << 24) - 1 {
                    assert_eq!(values, (50..102).collect_vec());
                } else {
                    assert!(values.is_empty());
                }
            }
        }
        Ok(())
    }

    #[test]
    fn multiple_writes() -> Result<()> {
        // let x = setup_global_subscriber();

        let timer = std::time::Instant::now();
        // build test data
        let mut trigrams = vec![];
        let mut prng = rand::rngs::StdRng::seed_from_u64(0);

        for ii in (1..21).rev() {
            let mut data = BitVec::repeat(false, 1 << 24);
            for jj in 0..(1 << 24) {
                data.set(jj, prng.gen_bool(0.2));
            }
            trigrams.push((ii, data));
        }
        println!("generate {:.2}", timer.elapsed().as_secs_f64());

        // write it
        let tempdir = tempfile::tempdir()?;
        let location = tempdir.path().join("test");
        {
            let timer = std::time::Instant::now();
            let mut file = ExtensibleTrigramFile::new(&location, 16, 16)?;
            println!("open new {:.2}", timer.elapsed().as_secs_f64());
            let timer = std::time::Instant::now();
            let x = file.write_batch(&mut trigrams[0..10])?;
            file.apply_operations(x.0, x.1);
            println!("write batch {:.2}", timer.elapsed().as_secs_f64());
            let timer = std::time::Instant::now();
            let x = file.write_batch(&mut trigrams[10..20])?;
            file.apply_operations(x.0, x.1);
            println!("write batch {:.2}", timer.elapsed().as_secs_f64());
        }

        // Recreate the trigrams
        let timer = std::time::Instant::now();
        {
            let mut recreated: Vec<BitVec> = vec![];
            for _ in 0..20 {
                recreated.push(BitVec::repeat(false, TRIGRAM_RANGE as usize))
            }

            let mut file = ExtensibleTrigramFile::open(&location)?;
            for trigram in 0..(1<<24) {
                let values = file.read_trigram(trigram)?;
                for index in values {
                    recreated[index as usize - 1].set(trigram as usize, true);
                }
            }

            let trigrams: HashMap<u64, BitVec> = trigrams.into_iter().collect();

            for (index, values) in recreated.into_iter().enumerate() {
                assert_eq!(*trigrams.get(&(index as u64+1)).unwrap(), values)
            }
        }
        println!("read {:.2}", timer.elapsed().as_secs_f64());
        Ok(())
    }

    #[test]
    fn duplicate_batch() -> Result<()> {
        todo!()
    }

    #[test]
    fn large_batch() -> Result<()> {
        let timer = std::time::Instant::now();
        // build test data
        let mut trigrams = vec![];
        let mut prng = rand::rngs::StdRng::seed_from_u64(0);
        for ii in 1..500 {
            let mut data = BitVec::repeat(false, 1 << 24);
            for jj in 0..(1 << 24) {
                data.set(jj, prng.gen_bool(0.2));
            }
            trigrams.push((ii, data));
        }
        println!("generate {:.2}", timer.elapsed().as_secs_f64());

        // write it
        let tempdir = tempfile::tempdir()?;
        let location = tempdir.path().join("test");
        {
            let timer = std::time::Instant::now();
            let mut file = ExtensibleTrigramFile::new(&location, 256, 1024)?;
            println!("open new {:.2}", timer.elapsed().as_secs_f64());
            let timer = std::time::Instant::now();
            let x = file.write_batch(&mut trigrams)?;
            file.apply_operations(x.0, x.1)?;
            println!("write batch {:.2}", timer.elapsed().as_secs_f64());
        }

        // // Recreate the trigrams
        // let timer = std::time::Instant::now();
        // {
        //     let mut recreated: Vec<BitVec> = vec![];
        //     for _ in 0..20 {
        //         recreated.push(BitVec::repeat(false, TRIGRAM_RANGE as usize))
        //     }

        //     let mut file = ExtensibleTrigramFile::open(&location)?;
        //     for trigram in 0..(1<<24) {
        //         let values = file.read_trigram(trigram)?;
        //         for index in values {
        //             recreated[index as usize - 1].set(trigram as usize, true);
        //         }
        //     }

        //     for (index, values) in recreated.into_iter().enumerate() {
        //         assert_eq!(trigrams[index].1, values)
        //     }
        // }
        // println!("read {}", timer.elapsed().as_secs_f64());
        Ok(())
    }
}