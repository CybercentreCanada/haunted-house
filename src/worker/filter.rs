use bitvec::vec::BitVec;
use anyhow::{Result, Context};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use log::{error, info};
use memmap2::MmapMut;
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::{Write, Seek, SeekFrom, Read, BufRead};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::RwLock;

// use crate::mark;
use crate::query::Query;
use crate::timing::{TimingCapture, mark, NullCapture};
use crate::types::FilterID;

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

fn get_next_journal(directory: &Path, id: FilterID) -> (File, PathBuf, u64) {
    loop {
        let next_id = match get_operation_journals(directory, id) {
            Ok(journals) => match journals.last() {
                Some((num, _)) => *num + 1,
                None => 0,
            },
            Err(err) => {
                error!("{err}");
                continue;
            },
        };
        let name = format!("{id}.{next_id:08}");
        let location = directory.join(name);
        let file = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&location);
        match file {
            Ok(file) => return (file, location, next_id),
            Err(_) => continue,
        }
    }
}

fn get_operation_journals(directory: &Path, id: FilterID) -> Result<Vec<(u64, PathBuf)>> {
    let id_str = format!("{id}");
    let mut listing = std::fs::read_dir(directory)?;
    let mut found = vec![];
    while let Some(file) = listing.next() {
        let file = match file {
            Ok(file) => file,
            _ => continue
        };

        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            _ => continue
        };

        if !metadata.is_file() {
            continue
        }

        let name = file.file_name();
        let name = match name.to_str() {
            Some(name) => name,
            _ => continue
        };

        let mut parts = name.split(".");
        let owner = match parts.next() {
            Some(owner) => owner,
            None => continue,
        };
        if owner != &id_str {
            continue
        }

        let code = match parts.next() {
            Some(owner) => owner,
            None => continue,
        };

        let counter: u64 = match code.parse() {
            Ok(counter) => counter,
            Err(_) => continue,
        };

        found.push((counter, file.path()));
    }

    found.sort_unstable();
    return Ok(found)
}

pub struct ExtensibleTrigramFile {
    initial_segment_size: u32,
    extended_segment_size: u32,
    data: Arc<RwLock<memmap2::MmapMut>>,
    extended_segments: u32,
    directory: PathBuf,
    location: PathBuf,
    id: FilterID,
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

struct RawSegmentInfo {
    data: Vec<u8>,
}

impl RawSegmentInfo {
    fn new(data: Vec<u8>) -> Self {
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

    pub fn new(directory: PathBuf, id: FilterID, initial_segment_size: u32, extended_segment_size: u32) -> Result<Self> {
        // prepare file
        let location = directory.join(format!("{id}"));
        let mut data = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(&location).context("Creating data file")?;
        data.set_len(HEADER_SIZE + TRIGRAM_RANGE * initial_segment_size as u64).context("Adjusting file size")?;

        // write header
        data.write_u32::<LittleEndian>(HEADER_MAGIC).context("Writing header")?;
        data.write_u32::<LittleEndian>(HEADER_SIZE as u32).context("Writing header")?;
        data.write_u32::<LittleEndian>(initial_segment_size).context("Writing header")?;
        data.write_u32::<LittleEndian>(extended_segment_size).context("Writing header")?;
        data.sync_all()?;

        // return object
        let data = unsafe { memmap2::MmapMut::map_mut(&data)? };
        data.advise(memmap2::Advice::Random)?;
        let data = Arc::new(RwLock::new(data));
        Ok(Self { initial_segment_size, extended_segment_size, data, extended_segments: 0, directory, location, id })
    }

    pub fn open(directory: PathBuf, id: FilterID) -> Result<Self> {
        // Open the file
        let location = directory.join(format!("{id}"));
        let mut data = std::fs::OpenOptions::new().write(true).read(true).truncate(false).open(&location).context("Creating data file")?;

        // Read the header
        let mut header_buffer = vec![0; HEADER_SIZE as usize];
        data.read_exact(&mut header_buffer).context("Could not read header")?;

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

        let data = unsafe { memmap2::MmapMut::map_mut(&data)? };
        data.advise(memmap2::Advice::Random)?;
        let data = Arc::new(RwLock::new(data));
        let edit_buffers = get_operation_journals(&directory, id)?;
        let mut filter = Self{ initial_segment_size, extended_segment_size, data, extended_segments, directory, location, id };

        if !edit_buffers.is_empty() {
            info!("Filter {id} found {} unfinished write buffers. Applying...", edit_buffers.len());
        }

        // Check for a edit buffer
        for (num, buffer) in edit_buffers {
            info!("Filter {id} applying edit buffer {num}.");
            filter.check_and_apply_operations(buffer, num)?;
        }
        info!("Filter {id} ready.");
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
        let data = self.data.blocking_read();
        let data = data[location..location+self.initial_segment_size as usize].to_vec();
        return Ok(RawSegmentInfo::new(data))
    }

    // #[instrument]
    fn read_extended_segment(&self, segment: u32) -> Result<RawSegmentInfo> {
        let location = self.get_extended_segment_offset(segment) as usize;
        let data = self.data.blocking_read();
        let data = data[location..location+self.extended_segment_size as usize].to_vec();
        return Ok(RawSegmentInfo::new(data))
    }

    // #[instrument]
    pub fn write_batch(&self, files: &mut [(u64, BitVec)], timing: impl TimingCapture) -> Result<(File, u64, u32)> {
        let capture = mark!(timing, "write_batch");
        // prepare the buffer for operations
        let (write_buffer, _buffer_location, buffer_counter) = get_next_journal(&self.directory, self.id);
        // let write_buffer = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(&self.edit_buffer_location)?;
        let mut skipped = HashSet::<u64>::new();

        // Leave room for the end offset at the start
        let mut writer = std::io::BufWriter::new(write_buffer);
        writer.write_u64::<LittleEndian>(0)?;

        // Timing information
        // let mut invert_time: f64 = 0.0;
        // let mut build_ops_time: f64 = 0.0;
        // let mut op_write_time: f64 = 0.0;
        // let mut seg_read_time: f64 = 0.0;

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
            {
                let _mark = mark!(capture, "invert");
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
            }

            // file_ids.sort_unstable_by(|a, b| b.cmp(&a));
            let operations_span = mark!(capture, "build_ops");

            // move through segments until we get the last one
            let mut address = SegmentAddress::Trigram(trigram);
            let mut active_segment = {
                let _span = mark!(operations_span, "read_segment");
                self.read_initial_segment(trigram)?
            };
            while let Some(extension) = active_segment.extension() {
                let _span = mark!(operations_span, "read_segment");
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
                let _span = mark!(operations_span, "write_operation");
                encode_data_buffer.clear();
                encode_into(&content, &mut encode_data_buffer);
                write_data_buffer.resize(encode_data_buffer.len() + 128, 0);
                writer.write_all(&postcard::to_slice_cobs(&UpdateOperations::WriteSegment { segment: address, data: &encode_data_buffer }, &mut write_data_buffer)?)?;
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
                let _span = mark!(operations_span, "write_operation");
                writer.write_all(&postcard::to_slice_cobs(&UpdateOperations::ExtendSegment { segment: address, new_segment }, &mut extend_data_buffer)?)?;
                encode_data_buffer.clear();
                encode_into(&content, &mut encode_data_buffer);
                write_data_buffer.resize(encode_data_buffer.len() + 128, 0);
                writer.write_all(&postcard::to_slice_cobs(&UpdateOperations::WriteSegment { segment: new_address, data: &encode_data_buffer }, &mut write_data_buffer)?)?;
                address = new_address;
            }
        }

        // println!("Operation file built {:.2} [in {:.2}, bu {:.2}, bu>wr {:.2}, bu>sr {:.2}]", start.elapsed().as_secs_f64(), invert_time, build_ops_time, op_write_time, seg_read_time);
        // let stamp = std::time::Instant::now();
        let _mark = mark!(capture, "operation_finalize");

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

        // println!("Operation file synced {:.2}", stamp.elapsed().as_secs_f64());
        // let stamp = std::time::Instant::now();

        // Apply operation set
        // self.apply_operations(write_buffer, self.extended_segments + added_segments).context("apply operations")?;
        // println!("Operation file applied {:.2}", stamp.elapsed().as_secs_f64());
        return Ok((write_buffer, buffer_counter, self.extended_segments + added_segments))
    }

    fn check_and_apply_operations(&mut self, location: PathBuf, buffer_counter: u64) -> Result<()> {
        // Check file is above minimum size
        let mut buffer = File::open(&location).context("could not open edit buffer")?;
        if buffer.metadata()?.len() < 16 {
            std::fs::remove_file(&location)?;
            return Ok(())
        }

        // Read the length
        let offset = buffer.read_u64::<LittleEndian>()?;
        if offset == 0 {
            std::fs::remove_file(&location)?;
            return Ok(())
        }

        // read the resize and commit marker
        buffer.seek(SeekFrom::Start(offset))?;
        let added_segments = buffer.read_u32::<LittleEndian>()?;
        let commit = buffer.read_u8()?;
        if commit != 1 {
            std::fs::remove_file(&location)?;
            return Ok(())
        }

        // apply
        self.apply_operations(buffer, buffer_counter, self.extended_segments + added_segments, NullCapture::new())
    }

    // #[instrument]
    pub fn apply_operations(&mut self, mut source: File, counter: u64, extended_segments: u32, timing: impl TimingCapture) -> Result<()> {
        let capture = mark!(timing, "apply_operations");

        // Resize the mapping, but only if we need to
        if extended_segments > self.extended_segments {
            let _mark = mark!(capture, "resize");

            let new_size = HEADER_SIZE + TRIGRAM_RANGE * self.initial_segment_size as u64 + extended_segments as u64 * self.extended_segment_size as u64;
            let data = std::fs::OpenOptions::new().write(true).read(true).truncate(false).open(&self.location)?;
            data.set_len(new_size).context("Resizing data file")?;
            let data = unsafe { memmap2::MmapMut::map_mut(&data)? };
            data.advise(memmap2::Advice::Random)?;
            let old_data = self.data.clone();
            self.data = Arc::new(RwLock::new(data));
            self.extended_segments = extended_segments;

            // If we have a duplicate mapping, use the old one for an opertunistic flush
            if counter > 0 {
                self.flush_threaded(Some(old_data), Some(counter - 1));
            }
        }

        // Apply new operations
        {
            // Capture time for this section
            let _mark = mark!(capture, "apply");

            // Read how much data to expect
            source.seek(SeekFrom::Start(0)).context("reseting the operation source")?;
            let mut reader = std::io::BufReader::new(source);
            let offset = reader.read_u64::<LittleEndian>().context("reading change offset value")?;

            // Start the read count ahead by the length
            let mut bytes_read = 8;

            let mut buffer = vec![];
            let mut mmap = self.data.blocking_write();
            while bytes_read < offset {
                buffer.clear();
                bytes_read += reader.read_until(0, &mut buffer)? as u64;
                if !buffer.is_empty() {
                    let operation: UpdateOperations = postcard::from_bytes_cobs(&mut buffer).context("parsing operation")?;
                    match operation {
                        UpdateOperations::WriteSegment { segment, data } => {
                            let segment_offset = self.get_segment_offset(segment) as usize;
                            mmap[segment_offset..segment_offset + data.len()].copy_from_slice(&data[..]);
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
                            mmap[write_location as usize..write_location as usize+4].copy_from_slice(&new_segment.to_le_bytes());
                        },
                    }
                }
            }
        }

        return Ok(())
    }

    pub fn outstanding_journals(&self) -> Result<usize> {
        Ok(get_operation_journals(&self.directory, self.id)?.len())
    }

    pub fn flush_threaded(&self, map: Option<Arc<RwLock<MmapMut>>>, counter: Option<u64>) -> JoinHandle<()> {
        // get a memory mapping
        let data = match map {
            Some(data) => data,
            None => self.data.clone(),
        };

        // Commit operations
        std::thread::spawn({
            let directory = self.directory.clone();
            let id = self.id.clone();
            move || {
                // Wait until all outstanding data has flushed
                {
                    let data = data.blocking_read();
                    if let Err(err) = data.flush() {
                        error!("Flush error: {err}");
                        return
                    };
                }

                // Get journals still on disk
                let buffers = match get_operation_journals(&directory, id) {
                    Ok(buffers) => buffers,
                    Err(err) => {
                        error!("Post Flush error: {err}");
                        return
                    },
                };

                // delete journals before the counter we were given
                for (number, location) in buffers {
                    if let Some(counter) = counter {
                        if number > counter {
                            continue
                        }
                    }

                    if let Err(err) = std::fs::remove_file(&location) {
                        if location.exists() {
                            error!("Cleanup error: {err}");
                        }
                    }
                }
            }
        })
    }

    pub fn flush_blocking(&self) {
        let handle = self.flush_threaded(None, None);
        if let Err(err) = handle.join() {
            error!("Flush error {err:?}");
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use anyhow::{Result, Context};
    use bitvec::vec::BitVec;
    use itertools::Itertools;
    use rand::{Rng, SeedableRng, thread_rng};

    use crate::timing::{NullCapture, Capture};
    use crate::types::FilterID;
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
        let id = FilterID::from(1);
        let location = tempdir.path().to_path_buf();
        {
            let mut file = ExtensibleTrigramFile::new(location.clone(), id, 128, 128)?;
            let x = file.write_batch(&mut trigrams, NullCapture::new()).context("write batch")?;
            file.apply_operations(x.0, x.1, x.2, NullCapture::new())?;
            assert_eq!(file.extended_segments, 0)
        }

        // Read it again
        {
            let file = ExtensibleTrigramFile::open(location, id)?;
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
        let id = FilterID::from(1);
        let location = tempdir.path().to_path_buf();
        {
            let mut file = ExtensibleTrigramFile::new(location.clone(), id, 16, 16)?;
            let x = file.write_batch(&mut trigrams, NullCapture::new())?;
            file.apply_operations(x.0, x.1, x.2, NullCapture::new())?;
            assert!(file.extended_segments > 0)
        }

        // Read it again
        {
            let file = ExtensibleTrigramFile::open(location, id)?;
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
            let raw = data.as_raw_mut_slice();
            for part in raw {
                *part = prng.gen::<usize>() & prng.gen::<usize>() & prng.gen::<usize>();
            }
            // for jj in 0..(1 << 24) {
            //     data.set(jj, prng.gen_bool(0.2));
            // }
            trigrams.push((ii, data));
        }
        println!("generate {:.2}", timer.elapsed().as_secs_f64());

        trigrams.sort();

        // write it
        let tempdir = tempfile::tempdir()?;
        let location = tempdir.path().to_path_buf();
        let id = FilterID::from(1);
        let capture = Capture::new();
        {
            let timer = std::time::Instant::now();
            let mut file = ExtensibleTrigramFile::new(location.clone(), id, 16, 16)?;
            println!("open finished {:.2}", timer.elapsed().as_secs_f64());
            let timer = std::time::Instant::now();
            let x = file.write_batch(&mut trigrams[0..10], &capture)?;
            println!("write_batch 1 finished");
            file.apply_operations(x.0, x.1, x.2, &capture)?;
            println!("apply_operations 1 finished {:.2}", timer.elapsed().as_secs_f64());
            let timer = std::time::Instant::now();
            let x = file.write_batch(&mut trigrams[10..20], &capture)?;
            println!("write_batch 2 finished");
            file.apply_operations(x.0, x.1, x.2, &capture)?;
            println!("apply_operations 2 finished {:.2}", timer.elapsed().as_secs_f64());
        }

        capture.print();

        // Recreate the trigrams
        let timer = std::time::Instant::now();
        {
            let mut recreated: Vec<BitVec> = vec![];
            for _ in 0..20 {
                recreated.push(BitVec::repeat(false, TRIGRAM_RANGE as usize))
            }

            let file = ExtensibleTrigramFile::open(location.clone(), id)?;
            for trigram in 0..(1<<24) {
                let values = file.read_trigram(trigram)?;
                for index in values {
                    recreated[index as usize - 1].set(trigram as usize, true);
                }
            }

            let trigrams: HashMap<u64, BitVec> = trigrams.into_iter().collect();

            for (index, values) in recreated.into_iter().enumerate() {
                assert!(*trigrams.get(&(index as u64+1)).unwrap() == values, "{index}")
            }
        }
        println!("read {:.2}", timer.elapsed().as_secs_f64());
        Ok(())
    }

    #[test]
    fn duplicate_batch() -> Result<()> {
        let mut data = BitVec::repeat(false, 1 << 24);
        let raw = data.as_raw_mut_slice();
        let mut prng = thread_rng();
        for part in raw {
            *part = prng.gen::<usize>() & prng.gen::<usize>() & prng.gen::<usize>();
        }

        let tempdir = tempfile::tempdir()?;
        let id = FilterID::from(1);
        let location = tempdir.path().to_path_buf();
        {
            println!("### First");
            let mut file = ExtensibleTrigramFile::new(location.clone(), id, 16, 16)?;
            let mut trigrams = vec![(1, data.clone())];
            let x = file.write_batch(&mut trigrams, NullCapture::new())?;
            file.apply_operations(x.0, x.1, x.2, NullCapture::new())?;
        }

        {
            println!("### Duplicate");
            let size = std::fs::metadata(&location)?.len();
            let mut file = ExtensibleTrigramFile::open(location.clone(), id)?;
            let mut trigrams = vec![(1, data.clone())];
            let x = file.write_batch(&mut trigrams, NullCapture::new())?;
            file.apply_operations(x.0, x.1, x.2, NullCapture::new())?;
            assert_eq!(size, std::fs::metadata(&location)?.len());
        }

        {
            let mut recreated: BitVec<usize> = BitVec::repeat(false, TRIGRAM_RANGE as usize);

            let file = ExtensibleTrigramFile::open(location, id)?;
            for trigram in 0..(1<<24) {
                let values = file.read_trigram(trigram)?;
                if values.contains(&1) {
                    recreated.set(trigram as usize, true);
                }
            }

            assert!(data == recreated);
        }


        return Ok(())
    }

    #[test]
    fn large_batch() -> Result<()> {
        // build test data
        let mut trigrams = vec![];
        let mut prng = rand::rngs::StdRng::seed_from_u64(0);
        for ii in 1..500 {
            let mut data = BitVec::repeat(false, 1 << 24);
            for part in data.as_raw_mut_slice() {
                *part = prng.gen::<usize>() & prng.gen::<usize>() & prng.gen::<usize>();
            }
                // for jj in 0..(1 << 24) {
            //     data.set(jj, prng.gen_bool(0.2));
            // }
            trigrams.push((ii, data));
        }

        // write it
        let tempdir = tempfile::tempdir()?;
        let location = tempdir.path().to_path_buf();
        let id = FilterID::from(1);
        let capture = Capture::new();
        {
            let mut file = ExtensibleTrigramFile::new(location, id, 256, 1024)?;
            let x = file.write_batch(&mut trigrams, &capture)?;
            file.apply_operations(x.0, x.1, x.2, &capture)?;
        }

        capture.print();
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