//! A trigram index format that favours ingestion speed over searching speed
//! while still providing fairly good search speed.
//!
//! The index file contains four parts:
//!  - A fixed length header identifiying the format and describing parameters required to parse the rest of the file.
//!  - A hint table of segment ids letting writers jump to the last active segment for a given trigram.
//!  - A fixed size table of segments, one for each possible trigram.
//!  - A dynamic table of segments that can be used to extend the file quickly.
//!
//! Writes to the index file are done via a two stage commit where:
//!  - The set of all change to be made are written to a journal file which is then synced.
//!  - The changes are then applied to the memory map of the index file.
//!  - When convinent (or when triggered explicitly, controlled externally by a timeout) the
//!    memory map is flushed and the journal files are erased.
//!
//! This file defines access to the file format using syncronous apis.
//! The async interface is defined in filter_worker.rs
//!

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
use std::iter::Peekable;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;

use crate::query::Query;
use crate::timing::{TimingCapture, mark, NullCapture};
use crate::types::FilterID;

use super::encoding::{cost_to_add, encode_into, decode_into, encoded_number_size};
use super::trigrams::{TrigramSet, TrigramIterator};

/// A convinence constant defining how many possible trigrams exist
const TRIGRAM_RANGE: u64 = 1 << 24;
/// Number of bytes used for each segment id
const POINTER_SIZE: u64 = 4;
/// Size of the fixed length header
const HEADER_SIZE: u64 = 4 + 4 + 4 + 4;
/// Size of segment id table identifying the last segment in use for each trigram
const PREFIX_SIZE: u64 = TRIGRAM_RANGE * POINTER_SIZE;
/// A magic number used in the header, generated randomly once.
const HEADER_MAGIC: u32 = 0x0e3d9def;

/// Encapsulates a single filter file.
pub struct ExtensibleTrigramFile {
    /// How large are the manditory segments
    initial_segment_size: u32,
    /// How large are the extension segments
    extended_segment_size: u32,
    /// Memory map of the trigram index
    data: memmap2::MmapMut,
    /// How many extension segments are there
    extended_segments: u32,
    /// Directory where the index file and all journals are stored, may be shared with other files.
    directory: PathBuf,
    /// Path to the index file
    location: PathBuf,
    /// Id used to map this filter file to the corresponding database with file data
    id: FilterID,
}

/// Label that can reference any segment, both manditory and extension
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
enum SegmentAddress {
    /// Describes a manditory segment, value inside is the trigram value
    Trigram(u32),
    /// Describes an extended segment, value inside is the id of the segment
    Segment(u32),
}

/// An entry in the operation journal
#[derive(Serialize, Deserialize)]
enum UpdateOperations<'a> {
    /// Overwrite the content of a segment
    WriteSegment{
        /// Which segment should be overwritten
        segment: SegmentAddress,
        /// The data to write (not copied, stored as a reference to the external buffer for efficency)
        data: &'a[u8]
    },
    /// Extend the chain of segments for this trigram into this new segment
    ExtendSegment{
        /// Which trigram is getting an additional segment
        trigram: u32,
        /// The last segment currently assigned to that trigram
        segment: SegmentAddress,
        /// ID of the new segment to be assigned
        new_segment: u32
    },
}

/// A wrapper for a pointer to a segment of memmapped data that provides
/// methods for parsing the content of the buffer
struct RawSegmentInfo<'a> {
    /// Underlying pointer
    data: &'a [u8],
}

impl<'a> RawSegmentInfo<'a> {
    /// Wrap pointer in this struct
    fn new(data: &'a [u8]) -> Self {
        RawSegmentInfo { data }
    }

    /// How much of the buffer is used for payload
    fn payload_bytes(&self) -> u32 {
        self.data.len() as u32 - POINTER_SIZE as u32
    }

    /// Parse encoded data into a provided buffer
    fn decode_into(&self, buffer: &mut Vec<u64>) -> u32 {
        decode_into(&self.data[0..self.data.len() - POINTER_SIZE as usize], buffer)
    }

    /// Read the extension pointer at the end of this segment if set
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

/// Helper function to remove a file and succeed when missing
fn remove_file(path: &Path) -> Result<()> {
    if let Err(err) = std::fs::remove_file(path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            return Err(err.into());
        }
    };
    return Ok(())
}

impl ExtensibleTrigramFile {
    /// Create a new trigram index file.
    ///
    /// Fails if this trigram file already exists.
    pub fn new(directory: PathBuf, id: FilterID, initial_segment_size: u32, extended_segment_size: u32) -> Result<Self> {
        // prepare file
        let location = directory.join(format!("{id}"));
        let mut data = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(&location).context("Creating data file")?;
        data.set_len(HEADER_SIZE + PREFIX_SIZE + TRIGRAM_RANGE * initial_segment_size as u64).context("Adjusting file size")?;

        // write header
        data.write_u32::<LittleEndian>(HEADER_MAGIC).context("Writing header")?;
        data.write_u32::<LittleEndian>(HEADER_SIZE as u32).context("Writing header")?;
        data.write_u32::<LittleEndian>(initial_segment_size).context("Writing header")?;
        data.write_u32::<LittleEndian>(extended_segment_size).context("Writing header")?;
        data.sync_all()?;

        // return object
        let data = unsafe { memmap2::MmapMut::map_mut(&data)? };
        data.advise(memmap2::Advice::Random)?;
        Ok(Self { initial_segment_size, extended_segment_size, data, extended_segments: 0, directory, location, id })
    }

    /// Open an existing trigram file.
    ///
    /// Applies any outstanding operation journals.
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
        if metadata.len() < HEADER_SIZE + PREFIX_SIZE + initial_segment_size as u64 * TRIGRAM_RANGE {
            return Err(anyhow::anyhow!("Corrupt data file: below minimum size for configuration"));
        }
        let extended_size = metadata.len() - HEADER_SIZE - initial_segment_size as u64 * TRIGRAM_RANGE;
        let extended_segments = (extended_size/extended_segment_size as u64) as u32;

        let data = unsafe { memmap2::MmapMut::map_mut(&data)? };
        data.advise(memmap2::Advice::Random)?;
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

    /// Delete the indicated trigram file and all related journal files.
    pub fn delete(directory: PathBuf, id: FilterID) -> Result<()> {
        let journals = get_operation_journals(&directory, id)?;
        for (_, path) in journals {
            remove_file(&path)?;
        }
        remove_file(&directory.join(format!("{id}")))?;
        return Ok(())
    }

    /// Read the file list for a single trigram
    pub fn read_trigram(&self, trigram: u32) -> Result<Vec<u64>> {
        let mut output = vec![];
        // Get and parse the manditory segment for this trigram
        let mut segment = self.read_initial_segment(trigram).context("reading initial segment")?;
        segment.decode_into(&mut output);
        // Continue parsing segments until there are no more linked
        while let Some(extension) = segment.extension() {
            segment = self.read_extended_segment(extension).context("reading extended segment")?;
            segment.decode_into(&mut output);
        }
        return Ok(output);
    }

    /// Calculate the intersection of the file lists for all the given trigrams
    pub fn read_trigrams(&self, trigrams: Vec<u32>) -> Result<Vec<u64>> {
        // Handle corner cases
        if trigrams.is_empty() {
            return Ok(vec![])
        }
        if trigrams.len() == 1 {
            return self.read_trigram(trigrams[0])
        }

        // build a collection if iterators that will navigate the linked list of segments
        // yielding file ids in order. They will only read segments when the current one is
        // exhausted, this potentially lets us avoid loading unused memory pages.
        let mut sources = vec![];
        for trigram in trigrams {
            sources.push(TrigramCursor::new(self, trigram)?.peekable());
        }

        // Get the first file that might be in all the file lists
        let mut candidate = match sources[0].peek() {
            Some(item) => *item,
            None => return Ok(vec![]),
        };

        // Loop over all the cursors until all files have been considered
        let mut output = vec![];
        'next_candidate: loop {
            // For the current candidate value check all the file lists to see if they all have it
            'next_cursor: for cursor in &mut sources {
                // This loop iterates through values in the current cursor until we catch up with the candidate value
                loop {
                    let next = match cursor.peek() {
                        Some(next) => *next,
                        // if we have reached the end of any of the cursors we can terminate this
                        // search and return whatever we have found up until now
                        None => break 'next_candidate,
                    };

                    match next.cmp(&candidate) {
                        // if the current value on this cursor is behind the candidate keep
                        // moving forward until we catch up with the candidate, all the values we are skipping
                        // over must be missing in at least one other cursor for this to happen
                        std::cmp::Ordering::Less => { cursor.next(); continue },
                        // If this cursor has the candidate in it, move to the next cursor, candidate is still valid
                        std::cmp::Ordering::Equal => { continue 'next_cursor },
                        // If this cursor has passed the candidate the candidate is invalid (it needs to be
                        // in all of the cursors) take the current value as the new candidate and restart
                        // the loop over the cursors
                        std::cmp::Ordering::Greater => { candidate = next; continue 'next_candidate },
                    }
                }
            }

            // If we have checked all the cursors and haven't skipped to the next iteration
            // of the 'next_candidate loop then we have found this candidates in all the cursors
            output.push(candidate);

            // advance to the next file ID as a candidate. It doesn't matter if the cursors don't
            // actually have this value, since we are about to check them.
            candidate += 1;
        }
        return Ok(output)
    }

    /// Run a query over the trigram table.
    /// Setup a hash table as a cache, so that we don't search the same literal repeatedly.
    pub fn query(&self, query: &Query) -> Result<Vec<u64>> {
        let mut cache = Default::default();
        self._query(query, &mut cache)
    }

    /// Actual logic for the 'query' methods
    fn _query(&self, query: &Query, cache: &mut HashMap<Vec<u8>, Vec<u64>>) -> Result<Vec<u64>> {
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

    /// Get the offset within the file map for a segment start
    fn get_segment_offset(&self, address: SegmentAddress) -> u64 {
        match address {
            SegmentAddress::Trigram(address) => self.get_initial_segment_offset(address),
            SegmentAddress::Segment(address) => self.get_extended_segment_offset(address),
        }
    }

    /// Get the memory offset for the start of a manditory segment by trigram
    fn get_initial_segment_offset(&self, trigram: u32) -> u64 {
        HEADER_SIZE + PREFIX_SIZE + trigram as u64 * self.initial_segment_size as u64
    }

    /// Get the memory offset for the start of an extended segment
    fn get_extended_segment_offset(&self, address: u32) -> u64 {
        HEADER_SIZE + PREFIX_SIZE + TRIGRAM_RANGE * self.initial_segment_size as u64 + (address - 1) as u64 * self.extended_segment_size as u64
    }

    /// Get the raw location in the hint table that holds the pointer to the last segment for the given trigram
    fn get_tail_hint_offset(&self, trigram: u32) -> u64 {
        HEADER_SIZE + trigram as u64 * 4
    }

    /// Get the value of the hint table for the given trigram
    fn get_tail_hint(&self, trigram: u32) -> SegmentAddress {
        // let data = self.data.blocking_read();
        let address = self.get_tail_hint_offset(trigram) as usize;
        let value = u32::from_le_bytes(self.data[address..address+4].try_into().unwrap());
        if value == 0 {
            SegmentAddress::Trigram(trigram)
        } else {
            SegmentAddress::Segment(value)
        }
    }

    /// Read the content of the manditory initial segment for a trigram
    fn read_initial_segment(&self, trigram: u32) -> Result<RawSegmentInfo> {
        let location = self.get_initial_segment_offset(trigram) as usize;
        let data = &self.data[location..location+self.initial_segment_size as usize];
        return Ok(RawSegmentInfo::new(data))
    }

    /// Read the content of an extended segment
    fn read_extended_segment(&self, segment: u32) -> Result<RawSegmentInfo> {
        let location = self.get_extended_segment_offset(segment) as usize;
        let data = &self.data[location..location+self.extended_segment_size as usize];
        return Ok(RawSegmentInfo::new(data))
    }

    /// Read the content of a segment
    fn read_segment(&self, segment: SegmentAddress) -> Result<RawSegmentInfo> {
        match segment {
            SegmentAddress::Trigram(trigram) => self.read_initial_segment(trigram),
            SegmentAddress::Segment(segment) => self.read_extended_segment(segment),
        }
    }

    /// Prepare an operation to write a set of file trigrams to this file
    pub fn write_batch(&self, files: &mut [(u64, TrigramSet)], timing: impl TimingCapture) -> Result<(File, u64, u32)> {
        let capture = mark!(timing, "write_batch");
        // prepare the buffer for operations
        let (write_buffer, _buffer_location, buffer_counter) = {
            let _mark = mark!(capture, "get_journal");
            get_next_journal(&self.directory, self.id)
        };
        // let write_buffer = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(&self.edit_buffer_location)?;
        let mut skipped = HashSet::<u64>::new();

        // Leave room for the end offset at the start
        let mut writer = std::io::BufWriter::new(write_buffer);
        writer.write_u64::<LittleEndian>(0)?;

        // Buffers reused in this loop
        let mut file_ids = vec![];
        let mut extend_data_buffer = vec![0u8; 128];
        let mut write_data_buffer = vec![];
        let mut encode_data_buffer = vec![];
        let mut content = vec![];

        // sort the files so that file ids always end up reversed below
        let mut collector = {
            let _mark = mark!(capture, "prepare_inputs");
            files.sort_unstable_by(|a, b| b.0.cmp(&a.0));

            IdCollector {
                acceptable: 0,
                iterators: files.iter().map(|(id, row)|(*id, row.iter().peekable())).collect_vec(),
            }
        };

        // track how many segments are added in this batch
        let mut added_segments = 0u32;
        // 'trigrams: for trigram in 0usize..TRIGRAM_RANGE as usize {
        'trigrams: loop {
            let trigram = {
                let _mark = mark!(capture, "invert");
                // Invert batch into REVERSED index lists
                match collector.next(&mut file_ids) {
                    Some(trigram) => trigram,
                    None => break,
                }
            };

            let operations_span = mark!(capture, "build_ops");

            // Get the last segment used by this trigram
            let mut address = self.get_tail_hint(trigram);
            let mut active_segment = {
                let _span = mark!(operations_span, "read_segment");
                self.read_segment(address)?
            };
            content.clear();
            let mut encoded_size = active_segment.decode_into(&mut content);

            // Check if we are inserting values out of order with ones already there
            let out_of_order = if let Some(highest_old_value) = content.last() {
                let lowest_new_value = match file_ids.last() {
                    Some(next) => *next,
                    None => continue 'trigrams,
                };
                lowest_new_value <= *highest_old_value
            } else {
                false
            };

            if out_of_order {
                // If we are inserting out of order we are going to load the entire
                // set of values currently in the database and merge in our new values where
                // we have to. Once current segments are full, merge the lists and fall through
                // to adding new segments
                file_ids.extend(self.read_trigram(trigram)?);
                file_ids.sort_unstable_by(|a, b| b.cmp(a));
                file_ids.dedup();

                // Move to the first segment
                let mut address = SegmentAddress::Trigram(trigram);

                loop {
                    // Load the segment
                    active_segment = {
                        let _span = mark!(operations_span, "read_segment");
                        self.read_segment(address)?
                    };
                    content.clear();
                    active_segment.decode_into(&mut content);

                    // write values into this segment
                    let mut new_content = vec![];
                    if let Some(next) = file_ids.pop() {
                        new_content.push(next);
                        let mut data_size = encoded_number_size(next);
                        while let Some(next) = file_ids.pop() {
                            let cost = cost_to_add(&new_content, next);
                            if data_size + cost <= active_segment.payload_bytes() {
                                data_size += cost;
                                new_content.push(next)
                            } else {
                                file_ids.push(next);
                                break
                            }
                        }
                    }

                    // fill the entire data section of that buffer with these values (even if they are zeros)
                    if content != new_content {
                        let _span = mark!(operations_span, "write_operation");
                        encode_data_buffer.clear();
                        encode_into(&new_content, &mut encode_data_buffer);
                        encode_data_buffer.resize(active_segment.payload_bytes() as usize, 0);
                        write_data_buffer.resize(encode_data_buffer.len() + 128, 0);
                        writer.write_all(postcard::to_slice_cobs(&UpdateOperations::WriteSegment { segment: address, data: &encode_data_buffer }, &mut write_data_buffer)?)?;
                    }

                    // point to next segment
                    address = match active_segment.extension() {
                        Some(address) => SegmentAddress::Segment(address),
                        None => break
                    };
                }
            } else {
                // if we are inserting in order we can add values to the current segment
                // until it is full then fall through to adding new segments

                // Load the existing segment content
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
                    // in case we are overwriting existing data that could have been longer due to
                    // delta encoding or similar we will add an extra zero if there is room to
                    // mark the end of the encoded data. If there is no extra space we don't need
                    // to worry because there can't be hanging data
                    if encode_data_buffer.len() < limit as usize - POINTER_SIZE as usize {
                        encode_data_buffer.push(0);
                    }
                    write_data_buffer.resize(encode_data_buffer.len() + 128, 0);
                    writer.write_all(postcard::to_slice_cobs(&UpdateOperations::WriteSegment { segment: address, data: &encode_data_buffer }, &mut write_data_buffer)?)?;
                }
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
                writer.write_all(postcard::to_slice_cobs(&UpdateOperations::ExtendSegment { trigram, segment: address, new_segment }, &mut extend_data_buffer)?)?;
                encode_data_buffer.clear();
                encode_into(&content, &mut encode_data_buffer);
                write_data_buffer.resize(encode_data_buffer.len() + 128, 0);
                writer.write_all(postcard::to_slice_cobs(&UpdateOperations::WriteSegment { segment: new_address, data: &encode_data_buffer }, &mut write_data_buffer)?)?;
                address = new_address;
            }
        }

        // println!("Operation file built {:.2} [in {:.2}, bu {:.2}, bu>wr {:.2}, bu>sr {:.2}]", start.elapsed().as_secs_f64(), invert_time, build_ops_time, op_write_time, seg_read_time);
        // let stamp = std::time::Instant::now();
        let _mark = mark!(capture, "operation_finalize");

        // Write where the finalization is
        let mut write_buffer = writer.into_inner()?;
        let ending_offset = write_buffer.stream_position()?;
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

    /// Check for outstanding operation sets
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

    /// Apply the operation set prepared
    pub fn apply_operations(&mut self, mut source: File, counter: u64, extended_segments: u32, timing: impl TimingCapture) -> Result<()> {
        let capture = mark!(timing, "apply_operations");

        // Resize the mapping, but only if we need to
        if extended_segments > self.extended_segments {
            let _mark = mark!(capture, "resize");

            let new_size = HEADER_SIZE + PREFIX_SIZE + TRIGRAM_RANGE * self.initial_segment_size as u64 + extended_segments as u64 * self.extended_segment_size as u64;
            let data = std::fs::OpenOptions::new().write(true).read(true).truncate(false).open(&self.location)?;
            data.set_len(new_size).context("Resizing data file")?;
            let mut data = unsafe { memmap2::MmapMut::map_mut(&data)? };
            data.advise(memmap2::Advice::Random)?;
            std::mem::swap(&mut data, &mut self.data);
            // self.data = Arc::new(RwLock::new(data));
            self.extended_segments = extended_segments;

            // If we have a duplicate mapping, use the old one for an opertunistic flush
            if counter > 0 {
                self.flush_threaded(Some(data), Some(counter - 1))?;
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
            // let mut mmap = self.data.blocking_write();
            while bytes_read < offset {
                buffer.clear();
                bytes_read += reader.read_until(0, &mut buffer)? as u64;
                if !buffer.is_empty() {
                    let operation: UpdateOperations = postcard::from_bytes_cobs(&mut buffer).context("parsing operation")?;
                    match operation {
                        UpdateOperations::WriteSegment { segment, data } => {
                            let segment_offset = self.get_segment_offset(segment) as usize;
                            self.data[segment_offset..segment_offset + data.len()].copy_from_slice(data);
                        },
                        UpdateOperations::ExtendSegment { trigram, segment, new_segment } => {
                            // Write the new segment value into the segment being extended for forward reading
                            let segment_offset = self.get_segment_offset(segment);
                            let segment_length = match segment {
                                SegmentAddress::Trigram(_) => self.initial_segment_size as u64,
                                SegmentAddress::Segment(_) => self.extended_segment_size as u64,
                            };
                            let write_location = segment_offset + segment_length - POINTER_SIZE;
                            self.data[write_location as usize..write_location as usize+4].copy_from_slice(&new_segment.to_le_bytes());

                            // write the new segment value into the hint table for fast appends
                            let hint_location = self.get_tail_hint_offset(trigram);
                            self.data[hint_location as usize..hint_location as usize+4].copy_from_slice(&new_segment.to_le_bytes());
                        },
                    }
                }
            }
        }

        return Ok(())
    }

    /// Count the number of outstanding journals
    pub fn outstanding_journals(&self) -> Result<usize> {
        Ok(get_operation_journals(&self.directory, self.id)?.len())
    }

    /// Flush the outstanding operations and delete the corresponding journals (in a background thread)
    pub fn flush_threaded(&mut self, map: Option<MmapMut>, counter: Option<u64>) -> Result<JoinHandle<()>> {
        // get a memory mapping
        let data = match map {
            Some(data) => data,
            None => {
                let data = std::fs::OpenOptions::new().write(true).read(true).truncate(false).open(&self.location)?;
                let mut data = unsafe { memmap2::MmapMut::map_mut(&data)? };
                data.advise(memmap2::Advice::Random)?;
                std::mem::swap(&mut data, &mut self.data);
                data
            },
        };

        // Commit operations
        let handle = std::thread::spawn({
            let directory = self.directory.clone();
            let id = self.id;
            move || {
                // Wait until all outstanding data has flushed
                {
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
        });
        return Ok(handle)
    }

    /// Check if the index still exists
    pub fn blocking_exists(&mut self) -> bool {
        self.location.exists()
    }

    /// Flush the outstanding operations and delete the corresponding journals and wait for the operation to finish
    pub fn flush_blocking(&mut self) -> Result<()> {
        let handle = self.flush_threaded(None, None)?;
        if let Err(err) = handle.join() {
            error!("Join error: {err:?}")
        };
        return Ok(())
    }
}

/// Take a collection of files (represented by their id and a trigram set) and
/// yield the trigrams in sequence that they occur. If a trigram doesn't occur
/// in one of the files it is skipped. When the trigram is returned the file
/// ids of the files that contain it are returned by argument.
struct IdCollector<'a> {
    /// counter for which trigram we are on
    acceptable: u32,
    /// Set of files (ids and trigrams) to pass through
    iterators: Vec<(u64, Peekable<TrigramIterator<'a>>)>
}

impl IdCollector<'_> {
    /// Function to drive the iteration described above
    fn next(&mut self, hits: &mut Vec<u64>) -> Option<u32> {
        // hits.clear();

        let mut selected_trigram = u32::MAX;

        for (id, input) in self.iterators.iter_mut() {
            while let Some(&value) = input.peek() {
                if value < self.acceptable {
                    input.next();
                    continue
                }

                match value.cmp(&selected_trigram) {
                    std::cmp::Ordering::Less => {
                        hits.clear();
                        hits.push(*id);
                        selected_trigram = value;
                    },
                    std::cmp::Ordering::Equal => {
                        hits.push(*id);
                    },
                    std::cmp::Ordering::Greater => {},
                }
                break
            }
        }

        if selected_trigram == u32::MAX {
            None
        } else {
            self.acceptable = selected_trigram + 1;
            Some(selected_trigram)
        }
    }
}

/// An iterator over the file id list stored for a given trigram spread over several blocks
struct TrigramCursor<'a> {
    /// File containing the blocks we want to iterate over
    host: &'a ExtensibleTrigramFile,
    /// Content of the current block
    current: Vec<u64>,
    /// which value in the current block we are on
    offset: usize,
    /// Next block we need to look at
    next: Option<u32>
}

impl<'a> TrigramCursor<'a> {
    /// Create a cursor for the file ids under a given trigram
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

impl Iterator for TrigramCursor<'_> {
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
                self.current.first().copied()
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

    for index in 2..bytes.len() {
        trigram = (trigram & 0x00FFFF) << 8 | (bytes[index] as u32);
        trigrams.push(trigram);
    }

    return trigrams;
}

/// Calculate the union of two sets of numbers into the the first set
///
/// list need not be ordered
fn union(base: &mut Vec<u64>, other: &Vec<u64>) {
    base.extend(other);
    base.sort_unstable();
    base.dedup();
}

/// Calculate the intersection of two sets of numbers into the the first set
///
/// list must be ordered
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

        let mut parts = name.split('.');
        let owner = match parts.next() {
            Some(owner) => owner,
            None => continue,
        };
        if owner != id_str {
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

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use anyhow::{Result, Context};
    use itertools::Itertools;
    use rand::{Rng, thread_rng};

    use crate::query::Query;
    use crate::timing::{NullCapture, Capture};
    use crate::types::FilterID;
    use crate::worker::filter::{ExtensibleTrigramFile, into_trigrams, intersection};
    use crate::worker::trigrams::{build_buffer, TrigramSet};

    use super::{TRIGRAM_RANGE, IdCollector, union};

    #[test]
    fn simple_save_and_load() -> Result<()> {
        // build test data
        let mut trigrams = vec![];
        for ii in 1..102 {
            let mut data = TrigramSet::new();
            data.insert(500);
            if ii < 50 {
                data.insert(0);
            } else {
                data.insert(TRIGRAM_RANGE as u32 - 1);
            }
            data.compact();
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
            let mut data = TrigramSet::new();
            data.insert(500);
            if ii < 50 {
                data.insert(0);
            } else {
                data.insert(TRIGRAM_RANGE as u32 - 1);
            }
            data.compact();
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

        for ii in (1..21).rev() {
            trigrams.push((ii, TrigramSet::random(ii)));
        }
        println!("generate {:.2}", timer.elapsed().as_secs_f64());

        trigrams.sort_by(|a, b|a.0.cmp(&b.0));

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
            let mut recreated: Vec<TrigramSet> = vec![];
            for _ in 0..20 {
                recreated.push(TrigramSet::new())
            }

            let file = ExtensibleTrigramFile::open(location, id)?;
            for trigram in 0..(1<<24) {
                let values = file.read_trigram(trigram)?;
                for index in values {
                    recreated[index as usize - 1].insert(trigram & 0xFFFFFF);
                }
            }

            let trigrams: HashMap<u64, TrigramSet> = trigrams.into_iter().collect();

            for (index, values) in recreated.into_iter().enumerate() {
                assert!(*trigrams.get(&(index as u64+1)).unwrap() == values, "{index}")
            }
        }
        println!("read {:.2}", timer.elapsed().as_secs_f64());
        Ok(())
    }

    #[test]
    fn duplicate_batch() -> Result<()> {
        let data = TrigramSet::random(thread_rng().gen());

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
            let mut recreated = TrigramSet::new();

            let file = ExtensibleTrigramFile::open(location, id)?;
            for trigram in 0..(1<<24) {
                let values = file.read_trigram(trigram)?;
                if values.contains(&1) {
                    recreated.insert(trigram);
                }
                if trigram % (1 << 16) == 0 {
                    recreated.compact()
                }
            }

            assert!(data == recreated);
        }


        return Ok(())
    }

    #[test]
    fn out_of_order_batch() -> Result<()> {
        let mut bits = TrigramSet::new();
        bits.insert(0);

        let mut trigrams_a = vec![];
        let mut trigrams_b = vec![];
        for ii in 1..=50000 {
            if ii % 2 == 0 {
                trigrams_a.push((ii, bits.clone()));
            } else {
                trigrams_b.push((ii, bits.clone()));
            }
        }

        let tempdir = tempfile::tempdir()?;
        let id = FilterID::from(1);
        let location = tempdir.path().to_path_buf();
        {
            let mut file = ExtensibleTrigramFile::new(location.clone(), id, 16, 16)?;
            let x = file.write_batch(&mut trigrams_a, NullCapture::new())?;
            file.apply_operations(x.0, x.1, x.2, NullCapture::new())?;
        }

        {
            let mut file = ExtensibleTrigramFile::open(location.clone(), id)?;
            let x = file.write_batch(&mut trigrams_b, NullCapture::new())?;
            file.apply_operations(x.0, x.1, x.2, NullCapture::new())?;
        }

        {
            let file = ExtensibleTrigramFile::open(location, id)?;
            let values = file.read_trigram(0)?;
            assert_eq!(values, (1..=50000).collect_vec())
        }


        return Ok(())
    }

    #[test]
    fn out_of_order_batch_reversed() -> Result<()> {
        let mut bits = TrigramSet::new();
        bits.insert(0);

        let mut trigrams_a = vec![];
        let mut trigrams_b = vec![];
        for ii in 1..=50000 {
            if ii % 2 == 0 {
                trigrams_a.push((ii, bits.clone()));
            } else {
                trigrams_b.push((ii, bits.clone()));
            }
        }

        let tempdir = tempfile::tempdir()?;
        let id = FilterID::from(1);
        let location = tempdir.path().to_path_buf();
        {
            let mut file = ExtensibleTrigramFile::new(location.clone(), id, 16, 16)?;
            let x = file.write_batch(&mut trigrams_b, NullCapture::new())?;
            file.apply_operations(x.0, x.1, x.2, NullCapture::new())?;
        }

        {
            let mut file = ExtensibleTrigramFile::open(location.clone(), id)?;
            let x = file.write_batch(&mut trigrams_a, NullCapture::new())?;
            file.apply_operations(x.0, x.1, x.2, NullCapture::new())?;
        }

        {
            let file = ExtensibleTrigramFile::open(location, id)?;
            let values = file.read_trigram(0)?;
            assert_eq!(values.len(), 50000);
            assert_eq!(values, (1..=50000).collect_vec())
        }

        return Ok(())
    }

    // #[test]
    // fn large_batch() -> Result<()> {
    //     // build test data
    //     let mut trigrams = vec![];
    //     let timestamp = std::time::Instant::now();
    //     for ii in 1..500 {
    //         trigrams.push((ii, SparseBits::random(ii)));
    //     }
    //     println!("generate {:.2}", timestamp.elapsed().as_secs_f64());

    //     // write it
    //     let tempdir = tempfile::tempdir()?;
    //     let location = tempdir.path().to_path_buf();
    //     let id = FilterID::from(1);
    //     let capture = Capture::new();
    //     {
    //         let mut file = ExtensibleTrigramFile::new(location, id, 256, 1024)?;
    //         let x = file.write_batch(&mut trigrams, &capture)?;
    //         file.apply_operations(x.0, x.1, x.2, &capture)?;
    //     }

    //     capture.print();
    //     // // Recreate the trigrams
    //     // let timer = std::time::Instant::now();
    //     // {
    //     //     let mut recreated: Vec<BitVec> = vec![];
    //     //     for _ in 0..20 {
    //     //         recreated.push(BitVec::repeat(false, TRIGRAM_RANGE as usize))
    //     //     }

    //     //     let mut file = ExtensibleTrigramFile::open(&location)?;
    //     //     for trigram in 0..(1<<24) {
    //     //         let values = file.read_trigram(trigram)?;
    //     //         for index in values {
    //     //             recreated[index as usize - 1].set(trigram as usize, true);
    //     //         }
    //     //     }

    //     //     for (index, values) in recreated.into_iter().enumerate() {
    //     //         assert_eq!(trigrams[index].1, values)
    //     //     }
    //     // }
    //     // println!("read {}", timer.elapsed().as_secs_f64());
    //     Ok(())
    // }

    #[test]
    fn collector() {
        let mut objects = vec![];
        for ii in 0..256 {
            let mut abc = TrigramSet::new();
            for jj in 0..=0xFFFF {
                abc.insert(ii << 16 | jj);
            }
            abc.compact();
            objects.push(((ii + 1) as u64, abc));
        }

        let mut collector = IdCollector {
            acceptable: 0,
            iterators: objects.iter().map(|(id, row)|(*id, row.iter().peekable())).collect(),
        };

        let mut rounds = 0;
        let mut ids = vec![];
        while let Some(trigram) = collector.next(&mut ids) {
            assert_eq!(vec![(trigram as u64 >> 16) + 1], ids);
            rounds += 1;
        }
        assert_eq!(rounds, 1 << 24)
    }

    #[test]
    fn simple_queries() -> Result<()> {
        // setup data
        let raw_data = std::include_bytes!("./filter.rs");
        let trigrams = build_buffer(raw_data)?;

        // build table
        let tempdir = tempfile::tempdir()?;
        let id = FilterID::from(1);
        let location = tempdir.path().to_path_buf();
        let mut file = ExtensibleTrigramFile::new(location, id, 16, 16)?;
        let x = file.write_batch(&mut [(1, trigrams)], NullCapture::new())?;
        file.apply_operations(x.0, x.1, x.2, NullCapture::new())?;

        // run literal query
        let query = Query::Literal(b"query_literal".to_vec());
        let hits = file.query(&query)?;
        assert_eq!(hits, vec![1]);

        // run or query
        let query = Query::Or(vec![Query::Literal(b"1".to_vec()), Query::Literal(b"query_literal".to_vec())]);
        let hits = file.query(&query)?;
        assert_eq!(hits, vec![1]);

        // run and query
        let query = Query::And(vec![Query::Literal(b"1".to_vec()), Query::Literal(b"query_literal".to_vec())]);
        let hits = file.query(&query)?;
        assert!(hits.is_empty());

        return Ok(())
    }

    #[test]
    fn test_into_trigrams() {
        assert_eq!(into_trigrams(&vec![]), Vec::<u32>::new());
        assert_eq!(into_trigrams(&vec![0x10, 0xff]), Vec::<u32>::new());
        assert_eq!(into_trigrams(&vec![0x10, 0xff, 0x44]), vec![0x10ff44]);
        assert_eq!(into_trigrams(&vec![0x10, 0xff, 0x44, 0x22]), vec![0x10ff44, 0xff4422]);
    }

    #[test]
    fn test_union() {
        {
            let mut base = vec![];
            union(&mut base, &vec![]);
            assert!(base.is_empty());
        }
        {
            let mut base = vec![0x0];
            union(&mut base, &vec![]);
            assert_eq!(base, vec![0x0]);
        }
        {
            let mut base = vec![];
            union(&mut base, &vec![0x0]);
            assert_eq!(base, vec![0x0]);
        }
        {
            let mut base = vec![0x0, 0x1];
            union(&mut base, &vec![]);
            assert_eq!(base, vec![0x0, 0x1]);
        }
        {
            let mut base = vec![];
            union(&mut base, &vec![0x0, 0x1]);
            assert_eq!(base, vec![0x0, 0x1]);
        }
        {
            let mut base = vec![0xff];
            union(&mut base, &vec![0x0, 0xff]);
            assert_eq!(base, vec![0x0, 0xff]);
        }
        {
            let mut base = vec![0xff];
            union(&mut base, &vec![0xff, 0x0]);
            assert_eq!(base, vec![0x0, 0xff]);
        }
        {
            let mut base = vec![0x0, 0x1];
            union(&mut base, &vec![0x0, 0x1, 0xff]);
            assert_eq!(base, vec![0x0, 0x1, 0xff]);
        }
        {
            let mut base = vec![0xff, 0x0, 0xff];
            union(&mut base, &vec![0x1, 0x0]);
            assert_eq!(base, vec![0x0, 0x1, 0xff]);
        }
    }

    #[test]
    fn test_intersection() {
        {
            let mut base = vec![];
            intersection(&mut base, &vec![]);
            assert!(base.is_empty());
        }
        {
            let mut base = vec![0x0];
            intersection(&mut base, &vec![]);
            assert!(base.is_empty());
        }
        {
            let mut base = vec![];
            intersection(&mut base, &vec![0x0]);
            assert!(base.is_empty());
        }
        {
            let mut base = vec![0x0, 0x1];
            intersection(&mut base, &vec![]);
            assert!(base.is_empty());
        }
        {
            let mut base = vec![];
            intersection(&mut base, &vec![0x0, 0x1]);
            assert!(base.is_empty());
        }
        {
            let mut base = vec![0x0];
            intersection(&mut base, &vec![0x0]);
            assert_eq!(base, vec![0x0]);
        }
        {
            let mut base = vec![0xff];
            intersection(&mut base, &vec![0x0, 0xff]);
            assert_eq!(base, vec![0xff]);
        }
        {
            let mut base = vec![0xff];
            intersection(&mut base, &vec![0xff, 0x0]);
            assert_eq!(base, vec![0xff]);
        }
        {
            let mut base = vec![0x0, 0x1];
            intersection(&mut base, &vec![0x0, 0x1, 0xff]);
            assert_eq!(base, vec![0x0, 0x1]);
        }
        {
            let mut base = vec![0x1, 0xff];
            intersection(&mut base, &vec![0x0, 0x1]);
            assert_eq!(base, vec![0x1]);
        }
    }

}