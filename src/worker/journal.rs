#![allow(unused)]

use std::{cmp::Reverse, collections::{hash_map::Entry, BTreeSet, HashMap}, fs::File, io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}, sync::{mpsc::{Receiver, Sender}, Arc}};
use std::iter::Peekable;

// todo ensure ordered file ids

use anyhow::{Context, Result};
use bitvec::order::{BitOrder, Lsb0};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::{error, info};
use rand::{thread_rng, Rng};
use tokio::sync::mpsc;
use itertools::Itertools;
use parking_lot::Mutex;
// use serde::{Deserialize, Serialize};

use crate::{config::FILTER_TEMP_SUBDIRECTORY, query::Query, timing::{mark, Capture, TimingCapture}, types::FilterID, worker::encoding::{encode_value_into, DecreasingEncoder}};
use crate::worker::encoding::encode_into_increasing;

use super::{encoding::{decode_decreasing_into, decode_into, decode_value, StreamDecode}, intersection, into_trigrams, remove_file, union};

/// A convinence constant defining how many possible trigrams exist
const TRIGRAM_RANGE: u64 = 1 << 24;
/// A magic number used in the header, generated randomly once.
const HEADER_MAGIC: u32 = 0xd42e1880;
/// Size of the fixed length header
const HEADER_SIZE: u64 = 4;

const TAIL_ROW_SIZE: u64 = 8 + 8;
const TAIL_FILE_SIZE: u64 = TAIL_ROW_SIZE * TRIGRAM_RANGE + 8;

const BUFFER_SIZE: usize = 1 << 14;

struct Size(u64);

impl std::ops::Add<usize> for Size {
    type Output = Self;

    fn add(self, rhs: usize) -> Self::Output {
        Size(self.0 + rhs as u64)
    }
}

#[derive(Clone, Copy)]
struct Address(u64);


pub struct JournalFilter {
    /// identifier for this journal, ties files on disk to database entries elsewhere
    id: FilterID,
    /// directory where files related to this journal are stored, other things will be in this directory
    directory: PathBuf,
    /// A file handle used while writing
    write_handle: Mutex<Arc<File>>,
    /// A file handle used while reading. This could be a pool if we are having search throughput issues.
    read_handle: Mutex<File>,
    /// A flag indicating if this journal is active, this lets different components that might
    /// want to use this journal to coordinate starting, stoping, garbage collection, etc.
    running: std::sync::atomic::AtomicBool,
    /// A notice to let people know if the status of this journal has changed
    running_notice: tokio::sync::Notify,
}


impl JournalFilter {
    pub async fn new(directory: PathBuf, id: FilterID) -> Result<Arc<Self>> {
        tokio::task::spawn_blocking(move || Self::blocking_new(directory, id)).await?
    }

    fn blocking_new(directory: PathBuf, id: FilterID) -> Result<Arc<Self>> {
        // prepare file
        let location = directory.join(format!("{id}"));
        let mut data = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(&location).context("Creating data file")?;

        // write header
        data.write_u32::<LittleEndian>(HEADER_MAGIC).context("Writing header")?;
        data.sync_all()?;

        // setup tail
        AddressBuilder::init_empty(&directory, id).context("init empty address table")?;

        // return object
        let reader = std::fs::OpenOptions::new().create_new(false).write(false).read(true).open(&location).context("Open file to read")?;
        Ok(Arc::new(Self {
            write_handle: Mutex::new(Arc::new(data)),
            read_handle: Mutex::new(reader),
            directory,
            id,
            running: std::sync::atomic::AtomicBool::new(true),
            running_notice: tokio::sync::Notify::new(),
        }))
    }

    pub async fn open(directory: PathBuf, id: FilterID) -> Result<Arc<Self>> {
        tokio::task::spawn_blocking(move || {
            // open file
            let location = directory.join(format!("{id}"));
            
            // check for files that haven't been fully initialized
            if let Ok(metadata) = location.metadata() {
                if metadata.len() <= 4 {
                    AddressBuilder::clear_all(&directory, id);
                    remove_file(&location)?;
                }
            }
            if !location.exists() {
                return Self::blocking_new(directory, id)
            }

            // if a defrag just finished but the files are still being installed finish that now
            Self::_install_defrag(&directory, id);

            let mut data = std::fs::OpenOptions::new().truncate(false).write(true).read(true).open(&location).context("Open file to write")?;

            // verify header
            let read_magic = data.read_u32::<LittleEndian>()?;
            if read_magic != HEADER_MAGIC {
                let mut size = 0;
                if let Ok(metadata) = location.metadata() {
                    size = metadata.len();
                }

                return Err(anyhow::anyhow!("Corrupt data file: header magic wrong {read_magic:08x}; {location:?}; size: {size}"));
            }

            // clear temporary
            AddressBuilder::init_ready(&directory, id)?;

            // Open current tail and read last location
            let mut reader = AddressReader::open(&directory, id)?;
            let mut address: u64 = 0;
            while let Some((_, aa, _)) = reader.next()? {
                if let Some(aa) = aa {
                    address = address.max(aa.0);
                }
            }

            // read last record and truncate after
            if address > 0 {
                let mut reader = BufReader::new(&mut data);
                if reader.seek(SeekFrom::Start(address))? != address {
                    error!("{} data missing", id);
                };
                let mut buffer = vec![];
                let size = reader.read_until(0, &mut buffer)?;
                Self::load_block_data(Address(address), &mut buffer)?;
                data.set_len(address + size as u64)?;
            }
            data.seek(SeekFrom::End(0))?;

            // return object
            let reader = Self::open_reader(&directory, id)?;
            Ok(Arc::new(Self {
                write_handle: Mutex::new(Arc::new(data)),
                read_handle: Mutex::new(reader),
                directory,
                id,
                running: std::sync::atomic::AtomicBool::new(true),
                running_notice: tokio::sync::Notify::new(),
            }))
        }).await?
    }

    fn open_reader(directory: &Path, id: FilterID) -> Result<File> {
        let location = directory.join(format!("{}", id));
        std::fs::OpenOptions::new().create_new(false).write(false).read(true).open(location).context("Open file to read")
    }

    fn _open_reader(&self) -> Result<File> {
        Self::open_reader(&self.directory, self.id)
    }

    fn open_writer(directory: &Path, id: FilterID) -> Result<File> {
        let location = directory.join(format!("{}", id));
        std::fs::OpenOptions::new().truncate(false).write(true).read(true).open(location).context("Open file to write")
    }

    fn _open_writer(&self) -> Result<File> {
        Self::open_writer(&self.directory, self.id)
    }

    pub fn delete(&self) -> Result<()> {
        AddressBuilder::clear_all(&self.directory, self.id);
        remove_file(&self.directory.join(format!("{}", self.id)))?;
        return Ok(())
    }

    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Acquire)
    }

    pub async fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Release);
        self.running_notice.notify_waiters();
        self.write_handle.lock();
        self.read_handle.lock();
    }

    pub fn notify(&self) {
        self.running_notice.notify_waiters();
    }

    pub async fn notified(&self) {
        if self.is_running() {
            self.running_notice.notified().await
        }
    }

    pub async fn defrag(self: &Arc<Self>) -> Result<bool> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            this._defrag()
        }).await?
    }

    fn _defrag(&self) -> Result<bool> {
        let capture = &Capture::new();
        let mut guard = self.write_handle.lock();

        if !self.is_running() {
            return Ok(false)
        }

        // create temp files to write into
        let temp_journal_path = self.directory.join(FILTER_TEMP_SUBDIRECTORY).join(thread_rng().gen::<u128>().to_string());
        let mut temp_journal = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(&temp_journal_path).context("Creating data file")?;
        let mut address_writer = AddressBuilder::new(self.directory.clone(), self.id)?;
       
        // write header
        temp_journal.write_u32::<LittleEndian>(HEADER_MAGIC).context("Writing header")?;
        let mut position = 4u64;

        // spawn thread to read defragmented data
        let mut trigram_stream = self.read_all();

        // write the data as it comes in
        let mut encode_data_buffer = vec![];
        let mut write_data_buffer = vec![];
        while let Some(trigram) = trigram_stream.blocking_recv() {
            let (trigram, file_ids) = trigram?;

            // handle trigrams that never occured
            if file_ids.is_empty() {
                address_writer.write(trigram, None, Size(0))?;
                continue
            }

            // write out the tail file for a defragmented section
            address_writer.write(trigram, Some(Address(position)), Size(file_ids.len() as u64))?;

            // Encode the file ids
            Self::encode_block_data(&mut encode_data_buffer, None, position, &file_ids, &mut write_data_buffer).context("defrag::encode_block_data")?;

            // write the data to the journal
            position += write_data_buffer.len() as u64;
            temp_journal.write_all(&write_data_buffer)?;
        }
        
        // write trailing data
        let tail_location = address_writer.finish(0)?;
        temp_journal.flush()?;
        temp_journal.sync_all()?;

        // Install the files
        let mut reader = self.read_handle.lock();
        self._prepare_install_defrag(&temp_journal_path, &tail_location)?;
        Self::_install_defrag(&self.directory, self.id)?;

        // replace the file handles in the locks
        *reader = self._open_reader()?;
        *guard = Arc::new(self._open_writer()?);
        guard.seek(SeekFrom::End(0))?;

        Ok(true)
    }

    fn _prepare_install_defrag(&self, temp_data: &Path, temp_tails: &Path) -> Result<()> {
        std::fs::rename(temp_data, self.directory.join(format!(".{}", self.id)))?;
        std::fs::rename(temp_tails, self.directory.join(format!(".{}.tail", self.id)))?;
        Ok(())
    }

    fn _install_defrag(directory: &Path, id: FilterID) -> Result<()> {
        let temp_data = directory.join(format!(".{}", id));
        let temp_tails = directory.join(format!(".{}.tail", id));
        if temp_tails.exists() {
            if temp_data.exists() {        
                std::fs::rename(temp_data, directory.join(format!("{}", id)))?;
            }
            std::fs::rename(temp_tails, directory.join(format!("{}.tail", id)))?;
        } else {
            remove_file(&temp_data)?;
        }
        Ok(())
    }

    pub async fn write_batch(self: &Arc<Self>, files: Vec<(u64, Vec<u8>)>) -> Result<bool> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            this._write_batch(files)
        }).await?
    }

    fn _write_batch(&self, mut files: Vec<(u64, Vec<u8>)>) -> Result<bool> {
        let capture = &Capture::new();
        let guard = self.write_handle.lock();

        if !self.is_running() {
            return Ok(false)
        }

        {
            let _capture_batch = mark!(capture, "write_batch");

            // get the write handle
            let _mark = mark!(_capture_batch, "setup_writer");
            let writer: Arc<std::fs::File> = guard.clone();
            let mut writer = BufWriter::with_capacity(BUFFER_SIZE, writer);
            let mut encode_data_buffer: Vec<u8> = vec![];
            let mut write_data_buffer: Vec<u8> = vec![];
            let mut position = writer.stream_position()?;
            drop(_mark);

            // Prepare the trigram reader, make sure the files are sorted so the output later will
            // always be sorted (high to low)
            let _mark = mark!(_capture_batch, "prepare_input");
            files.sort_unstable_by_key(|row|Reverse(row.0));
            let mut collector = IdCollector::new_from_vec(&files);

            // start old and new address block
            let mut address_writer = AddressBuilder::new(self.directory.clone(), self.id)?;
            let mut address_reader = AddressReader::open(&self.directory, self.id)?;
            drop(_mark);

            // write each new block into the journal and address block
            let mut file_ids = vec![];
            let _main_loop = mark!(_capture_batch, "main_loop");
            while let Some(trigram) = { let _x = mark!(_main_loop, "collect_ids"); collector.next(&mut file_ids) } {
                let _write_loop = mark!(_main_loop, "loop_body");
                let (old_address, old_size) = loop {
                    let (tg, address, size) = address_reader.next()?.unwrap();
                    if tg < trigram {
                        address_writer.write(tg, address, size)?;
                        continue
                    }
                    break (address, size)
                };

                // write the new block
                let new_address = position;
                {
                    let _mark = mark!(_write_loop, "process_batch");
                    // file_ids.sort_unstable(reverse); // we need them to be sorted, but they should be already

                    Self::encode_block_data(&mut encode_data_buffer, old_address, new_address, &file_ids, &mut write_data_buffer)?;

                    // write the data to the journal
                    position += write_data_buffer.len() as u64;
                    writer.write_all(&write_data_buffer)?;
                }

                // fill in the new address data
                address_writer.write(trigram, Some(Address(new_address)), old_size + file_ids.len())?;
            }
            drop(_main_loop);

            // finish writing incomplete sections
            {
                let _mark = mark!(_capture_batch, "finish_addresses");
                while let Some((trigram, address, size)) = address_reader.next()? {
                    address_writer.write(trigram, address, size)?;
                }
            }

            // Flush the files
            let _mark = mark!(_capture_batch, "finish");
            let mut writer = writer.into_inner()?;
            writer.flush()?;
            writer.sync_all()?;
            address_writer.install(1 + address_reader.read_generation().context("read_generation")?)?;
        }
        info!("write_batch metrics \n{}", capture.format());
        Ok(true)
    }

    fn encode_block_data(encode_data_buffer: &mut Vec<u8>, old_address: Option<Address>, new_address: u64, file_ids: &[u64], write_data_buffer: &mut Vec<u8>) -> Result<()> {
        // Write the header info
        encode_data_buffer.clear();
        let address_offset = match old_address {
            Some(address) => new_address - address.0,
            None => 0,
        };
        encode_value_into(address_offset, encode_data_buffer);
        encode_value_into(file_ids[0], encode_data_buffer);

        // write the bulk of the data
        DecreasingEncoder::new(file_ids[0], encode_data_buffer).write(&file_ids[1..]);

        // prepare the buffer for cobs tranform with some extra space for header
        write_data_buffer.resize(cobs::max_encoding_length(encode_data_buffer.len()) + 128, 0);

        // do the cobs encoding
        let write_length = cobs::encode(encode_data_buffer, write_data_buffer);
        write_data_buffer.truncate(write_length);
        write_data_buffer.push(0);
        Ok(())
    }

    fn load_block_data(address: Address, data: &mut [u8]) -> Result<Block<'_>> {
        let bytes = cobs::decode_in_place(data).map_err(|_| anyhow::anyhow!("cobs"))?;
        let data = &data[..bytes];
        let (offset, data) = decode_value(data);
        let (first_id, data) = decode_value(data);
        Ok(Block {
            address,
            previous_offset: if offset > 0 { Some(offset) } else { None },
            first_id,
            deltas: data
        })
    }

    fn load_block<'a>(&self, address: Address, buffer: &'a mut Vec<u8>) -> Result<Block<'a>> {
        let mut guard = self.read_handle.lock();
        let reader: &mut std::fs::File = &mut guard;
        let mut reader = BufReader::new(reader);
        Self::load_block_into(address, &mut reader, buffer)
    }

    fn load_block_into<'a>(address: Address, reader: &mut BufReader<&mut File>, buffer: &'a mut Vec<u8>) -> Result<Block<'a>> {
        reader.seek(SeekFrom::Start(address.0))?;
        buffer.clear();
        let _size = reader.read_until(0, buffer)?;
        Self::load_block_data(address, buffer)
    }

    fn read_all(&self) -> mpsc::Receiver<Result<(u32, Vec<u64>)>> {
        let (send, recv) = mpsc::channel(32);

        let directory = self.directory.clone();
        let id = self.id;
        let mut reader = match self._open_reader() {
            Ok(reader) => reader,
            Err(err) => {
                _ = send.blocking_send(Err(err));
                return recv
            },
        };

        tokio::task::spawn_blocking(move ||{
            let send_err = send.clone();
            let result = move || -> Result<()> {
                let mut reader = BufReader::new(&mut reader);
                let mut buffer = vec![];

                let mut addresses = AddressReader::open(&directory, id)?;
                while let Some((val, mut address, size)) = addresses.next()? {
                    let mut file_ids = Vec::with_capacity(size.0 as usize);
                    while let Some(current) = address {
                        buffer.clear();
                        let block = Self::load_block_into( current, &mut reader, &mut buffer)?;
                        block.decode_into(&mut file_ids);
                        address = block.previous();
                    }
                    send.blocking_send(Ok((val, file_ids)))?;
                }
                Ok(())
            }();

            if let Err(err) = result {
                send_err.blocking_send(Err(err)).unwrap()
            }
        });

        recv
    }

    /// Read the file list for a single trigram
    /// set is sorted low to high
    fn read_trigram(&self, trigram: u32) -> Result<Vec<u64>> {
        let mut collected = vec![];
        let (mut address, size) = AddressReader::read_single(&self.directory, self.id, trigram)?;
        if size.0 == 0 {
            return Ok(vec![])
        }
        let mut buffer = vec![];
        while let Some(current) = address {
            let block = self.load_block(current, &mut buffer)?;
            block.decode_into(&mut collected);
            address = block.previous();
        }
        collected.reverse();
        Ok(collected)
    }

    /// Calculate the intersection of the file lists for all the given trigrams
    /// set is sorted low to high
    fn read_trigrams(&self, trigrams: Vec<u32>) -> Result<Vec<u64>> {
        // Handle corner cases
        if trigrams.is_empty() {
            return Ok(vec![])
        }
        if trigrams.len() == 1 {
            return self.read_trigram(trigrams[0])
        }

        // Load all the offset and sizes
        let trigrams = AddressReader::read_set(&self.directory, self.id, trigrams)?;

        // TODO there is an optimization here were if any of the sets are quite small we could load them
        // separately before moving onto the larger ones

        // build a collection if iterators that will navigate the linked list of segments
        // yielding file ids in decending order. They will only read segments when the current one is
        // exhausted, this potentially lets us avoid loading unused sections of the underlying file.
        let mut sources = vec![];
        for (_trigram, address, size) in trigrams {
            // TODO sort by size
            if size.0 == 0 {
                return Ok(vec![])
            }
            sources.push(TrigramCursor::new(self, address));
        }

        // Get the first file that might be in all the file lists
        let mut candidate = match sources[0].peek()? {
            Some(item) => item,
            None => return Ok(vec![]),
        };

        // Loop over all the cursors until all files have been considered
        let mut output = vec![];
        'next_candidate: loop {
            // For the current candidate value check all the file lists to see if they all have it
            'next_cursor: for cursor in &mut sources {
                // This loop iterates through values in the current cursor until we catch up with the candidate value
                loop {
                    let next = match cursor.peek()? {
                        Some(next) => next,
                        // if we have reached the end of any of the cursors we can terminate this
                        // search and return whatever we have found up until now
                        None => break 'next_candidate,
                    };

                    match next.cmp(&candidate) {
                        // if the current value on this cursor is behind the candidate keep
                        // moving forward until we catch up with the candidate, all the values we are skipping
                        // over must be missing in at least one other cursor for this to happen
                        std::cmp::Ordering::Greater => { cursor.next()?; continue },
                        // If this cursor has the candidate in it, move to the next cursor, candidate is still valid
                        std::cmp::Ordering::Equal => { continue 'next_cursor },
                        // If this cursor has passed the candidate the candidate is invalid (it needs to be
                        // in all of the cursors) take the current value as the new candidate and restart
                        // the loop over the cursors
                        std::cmp::Ordering::Less => { candidate = next; continue 'next_candidate },
                    }
                }
            }

            // If we have checked all the cursors and haven't skipped to the next iteration
            // of the 'next_candidate loop then we have found this candidates in all the cursors
            output.push(candidate);

            // advance to the 'next' file ID as a candidate. It doesn't matter if the cursors don't
            // actually have this value, since we are about to check them.
            candidate -= 1;
        }
        return Ok(output)
    }

    /// Run a query over the trigram table.
    /// Setup a hash table as a cache, so that we don't search the same literal repeatedly.
    pub async fn query(self: &Arc<Self>, query: Query) -> Result<Vec<u64>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut cache = Default::default();
            this._query(&query, &mut cache)
        }).await?
    }

    /// Actual logic for the 'query' methods
    fn _query(&self, query: &Query, cache: &mut HashMap<Vec<u8>, Vec<u64>>) -> Result<Vec<u64>> {
        if !self.is_running() {
            return Err(anyhow::anyhow!("Filter stopping."));
        }

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

    pub async fn generation_counter(self: &Arc<Self>) -> Result<u64> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            AddressReader::open(&this.directory, this.id)?.read_generation()
        }).await?
    }

    pub async fn last_write(self: &Arc<Self>) -> Result<std::time::Duration> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let metadata = this.write_handle.lock().metadata()?;
            Ok(metadata.modified()?.elapsed()?)
        }).await?
    }    
}

struct TrigramCursor<'a> {
    host: &'a JournalFilter,
    current: Vec<u64>,
    index: usize,
    next_address: Option<Address>,
    data_buffer: Vec<u8>,
}

impl<'a> TrigramCursor<'a> {
    fn new(host: &'a JournalFilter, next_address: Option<Address>) -> Self {
        Self {
            host,
            next_address,
            index: 0,
            data_buffer: vec![],
            current: vec![]
        }
    }

    fn fill(&mut self) -> Result<bool> {
        if self.index < self.current.len() { return Ok(true) }
        if let Some(address) = self.next_address {
            let block = self.host.load_block(address, &mut self.data_buffer)?;
            self.next_address = block.previous();
            self.index = 0;
            self.current.clear();
            block.decode_into(&mut self.current);
            return Ok(!self.current.is_empty())
        }
        Ok(false)
    }

    fn next(&mut self) -> Result<Option<u64>> {
        self.fill()?;
        self.index += 1;
        Ok(self.current.get(self.index - 1).cloned())
    }

    fn peek(&mut self) -> Result<Option<u64>> {
        self.fill()?;
        Ok(self.current.get(self.index).cloned())
    }
}

/// Take a collection of files (represented by their id and a trigram set) and
/// yield the trigrams in sequence that they occur. If a trigram doesn't occur
/// in one of the files it is skipped. When the trigram is returned the file
/// ids of the files that contain it are returned by argument.
pub struct IdCollector<'a> {
    /// counter for which trigram we are on
    acceptable: u32,
    /// Set of files (ids and trigrams) to pass through
    iterators: Vec<(u64, StreamDecode<'a>)>,
    // candidates: BTreeSet<u32>,
    remove: Vec<u64>,
}

impl<'a> IdCollector<'a> {
    pub fn new(iterators: Vec<(u64, StreamDecode<'a>)>) -> Self {
        Self {
            acceptable: 0,
            iterators,
            remove: vec![],
        }
    }

    pub fn new_from_vec(iterators: &'a [(u64, Vec<u8>)]) -> Self {
        Self::new(iterators.iter().map(|(id, row)|(*id, StreamDecode::new(row))).collect_vec())
    }

    /// Function to drive the iteration described above
    #[inline(always)]
    pub fn next(&mut self, hits: &mut Vec<u64>) -> Option<u32> {
        // hits.clear(); // if we return Some we will always call clear in the inner loop

        let mut selected_trigram = u32::MAX;

        'outer: for (id, input) in self.iterators.iter_mut() {
            while let Some(value) = input.peek() {
                let value = value as u32;
                if value < self.acceptable {
                    input.skip_one();
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
                continue 'outer
            }
            self.remove.push(*id)
        }

        if !self.remove.is_empty() {
            self.iterators.retain(|(id, _)|!self.remove.contains(id));
            self.remove.clear();
        }

        if selected_trigram == u32::MAX {
            None
        } else {
            self.acceptable = selected_trigram + 1;
            Some(selected_trigram)
        }
    }
}

struct AddressReader {
    next_trigram: u32,
    handle: BufReader<std::fs::File>,
}

impl AddressReader {
    fn open(directory: &Path, id: FilterID) -> Result<Self> {
        let tail_location = directory.join(format!("{id}.tail"));
        let tail = std::fs::OpenOptions::new().create_new(false).write(false).read(true).open(tail_location).context("Opening tail file")?;
        Ok(AddressReader{
            next_trigram: 0,
            handle: BufReader::with_capacity(BUFFER_SIZE, tail)
        })
    }

    fn read_set(directory: &Path, id: FilterID, mut trigrams: Vec<u32>) -> Result<Vec<(u32, Option<Address>, Size)>> {
        let tail_location = directory.join(format!("{id}.tail"));
        let tail = std::fs::OpenOptions::new().create_new(false).write(false).read(true).open(tail_location).context("Opening tail file")?;
        let mut tail = BufReader::with_capacity(1 << 20, tail);
        let mut out = vec![];
        trigrams.sort_unstable();
        trigrams.dedup();

        let mut location = 0;
        for trigram in trigrams {
            // move forward as needed
            let new_location = trigram as u64 * TAIL_ROW_SIZE;
            let forward_move = new_location - location;
            if forward_move > 0 {
                tail.seek_relative(forward_move as i64)?;
                location += forward_move;
            }

            let address = tail.read_u64::<LittleEndian>()?;
            let address = if address == 0 {
                None
            } else {
                Some(Address(address))
            };
            let size = Size(tail.read_u64::<LittleEndian>()?);
            out.push((trigram, address, size));
            location += 16;
        }

        Ok(out)
    }

    fn read_single(directory: &Path, id: FilterID, trigram: u32) -> Result<(Option<Address>, Size)> {
        let tail_location = directory.join(format!("{id}.tail"));
        let mut tail = std::fs::OpenOptions::new().create_new(false).write(false).read(true).open(tail_location).context("Opening tail file")?;
        tail.seek(SeekFrom::Start(trigram as u64 * TAIL_ROW_SIZE))?;
        let address = tail.read_u64::<LittleEndian>()?;
        let address = if address == 0 {
            None
        } else {
            Some(Address(address))
        };
        let size = Size(tail.read_u64::<LittleEndian>()?);
        Ok((address, size))
    }

    fn read_generation(mut self) -> Result<u64> {
        self.handle.seek(SeekFrom::Start(TRIGRAM_RANGE * TAIL_ROW_SIZE))?;
        let generation = self.handle.read_u64::<LittleEndian>()?;
        Ok(generation)
    }

    fn next(&mut self) -> Result<Option<(u32, Option<Address>, Size)>> {
        if self.next_trigram as u64 >= TRIGRAM_RANGE {
            Ok(None)
        } else {
            let trigram = self.next_trigram;
            self.next_trigram += 1;
            let address = self.handle.read_u64::<LittleEndian>()?;
            let address = if address == 0 {
                None
            } else {
                Some(Address(address))
            };
            let size = Size(self.handle.read_u64::<LittleEndian>()?);
            Ok(Some((trigram, address, size)))
        }
    }
}

struct AddressBuilder {
    location: PathBuf,
    directory: PathBuf,
    id: FilterID,
    expected_trigram: u32,
    handle: BufWriter<std::fs::File>,
}

impl AddressBuilder {
    fn new(directory: PathBuf, id: FilterID) -> Result<Self> {
        let temp_id: u128 = thread_rng().gen();
        let temp_dir = directory.join(FILTER_TEMP_SUBDIRECTORY);
        std::fs::create_dir_all(&temp_dir)?;
        let tail_location = temp_dir.join(format!("{temp_id}"));
        let tail = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(&tail_location).context("Creating tail file")?;
        Ok(Self {
            location: tail_location,
            directory,
            id,
            expected_trigram: 0,
            handle: BufWriter::with_capacity(BUFFER_SIZE, tail)
        })
    }

    fn init_empty(directory: &Path, id: FilterID) -> Result<()> {
        let tail_location = directory.join(format!("{id}.tail"));
        let tail = std::fs::OpenOptions::new().create_new(true).write(true).read(true).open(tail_location).context("Creating empty tail file")?;
        tail.set_len(TAIL_FILE_SIZE).context("Initializing tail file")?;
        Ok(())
    }

    fn init_ready(directory: &Path, id: FilterID) -> Result<()> {
        if !directory.join(format!("{id}.tail")).exists() {
            Self::init_empty(directory, id)?;
        }
        Ok(())
    }

    fn clear_all(directory: &Path, id: FilterID) -> Result<()> {
        let tail_location = directory.join(format!("{id}.tail"));
        remove_file(&tail_location)
    }

    fn write(&mut self, trigram: u32, address: Option<Address>, size: Size) -> Result<()> {
        if trigram != self.expected_trigram {
            return Err(anyhow::anyhow!("Address build out of order"));
        }
        self.expected_trigram += 1;
        match address {
            Some(address) => self.handle.write_u64::<LittleEndian>(address.0)?,
            None => self.handle.write_u64::<LittleEndian>(0)?,
        };
        self.handle.write_u64::<LittleEndian>(size.0)?;
        Ok(())
    }

    fn finish(mut self, generation: u64) -> Result<PathBuf> {
        self.handle.write_u64::<LittleEndian>(generation);
        let mut handle = self.handle.into_inner()?;
        handle.flush()?;
        handle.sync_all()?;
        Ok(self.location)
    }

    fn install(mut self, generation: u64) -> Result<()> {
        self.handle.write_u64::<LittleEndian>(generation);
        let mut handle = self.handle.into_inner()?;
        handle.flush()?;
        handle.sync_all()?;
        let active = self.directory.join(format!("{}.tail", self.id));
        std::fs::rename(self.location, active)?;
        Ok(())
    }
}

struct Block<'a> {
    address: Address,
    previous_offset: Option<u64>,
    first_id: u64,
    deltas: &'a [u8],
}

impl Block<'_> {
    fn previous(&self) -> Option<Address> {
        self.previous_offset.map(|offset| Address(self.address.0 - offset))
    }

    fn decode_into(&self, output: &mut Vec<u64>) {
        decode_decreasing_into(self.deltas, self.first_id, output)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use anyhow::{Result, Context};
    use bitvec::vec::BitVec;
    use itertools::Itertools;
    use rand::{Rng, SeedableRng};

    use crate::worker::encoding::StreamDecode;
    use crate::worker::journal::AddressReader;
    use crate::{query::Query, worker::encoding::encode_into_increasing};
    use crate::types::FilterID;
    use super::JournalFilter;
    use crate::worker::trigrams::{build_buffer, build_buffer_to_offsets, random_trigrams, Bits};

    use super::{TRIGRAM_RANGE, IdCollector};

    fn init() {
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "haunted_house=debug")
        }
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn simple_save_and_load() -> Result<()> {
        // build test data
        let mut trigrams = vec![];
        for ii in 1..102 {
            let mut data = vec![];
            if ii < 50 {
                encode_into_increasing(&[0, 500], &mut data);
            } else {
                encode_into_increasing(&[500, TRIGRAM_RANGE - 1], &mut data);
            }
            trigrams.push((ii, data));
        }

        // write it
        let tempdir = tempfile::tempdir()?;
        let id = FilterID::from(1);
        let location = tempdir.path().to_path_buf();
        {
            let timestamp = std::time::Instant::now();
            let file = JournalFilter::new(location.clone(), id).await?;
            file.write_batch( trigrams).await.context("write batch")?;
            println!("Write time: {}", timestamp.elapsed().as_secs_f64());
        }

        // Read it again
        {
            let timestamp = std::time::Instant::now();
            let file = JournalFilter::open(location.clone(), id).await?;
            println!("Open time: {}", timestamp.elapsed().as_secs_f64());

            let timestamp = std::time::Instant::now();
            let mut output = file.read_all();

            while let Some(row) = output.recv().await {
                let (trigram, mut values) = row?;
                values.reverse();
                if trigram == 0 {
                    assert_eq!(*values, (1..50).collect_vec());
                } else if trigram == 500 {
                    assert_eq!(*values, (1..102).collect_vec());
                } else if trigram == TRIGRAM_RANGE as u32 - 1 {
                    assert_eq!(*values, (50..102).collect_vec());
                } else {
                    assert!(values.is_empty());
                };
            }

            println!("Read time: {}; {} each", timestamp.elapsed().as_secs_f64(), timestamp.elapsed().as_secs_f64()/TRIGRAM_RANGE as f64);
        }

        assert_eq!(AddressReader::open(&location, id).unwrap().read_generation().unwrap(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn multiple_writes() -> Result<()> {
        // let x = setup_global_subscriber();

        let timer = std::time::Instant::now();
        // build test data
        // let mut original = vec![];
        let mut trigrams = vec![];

        for ii in 1..31 {
            let (a, b) = random_trigrams(ii);
            // original.push(a);
            trigrams.push((ii, b));
        }
        println!("generate {:.2}", timer.elapsed().as_secs_f64());

        // write it
        let tempdir = tempfile::tempdir()?;
        let location = tempdir.path().to_path_buf();
        let id = FilterID::from(1);
        {
            let timer = std::time::Instant::now();
            let file = JournalFilter::new(location.clone(), id).await?;
            println!("open finished {:.2}", timer.elapsed().as_secs_f64());
            let timer: std::time::Instant = std::time::Instant::now();
            file.write_batch(trigrams[0..10].iter().cloned().collect_vec()).await?;
            println!("write 1 finished {:.2}", timer.elapsed().as_secs_f64());
            let timer = std::time::Instant::now();
            file.write_batch(trigrams[10..20].iter().cloned().collect_vec()).await?;
            println!("write 2 finished {:.2}", timer.elapsed().as_secs_f64());
            let timer = std::time::Instant::now();
            file.write_batch(trigrams[20..30].iter().cloned().collect_vec()).await?;
            println!("write 2 finished {:.2}", timer.elapsed().as_secs_f64());
        }

        // Recreate the trigrams
        {
            let file = JournalFilter::open(location.clone(), id).await?;

            let mut files = trigrams.iter().map(|row|StreamDecode::new(&row.1)).collect_vec();
            let mut output = file.read_all();

            while let Some(row) = output.recv().await {
                let (trigram, values) = row?;
                for &file_index in values.iter().rev() {
                    assert!(files[file_index as usize - 1].next().unwrap() == trigram as u64);
                }
            }

            for mut file in files {
                assert!(file.next().is_none())
            }
        }

        assert_eq!(AddressReader::open(&location, id).unwrap().read_generation().unwrap(), 3);

        {
            let file = JournalFilter::open(location.clone(), id).await?;
            file.defrag().await.unwrap();
            assert_eq!(AddressReader::open(&location, id).unwrap().read_generation().unwrap(), 0);

            let mut files = trigrams.iter().map(|row|StreamDecode::new(&row.1)).collect_vec();
            let mut output = file.read_all();

            while let Some(row) = output.recv().await {
                let (trigram, values) = row?;
                for &file_index in values.iter().rev() {
                    assert!(files[file_index as usize - 1].next().unwrap() == trigram as u64);
                }
            }

            for mut file in files {
                assert!(file.next().is_none())
            }
        }

        assert_eq!(AddressReader::open(&location, id).unwrap().read_generation().unwrap(), 0);

        Ok(())
    }

    /// rollback the tail file to simulate a failure mid write
    #[tokio::test]
    async fn failed_write() -> Result<()> {
        let timer = std::time::Instant::now();
        // build test data
        let mut trigrams = vec![];

        for ii in 1..21 {
            let (_, b) = random_trigrams(ii);
            trigrams.push((ii, b));
        }
        println!("generate {:.2}", timer.elapsed().as_secs_f64());

        // write it
        let tempdir = tempfile::tempdir()?;
        let location = tempdir.path().to_path_buf();
        let id = FilterID::from(1);
        {
            let timer = std::time::Instant::now();
            let file = JournalFilter::new(location.clone(), id).await?;
            println!("open finished {:.2}", timer.elapsed().as_secs_f64());
            let timer = std::time::Instant::now();
            file.write_batch(trigrams[0..10].iter().cloned().collect_vec()).await?;
            println!("write 1 finished {:.2}", timer.elapsed().as_secs_f64());

            std::fs::copy(location.join(format!("{id}.tail")), location.join("back"))?;

            let timer = std::time::Instant::now();
            file.write_batch(trigrams[10..20].iter().cloned().collect_vec()).await?;
            println!("write 2 finished {:.2}", timer.elapsed().as_secs_f64());
        }

        std::fs::rename(location.join("back"), location.join(format!("{id}.tail")))?;

        // Recreate the trigrams
        let timer = std::time::Instant::now();
        {
            let file = JournalFilter::open(location.clone(), id).await?;

            let mut files = trigrams.iter().take(10).map(|row|StreamDecode::new(&row.1)).collect_vec();
            let mut output = file.read_all();

            while let Some(row) = output.recv().await {
                let (trigram, mut values) = row?;
                values.reverse();
                for file_index in values {
                    let file_index = file_index as usize - 1;
                    if file_index < 10 {
                        assert!(files[file_index].next().unwrap() == trigram as u64);
                    } else {
                        panic!("Unexpected id: {file_index}");
                    }
                }
            }

            for mut file in files {
                assert!(file.next().is_none())
            }
        }
        println!("read {:.2}", timer.elapsed().as_secs_f64());
        assert_eq!(AddressReader::open(&location, id).unwrap().read_generation().unwrap(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn large_batch() -> Result<()> {
        init();

        // build test data
        // let mut original = vec![];
        let mut trigrams = vec![];
        let timestamp = std::time::Instant::now();
        for ii in 1..1001 {
            let (bits, buffer) = random_trigrams(ii);
            // original.push(bits);
            trigrams.push((ii, buffer))
        }
        println!("generate {:.2}", timestamp.elapsed().as_secs_f64());

        // write it
        let tempdir = tempfile::tempdir()?;
        let location = tempdir.path().to_path_buf();
        let id = FilterID::from(1);

        {
            let time = std::time::Instant::now();
            let file = JournalFilter::new(location.clone(), id).await?;
            file.write_batch(trigrams.clone()).await?;
            println!("write data: {:.2}", time.elapsed().as_secs_f64());
        }

        // Recreate the trigrams
        let timer = std::time::Instant::now();
        {
            let file = JournalFilter::open(location.clone(), id).await?;

            let mut files = trigrams.iter().map(|row|StreamDecode::new(&row.1)).collect_vec();
            let mut output = file.read_all();

            while let Some(row) = output.recv().await {
                let (trigram, mut values) = row?;
                values.reverse();
                for file_index in values {
                    assert!(files[file_index as usize - 1].next().unwrap() == trigram as u64);
                }
            }

            for mut file in files {
                assert!(file.next().is_none())
            }
        }
        println!("read {}", timer.elapsed().as_secs_f64());
        assert_eq!(AddressReader::open(&location, id).unwrap().read_generation().unwrap(), 1);
        Ok(())
    }

    #[test]
    fn collector() {
        let mut objects: Vec<(u64, Vec<u8>)> = vec![];
        for ii in 0..256 {
            let mut indices = vec![];
            for jj in 0..=0xFFFF {
                indices.push(ii << 16 | jj);
            }
            indices.sort_unstable();
            let mut buffer = vec![];
            encode_into_increasing(&indices, &mut buffer);
            objects.push((ii + 1, buffer));
        }

        let mut collector = IdCollector::new_from_vec(&objects);

        let mut rounds = 0;
        let mut ids = vec![];
        while let Some(trigram) = collector.next(&mut ids) {
            assert_eq!(vec![(trigram as u64 >> 16) + 1], ids);
            rounds += 1;
        }
        assert_eq!(rounds, 1 << 24)
    }

    #[tokio::test]
    async fn simple_queries() -> Result<()> {
        // setup data
        let raw_data1 = std::include_bytes!("./journal.rs");
        let raw_data2 = std::include_bytes!("./manager.rs");
        let raw_data3 = std::include_bytes!("./trigrams.rs");
        let trigrams1 = build_buffer_to_offsets(raw_data1);
        let trigrams2 = build_buffer_to_offsets(raw_data2);
        let trigrams3 = build_buffer_to_offsets(raw_data3);

        // build table
        let tempdir = tempfile::tempdir()?;
        let id = FilterID::from(1);
        let location = tempdir.path().to_path_buf();
        let file = JournalFilter::new(location, id).await?;
        println!("write 1");
        file.write_batch(vec![(1, trigrams1)]).await?;
        println!("write 2");
        file.write_batch(vec![(2, trigrams2)]).await?;
        println!("write 3");
        file.write_batch(vec![(3, trigrams3)]).await?;

        // run literal query that should only be in this file
        println!("query 1");
        let query = Query::Literal(b"This shouldn't occur in any files and the odds of a false positive should be low.".to_vec());
        let hits = file.query(query.clone()).await?;
        assert_eq!(hits, vec![1]);

        // run literal query that should not exist anywhere. False positives should also be low given the length.
        println!("query 2");
        let bytes: Vec<u8> = (0..48).map(|_| rand::thread_rng().gen()).collect();
        let query = Query::Literal(bytes.clone());
        let hits = file.query(query.clone()).await?;
        assert!(hits.is_empty());

        // run a query that should hit on everything
        println!("query 3");
        let query = Query::Or(vec![Query::Literal(b"fn ".to_vec()), Query::Literal(bytes.clone())]);
        let hits = file.query(query.clone()).await?;
        assert_eq!(hits, vec![1, 2, 3]);

        // run a query that should hit on two of the files
        println!("query 4");
        let query = Query::Or(vec![Query::Literal(b"JournalFilter".to_vec()), Query::Literal(bytes)]);
        let hits = file.query(query.clone()).await?;
        assert_eq!(hits, vec![1, 2]);

        return Ok(())
    }

}