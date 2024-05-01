//! Tools for handling the trigram sets from files.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::iter::Peekable;

use anyhow::Result;
use itertools::Itertools;
use log::error;
use tokio::sync::{Semaphore, RwLock};
use tokio::task::JoinHandle;
use bitvec::{bitarr, BitArr};
use bitvec::prelude::BitArray;
use serde::{Serialize, Deserialize, de::Error};

use crate::config::WorkerSettings;
use crate::error::ErrorKinds;
use crate::storage::BlobStorage;
use crate::types::{Sha256, FilterID, FileInfo};

use super::encoding::encode_into;

/// A manager for a directory holding the trigram sets yet to be written to filters.
pub struct TrigramCache {
    /// Directory for the trigrams to be stored while waiting to be written
    cache_dir: PathBuf,
    /// A temporary directory used for files in the middle of writing (so they can be installed by atomic rename)
    temp_dir: PathBuf,
    /// A semaphore to track the number of trigram sets currently being calculated to limit resource consumption
    permits: Semaphore,
    /// List of jobs either building trigram sets or waiting for a permit
    pending: RwLock<HashMap<(FilterID, Sha256), JoinHandle<()>>>,
    /// List of jobs that have been rejected and can't be completed
    rejected: RwLock<HashSet<(FilterID, Sha256)>>,
    /// A storage driver where the target files can be loaded from
    files: BlobStorage
}


impl TrigramCache {
    /// Setup the cache
    pub async fn new(config: &WorkerSettings, files: BlobStorage) -> Result<Arc<Self>> {
        let temp_dir = config.get_trigram_cache_directory().join("temp");
        if temp_dir.exists() {
            tokio::fs::remove_dir_all(&temp_dir).await?;
        }
        tokio::fs::create_dir_all(&temp_dir).await?;
        Ok(Arc::new(Self {
            cache_dir: config.get_trigram_cache_directory(),
            temp_dir,
            permits: Semaphore::new(config.parallel_file_downloads),
            pending: RwLock::new(Default::default()),
            rejected: Default::default(),
            files,
        }))
    }

    /// Expire all files related to a particular filter
    pub async fn expire(&self, filter: FilterID) -> Result<()> {
        // Flush out pending tasks
        {
            let mut pending = self.pending.write().await;
            let mut remove = vec![];
            for (key, task) in pending.iter() {
                if key.0 == filter {
                    remove.push(key.clone());
                    task.abort();
                }
            }
            for key in remove {
                pending.remove(&key);
            }
        }

        // Erase directory
        let dir = self._filter_path(filter);
        if dir.exists() {
            tokio::fs::remove_dir_all(dir).await?;
        }
        return Ok(())
    }

    /// Start pulling a file into the trigram cache
    pub async fn start_fetch(self: &Arc<Self>, filter: FilterID, hash: Sha256) -> Result<()> {
        if self.is_ready(filter, &hash).await? {
            return Ok(())
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = self.pending.write().await.entry((filter, hash.clone())) {
            let core = self.clone();
            entry.insert(tokio::spawn(async move {
                let mut not_found_errors = 0;
                let rejected = loop {
                    if let Err(err) = core._fetch_file(filter, &hash).await {
                        error!("Fetch file error: {err} [attempt {not_found_errors}]");
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        if ErrorKinds::BlobNotFound == err {
                            not_found_errors += 1;
                            if not_found_errors < 10 {
                                continue
                            }
                        }
                        break true
                    }
                    break false
                };
                if rejected {
                    let mut rejected = core.rejected.write().await;
                    rejected.insert((filter, hash.clone()));
                }

                let mut pending = core.pending.write().await;
                pending.remove(&(filter, hash));

            }));
        }
        return Ok(())
    }

    async fn _fetch_file(&self, filter: FilterID, hash: &Sha256) -> Result<(), ErrorKinds> {
        let _permit = self.permits.acquire().await?;

        // Gather the file content
        let stream = self.files.stream(&hash.hex()).await?;
        let trigrams = build_file(stream).await?;

        // Store the trigrams
        let cache_path = self._path(filter, hash);
        let temp_dir = self.temp_dir.clone();
        let filter_dir = self._filter_path(filter);
        tokio::task::spawn_blocking(move ||{
            let mut temp = tempfile::NamedTempFile::new_in(&temp_dir)?;
            temp.write_all(&postcard::to_allocvec(&trigrams)?)?;
            temp.flush()?;
            std::fs::create_dir_all(filter_dir)?;
            temp.persist(cache_path)?;
            Result::<(), ErrorKinds>::Ok(())
        }).await??;
        return Ok(())
    }

    pub async fn strip_pending(&self, collection: &mut Vec<(FilterID, FileInfo)>) {
        let workers = self.pending.read().await;
        collection.retain(|(filter, info)|{
            !workers.contains_key(&(*filter, info.hash.clone()))
        });
    }

    pub async fn add_pending(&self, collection: &mut HashMap<FilterID, HashSet<Sha256>>) {
        let workers = self.pending.read().await;
        for (filter, sha) in workers.keys() {
            match collection.entry(*filter) {
                std::collections::hash_map::Entry::Occupied(mut entry) => { entry.get_mut().insert(sha.clone()); },
                std::collections::hash_map::Entry::Vacant(entry) => { entry.insert([sha.clone()].into()); },
            }
        }
    }

    pub async fn is_ready(&self, filter: FilterID, hash: &Sha256) -> Result<bool> {
        Ok(tokio::fs::try_exists(self._path(filter, hash)).await?)
    }

    pub async fn clear_rejected(&self, filter: FilterID, hash: Sha256) -> bool {
        let mut rejected = self.rejected.write().await;
        rejected.remove(&(filter, hash))
    }

    /// Load the content of a file
    pub async fn get(&self, filter: FilterID, hash: &Sha256) -> Result<Vec<u8>> {
        let data = tokio::fs::read(self._path(filter, hash)).await?;
        Ok(TrigramFile::read_bytes(&data)?.into_packed_deltas())
    }

    /// Free a specific file from the cache
    pub async fn release(&self, filter: FilterID, hash: &Sha256) -> Result<()> {
        let path = self._path(filter, hash);
        match tokio::fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(err) => {
                if !tokio::fs::try_exists(path).await? {
                    Ok(())
                } else {
                    Err(err.into())
                }
            },
        }
    }

    /// helper function to map filter ids to cache subdirectory
    pub fn _filter_path(&self, filter: FilterID) -> PathBuf {
        self.cache_dir.join(filter.to_string())
    }

    /// Get the path where a specific file will be saved
    pub fn _path(&self, filter: FilterID, hash: &Sha256) -> PathBuf {
        self._filter_path(filter).join(hash.to_string())
    }
}

#[cfg(test)]
pub (crate) fn build_buffer(data: &[u8]) -> TrigramSet {
    // Prepare accumulators
    let mut output = TrigramSet::new();

    if data.len() < 3 {
        return output
    }

    // Initialize trigram
    let mut trigram: u32 = (data[0] as u32) << 8 | (data[1] as u32);

    for (byte_index, byte) in data[2..].iter().enumerate() {
        trigram = (trigram & 0x00FFFF) << 8 | (*byte as u32);
        output.insert(trigram);
        if byte_index % 16000 == 0 {
            output.compact();
        }
    }
    output.compact();

    return output
}

#[cfg(test)]
pub (crate) type Bits = bitvec::vec::BitVec::<usize, bitvec::prelude::Lsb0>;

#[cfg(test)]
pub (crate) fn build_buffer_to_offsets(input: &[u8]) -> Vec<u8> {
    // Prepare accumulators
    let mut mask = Box::<BitArray::<[u64; 1 << 24]>>::default();

    let mut index = 2;
    let mut trigram: u32 = (input[0] as u32) << 8 | (input[1] as u32);

    while index < input.len() {
        trigram = ((trigram & 0x00FFFF) << 8) | (input[index] as u32);
        unsafe { mask.set_unchecked(trigram as usize, true); }
        index += 1;
    }

    let mut output = vec![];
    let trigrams = mask.iter_ones().map(|v| v as u64).collect_vec();
    encode_into(&trigrams, &mut output);

    return output
}

/// Convert a stream of buffers into a trigram set
async fn build_file(mut input: tokio::sync::mpsc::Receiver<Result<Vec<u8>, ErrorKinds>>) -> Result<TrigramFile, ErrorKinds> {
    // Prepare accumulators
    let mut mask = Box::<BitArray::<[u64; 1 << 24]>>::default();

    // Read the initial block
    let mut buffer = vec![];
    while buffer.len() <= 2 {
        let sub_buffer = match input.recv().await {
            Some(sub_buffer) => sub_buffer?,
            None => return Ok(TrigramFile::Deltas(vec![])),
        };

        buffer.extend(sub_buffer);
    }

    // Initialize with first 2/3rds of first trigram
    let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
    let mut index_start = 2;

    loop {
        for byte in &buffer[index_start..] {
            trigram = (trigram & 0x00FFFF) << 8 | (*byte as u32);
            unsafe {mask.set_unchecked(trigram as usize, true)};
        }

        buffer = match input.recv().await {
            Some(buffer) => buffer?,
            None => break,
        };
        if buffer.is_empty() {
            break;
        }
        index_start = 0;
    }

    // convert mask into offsets
    let mut buffer = vec![];
    let indices = mask.iter_ones().map(|v|v as u64).collect_vec();
    encode_into(&indices, &mut buffer);
    return Ok(TrigramFile::Deltas(buffer))
}


/// For underlying reasons _NO_ varients of this enum can be removed, changed, or reordered.
/// If you want to change or add data do it by adding a new varient at the end.
#[derive(Serialize, Deserialize)]
#[repr(u32)]
pub enum TrigramFile {
    Chunked(TrigramSet) = 0,
    Deltas(Vec<u8>) = 1,
}

impl TrigramFile {
    #[cfg(test)]
    pub fn into_trigramset(self) -> TrigramSet {
        use super::encoding::StreamDecode;

        match self {
            TrigramFile::Chunked(set) => set,
            TrigramFile::Deltas(data) => {
                let mut set = TrigramSet::new();
                for trigram in StreamDecode::new(&data) {
                    set.insert(trigram as u32);
                }
                set.compact();
                set
            },
        }
    }

    pub fn into_packed_deltas(self) -> Vec<u8> {
        match self {
            TrigramFile::Chunked(set) => {
                let trigrams = set.iter().map(|v|v as u64).collect_vec();
                let mut buffer = vec![];
                encode_into(&trigrams, &mut buffer);
                buffer
            },
            TrigramFile::Deltas(data) => data,
        }
    }

    pub fn read_bytes(data: &[u8]) -> Result<Self> {
        if data.len() > 2 && data[0..2] == [0x80, 0x02] {
            return Ok(TrigramFile::Chunked(postcard::from_bytes(data)?))
        }
        Ok(postcard::from_bytes(data)?)
    }
}

/// A helper class for storing a set of trigrams efficently.
///
/// The trigrams are stored using a dynamic encoding where each block of
/// 2^16 trigrams are each encoded separatly.
/// For small sets of trigrams explicit lists of trigrams are used, for dense sets it
/// falls back into a bitmap.
#[derive(Debug, Clone)]
pub struct TrigramSet {
    /// A list of chunks of the bitmap addressed by the highest 8 bits of the trigram.
    /// This lets each chunk choose the storage method that suits it best.
    chunks: Box<[Chunk; 256]>
}

/// An iterator over the trigrams in a SparseBits struct
pub struct TrigramIterator<'a> {
    /// Iterator over chunks in the struct being traversed
    iter: std::slice::Iter<'a, Chunk>,
    /// Index of the current chunk
    current_index: u32,
    /// Iterator of values within the current chunk
    current: ChunkIter<'a>
}

impl Iterator for TrigramIterator<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(value) = self.current.next() {
                return Some(self.current_index << 16 | value as u32)
            }

            match self.iter.next() {
                Some(new) => {
                    self.current_index += 1;
                    self.current = new.iter();
                },
                None => return None
            }
        }
    }
}

impl PartialEq for TrigramSet {
    fn eq(&self, other: &Self) -> bool {
        self.chunks == other.chunks
    }
}

impl Eq for TrigramSet {}

impl Serialize for TrigramSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer
    {
        let mut chunks = vec![];
        for item in self.chunks.iter() {
            chunks.push(item);
        }
        chunks.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TrigramSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let chunks = Vec::<Chunk>::deserialize(deserializer)?;
        Ok(Self {
            chunks: match chunks.try_into() {
                Ok(chunks) => chunks,
                Err(_) => return Err(D::Error::custom("incomplete file")),
            }
        })
    }
}

impl TrigramSet {
    /// Create an empty bitset
    pub fn new() -> Self {
        TrigramSet {
            chunks: Box::new([Chunk::EMPTY; 256])
        }
    }

    // pub fn num_set(&self) -> usize {
    //     let mut set = 0;
    //     for chunk in self.chunks.iter() {
    //         set += chunk.num_set()
    //     }
    //     set
    // }

    /// Create an iterator over the indices of the set bits
    pub fn iter(&self) -> TrigramIterator {
        let mut iter = self.chunks.iter();
        let current = iter.next().unwrap();
        TrigramIterator { iter, current_index: 0, current: current.iter() }
    }

    /// Add a trigram to the bitset
    pub fn insert(&mut self, item: u32) {
        let bin = (item >> 16) as usize;
        self.chunks[bin].insert(item as u16);
    }

    /// Adjust the encoding methods to suit the current content
    pub fn compact(&mut self) {
        for part in self.chunks.iter_mut() {
            part.compact();
        }
    }

    #[cfg(test)]
    pub fn random(seed: u64) -> Self {
        use rand::SeedableRng;

        let mut prng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut values = vec![];
        values.resize_with(256, ||Chunk::random(&mut prng));
        Self {
            chunks: values.try_into().unwrap()
        }
    }
}

/// A bitset over 2^16 values
#[derive(Serialize, Deserialize, Debug, Clone)]
enum Chunk {
    Empty,
    /// Bitset encoded as indices of each set bit
    Added(Vec<u16>),
    /// Bitset incoded as a literal bit flag per value
    Mask(u16, Box<BitArray<[u64; 1024]>>),
    /// Bitset encoded as indices of each unset bit
    Removed(Vec<u16>)
}

impl Ord for Chunk {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.iter().cmp(other.iter())
    }
}

impl PartialOrd for Chunk {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(std::cmp::Ord::cmp(self, other))
    }
}

impl PartialEq for Chunk {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for Chunk {

}

/// Iterator over the set indices in the bitset chunk
enum ChunkIter<'a> {
    Empty,
    /// Iterate over encoded indices directly
    Added(std::slice::Iter<'a, u16>),
    /// Use bitset set bit index iterator directly
    Mask(bitvec::slice::IterOnes<'a, u64, bitvec::prelude::Lsb0>),
    /// Iterate over exastive set of ranges, selecting the ones not set in the second iterartor
    /// over the list of unset bits
    Removed(std::ops::RangeInclusive<u16>, Peekable<std::slice::Iter<'a, u16>>)
}

impl Iterator for ChunkIter<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ChunkIter::Empty => None,
            ChunkIter::Added(iter) => iter.next().copied(),
            ChunkIter::Mask(iter) => iter.next().map(|v|v as u16),
            ChunkIter::Removed(iter, skip_list) => {
                while let Some(next) = iter.next() {
                    match skip_list.peek() {
                        Some(skip) => match next.cmp(skip) {
                            std::cmp::Ordering::Less => return Some(next),
                            std::cmp::Ordering::Equal => { skip_list.next(); continue },
                            std::cmp::Ordering::Greater => break,
                        },
                        None => return Some(next),
                    }
                }
                return None
            },
        }
    }
}


impl Chunk {
    /// Default empty chunk
    const EMPTY: Self = Self::Empty;

    #[cfg(test)]
    pub fn empty() -> Self { Self::EMPTY }

    // pub fn num_set(&self) -> usize {
    //     match self  {
    //         Chunk::Empty => 0,
    //         Chunk::Added(items) => items.len(),
    //         Chunk::Mask(count, _) => *count as usize,
    //         Chunk::Removed(items) => u16::MAX as usize - items.len(),
    //     }
    // }

    /// Create an iterator over the indices of set bits
    pub fn iter(&self) -> ChunkIter {
        match self {
            Chunk::Empty => ChunkIter::Empty,
            Chunk::Added(items) => ChunkIter::Added(items.iter()),
            Chunk::Mask(_, mask) => ChunkIter::Mask(mask.iter_ones()),
            Chunk::Removed(items) => ChunkIter::Removed(0..=u16::MAX, items.iter().peekable()),
        }
    }

    #[cfg(test)]
    pub fn memory(&self) -> usize {
        core::mem::size_of::<Self>() + match self {
            Chunk::Empty => 0,
            Chunk::Added(items) => items.capacity() * core::mem::size_of::<u16>(),
            Chunk::Mask(_, _) => 1024 * core::mem::size_of::<u64>(),
            Chunk::Removed(items) => items.capacity() * core::mem::size_of::<u16>(),
        }
    }

    /// Add a value to the bitset
    #[inline]
    pub fn insert(&mut self, item: u16) {
        match self {
            Chunk::Empty => {
                *self = Chunk::Added(vec![item]);
            }
            Chunk::Added(items) => { items.push(item); },
            Chunk::Mask(count, items) => {
                if unsafe { !items.get_unchecked(item as usize) } {
                    *count += 1;
                }
                items.set(item as usize, true);
            },
            Chunk::Removed(items) => {
                for (index, value) in items.iter().enumerate() {
                    if item == *value {
                        items.remove(index);
                        return
                    }
                }
            },
        }
    }

    /// Adjust the encoding to suit the current content
    pub fn compact(&mut self) {
        match self {
            Chunk::Empty => {}
            Chunk::Added(items) => {
                items.sort_unstable();
                items.dedup();

                if items.len() >= 4096 {
                    let count = items.len() as u16;
                    let mut mask: Box<BitArr!(for 1 << 16, in u64)> = Box::new(bitarr!(u64, bitvec::prelude::Lsb0; 0; 1 << 16));
                    for item in items {
                        mask.set(*item as usize, true);
                    }
                    *self = Chunk::Mask(count, mask);
                    self.compact()
                }
            },
            Chunk::Mask(count, mask) => {
                if *count > (u16::MAX - 4096) {
                    let mut items = vec![];
                    for index in mask.iter_zeros() {
                        items.push(index as u16);
                    }
                    *self = Chunk::Removed(items);
                    self.compact()
                }
            },
            Chunk::Removed(_items) => {
                // items.reserve_exact(0);
            },
        }
    }

    #[cfg(test)]
    fn random(prng: &mut impl rand::Rng) -> Self {
        use bitvec::vec::BitVec;

        let mut data = BitVec::<usize, bitvec::prelude::Lsb0>::repeat(false, u16::MAX.into());
        let raw = data.as_raw_mut_slice();
        for part in raw {
            *part = prng.gen::<usize>() & prng.gen::<usize>() & prng.gen::<usize>();
        }

        let mut new = Chunk::Added(vec![]);
        for index in data.iter_ones() {
            new.insert(index as u16)
        }
        new.compact();
        return new
    }
}

#[cfg(test)]
pub (crate) fn random_trigrams(seed: u64) -> (Bits, Vec<u8>) {
    use rand::{Rng, SeedableRng};
    use crate::worker::filter::TRIGRAM_RANGE;

    let mut prng = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut data = Bits::repeat(false, TRIGRAM_RANGE as usize);

    let raw = data.as_raw_mut_slice();
    for part in raw {
        *part = prng.gen::<usize>() & prng.gen::<usize>() & prng.gen::<usize>();
    }

    let trigrams = data.iter_ones().map(|i| i as u64).collect_vec();
    let mut buffer = vec![];
    encode_into(&trigrams, &mut buffer);
    (data, buffer)
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeSet, HashSet};

    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    use crate::worker::{encoding::{encode_into, StreamDecode}, trigrams::{build_buffer, build_buffer_to_offsets}};

    use super::{Chunk, TrigramFile, TrigramSet};


    #[test]
    fn build_random_chunk() {
        let mut values = (0..u16::MAX).collect_vec();

        let mut a = Chunk::empty();
        let mut b = BTreeSet::<u16>::new();
        let mut prng = thread_rng();

        while !values.is_empty() {
            let value = values.swap_remove(prng.gen_range(0..values.len()));
            a.insert(value);
            b.insert(value);
            let before = a.memory();
            a.compact();
            assert!(a.iter().eq(b.iter().cloned()));
            assert!(a.memory() <= before);
        }
    }

    #[test]
    fn single_values() {
        for ii in 0..=0xFFFFFF {
            let mut bits = TrigramSet::new();
            bits.insert(ii);
            assert!(bits.iter().eq([ii].iter().cloned()))
        }
    }

    #[test]
    fn adding_values() {
        // Have the first chunk of this map get fully saturated plus a little more
        let mut bits = TrigramSet::new();
        let mut values = vec![];
        for ii in 0..=0x01FFFF {
            bits.insert(ii);
            bits.compact();
            values.push(ii);
            if bits.iter().ne(values.iter().cloned()) {
                let a: HashSet<u32> = bits.iter().collect();
                let b: HashSet<u32> = values.iter().cloned().collect();
                println!("Extra {:?}", a.difference(&b));
                println!("Missing {:?}", b.difference(&a));
                panic!();
            }
        }
    }



    #[test]
    fn build_compare() {

        let data = std::fs::read("/bin/dockerd").unwrap();

        {
            let a = build_buffer(&data);
            let b = build_buffer_to_offsets(&data);
            let a = a.iter().collect_vec();
            let b = StreamDecode::new(&b).map(|a| a as u32).collect_vec();
            assert_eq!(a.first(), b.first());
            assert_eq!(a.last(), b.last());
            assert_eq!(a.len(), b.len());
            assert_eq!(a, b);
        }

        const ITERATIONS: usize = 10;
        let stamp = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            build_buffer(&data);
        }
        println!("{}", stamp.elapsed().as_secs_f64() / (ITERATIONS as f64) * 1000.0);

        let stamp = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            build_buffer_to_offsets(&data);
        }
        println!("{}", stamp.elapsed().as_secs_f64() / (ITERATIONS as f64) * 1000.0);

    }


    #[test]
    fn iteration() {
        // build a random trigram set
        // let trigrams = TrigramSet::random(88888);
        let trigrams = build_buffer(&std::fs::read("/bin/dockerd").unwrap());

        let values = trigrams.iter().map(|v|v as u64).collect_vec();

        let mut buffer = vec![];
        encode_into(&values, &mut buffer);

        assert_eq!(StreamDecode::new(&buffer).collect_vec(), values);

        println!("{}", postcard::to_allocvec(&trigrams).unwrap().len());
        println!("{}", buffer.len());

        let stamp = std::time::Instant::now();
        let mut scratch = vec![];
        for _ in 0..100 {
            let mut value: u32 = 0;
            for item in trigrams.iter() {
                value = value.overflowing_add(item).0;
            }
            scratch.push(value);
        }
        println!("{}", stamp.elapsed().as_secs_f64() / 100.0 * 1000.0);

        let stamp = std::time::Instant::now();
        for _ in 0..100 {
            let mut value: u32 = 0;
            for item in StreamDecode::new(&buffer) {
                value = value.overflowing_add(item as u32).0;
            }
            scratch.push(value);
        }

        println!("{}", stamp.elapsed().as_secs_f64() / 100.0 * 1000.0);

        assert!(scratch.iter().all_equal());
        assert_eq!(scratch.len(), 200);
    }

    #[test]
    fn file_format_switch() {        
        let data = TrigramSet::random(10);
        let bytes = postcard::to_allocvec(&data).unwrap();

        let file: TrigramFile = TrigramFile::read_bytes(&bytes).unwrap();
        let recreated = file.into_trigramset();
        assert_eq!(data, recreated);
    }

}

