//! A helper class for storing a set of trigrams efficently.
//!
//! In the worst case this just falls back into a dense bitmap. For many small files however
//! the dense bitmap uses far more memory than just storing a list of trigrams directly.

use std::iter::Peekable;

use bitvec::{bitarr, BitArr};
use bitvec::prelude::BitArray;
use serde::{Serialize, Deserialize, de::Error};

/// Store a set of trigrams using a sparse layout.
#[derive(Debug, Clone)]
pub struct SparseBits {
    /// A list of chunks of the bitmap addressed by the highest 8 bits of the trigram.
    /// This lets each chunk choose the storage method that suits it best.
    chunks: Box<[Chunk; 256]>
}

/// An iterator over the trigrams in a SparseBits struct
pub struct SparseIterator<'a> {
    /// Iterator over chunks in the struct being traversed
    iter: std::slice::Iter<'a, Chunk>,
    /// Index of the current chunk
    current_index: u32,
    /// Iterator of values within the current chunk
    current: Iter<'a>
}

impl Iterator for SparseIterator<'_> {
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

impl PartialEq for SparseBits {
    fn eq(&self, other: &Self) -> bool {
        self.chunks == other.chunks
    }
}

impl Eq for SparseBits {}

impl Serialize for SparseBits {
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

impl<'de> Deserialize<'de> for SparseBits {
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

impl SparseBits {
    /// Create an empty bitset
    pub fn new() -> Self {
        SparseBits {
            chunks: Box::new([Chunk::EMPTY; 256])
        }
    }

    /// Create an iterator over the indices of the set bits 
    pub fn iter(&self) -> SparseIterator {
        let mut iter = self.chunks.iter();
        let current = iter.next().unwrap();
        SparseIterator { iter, current_index: 0, current: current.iter() }
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
        Some(self.iter().cmp(other.iter()))
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
enum Iter<'a> {
    /// Iterate over encoded indices directly
    Added(std::slice::Iter<'a, u16>),
    /// Use bitset set bit index iterator directly
    Mask(bitvec::slice::IterOnes<'a, u64, bitvec::prelude::Lsb0>),
    /// Iterate over exastive set of ranges, selecting the ones not set in the second iterartor 
    /// over the list of unset bits
    Removed(std::ops::RangeInclusive<u16>, Peekable<std::slice::Iter<'a, u16>>)
}

impl Iterator for Iter<'_> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Added(iter) => iter.next().copied(),
            Iter::Mask(iter) => iter.next().map(|v|v as u16),
            Iter::Removed(iter, skip_list) => {
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
    const EMPTY: Self = Self::Added(vec![]);

    #[cfg(test)]
    pub fn empty() -> Self { Self::EMPTY }

    /// Create an iterator over the indices of set bits
    pub fn iter(&self) -> Iter {
        match self {
            Chunk::Added(items) => Iter::Added(items.iter()),
            Chunk::Mask(_, mask) => Iter::Mask(mask.iter_ones()),
            Chunk::Removed(items) => Iter::Removed(0..=u16::MAX, items.iter().peekable()),
        }
    }

    #[cfg(test)]
    pub fn memory(&self) -> usize {
        core::mem::size_of::<Self>() + match self {
            Chunk::Added(items) => items.capacity() * core::mem::size_of::<u16>(),
            Chunk::Mask(_, _) => 1024 * core::mem::size_of::<u64>(),
            Chunk::Removed(items) => items.capacity() * core::mem::size_of::<u16>(),
        }
    }

    /// Add a value to the bitset
    pub fn insert(&mut self, item: u16) {
        match self {
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
mod test {
    use std::collections::{BTreeSet, HashSet};

    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    use super::{Chunk, SparseBits};


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
            let mut bits = SparseBits::new();
            bits.insert(ii);
            assert!(bits.iter().eq([ii].iter().cloned()))
        }
    }

    #[test]
    fn adding_values() {
        // Have the first chunk of this map get fully saturated plus a little more
        let mut bits = SparseBits::new();
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


}