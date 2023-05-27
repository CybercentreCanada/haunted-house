use std::iter::Peekable;

use bitvec::{bitarr, BitArr};
use bitvec::prelude::BitArray;
use serde::{Serialize, Deserialize, de::Error};

#[derive(Debug, Clone)]
pub struct SparseBits {
    chunks: Box<[Chunk; 256]>
}

impl Ord for SparseBits {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.chunks.iter().cmp(other.chunks.iter())
    }
}

impl PartialOrd for SparseBits {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.chunks.iter().cmp(other.chunks.iter()))
    }
}

impl PartialEq for SparseBits {
    fn eq(&self, other: &Self) -> bool {
        self.chunks == other.chunks
    }
}

impl Eq for SparseBits {
}

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
    pub fn new() -> Self {
        const EMPTY_CHUNK: Chunk = Chunk::empty();
        SparseBits { 
            chunks: Box::new([EMPTY_CHUNK; 256]) 
        }
    }

    // #[cfg(test)]
    // pub fn memory(&self) -> usize {
    //     let mut memory = core::mem::size_of::<Self>();
    //     for part in self.chunks.iter() {
    //         memory += part.memory();
    //     }
    //     memory
    // }

    pub fn insert(&mut self, item: u32) {
        let bin = (item >> 16) as usize;
        self.chunks[bin].insert(item as u16);
    }

    pub fn compact(&mut self) {
        for part in self.chunks.iter_mut() {
            part.compact();
        }
    }

    pub fn has(&self, index: usize) -> bool {
        let chunk = &self.chunks[index >> 16];
        match chunk {
            Chunk::Added(items) => items.binary_search(&(index as u16)).is_ok(),
            Chunk::Mask(_, items) => unsafe {*items.get_unchecked(index & 0xFFFF)},
            Chunk::Removed(items) => !items.binary_search(&(index as u16)).is_ok(),
        }
    }

    #[cfg(test)]
    pub fn random(seed: u64) -> Self {
        use rand::SeedableRng;

        let mut prng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut values = vec![];
        for _ in 0..0xFF {
            values.push(Chunk::random(&mut prng));
        }
        Self {
            chunks: values.try_into().unwrap()
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
enum Chunk {
    Added(Vec<u16>),
    Mask(u16, Box<BitArray<[u64; 1024]>>),
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

enum Iter<'a> {
    Added(std::slice::Iter<'a, u16>),
    Mask(bitvec::slice::IterOnes<'a, u64, bitvec::prelude::Lsb0>),
    Removed(std::ops::Range<u16>, Peekable<std::slice::Iter<'a, u16>>)
}

impl<'a> Iterator for Iter<'a> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Added(iter) => iter.next().as_deref().copied(),
            Iter::Mask(iter) => match iter.next() {
                Some(val) => Some(val as u16),
                None => None,
            },
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
    pub const fn empty() -> Self {
        Self::Added(vec![])
    }

    pub fn iter(&self) -> Iter {
        match self {
            Chunk::Added(items) => Iter::Added(items.iter()),
            Chunk::Mask(_, mask) => Iter::Mask(mask.iter_ones()),
            Chunk::Removed(items) => Iter::Removed(0..u16::MAX, items.iter().peekable()),
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
            Chunk::Removed(items) => {
                items.reserve_exact(0);
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
    use std::collections::BTreeSet;

    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    use super::Chunk;


    #[test]
    fn build_random() {
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
}