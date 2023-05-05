use std::ops::BitOr;

use bitvec::vec::BitVec;
use anyhow::Result;

use crate::filter::{Filter, LoadFilter};
use crate::query::Query;


pub const START_POWER: u64 = 10;
pub const END_POWER: u64 = 21;


#[derive(Clone)]
pub struct BloomFilter {
    hits: u32,
    hashes: u32,
    pub data: BitVec<u64>
}

fn hash(trigram: u32, hashes: u32) -> Vec<u64> {
    let a = (trigram >> 16) as u8;
    let b = (trigram >> 8) as u8;
    let c = trigram as u8;
    hash_bytes(a, b, c, hashes)
}

fn hash_bytes(a: u8, b: u8, c: u8, hashes: u32) -> Vec<u64> {
    let mut values = vec![];

    let bytes: Vec<u8> = vec![a, b, c];

    let mut counter: u32 = 0;
    while values.len() < hashes as usize{
        let mut data = bytes.clone();
        data.extend(counter.to_le_bytes());
        while data.last() == Some(&0) {
            data.pop();
        }

        values.push(seahash::hash(&data));
        values.sort_unstable();
        values.dedup();

        counter += 1;
    }
    return values;
}


pub struct PreparedTrigrams(Vec<Vec<u64>>);

impl BloomFilter {
    pub fn empty(size: u64, hits: u32, hashes: u32) -> Self {
        Self {
            hits,
            hashes,
            data: BitVec::<u64>::repeat(false, size as usize)
        }
    }

    pub fn prepare(hashes: u32, trigrams: &BitVec<u64>) -> PreparedTrigrams {
        let mut buffer = vec![];
        for index in trigrams.iter_ones() {
            buffer.push(hash(index as u32, hashes));
        }
        buffer.sort_unstable();
        buffer.dedup();
        PreparedTrigrams(buffer)
    }

    pub fn build(size: u64, hits: u32, hashes: u32, indices: &PreparedTrigrams) -> Self {
        let mut buffer = Self::empty(size, hits, hashes);
        for (counter, index_set) in indices.0.iter().enumerate() {
            let mut confirmed_hits = 0;
            let mut misses = vec![];

            for index in index_set {
                let index = (*index % size) as usize;
                if *buffer.data.get(index).unwrap() {
                    confirmed_hits += 1;
                } else {
                    misses.push(index);
                }
            }

            while confirmed_hits < hits {
                let chosen = counter % misses.len();
                let index = misses.swap_remove(chosen);
                buffer.data.set(index, true);
                confirmed_hits += 1;
            }
        }
        buffer
    }

    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    // pub fn cost(&self) -> f64 {
    //     let density = self.density();
    //     density / (1.0 - density)
    // }

    pub fn density(&self) -> f64 {
        (self.data.count_ones() as f64) / (self.data.len() as f64)
    }

    pub fn params(&self) -> (u64, u32, u32) {
        (self.size(), self.hits, self.hashes)
    }

    pub fn parse_kind(kind: &str) -> Result<(u64, u32, u32)> {
        let parts: Vec<&str> = kind.split(":").collect();
        if parts.len() != 4 || parts[0] != "in" {
            return Err(anyhow::anyhow!("Invalid filter kind: {kind}"));
        }
        let size = parts[1].parse::<u64>()?;
        let hits = parts[2].parse::<u32>()?;
        let hashes = parts[3].parse::<u32>()?;
        return Ok((size, hits, hashes))
    }
}

impl BloomFilter {
    pub fn query(&self, query: &Query) -> bool {
        match query {
            Query::And(items) => {
                for item in items {
                    if !self.query(item) {
                        return false
                    }
                }
                return true
            },
            Query::Or(items) => {
                for item in items {
                    if self.query(item) {
                        return true
                    }
                }
                return false
            },
            Query::Literal(term) => {
                let size = self.data.len() as u64;
                for trigram in term.windows(3) {
                    let mut count = 0;
                    for index in hash_bytes(trigram[0], trigram[1], trigram[2], self.hashes) {
                        if *self.data.get((index % size) as usize).unwrap() {
                            count += 1;
                        }
                    }
                    if count < self.hits {
                        return false
                    }
                }
                return true
            },
        }
    }
}

impl LoadFilter for BloomFilter {
    fn load(kind: &str, data: &Vec<u8>) -> Result<Self> where Self: Sized {
        let (size, hits, hashes) = Self::parse_kind(kind)?;
        let data = crate::encoding::decode(data, size as i64)?;
        return Ok(Self{hits, hashes, data})
    }
}

impl Filter for BloomFilter {
    fn kind(&self) -> String {
        format!("in:{:0>7}:{}:{}", self.size(), self.hits, self.hashes)
    }

    fn data<'a>(&'a self) -> &'a bitvec::vec::BitVec<u64> {
        &self.data
    }

    fn overlap(&self, other: &Self) -> Result<Self> where Self: Sized {
        if self.size() != other.size() {
            return Err(anyhow::anyhow!("Incompatable filter combination"))
        }
        Ok(BloomFilter{hits: self.hits, hashes: self.hashes, data: self.data.clone().bitor(&other.data)})
    }

    fn merge(items: Vec<&Self>) -> Result<Self> where Self: Sized {
        let (size, hits, hashes) = match items.first() {
            Some(item) => item.params(),
            None => return Err(anyhow::anyhow!("tried to merge empty set")),
        };
        let cover = items.iter()
            .fold(BloomFilter::empty(size, hits, hashes), |a, b|a.overlap(b).unwrap());
        return Ok(cover)
    }

}

#[cfg(test)]
mod test {
    use bitvec::vec::BitVec;
    use rand::{thread_rng, Rng};

    use crate::filter::{Filter, LoadFilter};
    use crate::query::Query;

    use super::{BloomFilter, START_POWER, END_POWER};


    #[test]
    fn save_and_load() {
        let mut trigrams = BitVec::new();
        let mut prng = thread_rng();
        while trigrams.len() < (1 << 24) {
            trigrams.push(prng.gen());
        }

        let prepared = BloomFilter::prepare(1, &trigrams);

        for power in START_POWER..=END_POWER {
            let filter = BloomFilter::build(1 << power, 1, 1, &prepared);
            let kind = filter.kind();
            let data = filter.to_buffer().unwrap();
            let unpacked = BloomFilter::load(&kind, &data).unwrap();
            assert_eq!(filter.data, unpacked.data)
        }
    }

    #[test]
    fn varible_hits() {
        let mut trigrams = BitVec::new();
        let mut prng = thread_rng();
        while trigrams.len() < (1 << 24) {
            trigrams.push(prng.gen());
        }

        let prepared = BloomFilter::prepare(3, &trigrams);

        for power in START_POWER..=END_POWER {
            let filtera = BloomFilter::build(1 << power, 3, 3, &prepared);
            let filterb = BloomFilter::build(1 << power, 2, 3, &prepared);
            assert!(filterb.density() <= filtera.density());

            for filter in [filtera, filterb] {
                let kind = filter.kind();
                let data = filter.to_buffer().unwrap();
                let unpacked = BloomFilter::load(&kind, &data).unwrap();
                assert_eq!(filter.data, unpacked.data)
            }
        }
    }

    #[test]
    fn query() {
        let mut prng = thread_rng();
        let mut data: Vec<u8> = vec![];
        while data.len() < (1 << 12) {
            data.push(prng.gen());
        }

        let mut trigrams = BitVec::repeat(false, 1 << 24);
        let mut trigram: u32 = (data[0] as u32) << 8 | (data[1] as u32);
        for byte in &data[2..] {
            trigram = (trigram & 0x00FFFF) << 8 | (*byte as u32);
            trigrams.set(trigram as usize, true);
        }

        for hashes in [1, 2, 3, 4] {
            let prepared = BloomFilter::prepare(hashes, &trigrams);
            for hits in 1..=hashes {
                println!("{hits} {hashes}");
                let filter = BloomFilter::build(1 << 10, hits, hashes, &prepared);

                for _ in 0..1000 {
                    let index = prng.gen_range(0..(data.len() - 8));
                    let sample = data[index..index+8].to_vec();
                    let query = Query::Literal(sample);
                    assert!(filter.query(&query));
                }
                println!("{}", filter.density());

                let mut hits = 0;
                for _ in 0..1000 {
                    let sample: [u8; 10] = prng.gen();
                    let query = Query::Literal(sample.to_vec());
                    if filter.query(&query) {
                        hits += 1;
                    }
                }
                assert!(hits < 900 * hits / hashes );
            }
        }
    }

}