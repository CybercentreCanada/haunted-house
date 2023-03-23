use std::ops::BitOr;

use bitvec::vec::BitVec;
use anyhow::Result;

use crate::query::Query;



pub const START_POWER: u64 = 10;
pub const END_POWER: u64 = 26;

#[derive(Clone)]
pub struct SimpleFilter {
    pub data: BitVec
}

impl SimpleFilter {
    pub fn empty(size: u64) -> Self {
        Self { data: bitvec::bitvec![0; size as usize] }
    }

    pub fn build(size: u64, trigrams: &BitVec) -> Self {
        let mut buffer = Self::empty(size);
        for index in trigrams.iter_ones() {
            buffer.data.set(index % size as usize, true);
        }
        buffer
    }

    pub fn load(kind: &str, data: &Vec<u8>) -> Result<Self> {
        let size = Self::parse_kind(kind)?;
        let data: BitVec = postcard::from_bytes(data)?;
        if data.len() != size as usize {
            return Err(anyhow::anyhow!("Data doesn't match kind"))
        }
        Ok(Self{data})
    }

    pub fn to_buffer(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(&self.data)?)
    }

    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn count_ones(&self) -> usize {
        self.data.count_ones()
    }

    pub fn density(&self) -> f64 {
        (self.count_ones() as f64) / (self.data.len() as f64)
    }

    pub fn kind(&self) -> String {
        format!("simple:{}", self.size())
    }

    pub fn parse_kind(kind: &str) -> Result<u64> {
        if let Some((kind, size)) = kind.split_once(":") {
            if kind == "simple" {
                if let Ok(size) = size.parse::<u64>() {
                    return Ok(size)
                }
            }
        }
        return Err(anyhow::anyhow!("Invalid filter kind: {kind}"));
    }

    pub fn overlap(&self, other: &SimpleFilter) -> Result<SimpleFilter> {
        if self.size() != other.size() {
            return Err(anyhow::anyhow!("Incompatable filter combination"))
        }
        Ok(SimpleFilter{data: self.data.clone().bitor(&other.data)})
    }

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
                for trigram in term.windows(3) {
                    let index = (trigram[0] as u32) << 16 | (trigram[1] as u32) << 8 | (trigram[2] as u32);
                    if !self.data.get(index as usize % self.data.len()).unwrap() {
                        return false
                    }
                }
                return true
            },
        }
    }
}

#[cfg(test)]
mod test {
    use bitvec::vec::BitVec;
    use rand::{thread_rng, Rng};

    use crate::query::Query;

    use super::{SimpleFilter, START_POWER, END_POWER};


    #[test]
    fn save_and_load() {
        let mut trigrams = BitVec::new();
        let mut prng = thread_rng();
        while trigrams.len() < (1 << 24) {
            trigrams.push(prng.gen());
        }

        for power in START_POWER..=END_POWER {
            let filter = SimpleFilter::build(1 << power, &trigrams);
            let kind = filter.kind();
            let data = filter.to_buffer().unwrap();
            let unpacked = SimpleFilter::load(&kind, &data).unwrap();
            assert_eq!(filter.data, unpacked.data)
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

        let filter = SimpleFilter::build(1 << 10, &trigrams);

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
        assert!(hits < 900);
    }

}