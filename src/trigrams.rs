use std::ops::{BitOr, BitXor};

use anyhow::Result;
use bitvec::vec::BitVec;

use crate::filter::{Filter, LoadFilter};
use crate::query::Query;





#[derive(Clone)]
pub struct TrigramFilter {
    pub data: BitVec<u64>
}

impl TrigramFilter {
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
                    if !*self.data.get(index as usize).unwrap() {
                        return false
                    }
                }
                return true
            },
        }
    }

}

impl LoadFilter for TrigramFilter {
    fn load(kind: &str, data: &Vec<u8>) -> Result<Self> where Self: Sized {
        if kind != "tri" {
            return Err(anyhow::anyhow!("Unknown trigram kind"))
        }
        Ok(Self{data: crate::encoding::decode(data, 1 << 24)?})
    }
}

impl Filter for TrigramFilter {
    fn kind(&self) -> String {
        format!("tri")
    }

    fn data<'a>(&'a self) -> &'a bitvec::vec::BitVec<u64> {
        &self.data
    }

    fn overlap(&self, other: &Self) -> Result<Self> where Self: Sized {
        Ok(TrigramFilter{data: self.data.clone().bitor(&other.data)})
    }

    fn merge(items: Vec<&Self>) -> Result<Self> where Self: Sized {
        let mut out = BitVec::repeat(false, 1 << 24);
        for item in items {
            out = out.bitxor(&item.data);
        }
        return Ok(Self{data: out});
    }
}


#[cfg(test)]
mod test {
    use bitvec::vec::BitVec;
    use rand::{thread_rng, Rng};

    use crate::filter::{Filter, LoadFilter};
    use crate::query::Query;

    use super::{TrigramFilter};


    #[test]
    fn save_and_load() {
        let mut trigrams = BitVec::new();
        let mut prng = thread_rng();
        while trigrams.len() < (1 << 24) {
            trigrams.push(prng.gen());
        }

        let filter = TrigramFilter{data: trigrams};
        let kind = filter.kind();
        let data = filter.to_buffer().unwrap();
        let unpacked = TrigramFilter::load(&kind, &data).unwrap();
        assert_eq!(filter.data, unpacked.data)
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

        let filter = TrigramFilter{data: trigrams};

        for _ in 0..1000 {
            let index = prng.gen_range(0..(data.len() - 8));
            let sample = data[index..index+8].to_vec();
            let query = Query::Literal(sample);
            assert!(filter.query(&query));
        }

        // let mut hits = 0;
        for _ in 0..1000 {
            let sample: [u8; 10] = prng.gen();
            let query = Query::Literal(sample.to_vec());
            if filter.query(&query) {
                //
            }
        }
    }

}