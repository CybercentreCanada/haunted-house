use core::panic;
use std::ops::BitOr;

use anyhow::Result;
use crate::bloom::BloomFilter;
use crate::query::Query;
use crate::trigrams::TrigramFilter;

pub trait LoadFilter {
    fn load(kind: &str, data: &Vec<u8>) -> Result<Self> where Self: Sized;
}

// Trait used to keep most of the database code generic for insertion
// and balancing collections
pub trait Filter: Clone + LoadFilter {
    fn kind(&self) -> String;

    fn data<'a>(&'a self) -> &'a bitvec::vec::BitVec<u64>;
    fn to_buffer(&self) -> Result<Vec<u8>> {
        crate::encoding::encode(self.data())
    }
    fn count_ones(&self) -> usize {
        self.data().count_ones()
    }
    fn count_zeros(&self) -> usize {
        self.data().count_zeros()
    }
    fn full(&self) -> bool {
        self.count_ones() == self.data().len()
    }
    fn density(&self) -> f64 {
        self.count_ones() as f64/self.data().len() as f64
    }

    fn overlap(&self, other: &Self) -> Result<Self> where Self: Sized;
    fn merge(items: Vec<&Self>) -> Result<Self> where Self: Sized;

    fn overlap_count_ones(&self, other: &Self) -> Result<(usize, usize)> where Self: Sized {
        let a = self.data().as_raw_slice();
        let b = other.data().as_raw_slice();
        let mut self_count = 0;
        let mut count = 0;
        for (a, b) in itertools::zip_eq(a, b) {
            self_count += a.count_ones() as usize;
            count += a.bitor(b).count_ones() as usize;
        }
        return Ok((self_count, count));
    }
}

// Sometimes its easier to bundle them togeather rather than
// use function parameters, like when buffering ingestion or
// during searching
pub enum GenericFilter {
    Bloom(BloomFilter),
    Trigram(TrigramFilter)
}

impl GenericFilter {
    pub fn query(&self, query: &Query) -> bool {
        match self {
            GenericFilter::Bloom(bloom) => bloom.query(query),
            GenericFilter::Trigram(trigram) => trigram.query(query),
        }
    }

    pub fn kind(&self) -> String {
        match self {
            GenericFilter::Bloom(bloom) => bloom.kind(),
            GenericFilter::Trigram(trigram) => trigram.kind(),
        }
    }

    pub fn density(&self) -> f64 {
        match self {
            GenericFilter::Bloom(bloom) => bloom.density(),
            GenericFilter::Trigram(trigram) => trigram.density(),
        }
    }

    pub fn overlap(&self, other: &GenericFilter) -> GenericFilter {
        match self {
            GenericFilter::Bloom(bloom) => match other {
                GenericFilter::Bloom(other) => GenericFilter::Bloom(bloom.overlap(other).unwrap()),
                GenericFilter::Trigram(_) => panic!(),
            },
            GenericFilter::Trigram(trigram) => match other {
                GenericFilter::Bloom(_) => panic!(),
                GenericFilter::Trigram(other) => GenericFilter::Trigram(trigram.overlap(other).unwrap()),
            },
        }
    }

    pub fn overlap_count_ones(&self, other: &GenericFilter) -> (usize, usize) {
        match self {
            GenericFilter::Bloom(bloom) => match other {
                GenericFilter::Bloom(other) => bloom.overlap_count_ones(other).unwrap(),
                GenericFilter::Trigram(_) => panic!(),
            },
            GenericFilter::Trigram(trigram) => match other {
                GenericFilter::Bloom(_) => panic!(),
                GenericFilter::Trigram(other) => trigram.overlap_count_ones(other).unwrap(),
            },
        }
    }

    pub fn count_ones(&self) -> usize {
        match self {
            GenericFilter::Bloom(bloom) => bloom.count_ones(),
            GenericFilter::Trigram(trigram) => trigram.count_ones(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            GenericFilter::Bloom(bloom) => bloom.size() as usize,
            GenericFilter::Trigram(trigram) => trigram.data().len(),
        }
    }

}

impl LoadFilter for GenericFilter {
    fn load(kind: &str, data: &Vec<u8>) -> Result<Self> where Self: Sized {
        if kind.starts_with("in") {
            Ok(GenericFilter::Bloom(BloomFilter::load(kind, data)?))
        } else if kind.starts_with("tri") {
            Ok(GenericFilter::Trigram(TrigramFilter::load(kind, data)?))
        } else {
            Err(anyhow::anyhow!("Unknown filter kind: <{kind}>"))
        }
    }
}