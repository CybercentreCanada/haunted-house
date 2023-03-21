use std::ops::BitOr;

use bitvec::vec::BitVec;
use anyhow::Result;
use serde::{Deserialize, Serialize};



pub const START_POWER: u64 = 10;
pub const END_POWER: u64 = 26;

#[derive(Serialize, Deserialize, Clone)]
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
}
