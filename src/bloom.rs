use bitvec::vec::BitVec;
use anyhow::Result;
use serde::{Deserialize, Serialize};



pub const START_POWER: u64 = 10;
pub const END_POWER: u64 = 23;

#[derive(Serialize, Deserialize, Clone)]
pub struct SimpleFilter {
    data: BitVec
}

impl SimpleFilter {
    pub fn build(size: u64, trigrams: &BitVec) -> Result<Self> {
        todo!();
    }

    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn density(&self) -> f64 {
        (self.data.count_ones() as f64) / (self.data.len() as f64)
    }

    pub fn as_bytes(&self) -> &[u8] {
        todo!()
    }

    pub fn kind(&self) -> String {
        format!("simple:{}", self.data.len())
    }

    pub fn empty(&self) -> Self {
        todo!();
    }
}