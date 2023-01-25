use std::collections::{HashSet, BinaryHeap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use anyhow::{anyhow, Result};
use bitvec::vec::BitVec;



pub trait Filter: Send + Sync {

    fn label(&self) -> String;
    fn data<'a>(&'a self) -> &'a BitVec;

    fn search(&self, target: &Vec<u8>) -> Result<bool>;
}

pub fn load(label: &str, data: BitVec) -> Result<Box<dyn Filter>> {
    if label.starts_with("simple:") {
        Ok(Box::new(SimpleFilter::load(label, data)?))
    } else if label.starts_with("kin:") {
        Ok(Box::new(KinFilter::load(label, data)?))
    } else {
        Err(anyhow!("Unknown label format: {label}"))
    }
}

pub struct SimpleFilter {
    data: BitVec
}

impl Filter for SimpleFilter {
    fn label(&self) -> String {
        SimpleFilter::label_as(self.data.len() as u32)
    }

    fn data<'a>(&'a self) -> &'a BitVec {
        &self.data
    }

    fn search(&self, target: &Vec<u8>) -> Result<bool> {
        if target.len() < 3 {
            return Ok(false);
        }
        let mut trigram = ((target[0] as u32) << 8) | target[1] as u32;

        for byte in target[2..].iter() {
            trigram = ((trigram & 0xFFFF) << 8) | *byte as u32;

            let mut hasher = DefaultHasher::new();
            trigram.hash(&mut hasher);

            let index = hasher.finish() as usize % self.data.len();
            if !self.data.get(index).unwrap() {
                return Ok(false)
            }
        }

        return Ok(true)
    }
}

impl SimpleFilter {
    pub fn build<IN: std::io::Read>(settings: u32, input: IN) -> Result<Self> {
        // init buffer
        let mut data = BitVec::new();
        data.resize(settings as usize, false);
        let mut bytes = input.bytes();

        // build initial state
        let first = match bytes.next() {
            Some(first) => first?,
            None => return Ok(SimpleFilter { data }),
        };
        let second = match bytes.next() {
            Some(first) => first?,
            None => return Ok(SimpleFilter { data }),
        };

        let mut trigram: u32 = ((first as u32) << 8) | second as u32;

        // injest data
        for byte in bytes.into_iter() {
            trigram = ((trigram & 0xFFFF) << 8) | byte? as u32;

            let mut hasher = DefaultHasher::new();
            trigram.hash(&mut hasher);

            let index = hasher.finish() % settings as u64;
            data.set(index as usize, true);
        }

        //
        Ok(Self { data })
    }

    pub fn label_as(size: u32) -> String {
        format!("simple:{}", size)
    }

    pub fn load(label: &str, data: BitVec) -> Result<Self> {
        let mut parts = label.split(":");
        let kind_name = parts.next().ok_or(anyhow!("Invalid label"))?;
        if kind_name != "simple" {
            return Err(anyhow!("Invalid label"));
        }
        let expected_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
        if data.len() != expected_size as usize {
            return Err(anyhow!("corrupt"));
        }
        return Ok(SimpleFilter { data })
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn expand(&mut self, other: &SimpleFilter) {
        todo!();
    }
}


pub struct OpenBucketFilter {
    data: BitVec,
    bucket_size: u32,
}

impl Filter for OpenBucketFilter {
    fn label(&self) -> String {
        format!("open:{}:{}", self.data.len(), self.bucket_size)
    }

    fn data<'a>(&'a self) -> &'a BitVec {
        &self.data
    }

    fn search(&self, target: &Vec<u8>) -> Result<bool> {
        if target.len() < 3 {
            return Ok(false);
        }
        let mut trigram = ((target[0] as u32) << 8) | target[1] as u32;

        let mut active_offsets: Vec<usize> = (0..self.bucket_size as usize).collect();

        for (sub_index, byte) in target.iter().enumerate().skip(2) {
            trigram = ((trigram & 0xFFFF) << 8) | *byte as u32;

            let mut new_offsets = vec![];

            for offset in active_offsets {
                let mut hasher = DefaultHasher::new();
                trigram.hash(&mut hasher);
                ((offset + sub_index) % self.bucket_size as usize).hash(&mut hasher);

                let index = hasher.finish() as usize % self.data.len();
                if *self.data.get(index).unwrap() {
                    new_offsets.push(offset);
                }
            }

            if new_offsets.is_empty() {
                 return Ok(false)
            }
            active_offsets = new_offsets;
        }

        return Ok(true)
    }

}


impl OpenBucketFilter {
    pub fn build<IN: std::io::Read>(buffer_size: u32, bucket_size: u32, input: IN) -> Result<Self> {
        // init buffer
        let mut data = BitVec::new();
        data.resize(buffer_size as usize, false);
        let mut bytes = input.bytes();

        // build initial state
        let first = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, bucket_size }),
        };
        let second = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, bucket_size }),
        };

        let mut trigram: u32 = ((first as u32) << 8) | second as u32;

        // injest data
        for (index, byte) in bytes.into_iter().enumerate() {
            trigram = ((trigram & 0xFFFF) << 8) | byte? as u32;

            let mut hasher = DefaultHasher::new();
            trigram.hash(&mut hasher);
            (index % bucket_size as usize).hash(&mut hasher);

            let index = hasher.finish() % buffer_size as u64;
            data.set(index as usize, true);
        }

        //
        Ok(Self { data, bucket_size })
    }

    // fn load(label: String, data: BitVec) -> Result<Self> {
    //     let mut parts = label.split(":");
    //     let kind_name = parts.next().ok_or(anyhow!("Invalid label"))?;
    //     if kind_name != "open" {
    //         return Err(anyhow!("Invalid label"));
    //     }
    //     let expected_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
    //     if data.len() != expected_size as usize {
    //         return Err(anyhow!("corrupt"));
    //     }
    //     let bucket_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
    //     if data.len() != expected_size as usize {
    //         return Err(anyhow!("corrupt"));
    //     }
    //     return Ok(Self { data, bucket_size })
    // }

}


pub struct ClosedBucketFilter {
    data: BitVec,
    buckets: u64,
    bucket_size: u64,
}

impl Filter for ClosedBucketFilter {
    fn label(&self) -> String {
        format!("closed:{}:{}", self.data.len(), self.bucket_size)
    }

    fn data<'a>(&'a self) -> &'a BitVec {
        &self.data
    }

    fn search(&self, target: &Vec<u8>) -> Result<bool> {
        if target.len() < 3 {
            return Ok(false);
        }
        let mut trigram = ((target[0] as u32) << 8) | target[1] as u32;

        let mut active_offsets: Vec<u64> = (0..self.bucket_size).collect();

        for (sub_index, byte) in target.iter().enumerate().skip(2) {
            trigram = ((trigram & 0xFFFF) << 8) | *byte as u32;

            let mut new_offsets = vec![];

            for offset in active_offsets {
                let mut hasher = DefaultHasher::new();
                trigram.hash(&mut hasher);

                let index = (hasher.finish() % self.buckets) * self.bucket_size + (offset + sub_index as u64) % self.bucket_size;
                if *self.data.get(index as usize).unwrap() {
                    new_offsets.push(offset);
                }
            }

            if new_offsets.is_empty() {
                 return Ok(false)
            }
            active_offsets = new_offsets;
        }

        return Ok(true)
    }

}


impl ClosedBucketFilter {
    pub fn build<IN: std::io::Read>(buffer_size: u64, bucket_size: u64, input: IN) -> Result<Self> {
        // init buffer
        let mut data = BitVec::new();
        data.resize(buffer_size as usize, false);
        let mut bytes = input.bytes();
        let buckets = buffer_size / bucket_size;
        if buckets * bucket_size != buffer_size {
            return Err(anyhow!("bucket size must be factor of buffer size"));
        }

        // build initial state
        let first = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, buckets, bucket_size }),
        };
        let second = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, buckets, bucket_size }),
        };

        let mut trigram: u32 = ((first as u32) << 8) | second as u32;

        // injest data
        for (index, byte) in bytes.into_iter().enumerate() {
            trigram = ((trigram & 0xFFFF) << 8) | byte? as u32;

            let mut hasher = DefaultHasher::new();
            trigram.hash(&mut hasher);

            let index = (hasher.finish() % buckets) * bucket_size + (index as u64) % bucket_size;
            data.set(index as usize, true);
        }

        //
        Ok(Self { data, buckets, bucket_size })
    }

    // fn load(label: String, data: BitVec) -> Result<Self> {
    //     let mut parts = label.split(":");
    //     let kind_name = parts.next().ok_or(anyhow!("Invalid label"))?;
    //     if kind_name != "open" {
    //         return Err(anyhow!("Invalid label"));
    //     }
    //     let expected_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
    //     if data.len() != expected_size as usize {
    //         return Err(anyhow!("corrupt"));
    //     }
    //     let bucket_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
    //     if data.len() != expected_size as usize {
    //         return Err(anyhow!("corrupt"));
    //     }
    //     return Ok(Self { data, bucket_size })
    // }

}


pub struct KinFilter {
    data: BitVec,
    min_find: u32,
    max_set: u32,
}

impl Filter for KinFilter {
    fn label(&self) -> String {
        Self::label_as(self.data.len(), self.min_find, self.max_set)
    }

    fn data<'a>(&'a self) -> &'a BitVec {
        &self.data
    }

    fn search(&self, target: &Vec<u8>) -> Result<bool> {
        if target.len() < 3 {
            return Ok(false);
        }
        let mut trigram = ((target[0] as u32) << 8) | target[1] as u32;

        for byte in target[2..].iter() {
            trigram = ((trigram & 0xFFFF) << 8) | *byte as u32;

            let mut hits = 0;
            for index in Self::hash_trigram(trigram, self.max_set, self.data.len()) {
                if *self.data.get(index).unwrap() {
                    hits += 1;
                    if hits >= self.min_find {
                        break;
                    }
                }
            }

            if hits < self.min_find {
                return Ok(false)
            }
        }

        return Ok(true)
    }
}

#[derive(PartialEq, Eq)]
struct Item {
    index: usize,
    trigrams: HashSet<u32>
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.trigrams.len().cmp(&other.trigrams.len())
    }
}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl KinFilter {
    pub fn build<IN: std::io::Read>(size: usize, min_find: u32, max_set: u32, input: IN) -> Result<Self> {
        // init buffer
        let mut data = BitVec::new();
        data.resize(size, false);

        let mut heatmap: Vec<Vec<u32>> = Default::default();
        heatmap.resize_with(size,  ||Default::default());

        let mut bytes = input.bytes();

        // build initial state
        let first = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, min_find, max_set }),
        };
        let second = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, min_find, max_set }),
        };

        let mut trigram: u32 = ((first as u32) << 8) | second as u32;

        // injest data
        for byte in bytes.into_iter() {
            trigram = ((trigram & 0xFFFF) << 8) | byte? as u32;

            for index in Self::hash_trigram(trigram, max_set, size) {
                heatmap[index].push(trigram);
            }
        }

        let mut missing: BinaryHeap<Item> = heatmap.into_iter().enumerate().map(|(index, trigrams)|Item{index, trigrams: trigrams.into_iter().collect()}).collect();
        let mut finished: HashSet<u32> = Default::default();
        let mut assignments = vec![0; 1 << 24];

        while let Some(mut current) = missing.pop() {
            // println!("kin {size} {}", missing.len());

            // Update the trigram list
            current.trigrams.retain(|trigram| !finished.contains(trigram));
            if current.trigrams.is_empty() {
                continue;
            }

            // Check if we should drop this item back into the heap
            if let Some(next) = missing.peek() {
                if next.trigrams.len() > current.trigrams.len() {
                    missing.push(current);
                    continue;
                }
            }

            // We are processing this item
            data.set(current.index, true);

            for trigram in current.trigrams {
                assignments[trigram as usize] += 1;
                if assignments[trigram as usize] >= min_find {
                    finished.insert(trigram);
                }
            }
        }

        // let mut assignments = vec![0; 1 << 24];
        // for iteration in 0.. {
        //     println!("kin {size} {}", missing.len());
        //     if iteration % 10 == 0 {
        //         let mut ii = 0;
        //         while ii < missing.len() {
        //             if missing[ii].1.borrow().is_empty() {
        //                 missing.swap_remove(ii);
        //             } else {
        //                 ii += 1;
        //             }
        //         }
        //     }
        //     missing.sort_by_key(|a| a.1.borrow().len());

        //     let (index, trigrams) = match missing.pop() {
        //         Some(row) => row,
        //         None => break,
        //     };

        //     data.set(index, true);

        //     let trigrams: HashSet<u32> = trigrams.borrow_mut().clone();
        //     for trigram in trigrams {
        //         assignments[trigram as usize] += 1;
        //         if assignments[trigram as usize] >= min_find {
        //             for index in Self::hash_trigram(trigram, max_set, size) {
        //                 heatmap[index].borrow_mut().remove(&trigram);
        //             }

        //             // for row in missing.iter_mut() {
        //             //     row.1.remove(&trigram);
        //             // }
        //         }
        //     }
        // }

        return Ok(Self { data, min_find, max_set })
    }

    fn hash_trigram(trigram: u32, max_set: u32, bins: usize) -> Vec<usize> {
        let mut out: HashSet<usize> = HashSet::default();

        let mut hasher = DefaultHasher::new();
        trigram.hash(&mut hasher);

        let mut ii = 0;
        while out.len() < max_set as usize {
            out.insert(hasher.finish() as usize % bins);
            ii += 1;
            ii.hash(&mut hasher);
        }

        return out.into_iter().collect();
    }

    pub fn label_as(size: usize, min_find: u32, max_set: u32) -> String  {
        format!("kin:{}:{}:{}", size, min_find, max_set)
    }

    fn load(label: &str, data: BitVec) -> Result<Self> {
        let mut parts = label.split(":");
        let kind_name = parts.next().ok_or(anyhow!("Invalid label"))?;
        if kind_name != "kin" {
            return Err(anyhow!("Invalid label"));
        }
        let expected_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
        if data.len() != expected_size as usize {
            return Err(anyhow!("corrupt"));
        }
        let min_find = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
        let max_set = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
        if min_find > max_set {
            return Err(anyhow!("couldn't figure out kin label: {label}"));
        }
        return Ok(KinFilter { data, min_find, max_set })
    }

}