use bitvec::bitarr;
use bitvec::prelude::BitArray;
use bitvec::vec::BitVec;
use num_format::{Locale, ToFormattedString};
use anyhow::Result;

enum Chunk {
    Added(Vec<u16>),
    Mask(u16, Box<BitArray<[u64; 1024]>>),
    Removed(Vec<u16>)
}

impl Chunk {
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

                if items.len() > 4096 {
                    let count = items.len() as u16;
                    let mut mask = Box::new(bitarr!(u64, bitvec::prelude::Lsb0; 1024; 1 << 16));
                    for item in items {
                        mask.set(*item as usize, true);
                    }
                    *self = Chunk::Mask(count, mask);
                }
            },
            Chunk::Mask(count, mask) => {
                if *count >= (u16::MAX - 4096) {
                    let mut items = vec![];
                    for index in mask.iter_zeros() {
                        items.push(index as u16);
                    }
                    *self = Chunk::Removed(items);
                }
            },
            Chunk::Removed(items) => {
                items.sort_unstable();
                items.dedup();
                items.reserve_exact(0);
            },
        }
    }
}

struct SparseBits {
    // counts: Box<[u16; 256]>,
    chunks: Box<[Chunk; 256]>
}

impl SparseBits {
    pub fn new() -> Self {
        const EMPTY_CHUNK: Chunk = Chunk::Added(vec![]);
        SparseBits { 
            chunks: Box::new([EMPTY_CHUNK; 256]) 
        }
    }

    pub fn memory(&self) -> usize {
        let mut memory = core::mem::size_of::<Self>();
        for part in self.chunks.iter() {
            memory += part.memory();
        }
        memory
    }

    pub fn insert(&mut self, item: u32) {
        let bin = (item >> 16) as usize;
        self.chunks[bin].insert(item as u16);
    }

    pub fn compact(&mut self) {
        for part in self.chunks.iter_mut() {
            part.compact();
        }
    }
}


fn main() -> Result<()> {
    println!("{}", 2097152.to_formatted_string(&Locale::en));
    println!("{}", core::mem::size_of::<Chunk>());
    println!("{}", core::mem::size_of::<SparseBits>());
    println!("{}", core::mem::size_of::<[u16; 256]>());
    println!("{}", core::mem::size_of::<SparseBits>() + core::mem::size_of::<[Chunk; 256]>());

    // let file = std::fs::File::open("./src/blob_cache.rs")?;
    let file = std::fs::File::open("./target/debug/haunted-house")?;
    // let file = std::fs::File::open("./target/debug/counter")?;
    let mask = build_file(file)?;
    let mut sparse = SparseBits::new();
    println!("empty size: {}", sparse.memory());

    for (count, index) in mask.iter_ones().enumerate() {
        sparse.insert(index as u32);
        if count % (1 << 16) == 0 {
            println!("size: {}", sparse.memory().to_formatted_string(&Locale::en));
            sparse.compact();
        }
    }
    println!("size: {}", sparse.memory().to_formatted_string(&Locale::en));
    sparse.compact();
    println!("size: {}", sparse.memory().to_formatted_string(&Locale::en));

    Ok(())
}

pub fn build_file<IN: std::io::Read>(mut handle: IN) -> Result<BitVec> {
    // Prepare to read
    let mut buffer: Vec<u8> = vec![0; 1 << 20];

    // Prepare accumulators
    let mut mask = BitVec::repeat(false, 1 << 24);

    // Read the initial block
    let read = handle.read(&mut buffer)?;

    // Terminate if file too short
    if read <= 2 {
        return Ok(mask)
    }

    // Initialize trigram
    let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
    let mut index_start = 2;

    loop {
        for index in index_start..read {
            trigram = (trigram & 0x00FFFF) << 8 | (buffer[index] as u32);
            mask.set(trigram as usize, true);
        }

        let read = handle.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        index_start = 0;
    }

    return Ok(mask)
}