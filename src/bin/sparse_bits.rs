// use bitvec::bitarr;
// use bitvec::prelude::BitArray;
// use bitvec::vec::BitVec;
// use num_format::{Locale, ToFormattedString};
use anyhow::Result;

fn main() -> Result<()> {
//     println!("{}", 2097152.to_formatted_string(&Locale::en));
//     println!("{}", core::mem::size_of::<Chunk>());
//     println!("{}", core::mem::size_of::<SparseBits>());
//     println!("{}", core::mem::size_of::<[u16; 256]>());
//     println!("{}", core::mem::size_of::<SparseBits>() + core::mem::size_of::<[Chunk; 256]>());

//     // let file = std::fs::File::open("./src/blob_cache.rs")?;
//     let file = std::fs::File::open("./target/debug/haunted-house")?;
//     // let file = std::fs::File::open("./target/debug/counter")?;
//     let mask = build_file(file)?;
//     let mut sparse = SparseBits::new();
//     println!("empty size: {}", sparse.memory());

//     for (count, index) in mask.iter_ones().enumerate() {
//         sparse.insert(index as u32);
//         if count % (1 << 16) == 0 {
//             let mem = sparse.memory();
//             sparse.compact();
//             println!("size: {} -> {}", mem.to_formatted_string(&Locale::en), sparse.memory().to_formatted_string(&Locale::en));
//         }
//     }
//     println!("size: {}", sparse.memory().to_formatted_string(&Locale::en));
//     sparse.compact();
//     println!("size: {}", sparse.memory().to_formatted_string(&Locale::en));

    Ok(())
}

// pub fn build_file<IN: std::io::Read>(mut handle: IN) -> Result<BitVec> {
//     // Prepare to read
//     let mut buffer: Vec<u8> = vec![0; 1 << 20];

//     // Prepare accumulators
//     let mut mask = BitVec::repeat(false, 1 << 24);

//     // Read the initial block
//     let read = handle.read(&mut buffer)?;

//     // Terminate if file too short
//     if read <= 2 {
//         return Ok(mask)
//     }

//     // Initialize trigram
//     let mut trigram: u32 = (buffer[0] as u32) << 8 | (buffer[1] as u32);
//     let mut index_start = 2;

//     loop {
//         for index in index_start..read {
//             trigram = (trigram & 0x00FFFF) << 8 | (buffer[index] as u32);
//             mask.set(trigram as usize, true);
//         }

//         let read = handle.read(&mut buffer)?;
//         if read == 0 {
//             break;
//         }
//         index_start = 0;
//     }

//     return Ok(mask)
// }