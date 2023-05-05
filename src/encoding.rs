use std::io::Write;

use anyhow::Result;

use bitvec::vec::BitVec;
use int_enum::IntEnum;
use itertools::{chain, Itertools};



#[repr(u8)]
#[derive(Clone, Copy, IntEnum)]
enum Encodings {
    FixedLe = 0,
    OnesIndexVarInt = 1,
    ZeroIndexVarInt = 2,
}


pub fn encode(bitmap: &BitVec<u64>) -> Result<Vec<u8>> {
    // bitmap.force_align();

    let sparse = if bitmap.count_ones() < bitmap.len()/2 {
        encode_var(Encodings::OnesIndexVarInt, bitmap.iter_ones(), bitmap.len() as i64)?
    } else {
        encode_var(Encodings::ZeroIndexVarInt, bitmap.iter_zeros(), bitmap.len() as i64)?
    };

    let fixed_size = bitmap.len() * std::mem::size_of::<u64>() + 1;

    Ok(if sparse.len() < fixed_size {
        sparse
    } else {
        encode_le(bitmap.as_raw_slice().iter())
    })
}

pub fn decode(data: &Vec<u8>, size: i64) -> Result<BitVec<u64>> {
    let encoding = Encodings::from_int(data[0])?;
    match encoding {
        Encodings::FixedLe => decode_le(&data[1..], size),
        Encodings::OnesIndexVarInt => decode_var(&data[1..], size, false),
        Encodings::ZeroIndexVarInt => decode_var(&data[1..], size, true),
    }
}

fn encode_le<'a, Iter: Iterator<Item=&'a u64>>(values: Iter) -> Vec<u8> {
    let mut buffer: Vec<u8> = vec![];

    buffer.push(Encodings::FixedLe.int_value());

    for segment in values {
        buffer.extend(segment.to_le_bytes())
    }

    return buffer
}

fn decode_le(data: &[u8], size: i64) -> Result<BitVec<u64>> {
    let mut values: Vec<u64> = vec![];
    for index in (0..data.len()).step_by(8) {
        if index + 8 <= data.len() {
            let bytes = data[index..index+8].try_into()?;
            values.push(u64::from_le_bytes(bytes))
        }
    }
    let extra_bytes = data.len() % 8;
    if extra_bytes != 0 {
        let mut extra: Vec<u8> = data[(data.len() - extra_bytes)..data.len()].iter().cloned().collect_vec();
        while extra.len() < 8 {
            extra.push(0);
        }
        let bytes = extra[0..8].try_into()?;
        values.push(u64::from_le_bytes(bytes))
    }

    let out = BitVec::from_vec(values);
    if size == out.len() as i64 {
        Ok(out)
    } else {
        Err(anyhow::anyhow!("Data encoding wrong size."))
    }
}

fn encode_var<'a, Iter: Iterator<Item=usize>>(label: Encodings, values: Iter, length: i64) -> Result<Vec<u8>> {
    /*
        Scheme A
        encoding the distance between 1s, 0 costs one extra bit to encode relative to
        unary encoding, the remaining values use either the same or fewer bits.
        All sequential x for a single number are least significant bit first
            0x - 0~1 1,2
            10x - 2~3 3,4
            110xx - 4~7 5,8
            111 (1xxx)* 0xxxx
    */

    let mut writer = BitVec::<u8>::new();
    writer.write(&[label.int_value()])?;

    let mut last: i64 = -1;
    for index in chain!(values.map(|v|v as i64), [length]) {
        let gap: u64 = (index - last - 1) as u64;
        last = index;
        if gap <= 1 {
            writer.push(false);
            writer.push(gap == 1);
        } else if gap <= 3 {
            writer.push(true);
            writer.push(false);
            writer.push(gap == 3);
        } else if gap <= 7 {
            writer.push(true);
            writer.push(true);
            writer.push(false);
            let gap = gap - 4;
            writer.push((gap & 0b01) != 0);
            writer.push((gap & 0b10) != 0);
        } else {
            writer.push(true);
            writer.push(true);
            writer.push(true);
            let mut gap = gap - 8;
            while gap > 0b1111 {
                writer.push(true);
                writer.push((gap & 0b1) != 0);
                writer.push((gap & 0b10) != 0);
                writer.push((gap & 0b100) != 0);
                gap = gap >> 3;
            }
            writer.push(false);
            writer.push((gap & 0b1) != 0);
            writer.push((gap & 0b10) != 0);
            writer.push((gap & 0b100) != 0);
            writer.push((gap & 0b1000) != 0);
        }
    }

    // Add 2 and 1 until it reached byte alignment
    if writer.len() % 2 == 1 {
        writer.push(true);
        writer.push(false);
        writer.push(false);
    }
    while writer.len() % 8 != 0 {
        writer.push(false);
        writer.push(false);
    }

    return Ok(writer.into_vec())
}


struct BitReader<'a> {
    data: &'a[u8],
    index: usize,
    offset: usize
}

impl<'a> BitReader<'a> {
    fn new(data: &'a[u8]) -> Self {
        BitReader { data, index: 0, offset: 0 }
    }

    fn take(&mut self) -> bool {
        let value = match self.data.get(self.index) {
            Some(val) => {
                ((val >> self.offset) & 1) > 0
            },
            None => return false,
        };

        if self.offset == 7 {
            self.offset = 0;
            self.index += 1;
        } else {
            self.offset += 1;
        }
        value
    }

    fn decode_value(&mut self) -> i64 {
        if !self.take() {
            // 0x - 0~1
            self.take() as i64
        } else {
            if !self.take() {
                // 10x - 2~3
                2 + self.take() as i64
            } else {
                if !self.take() {
                    // 110xx - 4~7
                    4 + (self.take() as i64) | ((self.take() as i64) << 1)
                } else {
                    // 111 (1xxx)* 0xxxx
                    let mut value: i64 = 0;
                    let mut index = 0;
                    let mut switch = self.take();
                    while switch {
                        for _ in 0..3 {
                            if self.take() {
                                value |= 1 << index;
                            }
                            index += 1;
                        }
                        switch = self.take();
                    }
                    for _ in 0..4 {
                        if self.take() {
                            value |= 1 << index;
                        }
                        index += 1;
                    }
                    8 + value
                }
            }
        }
    }
}

fn decode_var(values: &[u8], size: i64, flip: bool) -> Result<BitVec<u64>> {
    let mut values = BitReader::new(values);
    let mut out = BitVec::repeat(false, size as usize);
    let mut index = -1;

    loop {
        let gap = values.decode_value();
        index += gap + 1;
        if index < size {
            out.set(index as usize, true)
        } else {
            break
        }
    }

    if flip {
        Ok(!out)
    } else {
        Ok(out)
    }
}


#[cfg(test)]
mod test {
    use super::{encode, decode, BitReader};
    // use bitvec::prelude::Lsb0;
    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    #[test]
    fn random_round_trip() {
        let peak = 100;
        let size = 1024;
        for index in 0..peak {
            let mut vec = bitvec::vec::BitVec::<u64>::repeat(false, size);
            let ratio = index as f64/peak as f64;
            for bit in 0..size {
                if thread_rng().gen::<f64>() < ratio {
                    vec.set(bit, true);
                }
            }
            assert_eq!(decode(&encode(&mut vec).unwrap(), size as i64).unwrap(), vec);
        }
    }

    #[test]
    fn gap() {
        for off in 1..64 {
            let mut vec = bitvec::vec::BitVec::<u64>::repeat(false, 64);
            vec.set(0, true);
            vec.set(off, true);

            let encoded = encode(&mut vec).unwrap();
            println!("encoded {:?}", encoded.iter().map(|n|format!("{:#010b}", n)).collect_vec());
            assert_eq!(decode(&encoded, 64).unwrap(), vec);
        }
    }

    #[test]
    fn bitreader() {
        {
            let mut x = BitReader::new(&[0b0011_1000, 0b1111_0101]);

            assert!(!x.take());
            assert!(!x.take());
            assert!(!x.take());
            assert!(x.take());

            assert!(x.take());
            assert!(x.take());
            assert!(!x.take());
            assert!(!x.take());

            assert!(x.take());
            assert!(!x.take());
            assert!(x.take());
            assert!(!x.take());

            assert!(x.take());
            assert!(x.take());
            assert!(x.take());
            assert!(x.take());
        }


        // 0x - 0~1 1,2
        assert_eq!(BitReader::new(&[0b00000000]).decode_value(), 0);
        assert_eq!(BitReader::new(&[0b00000010]).decode_value(), 1);

        // 10x - 2~3 3,4
        assert_eq!(BitReader::new(&[0b00000001]).decode_value(), 2);
        assert_eq!(BitReader::new(&[0b00000101]).decode_value(), 3);

        // 110xx - 4~7 5,8
        assert_eq!(BitReader::new(&[0b00000011]).decode_value(), 4);
        assert_eq!(BitReader::new(&[0b00001011]).decode_value(), 5);
        assert_eq!(BitReader::new(&[0b00010011]).decode_value(), 6);
        assert_eq!(BitReader::new(&[0b0001_1011]).decode_value(), 7);

        // 111 (1xxx)* 0xxxx
        assert_eq!(BitReader::new(&[0b00000111]).decode_value(), 8);
        assert_eq!(BitReader::new(&[0b00010111]).decode_value(), 9);
        assert_eq!(BitReader::new(&[0b10000111]).decode_value(), 16);
        assert_eq!(BitReader::new(&[0b00110111]).decode_value(), 11);

        assert_eq!(BitReader::new(&[0b00001111, 0b00000000]).decode_value(), 8);
        assert_eq!(BitReader::new(&[0b00001111, 0b00001000]).decode_value(), 72);
        assert_eq!(BitReader::new(&[0b00111111, 0b00001000]).decode_value(), 75);

    }

}
