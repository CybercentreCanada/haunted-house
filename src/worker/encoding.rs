
/// pack a u64 value into a buffer
#[inline(always)]
pub fn encode_value_into(mut value: u64, buffer: &mut Vec<u8>){
    if value == 0 {
        buffer.push(0x00);
        return
    }
    while value > 0 {
        let mut fragment = value & 0b0111_1111;
        value >>= 7;
        if value > 0{
            fragment |= 0b1000_0000;
        }
        buffer.push(fragment as u8);
    }
}

#[cfg(test)]
pub fn encode(indices: &[u64]) -> Vec<u8> {
    let mut buffer = vec![];
    encode_into_increasing(indices, &mut buffer);
    return buffer
}

/// pack a set of u64 into a buffer
#[inline(always)]
pub fn encode_into_increasing(indices: &[u64], buffer: &mut Vec<u8>) {
    if indices.is_empty() {
        return;
    }
    if indices.len() == 1 {
        encode_value_into(indices[0], buffer);
        return;
    }
    encode_value_into(indices[0], buffer);
    for pair in indices.windows(2) {
        let d = pair[1] - pair[0];
        encode_value_into(d, buffer);
    }
}

// /// Calculate how many bytes a value will need to be encoded
// pub fn encoded_number_size(value: u64) -> u32 {
//     value.ilog2()/7 + 1
// }

pub struct DecreasingEncoder<'a> {
    last_value: u64,
    buffer: &'a mut Vec<u8>,
}

impl<'a> DecreasingEncoder<'a> {
    pub fn new(first: u64, buffer: &'a mut Vec<u8>) -> Self {
        Self {last_value: first, buffer }
    }

    pub fn write(&mut self, values: &[u64]) {
        for &value in values {
            self.push(value)
        }
    }

    pub fn push(&mut self, value: u64) {
        let delta = self.last_value - value - 1;
        encode_value_into(delta, self.buffer);
        self.last_value = value
    }
}

// /// (upper bound on) How many additional bytes are needed to add the given value to the given sequence
// pub fn cost_to_add(values: &[u64], new_value: u64) -> u32 {
//     match values.last() {
//         Some(last) => encoded_number_size(new_value - last),
//         None => encoded_number_size(new_value)
//     }
// }

#[cfg(test)]
pub fn decode(data: &[u8]) -> (Vec<u64>, u32) {
    let mut values = vec![];
    let size = decode_into(data, &mut values);
    return (values, size)
}

pub fn decode_value(mut data: &[u8]) -> (u64, &[u8]) {
    let mut value = (data[0] & 0b0111_1111) as u64;
    let mut offset = 7;

    while data[0] & 0b1000_0000 > 0 {
        data = &data[1..];
        if data.is_empty() || data[0] == 0 {
            return (value, data)
        }
        value |= ((data[0] & 0b0111_1111) as u64) << offset;
        offset += 7;
    }

    (value, &data[1..])
}

pub fn try_decode_value(input: &[u8]) -> Option<(u64, usize)> {
    if input.is_empty() { return None }
    let mut data = input;
    let mut value = (data[0] & 0b0111_1111) as u64;
    let mut offset = 7;
    let mut bytes_used = 1;

    while data[0] & 0b1000_0000 > 0 {
        data = &data[1..];
        bytes_used += 1;
        if data.is_empty() || data[0] == 0 {
            return None
        }
        value |= ((data[0] & 0b0111_1111) as u64) << offset;
        offset += 7;
    }

    Some((value, bytes_used))
}


pub fn decode_decreasing_into(mut data: &[u8], mut value: u64, values: &mut Vec<u64>) {
    values.push(value);
    let mut delta;
    while !data.is_empty() {
        (delta, data) = decode_value(data);
        value -= delta + 1;
        values.push(value)
    }
}

/// Unpack a sequence of u64 from a buffer
pub fn decode_into(data: &[u8], values: &mut Vec<u64>) -> u32 {

    let mut used_bytes = 0;
    let mut index = 0;
    let mut last = 0;
    while index < data.len() && data[index] != 0 {
        let mut value = (data[index] & 0b0111_1111) as u64;
        let mut offset = 7;
        let mut taken = 0;

        while data[index] & 0b1000_0000 > 0 {
            index += 1;
            taken += 1;
            if index >= data.len() || data[index] == 0 {
                return used_bytes
            }
            value |= ((data[index] & 0b0111_1111) as u64) << offset;
            offset += 7;
        }

        index += 1;
        used_bytes += taken + 1;
        value += last;
        values.push(value);
        last = value;
    }

    return used_bytes;
}


pub struct StreamDecode<'a> {
    bytes: &'a[u8],
    next_value: Option<u64>,
}

impl<'a> StreamDecode<'a> {
    pub fn new(data: &'a[u8]) -> Self {
        let (bytes, next_value) = Self::decode_raw_next(data);
        StreamDecode { bytes, next_value }
    }

    #[inline(always)]
    fn decode_raw_next(mut data: &'a[u8]) -> (&'a[u8], Option<u64>) {
        if data.is_empty() {
            return (data, None)
        }

        let mut value = (data[0] & 0b0111_1111) as u64;
        let mut continued = data[0] & 0b1000_0000 > 0;
        data = &data[1..];        
        let mut offset = 7;

        while !data.is_empty() && continued {
            value |= ((data[0] & 0b0111_1111) as u64) << offset;
            continued = data[0] & 0b1000_0000 > 0;
            data = &data[1..];
            offset += 7;
        }

        (data, Some(value))
    }

    #[inline(always)]
    pub fn peek(&self) -> Option<u64> {
        return self.next_value
    }

    #[inline(always)]
    pub fn skip_one(&mut self) {
        if let Some(current) = self.next_value {
            let (remaining, delta) = Self::decode_raw_next(self.bytes);
            self.bytes = remaining;
            self.next_value = delta.map(|delta| current + delta);
        }
    }
}

impl Iterator for StreamDecode<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_value {
            Some(current) => {
                let (remaining, delta) = Self::decode_raw_next(self.bytes);
                self.bytes = remaining;
                match delta {
                    Some(delta) => self.next_value = Some(current + delta),
                    None => self.next_value = None
                }
                Some(current)
            },
            None => None,
        }
    }
}

pub struct StreamEncode {
    buffer: Vec<u8>,
    last_value: u64,
}

impl StreamEncode {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1 << 11),
            last_value: 0,
        }
    }

    pub fn add(&mut self, value: u64) {
        let delta = value - self.last_value;
        encode_value_into(delta, &mut self.buffer);
        self.last_value = value;
    }

    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }
}


#[cfg(test)]
mod test {
    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    use crate::worker::encoding::{try_decode_value, StreamDecode, StreamEncode};

    use super::{decode, decode_value, encode, encode_value_into};

    #[test]
    fn round_trip() {
        let mut data: Vec<u64> = vec![];
        let mut prng = thread_rng();
        while data.len() < 1000 {
            data.push(prng.gen());
        }
        data.sort_unstable();
        data.dedup();

        let buffer = encode(&data);
        assert_eq!(decode(&buffer), (data, buffer.len() as u32));
    }

    // #[test]
    // fn number_size() {
    //     let mut prng = thread_rng();
    //     for ii in 1..100_000 {
    //         let num: u64 = prng.gen();
    //         assert_eq!(encode(&[ii]).len() as u32, encoded_number_size(ii));
    //         assert_eq!(encode(&[num]).len() as u32, encoded_number_size(num));
    //     }
    // }

    #[test]
    fn incomplete_write() {
        let data = vec![1, 500, 77777777];
        let mut buffer = encode(&data);
        assert_eq!(decode(&buffer), (data, 7));
        buffer.pop();
        assert_eq!(decode(&buffer), (vec![1, 500], 3));
        buffer.push(0);
        assert_eq!(decode(&buffer), (vec![1, 500], 3));
    }

    // Test to make sure stream encoding/decoding matches the batch call
    #[test]
    fn stream_test() {
        let mut prng = thread_rng();
        let mut data: Vec<u64> = (0..10000).map(|_|prng.gen()).collect();
        data.sort_unstable();

        let buffer1 = encode(&data);
        
        let mut enc = StreamEncode::new();
        for number in &data {
            enc.add(*number);
        }
        let buffer2 = enc.finish();

        assert!(buffer1 == buffer2);

        assert!(decode(&buffer1).0 == data);
        assert!(StreamDecode::new(&buffer1).collect_vec() == data);
    }

    // Testa
    #[test]
    fn decode_single_values() {
        let empty: &[u8] = &[];

        let mut buffer = vec![];
        encode_value_into(1 << 13, &mut buffer);
        assert_eq!(decode_value(&buffer), (1 << 13, empty));

        let mut buffer = vec![];
        encode_value_into(1 << 20, &mut buffer);        
        assert_eq!(decode_value(&buffer), (1 << 20, empty));

        let mut buffer = vec![];
        encode_value_into(0xfff, &mut buffer);        
        assert_eq!(decode_value(&buffer), (0xfff, empty));

        let mut buffer = vec![];
        encode_value_into(1 << 20, &mut buffer);        
        buffer.push(0);
        assert_eq!(decode_value(&buffer), (1 << 20, &[0u8][..]));

        let mut buffer = vec![];
        encode_value_into(1 << 20, &mut buffer);        
        encode_value_into(1, &mut buffer);        
        encode_value_into(0xfff, &mut buffer);        
        buffer.push(0xff);

        let (value, buffer) = decode_value(&buffer);
        assert_eq!(value, 1 << 20);
        let (value, buffer) = decode_value(buffer);
        assert_eq!(value, 1);
        let (value, buffer) = decode_value(buffer);
        assert_eq!(value, 0xfff);
        assert_eq!(buffer, &[0xff])
    }

    #[test]
    fn decode_parts() {
        let mut buffer = vec![];
        encode_value_into(0xfffffffffff, &mut buffer);

        assert_eq!(buffer.len(), 7);

        assert_eq!(try_decode_value(&buffer[0..7]), Some((0xfffffffffff, 7)));
        assert_eq!(try_decode_value(&buffer[0..6]), None);
        assert_eq!(try_decode_value(&buffer[0..5]), None);
        assert_eq!(try_decode_value(&buffer[0..4]), None);
        assert_eq!(try_decode_value(&buffer[0..3]), None);
        assert_eq!(try_decode_value(&buffer[0..2]), None);
        assert_eq!(try_decode_value(&buffer[0..1]), None);
        assert_eq!(try_decode_value(&buffer[0..0]), None);

        let mut data: Vec<u64> = vec![];
        for _ in 0..10 {
            data.push(thread_rng().gen())            
        }
        data.push(0);
        for _ in 0..10 {
            data.push(thread_rng().gen())            
        }
        data.push(0);
        
        let mut buffer = vec![];
        for value in &data {
            encode_value_into(*value, &mut buffer);
        }
        buffer.push(0xff);

        let mut output = vec![];
        let mut cursor = 0;
        while let Some((value, bytes_read)) = try_decode_value(&buffer[cursor..]) {
            output.push(value);
            cursor += bytes_read;
        }
        assert_eq!(data, output);

        if cursor < buffer.len() {
            let remaining = buffer.len() - cursor;
            buffer.copy_within(cursor.., 0);
            buffer.truncate(remaining)
        }
        assert_eq!(vec![0xff], buffer)
    }
}