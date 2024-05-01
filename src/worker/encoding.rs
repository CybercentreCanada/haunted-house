
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
    encode_into(indices, &mut buffer);
    return buffer
}

/// pack a set of u64 into a buffer
#[inline(always)]
pub fn encode_into(indices: &[u64], buffer: &mut Vec<u8>) {
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

/// Calculate how many bytes a value will need to be encoded
pub fn encoded_number_size(value: u64) -> u32 {
    value.ilog2()/7 + 1
}

/// (upper bound on) How many additional bytes are needed to add the given value to the given sequence
pub fn cost_to_add(values: &[u64], new_value: u64) -> u32 {
    match values.last() {
        Some(last) => encoded_number_size(new_value - last),
        None => encoded_number_size(new_value)
    }
}

#[cfg(test)]
pub fn decode(data: &[u8]) -> (Vec<u64>, u32) {
    let mut values = vec![];
    let size = decode_into(data, &mut values);
    return (values, size)
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


#[cfg(test)]
mod test {
    use rand::{thread_rng, Rng};

    use super::{decode, encode, encoded_number_size};

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

    #[test]
    fn number_size() {
        let mut prng = thread_rng();
        for ii in 1..100_000 {
            let num: u64 = prng.gen();
            assert_eq!(encode(&[ii]).len() as u32, encoded_number_size(ii));
            if num != 0 {
                assert_eq!(encode(&[num]).len() as u32, encoded_number_size(num));
            }
        }
    }

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

}