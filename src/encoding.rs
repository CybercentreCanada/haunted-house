

pub fn encode_duplicates(mut count: u64) -> Vec<u8> {
    let mut buffer = Vec::new();

    while count > 0b0111_1111 {
        buffer.push(0b0111_1111);
        count -= 0b0111_1111;
    }

    if count > 0 {
        buffer.push(count as u8);
    }

    return buffer;
}


pub fn encode_value_into(mut value: u64, buffer: &mut Vec<u8>){
    while value > 0 {
        let mut fragment = value & 0b0111_1111;
        value >>= 7;
        if value > 0{
            fragment |= 0b1000_0000;
        }
        buffer.push(fragment as u8);
    }
}

pub fn encode(indices: &Vec<u64>) -> Vec<u8> {
    let mut buffer = vec![];
    encode_into(indices, &mut buffer);
    return buffer
}

pub fn encode_into(indices: &Vec<u64>, buffer: &mut Vec<u8>) {
    if indices.len() == 0 {
        return;
    }
    if indices.len() == 1 {
        encode_value_into(indices[0], buffer);
        return;
    }
    encode_value_into(indices[0], buffer);
    let mut last = indices[0];
    let mut run_length = 0;
    for pair in indices.windows(2) {
        let d = pair[1] - pair[0];
        // if d == last {
        //     run_length += 1;
        // } else {
        //     if run_length > 0 {
        //         buffer.extend(encode_duplicates(run_length));
        //         run_length = 0;
        //     }

        encode_value_into(d, buffer);
            // last = d;
        // }
    }

    // if run_length > 0 {
    //     buffer.extend(encode_duplicates(run_length));
    // }

}

pub fn encoded_number_size(value: u64) -> u32 {
    value.ilog2()/7 + 1
}

pub fn cost_to_add(values: &[u64], new_value: u64) -> u32 {
    match values.last() {
        Some(last) => encoded_number_size(new_value - last),
        None => encoded_number_size(new_value)
    }
}

pub fn decode(data: &[u8]) -> (Vec<u64>, u32) {
    let mut values = vec![];

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
                return (values, used_bytes)
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

    return (values, used_bytes);
}


#[cfg(test)]
mod test {
    use rand::{thread_rng, Rng};
    use crate::encoding::encoded_number_size;

    use super::{decode, encode};

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
    // fn run_compression() {
    //     {
    //         let data = vec![1, 2, 3, 4, 5, 6, 7];
    //         let buffer = encode(&data);
    //         assert_eq!(encoded_size(&data), 2);
    //         assert_eq!(decode(&buffer), data);
    //     }
    //     {
    //         let data = vec![1, 2, 3, 5, 7, 8, 9];
    //         let buffer = encode(&data);
    //         assert_eq!(encoded_size(&data), 5);
    //         assert_eq!(decode(&buffer), data);
    //     }
    // }

    #[test]
    fn number_size() {
        let mut prng = thread_rng();
        for ii in 1..100_000 {
            let num: u64 = prng.gen();
            assert_eq!(encode(&vec![ii]).len() as u32, encoded_number_size(ii));
            if num != 0 {
                assert_eq!(encode(&vec![num]).len() as u32, encoded_number_size(num));
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