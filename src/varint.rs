use anyhow::Result;

pub fn encode(mut value: u64) -> Vec<u8> {
    let mut buffer = Vec::new();

    loop {
        let mut fragment = value & 0b0111_1111;
        value >>= 7;
        if value > 0{
            fragment |= 0b1000_0000;
        }
        buffer.push(fragment as u8);
        if value == 0 {
            break;
        }
    }

    return buffer;
}

pub fn decode(data: Vec<u8>) -> Result<u64> {
    let mut buffer: Vec<u8> = Default::default();
    for byte in data {
        let fragment = byte & 0b0111_1111;
        buffer.push(fragment);
        if byte & 0b1000_0000 == 0 {
            let mut out: u64 = 0;
            for frag in buffer.iter().rev() {
                out <<= 7;
                out |= *frag as u64;
            }
            return Ok(out);
        }
    }
    return Err(crate::error::ErrorKinds::VarintIncomplete.into())
}


pub fn decode_from<D: std::io::Read>(data: &mut D) -> Result<u64> {
    let mut buffer: Vec<u8> = Default::default();
    loop {
        let mut byte = [0; 1];
        if let Err(_) = data.read_exact(&mut byte) {
            return Err(crate::error::ErrorKinds::VarintIncomplete.into())
        }
        let fragment = byte[0] & 0b0111_1111;
        buffer.push(fragment);
        if byte[0] & 0b1000_0000 == 0 {
            let mut out: u64 = 0;
            for frag in buffer.iter().rev() {
                out <<= 7;
                out |= *frag as u64;
            }
            return Ok(out);
        }
    }
}


