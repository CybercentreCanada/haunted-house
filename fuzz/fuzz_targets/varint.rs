#![no_main]
use libfuzzer_sys::fuzz_target;
use haunted_house::varint::{encode, decode, decode_from};

fuzz_target!(|data: (u64, u64)| {
    assert_eq!(data.0, decode(encode(data.0)).unwrap());
    assert_eq!(data.1, decode(encode(data.1)).unwrap());

    let mut buffer = vec![];
    buffer.extend(encode(data.0));
    buffer.extend(encode(data.1));

    let mut cursor = std::io::Cursor::new(&buffer);
    assert_eq!(data.0, decode_from(&mut cursor).unwrap());
    assert_eq!(data.1, decode_from(&mut cursor).unwrap());
});
