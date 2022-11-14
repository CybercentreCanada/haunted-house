#![no_main]
use libfuzzer_sys::fuzz_target;
use haunted_house::ursadb::UrsaDBTrigramFilter;

fuzz_target!(|data: &[u8]| {
    
    let mut numbers: Vec<u64> = Default::default();
    for batch in 0..data.len()/8 {
        let mut value: u64 = 0;
        for index in 0..8 {
            value = (value << 8) | data[batch*8 + index] as u64;
        }
        numbers.push(value);
    }
    numbers.sort();

    if numbers.len() > 0 {
        let bytes = UrsaDBTrigramFilter::encode_indices(numbers.clone());
        let out = UrsaDBTrigramFilter::decode_indices(bytes).unwrap();
        assert_eq!(numbers, out);
    }
});
