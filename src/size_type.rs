use serde::{Deserialize, Serializer, de::Error};



pub fn deserialize_size<'de, D>(de: D) -> Result<u64, D::Error> where D: serde::Deserializer<'de> {
    // de.deserialize_u64(SizeVisitor)
    // de.deserialize(SizeVisitor)
    match String::deserialize(de) {
        Ok(string) => {
            // Ok(parse_size::parse_size(&string)?)
            match parse_size::parse_size(&string) {
                Ok(value) => Ok(value),
                Err(err) => Err(D::Error::custom(err.to_string()))
            }
        },
        Err(err) => Err(err),
    }
}

pub fn serialize_size<S>(value: &u64, ser: S) -> Result<S::Ok, S::Error> where S: Serializer {
    ser.serialize_str(&value.to_string())
}
