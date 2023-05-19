
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};

use crate::access::AccessControl;
use crate::error::ErrorKinds;


/// A binary representation of a 256 bit hash. Heap allocated.
#[derive(SerializeDisplay, DeserializeFromStr, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct Sha256(Box<[u8; 32]>);

impl Sha256 {
    pub fn hex(&self) -> String {
        hex::encode(*self.0)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0[..]
    }
}

impl TryFrom<&[u8]> for Sha256 {
    type Error = ErrorKinds;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(match value.try_into(){
            Ok(val) => Box::new(val),
            Err(_) => return Err(ErrorKinds::Sha256Corrupt),
        }))
    }
}

impl std::fmt::Debug for Sha256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex())
    }
}

impl std::fmt::Display for Sha256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex())
    }
}

impl std::str::FromStr for Sha256 {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 64 {
            return Err(anyhow::anyhow!("Invalid hash: wrong number of bytes"));
        }
        let mut bytes = Box::new([0u8; 32]);
        hex::decode_to_slice(s, &mut *bytes)?;
        Ok(Self(bytes))
    }
}

/// An identifier for grouping filter tables by the day they expire
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash, PartialOrd, Ord)]
pub struct ExpiryGroup(String);

impl ExpiryGroup {
    pub fn create(expiry: &Option<DateTime<Utc>>) -> ExpiryGroup {
        ExpiryGroup(match expiry {
            Some(date) => format!("{}", date.format("%Y0%j")),
            None => format!("99990999"),
        })
    }

    pub fn from(data: &str) -> Self {
        Self(data.to_owned())
    }

    pub fn min() -> ExpiryGroup {
        ExpiryGroup(format!(""))
    }

    pub fn max() -> ExpiryGroup {
        ExpiryGroup(format!("99999999"))
    }

    pub fn as_str<'a>(&'a self) -> &'a str {
        &self.0
    }
}

impl std::fmt::Display for ExpiryGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileInfo {
    pub hash: Sha256,
    pub access: AccessControl,
    pub expiry: ExpiryGroup
}

#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq, Clone, Copy)]
pub struct FilterID(i64);

impl std::fmt::Display for FilterID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0.to_le_bytes()))
    }
}

impl From<i64> for FilterID {
    fn from(value: i64) -> Self {
        FilterID(value)
    }
}

impl FilterID {
    pub fn to_i64(&self) -> i64 {
        self.0
    }
}

// impl std::str::FromStr for FilterID {
//     type Err = ErrorKinds;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         if s.len() != 16 {
//             return Err(Self::Err::CorruptFilterID);
//         }
//         let mut bytes = [0u8; 8];
//         if let Err(_) = hex::decode_to_slice(s, &mut bytes) {
//             return Err(Self::Err::CorruptFilterID);
//         }
//         Ok(Self(i64::from_le_bytes(bytes)))
//     }
// }


#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq, Clone)]
pub struct WorkerID(String);

impl From<String> for WorkerID {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for WorkerID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}