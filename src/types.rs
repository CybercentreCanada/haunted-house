//!
//! Module containing simple types used throughout the crate.
//!

use chrono::{DateTime, Utc, Datelike};
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};

use crate::access::AccessControl;
use crate::error::ErrorKinds;


/// A binary representation of a 256 bit hash. Heap allocated.
#[derive(SerializeDisplay, DeserializeFromStr, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct Sha256(Box<[u8; 32]>);

impl Sha256 {
    /// Generate a new heap allocated string holding the hex encoded form
    pub fn hex(&self) -> String {
        hex::encode(*self.0)
    }

    /// Access the underlying data of the hash without copying
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

/// An identifier for grouping filter tables by the day they expire.
/// Expiry is tracked with daily resolution. Finer units of time are ignored.
/// The date is packed into an unsigned 32 bit number with the year occuping
/// the higher 16 bits, and the day in the lower 16 bits.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Hash, PartialOrd, Ord)]
pub struct ExpiryGroup(u32);

impl ExpiryGroup {
    /// Get the expiry group value for a given expiry date.
    /// If no expiry date is provided, returns an impossible date far in the future
    /// to represent "retain forever".
    pub fn create(expiry: &Option<DateTime<Utc>>) -> ExpiryGroup {
        ExpiryGroup(match expiry {
            Some(date) => ((date.year() as u32) << 16) | date.ordinal(),
            None => u32::MAX-1,
        })
    }

    /// Get the expiry group for items expiring today
    pub fn today() -> ExpiryGroup {
        ExpiryGroup::create(&Some(Utc::now()))
    }

    /// Get the expiry group for items that expired yesterday
    pub fn yesterday() -> ExpiryGroup {
        ExpiryGroup::create(&Some(Utc::now() - chrono::Duration::days(1)))
    }

    /// Get an expiry group that is always before any that should be used.
    /// It will appear as the zeroth day of year zero.
    pub fn min() -> ExpiryGroup {
        ExpiryGroup(u32::MIN)
    }

    /// Get an expiry group that is after any that should be used.
    /// It will appear as the 65535 day of the year 65535.
    /// Note that this is greater than than the normal "retain forever" group.
    pub fn max() -> ExpiryGroup {
        ExpiryGroup(u32::MAX)
    }

    /// Convert from a raw integer value. This is only intended for
    /// serialization in contexts where we aren't using serde
    pub fn from(data: u32) -> Self {
        Self(data)
    }

    /// Convert into a raw integer value. This is only intended for
    /// serialization in contexts where we aren't using serde
    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for ExpiryGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let year = self.0 >> 16;
        let day = self.0 & 0xFFFF;
        f.write_fmt(format_args!("{year}-{day:03}"))
    }
}

impl std::fmt::Debug for ExpiryGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ExpiryGroup").field(&self.0).finish()
    }
}

/// Summary of all the information tracked about a file in the hauntedhouse system.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileInfo {
    /// Hash of the file being referenced. We assume it uniquely identifies the file in question.
    pub hash: Sha256,
    /// Access control that determines if a given search is allowed to know about this file.
    pub access: AccessControl,
    /// Which day this file expires on.
    pub expiry: ExpiryGroup
}

/// Identifier tracking a particular filter.
/// Used to associate the filter file with database describing files in it.
/// signed 64 bit number, starts at 1, only uses positive values for compatibily reasons.
#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
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
    /// A constant representing a null or invalid filter id.
    /// Should always compare as less than all valid ids.
    pub const NULL: FilterID = FilterID(0);

    /// Access the raw integer value for seralization outside of serde.
    pub fn to_i64(self) -> i64 {
        self.0
    }

    /// Get the next possible filter ID assuming the current id
    /// is the highest ID current used.
    pub fn next(&self) -> FilterID {
        FilterID::from(self.0 + 1)
    }
}

/// Identifies a worker process uniquely.
#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq, Clone, PartialOrd, Ord)]
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