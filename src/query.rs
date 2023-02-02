use serde::{Serialize, Deserialize};
use anyhow::Result;


#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum Query {
    Not(Box<Query>),
    And(Vec<Query>),
    Or(Vec<Query>),
    Literal(Vec<u8>),
}
