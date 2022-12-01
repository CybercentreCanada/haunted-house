use serde::{Serialize, Deserialize};



#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum Query {
    Or(Vec<Query>),
    And(Vec<Query>),
    String(String),
    Literal(Vec<u8>),
}