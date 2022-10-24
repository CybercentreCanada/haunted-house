use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub enum AccessControl {
    Or(Vec<AccessControl>),
    And(Vec<AccessControl>),
    Token(String)
}

impl AccessControl {
    pub fn or(&self, other: &AccessControl) -> AccessControl {
        todo!();
    }
}