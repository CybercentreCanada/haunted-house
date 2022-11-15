use anyhow::Result;


pub enum Database {
    Local(LocalDatabase)
}

impl Database {

}

pub struct LocalDatabase {

}

impl LocalDatabase {
    pub fn new() -> Result<Database> {
        Ok(Database::Local(Self {

        }))
    }
}

