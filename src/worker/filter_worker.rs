use std::path::{PathBuf, Path};

use crate::query::Query;
use crate::types::FilterID;
use anyhow::Result;


pub struct FilterWorker {

}

impl FilterWorker {
    pub fn new(data_dir: &Path, id: FilterID) -> Result<Self> {
        todo!()
    }

    pub fn open(data_dir: &Path, id: FilterID) -> Result<Self> {
        todo!()
    }

    pub fn is_ready(&self) -> bool {
        todo!();
    }

    pub async fn query(&self, query: Query) -> Result<Vec<i64>> {
        todo!()
    }
}