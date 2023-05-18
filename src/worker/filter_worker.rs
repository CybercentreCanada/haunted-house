use crate::query::Query;
use anyhow::Result;


pub struct FilterWorker {

}

impl FilterWorker {


    pub fn is_ready(&self) -> bool {
        todo!();
    }

    pub async fn query(&self, query: Query) -> Result<Vec<i64>> {
        todo!()
    }
}