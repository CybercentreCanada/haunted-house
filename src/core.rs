use crate::database::Database;
use crate::storage::BlobStorage;
use anyhow::Result;



struct HouseCore {

}

impl HouseCore {
    fn new(index_storage: BlobStorage, file_storage: BlobStorage, database: Database) -> Result<Self> {
        todo!();
    }
}