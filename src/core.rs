use crate::auth::Authenticator;
use crate::database::Database;
use crate::storage::BlobStorage;
use anyhow::Result;


pub struct HouseCore {
    database: Database,
    file_storage: BlobStorage,
    index_storage: BlobStorage,
    authenticator: Authenticator
}

impl HouseCore {
    pub fn new(index_storage: BlobStorage, file_storage: BlobStorage, database: Database, authenticator: Authenticator) -> Result<Self> {
        Ok(Self {
            database,
            file_storage,
            index_storage,
            authenticator,
        })
    }
}