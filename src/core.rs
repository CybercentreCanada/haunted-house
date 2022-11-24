use std::sync::Arc;

use crate::auth::Authenticator;
use crate::database::Database;
use crate::storage::BlobStorage;
use crate::cache::LocalCache;
use crate::access::AccessControl;
use log::error;
use tokio::sync::{mpsc, oneshot};
use anyhow::Result;
use chrono::{DateTime, Utc};

type IngestMessage = (String, AccessControl, Option<DateTime<Utc>>, oneshot::Sender<Result<()>>);

pub struct HouseCore {
    pub database: Database,
    pub file_storage: BlobStorage,
    pub index_storage: BlobStorage,
    pub local_cache: LocalCache,
    pub authenticator: Authenticator,
    pub ingest_queue: mpsc::UnboundedSender<IngestMessage>,
}

impl HouseCore {
    pub fn new(index_storage: BlobStorage, file_storage: BlobStorage, database: Database, local_cache: LocalCache, authenticator: Authenticator) -> Result<Arc<Self>> {
        let (send_ingest, receive_ingest) = mpsc::unbounded_channel();

        let core = Arc::new(Self {
            database,
            file_storage,
            index_storage,
            local_cache,
            authenticator,
            ingest_queue: send_ingest
        });

        tokio::spawn(ingest_worker(core.clone(), receive_ingest));

        return Ok(core)
    }

    pub async fn update_file_access(&self, hash: Vec<u8>, access: AccessControl, expiry: Option<DateTime<Utc>>) -> Result<bool> {
        todo!()
    }
}


async fn ingest_worker(core: Arc<HouseCore>, mut input: mpsc::UnboundedReceiver<IngestMessage>) {
    loop {
        match _ingest_worker(core.clone(), &mut input).await {
            Err(err) => error!("Crash in ingestion system: {err}"),
            Ok(()) => break
        }
    }
}

async fn _ingest_worker(core: Arc<HouseCore>, input: &mut mpsc::UnboundedReceiver<IngestMessage>) -> Result<()> {

    // let waiting: HashMap<String>


    loop {
        // Read an update command
        let message = match input.recv().await {
            Some(message) => message,
            None => break
        };
        let (hash, access, expiry, respond) = message;

        // Try to update in place without changing indices
        todo!();

        // Merge with an existing index command
        todo!();

        // Add to

    }
    return Ok(())
}