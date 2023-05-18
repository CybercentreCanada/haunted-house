use std::path::{PathBuf, Path};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::query::Query;
use crate::types::FilterID;
use crate::worker::filter::ExtensibleTrigramFile;
use anyhow::Result;
use log::error;
use tokio::sync::{mpsc, oneshot, watch};

use super::database::Database;
use super::manager::{WorkerConfig, WorkerState};


#[derive(Debug)]
enum ReaderCommand {
    Query(Query, oneshot::Sender<Result<Vec<u64>>>)
}

pub struct FilterWorker {
    readiness: watch::Receiver<bool>,
    reader_connection: mpsc::Sender<ReaderCommand>,
}

impl FilterWorker {
    pub fn open(state: Weak<WorkerState>, id: FilterID) -> Result<Self> {
        let (ready_send, ready_recv) = watch::channel(false);
        let (reader_send, reader_recv) = mpsc::channel(64);
        std::thread::spawn(move || {
            writer_worker(state, id, ready_send, ready_recv, reader_recv)
        });
        Ok(Self { readiness: ready_recv, reader_connection: reader_send })
    }

    pub fn is_ready(&self) -> bool {
        *self.readiness.borrow()
    }

    pub fn notify(&self) {
        todo!();
    }

    pub async fn query(&self, query: Query) -> Result<Vec<u64>> {
        let (send, recv) = oneshot::channel();
        self.reader_connection.send(ReaderCommand::Query(query, send)).await?;
        return recv.await?
    }
}

pub fn writer_worker(state_weak: Weak<WorkerState>, id: FilterID, ready_send: watch::Sender<bool>, ready_recv: watch::Receiver<bool>, reader_recv: mpsc::Receiver<ReaderCommand>) {
    // Open the file
    let config = {
        match state_weak.upgrade() {
            Some(state) => state.config.clone(),
            None => return,
        }
    };
    let path = config.data_path.join(id.to_string());
    let filter = if path.exists() {
        ExtensibleTrigramFile::open(&path)
    } else {
        ExtensibleTrigramFile::new(&path, config.initial_segment_size, config.extended_segment_size)
    };
    let filter = match filter {
        Ok(filter) => Arc::new(filter),
        Err(err) => {
            error!("Could not load filter: {err}");
            return;
        },
    };

    // Spawn reader
    _ = ready_send.send(true);
    std::thread::spawn(move || {
        reader_worker(state_weak.clone(), filter, reader_recv);
    });

    loop {
        match _writer_worker(state_weak.clone(), id, filter.clone(), &ready_send) {
            Ok(()) => break,
            Err(err) => {
                error!("writer worker: {err}");
            }
        }
    }
}

pub fn _writer_worker(state: Weak<WorkerState>, id: FilterID, filter: Arc<ExtensibleTrigramFile>, ready_send: &watch::Sender<bool>) -> Result<()> {
    // Start loading and processing batches of data
    let running = match state.upgrade() {
        Some(state) => state.running.clone(),
        None => return Ok(()),
    };
    while *running.borrow() {
        // Get a batch to insert
        todo!();

        // Wait for change
        if batch.is_empty() {
            std::thread::park_timeout(std::time::Duration::from_secs(120))
            continue
        }

        // Gather file data
        todo!();

        // Insert the batch
        filter.write_batch(files)?;

        // Set that batch is
        match state.upgrade() {
            Some(state) => {
                state.database.set_ingested(id, batch);
            }
            None => return Ok(())
        }
    }
    return Ok(())
}

pub fn reader_worker(state: Weak<WorkerState>, filter: Arc<ExtensibleTrigramFile>, mut reader_recv: mpsc::Receiver<ReaderCommand>) {
    loop {
        match _reader_worker(state.clone(), filter.clone(), &mut reader_recv) {
            Ok(()) => break,
            Err(err) => {
                error!("reader worker: {err}");
            }
        }
    }
}

pub fn _reader_worker(state: Weak<WorkerState>, filter: Arc<ExtensibleTrigramFile>, reader_recv: &mut mpsc::Receiver<ReaderCommand>) -> Result<()> {
    while let Some(message) = reader_recv.blocking_recv() {
        match message {
            ReaderCommand::Query(query, response) => {
                response.send(filter.query(&query));
            },
        }
    }
    Ok(())
}
