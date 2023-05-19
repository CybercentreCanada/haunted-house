use std::path::{PathBuf, Path};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::query::Query;
use crate::types::{FilterID, Sha256};
use crate::worker::filter::ExtensibleTrigramFile;
use anyhow::Result;
use bitvec::vec::BitVec;
use log::error;
use tokio::sync::{mpsc, oneshot, watch};

use super::manager::WorkerConfig;


#[derive(Debug)]
enum ReaderCommand {
    Query(Query, oneshot::Sender<Result<Vec<u64>>>)
}

#[derive(Debug)]
pub enum WriterCommand {
    Ingest(Vec<(u64, Sha256)>, oneshot::Sender<()>)
}

pub struct FilterWorker {
    readiness: watch::Receiver<bool>,
    reader_connection: mpsc::Sender<ReaderCommand>,
    writer_connection: mpsc::Sender<WriterCommand>,
}

impl FilterWorker {
    pub fn open(config: WorkerConfig, id: FilterID) -> Result<Self> {
        let (ready_send, ready_recv) = watch::channel(false);
        let (reader_send, reader_recv) = mpsc::channel(64);
        let (writer_send, writer_recv) = mpsc::channel(64);
        std::thread::spawn(move || {
            writer_worker(writer_recv, config, id, ready_send, ready_recv, reader_recv)
        });
        Ok(Self { readiness: ready_recv, reader_connection: reader_send, writer_connection: writer_send })
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

pub fn writer_worker(writer_recv: mpsc::Receiver<WriterCommand>, config: WorkerConfig, id: FilterID, ready_send: watch::Sender<bool>, ready_recv: watch::Receiver<bool>, reader_recv: mpsc::Receiver<ReaderCommand>) {
    // Open the file
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
        reader_worker(filter, reader_recv);
    });

    while let Err(err) = _writer_worker(&mut writer_recv, id, filter.clone(), &ready_send) {
        error!("writer worker: {err}");
    }
}

pub fn _writer_worker(writer_recv: &mut mpsc::Receiver<WriterCommand>, id: FilterID, filter: Arc<ExtensibleTrigramFile>, ready_send: &watch::Sender<bool>) -> Result<()> {
    while let Some(message) = writer_recv.blocking_recv() {
        match message {
            WriterCommand::Ingest(mut batch, finished) => {
                // Insert the batch
                filter.write_batch(batch)?;
                finished.send(());
            }
        }
    }
    return Ok(())
}

pub fn reader_worker(filter: Arc<ExtensibleTrigramFile>, mut reader_recv: mpsc::Receiver<ReaderCommand>) {
    loop {
        match _reader_worker(filter.clone(), &mut reader_recv) {
            Ok(()) => break,
            Err(err) => {
                error!("reader worker: {err}");
            }
        }
    }
}

pub fn _reader_worker(filter: Arc<ExtensibleTrigramFile>, reader_recv: &mut mpsc::Receiver<ReaderCommand>) -> Result<()> {
    while let Some(message) = reader_recv.blocking_recv() {
        match message {
            ReaderCommand::Query(query, response) => {
                response.send(filter.query(&query));
            },
        }
    }
    Ok(())
}
