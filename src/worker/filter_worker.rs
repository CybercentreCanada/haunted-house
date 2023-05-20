use std::sync::{Arc};

use crate::query::Query;
use crate::types::FilterID;
use crate::worker::filter::ExtensibleTrigramFile;
use anyhow::Result;
use bitvec::vec::BitVec;
use log::error;
use tokio::sync::{mpsc, oneshot, watch, RwLock};

use super::manager::WorkerConfig;

const FILTER_SUBDIR: &str = "filters";

#[derive(Debug)]
enum ReaderCommand {
    Query(Query, oneshot::Sender<Result<Vec<u64>>>)
}

#[derive(Debug)]
pub enum WriterCommand {
    Ingest(Vec<(u64, BitVec)>, oneshot::Sender<()>)
}

pub struct FilterWorker {
    readiness: watch::Receiver<bool>,
    reader_connection: mpsc::Sender<ReaderCommand>,
    pub writer_connection: mpsc::Sender<WriterCommand>,
}

impl FilterWorker {
    pub fn open(config: WorkerConfig, id: FilterID) -> Result<Self> {
        {
            let directory = config.data_path.join(FILTER_SUBDIR);
            std::fs::create_dir_all(&directory)?;
        }

        let (ready_send, ready_recv) = watch::channel(false);
        let (reader_send, reader_recv) = mpsc::channel(64);
        let (writer_send, writer_recv) = mpsc::channel(2);
        std::thread::spawn({
            // let ready_recv = ready_recv.clone();
            move || { writer_worker(writer_recv, config, id, ready_send, reader_recv) }
        });
        Ok(Self { readiness: ready_recv, reader_connection: reader_send, writer_connection: writer_send })
    }

    pub fn is_ready(&self) -> bool {
        *self.readiness.borrow()
    }

    pub async fn query(&self, query: Query) -> Result<Vec<u64>> {
        let (send, recv) = oneshot::channel();
        self.reader_connection.send(ReaderCommand::Query(query, send)).await?;
        return recv.await?
    }
}

fn writer_worker(mut writer_recv: mpsc::Receiver<WriterCommand>, config: WorkerConfig, id: FilterID, ready_send: watch::Sender<bool>, reader_recv: mpsc::Receiver<ReaderCommand>) {
    // Open the file
    let directory = config.data_path.join(FILTER_SUBDIR);
    let path = directory.join(id.to_string());
    let filter = if path.exists() {
        ExtensibleTrigramFile::open(&path)
    } else {
        ExtensibleTrigramFile::new(&path, config.initial_segment_size, config.extended_segment_size)
    };
    let filter = match filter {
        Ok(filter) => Arc::new(RwLock::new(filter)),
        Err(err) => {
            error!("Could not load filter: {err}");
            return;
        },
    };

    // Spawn reader
    _ = ready_send.send(true);
    std::thread::spawn({
        let filter = filter.clone();
        move || { reader_worker(filter, reader_recv); }
    });

    while let Err(err) = _writer_worker(&mut writer_recv, id, filter.clone()) {
        error!("writer worker: {err}");
    }
}

pub fn _writer_worker(writer_recv: &mut mpsc::Receiver<WriterCommand>, _id: FilterID, filter: Arc<RwLock<ExtensibleTrigramFile>>) -> Result<()> {
    while let Some(message) = writer_recv.blocking_recv() {
        match message {
            WriterCommand::Ingest(mut batch, finished) => {
                // Insert the batch
                let batch = {
                    let filter = filter.blocking_read();
                    filter.write_batch(&mut batch)?
                };
                {
                    let mut filter = filter.blocking_write();
                    filter.apply_operations(batch.0, batch.1)?;
                }
                _ = finished.send(());
            }
        }
    }
    return Ok(())
}

fn reader_worker(filter: Arc<RwLock<ExtensibleTrigramFile>>, mut reader_recv: mpsc::Receiver<ReaderCommand>) {
    loop {
        match _reader_worker(filter.clone(), &mut reader_recv) {
            Ok(()) => break,
            Err(err) => {
                error!("reader worker: {err}");
            }
        }
    }
}

fn _reader_worker(filter: Arc<RwLock<ExtensibleTrigramFile>>, reader_recv: &mut mpsc::Receiver<ReaderCommand>) -> Result<()> {
    while let Some(message) = reader_recv.blocking_recv() {
        match message {
            ReaderCommand::Query(query, response) => {
                let filter = filter.blocking_read();
                _ = response.send(filter.query(&query));
            },
        }
    }
    Ok(())
}
