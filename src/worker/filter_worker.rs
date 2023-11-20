use std::sync::Arc;

use crate::query::Query;
use crate::timing::Capture;
use crate::types::FilterID;
use crate::worker::filter::ExtensibleTrigramFile;
use anyhow::Result;
use log::{error, info};
use tokio::sync::{mpsc, oneshot, watch, RwLock};

use crate::config::WorkerSettings;

use super::trigrams::CacheGuard;

#[derive(Debug)]
enum ReaderCommand {
    Query(Query, oneshot::Sender<Result<Vec<u64>>>)
}

// #[derive(Debug)]
// pub enum WriterCommand {
//     Ingest(Arc<CacheGuard>, u64, oneshot::Sender<()>),
//     Flush
// }
pub type WriterCommand = (Arc<CacheGuard>, u64, oneshot::Sender<()>);

pub struct FilterWorker {
    readiness: watch::Receiver<bool>,
    reader_connection: mpsc::Sender<ReaderCommand>,
    writer_thread: std::thread::JoinHandle<()>,
}

impl FilterWorker {
    pub fn open(config: WorkerSettings, id: FilterID) -> Result<(Self, mpsc::Sender<WriterCommand>)> {
        let (ready_send, ready_recv) = watch::channel(false);
        let (reader_send, reader_recv) = mpsc::channel(64);
        let (writer_send, writer_recv) = mpsc::channel(2);
        let writer_thread = std::thread::spawn({
            // let ready_recv = ready_recv.clone();
            move || { writer_worker(writer_recv, config, id, ready_send, reader_recv) }
        });
        Ok((Self { readiness: ready_recv, reader_connection: reader_send, writer_thread }, writer_send))
    }

    pub async fn join(self) {
        tokio::task::spawn_blocking(|| {
            if let Err(err) = self.writer_thread.join() {
                error!("{err:?}");
            }
        });
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

/// Worker that handles write operations on this index
fn writer_worker(mut writer_recv: mpsc::Receiver<WriterCommand>, config: WorkerSettings, id: FilterID, ready_send: watch::Sender<bool>, reader_recv: mpsc::Receiver<ReaderCommand>) {
    // Open the file
    let directory = config.get_filter_directory();
    let path = directory.join(id.to_string());
    let filter = if path.exists() {
        ExtensibleTrigramFile::open(directory, id)
    } else {
        ExtensibleTrigramFile::new(directory, id, config.initial_segment_size, config.extended_segment_size)
    };
    let filter = match filter {
        Ok(filter) => Arc::new(RwLock::new(filter)),
        Err(err) => {
            error!("Could not load filter {}: {err:?}", path.to_string_lossy());
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

/// Implementation method for writer_worker
pub fn _writer_worker(writer_recv: &mut mpsc::Receiver<WriterCommand>, id: FilterID, filter: Arc<RwLock<ExtensibleTrigramFile>>) -> Result<()> {
    // track how long we have had data pending a flush, None means no data to flush
    let mut flush_waiting: Option<std::time::Instant> = None;

    // iterate until our socket closes
    loop {
        // check if its time to flush data
        if let Some(waiting) = flush_waiting {
            if waiting.elapsed() > std::time::Duration::from_secs(600) {
                let mut filter = filter.blocking_write();
                if filter.outstanding_journals()? > 0 {
                    filter.flush_threaded(None, None)?;
                }
                flush_waiting = None;
            }
        }

        // read for more updates
        let message = match writer_recv.try_recv() {
            Ok(message) => message,
            Err(err) => match err {
                mpsc::error::TryRecvError::Empty => {
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    continue
                },
                mpsc::error::TryRecvError::Disconnected => {
                    break
                },
            },
        };

        // read all pending messages 
        let mut batch = vec![message];
        while let Ok(message) = writer_recv.try_recv() {
            batch.push(message);
        }
        batch.sort_by(|a, b|a.1.cmp(&b.1));

        // Read the data for the pending writes
        let mut responses = vec![];
        let mut data = vec![];
        for (guard, number, message) in batch {
            data.push((number, guard.load()?));
            responses.push((message, guard));
        }

        // write the data
        let capture = Capture::new();
        let batch = {
            let filter = filter.blocking_read();
            filter.write_batch(&mut data, &capture)?
        };
        {
            let mut filter = filter.blocking_write();
            filter.apply_operations(batch.0, batch.1, batch.2, &capture)?;
        }
        flush_waiting = Some(std::time::Instant::now());

        // notify the requesters and let the cache guards drop
        for (resp, _) in responses {
            resp.send(());
        }
    }

    info!("Stopping ingest writer for {id}");
    {
        let mut filter = filter.blocking_write();
        if filter.blocking_exists() {
            filter.flush_blocking()?;
        }
    }
    info!("Stopped ingest writer for {id}");
    return Ok(())
}

/// Outer function for running read operations on this index
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

/// implementation function for reader_worker
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
