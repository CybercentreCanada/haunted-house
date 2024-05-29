
use std::sync::Arc;

use log::error;
use tokio::sync::broadcast::error::RecvError;

pub fn channel<T: Clone>(depth: usize) -> (Sender<T>, Receiver<T>) {
    let notify = Arc::new(tokio::sync::Notify::new());

    let (sender, receiver) = tokio::sync::broadcast::channel(depth);

    (
        Sender {
            capacity: depth,
            broadcast: sender,
            notice: notify.clone(),
        },
        Receiver {
            broadcast: receiver,
            notice: notify,
            next: None,
        },
    )
}

pub struct Sender<T: Clone> {
    capacity: usize,
    broadcast: tokio::sync::broadcast::Sender<T>,
    notice: Arc<tokio::sync::Notify>,
}

impl<T: Clone> Sender<T> {
    pub fn is_connected(&self) -> bool {
        self.broadcast.receiver_count() > 0
    }

    pub async fn send(&mut self, value: T) -> bool {
        loop {
            if !self.is_connected() {
                return false
            }

            if self.broadcast.len() == self.capacity {
                self.notice.notified().await;
                continue
            }

            return self.broadcast.send(value).is_ok();
        }
    }

    pub fn subscribe(&self) -> Receiver<T> {
        Receiver { 
            broadcast: self.broadcast.subscribe(), 
            notice: self.notice.clone(),
            next: None
        }
    }
}

pub struct Receiver<T: Clone> {
    broadcast: tokio::sync::broadcast::Receiver<T>,
    notice: Arc<tokio::sync::Notify>,
    next: Option<Option<T>>,
}

impl<T: Clone> Receiver<T> {
    pub async fn next(&mut self) -> Option<T> {
        self.fetch_next().await;
        self.next.take().unwrap().take()
    }

    pub async fn peek(&mut self) -> Option<&T> {
        self.fetch_next().await;
        self.next.as_ref().unwrap().as_ref()
    }

    async fn fetch_next(&mut self) {
        if self.next.is_none() {
            self.next = Some(match self.broadcast.recv().await {
                Ok(value) => { 
                    self.notice.notify_waiters();
                    Some(value)
                },
                Err(RecvError::Closed) => None,
                Err(RecvError::Lagged(_)) => {
                    error!("Safe broadcast acted unsafely");
                    None
                }
            })
        }
    }
}