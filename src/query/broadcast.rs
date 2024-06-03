/// Reproduce a broadcast like interface using unbounded channels.

pub fn channel<T: Clone>(label: u32) -> (Sender<T>, Receiver<T>) {

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    (
        Sender {
            label,
            outputs: vec![sender],
        },
        Receiver {
            label,
            input: receiver,
            next: None,
        },
    )
}

pub struct Sender<T: Clone> {
    pub label: u32,
    outputs: Vec<tokio::sync::mpsc::UnboundedSender<T>>,
}

impl<T: Clone> Sender<T> {
    pub fn is_connected(&mut self) -> bool {
        self.outputs.retain(|con|!con.is_closed());
        !self.outputs.is_empty()
    }

    pub fn send(&mut self, value: T) -> bool {
        for channel in &self.outputs {
            _ = channel.send(value.clone());
        }

        self.is_connected()
    }

    pub fn subscribe(&mut self) -> Receiver<T> {
        let (send, recv) = tokio::sync::mpsc::unbounded_channel();
        self.outputs.push(send);

        Receiver { 
            label: self.label,
            input: recv,
            next: None
        }
    }
}

pub struct Receiver<T: Clone> {
    pub label: u32,
    input: tokio::sync::mpsc::UnboundedReceiver<T>,
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

    pub async fn skip(&mut self) {
        if self.next.is_some() {
            self.next = None;
            return
        }
        self.fetch_next().await;
        self.next = None;
    }

    async fn fetch_next(&mut self) {
        if self.next.is_none() {
            self.next = Some(self.input.recv().await)
        }
    }
}


#[tokio::test]
async fn test_drop_delivery() {
    let mut recv = {
        let (mut send, recv) = channel(0);
        assert!(send.send(1));
        assert!(send.send(2));
        assert!(send.send(3));
        recv
    };
    assert_eq!(recv.next().await, Some(1));
    assert_eq!(recv.next().await, Some(2));
    assert_eq!(recv.next().await, Some(3));
    assert!(recv.next().await.is_none());
}