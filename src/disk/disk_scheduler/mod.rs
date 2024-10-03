use std::{
    sync::{mpsc::{self, Receiver}, Arc},
    thread::{self, JoinHandle},
};

use super::disk_manager::PageID;

#[derive(Debug)]
pub enum DiskRequestType {
    Read,
    Write(Vec<u8>),
}

#[derive(Debug)]
pub struct DiskRequest {
    pub page_id: PageID,
    pub req_type: DiskRequestType,
}

pub struct DiskScheduler {
    sender: Arc<mpsc::Sender<(DiskRequest, mpsc::Sender<()>)>>,
    worker_handle: JoinHandle<()>,
}

impl DiskScheduler {
    /// Creates a new disk scheduler, which spawns a background worker thread that processes the scheduled requests.
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<(DiskRequest, mpsc::Sender<()>)>();

        // At the moment this doesn't bring any performance boost, since there is a single worker thread that blocks on access
        // of the database file. This will be improved later.
        let worker_handle = thread::spawn(move || {
            for (request, notification) in receiver {
                // TODO: actually process the disk requests

                notification.send(()).expect("Failed to send request completed notification");
            }
        });

        Self {
            sender: Arc::new(sender),
            worker_handle,
        }
    }

    /// Schedule `request` to be executed. Will automatically return a receiver that will receive a value once the request has been executed.
    pub fn schedule(&self, request: DiskRequest) -> Receiver<()> {
        let sender = Arc::clone(&self.sender);
        let (tx, rx) = mpsc::channel();

        sender
            .send((request, tx))
            .expect("Failed to send a request to the disk scheduler");

        rx
    }

    /// Joins the background worker thread. Should be called once done using the disk scheduler.
    pub fn shutdown(self) {
        drop(self.sender);
        self.worker_handle.join().expect("Failed to join the background worker thread");
    }
}
