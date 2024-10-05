use std::{
    sync::{
        mpsc::{self, Receiver},
        Arc,
    },
    thread::{self, JoinHandle},
};

use super::disk_manager::{DiskManager, PageID};

#[cfg(test)]
mod tests;

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

pub enum DiskResponse {
    /// Response payload will be `None` if trying to read from address that is beyond allocated space.
    ReadResponse(Option<Vec<u8>>),
    WriteResponse,
}

pub struct DiskScheduler {
    sender: Arc<mpsc::Sender<(DiskRequest, mpsc::Sender<DiskResponse>)>>,
    worker_handle: JoinHandle<()>,
}

impl DiskScheduler {
    /// Creates a new disk scheduler, which spawns a background worker thread that processes the scheduled requests.
    pub fn new(disk_manager: DiskManager) -> Self {
        let (sender, receiver) = mpsc::channel::<(DiskRequest, mpsc::Sender<DiskResponse>)>();

        // At the moment this doesn't bring any performance boost, since there is a single worker thread that blocks on access
        // of the database file. This will be improved later.
        let worker_handle = thread::spawn(move || {
            for (request, notification) in receiver {
                let response = DiskScheduler::process_request(&disk_manager, request);

                notification
                    .send(response)
                    .expect("Failed to send request completed notification");
            }
        });

        Self {
            sender: Arc::new(sender),
            worker_handle,
        }
    }

    fn process_request(disk_manager: &DiskManager, request: DiskRequest) -> DiskResponse {
        match request.req_type {
            DiskRequestType::Read => {
                let page = disk_manager.read_page(request.page_id);

                DiskResponse::ReadResponse(page)
            },
            DiskRequestType::Write(vec) => {
                disk_manager.write_page(request.page_id, &vec);

                DiskResponse::WriteResponse
            }
        }
    }

    /// Schedule `request` to be executed. Will automatically return a receiver that will receive a value once the request has been executed.
    pub fn schedule(&self, request: DiskRequest) -> Receiver<DiskResponse> {
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
        self.worker_handle
            .join()
            .expect("Failed to join the background worker thread");
    }
}
