use std::{sync::Arc, thread};

use config::DB_PAGE_SIZE;
use disk::{disk_manager::DiskManager, disk_scheduler::{DiskRequest, DiskRequestType, DiskScheduler}};

mod b_tree;
mod config;
mod disk;

fn main() {
    let dm = DiskManager::new(String::from("db/test.db"));
    let ds = Arc::new(DiskScheduler::new(dm));

    let mut handles = vec![];
    for i in 0..10 {
        let ds = Arc::clone(&ds);

        let handle = thread::spawn(move || {
            // TODO: make this a test where you also do reads on the data you saved
            let req_type = DiskRequestType::Write([i as u8; DB_PAGE_SIZE as usize].to_vec());

            let rx = ds.schedule(DiskRequest {
                page_id: i,
                req_type,
            });
            rx.recv().unwrap();

            println!("Executed {}", i);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let ds = match Arc::try_unwrap(ds) {
        Ok(ds) => ds,
        Err(_) => {
            panic!("Disk scheduler Arc has more than 1 strong reference left, can't cleanup")
        }
    };

    ds.shutdown();
}
