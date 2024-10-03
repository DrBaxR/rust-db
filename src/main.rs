use std::{sync::Arc, thread};

use disk::disk_scheduler::{DiskRequest, DiskRequestType, DiskScheduler};

mod config;
mod disk;
mod node;
mod tree;

fn main() {
    let ds = Arc::new(DiskScheduler::new());

    let mut handles = vec![];
    for i in 0..10 {
        let ds = Arc::clone(&ds);

        let handle = thread::spawn(move || {
            let req_type = if i % 2 == 0 {
                DiskRequestType::Read
            } else {
                DiskRequestType::Write(Vec::new())
            };

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
            panic!("Disk scheduler Arc has more than 1 strong reference left, can't shutdown")
        }
    };

    ds.shutdown();
}
