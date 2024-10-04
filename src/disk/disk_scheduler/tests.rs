use std::{env::temp_dir, fs::remove_file, sync::Arc, thread};

use crate::{
    config::DB_PAGE_SIZE,
    disk::{
        disk_manager::DiskManager,
        disk_scheduler::{DiskRequest, DiskRequestType},
    },
};

use super::{DiskResponse, DiskScheduler};

#[test]
fn multi_threaded_disk_requests() {
    // create temp file
    let db_path = temp_dir().join("multi_threaded_disk_requests.db");
    let db_file_path = db_path.to_str().unwrap().to_string();

    // run test
    let dm = DiskManager::new(String::from(db_file_path));
    let ds = Arc::new(DiskScheduler::new(dm));

    let mut handles = vec![];
    for i in 0..10 {
        let ds = Arc::clone(&ds);

        let handle = thread::spawn(move || {
            let data = [i as u8; DB_PAGE_SIZE as usize].to_vec();
            let req_type = DiskRequestType::Write(data.clone());

            let rx = ds.schedule(DiskRequest {
                page_id: i,
                req_type,
            });

            if let DiskResponse::WriteResponse = rx.recv().unwrap() {
            } else {
                panic!("Incorrect response type")
            }

            let req_type = DiskRequestType::Read;
            let rx = ds.schedule(DiskRequest {
                page_id: i,
                req_type,
            });

            if let DiskResponse::ReadResponse(read_data) = rx.recv().unwrap() {
                assert_eq!(read_data.unwrap(), data);
            } else {
                panic!("Incorrect response type");
            }
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

    // remove created db file
    remove_file(db_path).expect("Couldn't remove test DB file");
}
