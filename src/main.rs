use std::{sync::Arc, thread};

use disk::disk_manager::DiskManager;

const DB_PAGE_SIZE: u32 = 4096;
const DB_DEFAULT_PAGES_AMOUNT: usize = 16;

mod node;
mod tree;
mod disk;

fn main() {
    let dm = DiskManager::new("db/test.db".to_string());
    let dm = Arc::new(dm);

    let mut handles = vec![];
    for i in 0..10 {
        let dm_clone = Arc::clone(&dm);
        let handle = thread::spawn(move || {
            println!("Writing page with ID={}", i);
            dm_clone.write_page(i, &[i as u8; DB_PAGE_SIZE as usize]);
            let _ = dm_clone.read_page(i);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
