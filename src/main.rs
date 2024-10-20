use std::sync::Arc;

use disk::{buffer_pool_manager::BufferPoolManager, disk_manager::PageID};
use index::disk_extendible_hash_table::DiskExtendibleHashTable;

mod b_tree;
mod config;
mod disk;
mod index;

fn main() {
    let bpm = Arc::new(BufferPoolManager::new(String::from("db/test.db"), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 2, 2, String::from("index"));

    for i in 0..5000 {
        ht.insert(i, i).unwrap();
        ht.print();
    }

    bpm.flush_all_pages();

    println!("FINAL TABLE STATE FROM DISK:");
    read_from_disk(0, "index".to_string());
}

fn read_from_disk(header_pid: PageID, name: String) {
    let bpm = Arc::new(BufferPoolManager::new(String::from("db/test.db"), 2, 2));
    let ht = DiskExtendibleHashTable::<i32, i32>::from_disk(bpm, header_pid, name);

    ht.print();
}
