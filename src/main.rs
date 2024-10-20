use std::sync::Arc;

use disk::{buffer_pool_manager::BufferPoolManager, disk_manager::PageID};
use index::disk_extendible_hash_table::DiskExtendibleHashTable;

mod b_tree;
mod config;
mod disk;
mod index;

fn main() {
    let bpm = Arc::new(BufferPoolManager::new(String::from("db/test.db"), 2, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 2, String::from("index"));

    ht.print();
    ht.insert(1, 2).unwrap();
    ht.print();
    ht.insert(3, 4).unwrap();
    ht.print();
    ht.insert(5, 6).unwrap();
    ht.print();

    bpm.flush_all_pages();

    // TODO: see if reading it from disk works fine
}

fn read_from_disk(header_pid: PageID, name: String) {
    let bpm = Arc::new(BufferPoolManager::new(String::from("db/test.db"), 2, 2));
    let ht = DiskExtendibleHashTable::<i32, i32>::from_disk(bpm, header_pid, name);

    ht.print();
}
