use std::sync::Arc;

use disk::buffer_pool_manager::BufferPoolManager;
use index::disk_extendible_hash_table::DiskExtendibleHashTable;

mod b_tree;
mod config;
mod disk;
mod index;

fn main() {
    // TODO: fix the todos haha
    let bpm = Arc::new(BufferPoolManager::new(String::from("db/test.db"), 2, 2));
    let ht = DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 2);

    ht.insert(1, 2).unwrap();
    ht.insert(3, 4).unwrap();
    ht.insert(5, 6).unwrap();

    bpm.flush_all_pages();

    // TODO: see if reading it from disk works fine
    // TODO: printing of hash table for debugging
}
