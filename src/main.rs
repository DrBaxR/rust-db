use std::{fs::remove_file, sync::Arc, thread};

use disk::buffer_pool_manager::BufferPoolManager;
use index::disk_extendible_hash_table::DiskExtendibleHashTable;

mod b_tree;
mod config;
mod disk;
mod index;

fn main() {
    let size = 5;

    {
        let bpm = Arc::new(BufferPoolManager::new(String::from("db/test.db"), 100, 2));
        let ht =
            DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 4, String::from("index"));
        let ht = Arc::new(ht);
        single_thread(&ht, size);
    }

    remove_file("db/test.db").unwrap();

    {
        let bpm = Arc::new(BufferPoolManager::new(String::from("db/test.db"), 100, 2));
        let ht =
            DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 4, String::from("index"));
        let ht = Arc::new(ht);
        multi_thread(&ht, size);
    }
}

fn single_thread(ht: &Arc<DiskExtendibleHashTable<i32, i32>>, size: i32) {
    for i in 0..size {
        let start = i * 250;
        let end = start + 250;

        for i in start..end {
            ht.insert(i, i).unwrap();
        }
    }

    ht.print();
}

fn multi_thread(ht: &Arc<DiskExtendibleHashTable<i32, i32>>, size: i32) {
    let mut handles = vec![];
    for i in 0..size {
        let ht = Arc::clone(&ht);

        let handle = thread::spawn(move || {
            let start = i * 250;
            let end = start + 250;

            for i in start..end {
                ht.insert(i, i).unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    ht.print();
}
