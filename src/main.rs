use std::{sync::Arc, thread};

use disk::buffer_pool_manager::BufferPoolManager;
use index::disk_extendible_hash_table::DiskExtendibleHashTable;

mod b_tree;
mod config;
mod disk;
mod index;
mod parser;
mod table;

fn main() {
    let bpm = Arc::new(BufferPoolManager::new(String::from("db/test.db"), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 4, String::from("index"));
    let ht = Arc::new(ht);

    // insert initial elements
    for i in 0..3000 {
        ht.insert(i, i).unwrap();
    }

    let mut handles = vec![];
    // insert more elements
    for i in 0..4 {
        let ht = Arc::clone(&ht);

        let handle = thread::spawn(move || {
            let start = 3000 + i * 500;
            let end = start + 500;

            for i in start..end {
                ht.insert(i, i).unwrap();
            }
        });

        handles.push(handle);
    }

    // remove all elements
    for i in 0..4 {
        let ht = Arc::clone(&ht);

        let handle = thread::spawn(move || {
            let start = i * 750;
            let end = start + 750;

            for i in start..end {
                ht.remove(i);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    ht.print();

    for i in 0..3000 {
        assert_eq!(ht.lookup(i), vec![]);
    }

    for i in 3000..5000 {
        assert_eq!(ht.lookup(i), vec![i]);
    }
}
