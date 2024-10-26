use std::{env::temp_dir, fs::remove_file, sync::Arc, thread};

use crate::{
    disk::buffer_pool_manager::BufferPoolManager,
    index::{directory_page::HashTableDirectoryPage, disk_extendible_hash_table::DiskExtendibleHashTable},
};

use super::get_directories;

#[test]
fn insert_multi_threaded_splitting() {
    let size = 8;

    // init
    let db_path = temp_dir().join("deht_insert_multi_threaded_splitting_single.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 4, String::from("index"));
    let ht = Arc::new(ht);

    let directories_single_thread = single_thread(&ht, size);

    // cleanup
    remove_file(db_path).unwrap();

    // init
    let db_path = temp_dir().join("deht_insert_multi_threaded_splitting_multi.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 4, String::from("index"));
    let ht = Arc::new(ht);

    let directories_multi_thread = multi_thread(&ht, size);

    // cleanup
    remove_file(db_path).unwrap();

    assert_eq!(
        directories_single_thread.len(),
        directories_multi_thread.len()
    );
    for d_st in directories_single_thread.iter() {
        for d_mt in directories_multi_thread.iter() {
            assert_eq!(d_st.size(), d_mt.size());
        }
    }
}

fn single_thread(
    ht: &Arc<DiskExtendibleHashTable<i32, i32>>,
    size: i32,
) -> Vec<HashTableDirectoryPage> {
    for i in 0..size {
        let start = i * 250;
        let end = start + 250;

        for i in start..end {
            ht.insert(i, i).unwrap();
        }
    }

    get_directories(ht)
}

fn multi_thread(
    ht: &Arc<DiskExtendibleHashTable<i32, i32>>,
    size: i32,
) -> Vec<HashTableDirectoryPage> {
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

    get_directories(ht)
}

#[test]
fn insert_multi_threaded_semantics() {
    // init
    let db_path = temp_dir().join("deht_insert_multi_threaded_semantics.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 4, String::from("index"));
    let ht = Arc::new(ht);

    // test
    let mut handles = vec![];
    for i in 0..8 {
        // insert 8 * 250 = 2000 entries
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

    // check that all elements were successfully inserted
    for i in 0..2000 {
        assert_eq!(ht.lookup(i), vec![i]);
    }

    // cleanup
    remove_file(db_path).unwrap();
}

#[test]
fn remove_multi_threaded() {
    // init
    let db_path = temp_dir().join("deht_remove_multi_threaded.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 4, String::from("index"));
    let ht = Arc::new(ht);

    // insert initial elements
    for i in 0..4000 {
        ht.insert(i, i).unwrap();
    }

    // remove all elements
    let mut handles = vec![];
    for i in 0..8 {
        let ht = Arc::clone(&ht);

        let handle = thread::spawn(move || {
            let start = i * 500;
            let end = start + 500;

            for i in start..end {
                ht.remove(i);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let directory = &get_directories(&ht)[0];
    assert_eq!(directory.size(), 1);
    for i in 0..4000 {
        assert_eq!(ht.lookup(i), vec![]);
    }

    // cleanup
    remove_file(db_path).unwrap();
}

#[test]
fn multi_threaded_insert_remove() {
    // init
    let db_path = temp_dir().join("deht_multi_threaded_insert_remove.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
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

    // remove inital elements
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

    // check that inital elements were removed and new elements are in
    for i in 0..3000 {
        assert_eq!(ht.lookup(i), vec![]);
    }
    for i in 3000..5000 {
        assert_eq!(ht.lookup(i), vec![i]);
    }

    // cleanup
    remove_file(db_path).unwrap();
}