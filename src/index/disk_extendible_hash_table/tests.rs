use std::{env::temp_dir, fs::remove_file, sync::Arc, thread};

use crate::{
    disk::{
        buffer_pool_manager::{BufferPoolManager, DiskRead},
        disk_manager::PageID,
    },
    index::{
        bucket_page::HashTableBucketPage, directory_page::HashTableDirectoryPage,
        header_page::HashTableHeaderPage, serial::Deserialize,
    },
};

use super::DiskExtendibleHashTable;

#[test]
fn simple_insert() {
    // init
    let db_path = temp_dir().join("deht_simple_insert.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 0, String::from("index"));

    // simple inserts
    ht.insert(0, 1).unwrap();
    ht.insert(1, 2).unwrap();
    ht.insert(1, 9).unwrap();
    ht.insert(3, 4).unwrap();

    assert_eq!(ht.lookup(0), vec![1]);
    assert_eq!(ht.lookup(1), vec![2, 9]);
    assert_eq!(ht.lookup(3), vec![4]);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn bucket_single_split_insert_overflow() {
    // init
    let db_path = temp_dir().join("deht_bucket_bucket_single_split_insert_overflow.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 0, String::from("index")); // depths too small

    // simple inserts
    for i in 0..511 {
        ht.insert(i, i + 1).unwrap();
    }

    assert!(ht.insert(1, 1).is_err());

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn bucket_single_split_insert() {
    // init
    let db_path = temp_dir().join("deht_bucket_single_split_insert.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 1, String::from("index"));

    // simple inserts
    for i in 0..513 {
        ht.insert(i, i + 1).unwrap();
        ht.print();
    }

    for i in 0..513 {
        let res = ht.lookup(i);
        assert_eq!(res, vec![i + 1]);
    }

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn bucket_split_insert_directory_double() {
    // init
    let db_path = temp_dir().join("deht_bucket_split_insert_directory_double.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 2, String::from("index"));

    // simple inserts
    for i in 0..2000 {
        ht.insert(i, i + 1).unwrap();
    }

    for i in 0..2000 {
        let res = ht.lookup(i);
        assert_eq!(res, vec![i + 1]);
    }

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn serialization() {
    // init
    let db_path = temp_dir().join("deht_serialization.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 2, String::from("index"));

    // insert data
    for i in 0..513 {
        ht.insert(i, i + 1).unwrap();
    }
    bpm.flush_all_pages();

    // read data
    let disk_ht = DiskExtendibleHashTable::<i32, i32>::from_disk(
        Arc::clone(&bpm),
        ht.header_page_id,
        "disk index".to_string(),
    );

    for i in 0..513 {
        let res = disk_ht.lookup(i);
        assert_eq!(res, vec![i + 1]);
    }

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn remove_simple() {
    // init
    let db_path = temp_dir().join("deht_remove_simple.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 2, String::from("index"));

    // insert and remove
    ht.insert(1, 1).unwrap();
    assert_eq!(ht.lookup(1), vec![1]);

    assert_eq!(ht.remove(1), 1);
    assert_eq!(ht.lookup(1), vec![]);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn remove_multiple() {
    // init
    let db_path = temp_dir().join("deht_remove_multiple.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 2, String::from("index"));

    // insert and remove
    ht.insert(1, 1).unwrap();
    ht.insert(1, 2).unwrap();
    ht.insert(1, 3).unwrap();
    ht.insert(2, 2).unwrap();
    assert_eq!(ht.lookup(1), vec![1, 2, 3]);

    assert_eq!(ht.remove(1), 3);
    assert_eq!(ht.lookup(1), vec![]);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn remove_merges() {
    // init
    let db_path = temp_dir().join("deht_remove_merges.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let ht =
        DiskExtendibleHashTable::<i32, i32>::new(Arc::clone(&bpm), 0, 3, String::from("index"));

    // remove with merges
    assert_eq!(get_directories(&ht).len(), 0);

    // split once
    for i in 0..513 {
        ht.insert(i, i).unwrap();
    }
    assert_eq!(get_directories(&ht)[0].size(), 2);

    // split second time
    for i in 513..1025 {
        ht.insert(i, i).unwrap();
    }
    assert_eq!(get_directories(&ht)[0].size(), 4);

    // merge twice
    for i in 0..1025 {
        ht.remove(i);
    }
    assert_eq!(get_directories(&ht)[0].size(), 1);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

/// Returns all the managed directory pages (order is as stored internally, but shouldn't be relied on).
fn get_directories(ht: &DiskExtendibleHashTable<i32, i32>) -> Vec<HashTableDirectoryPage> {
    let mut directories = vec![];

    let h_page = ht.bpm.get_read_page(ht.header_page_id);
    let header = HashTableHeaderPage::deserialize(h_page.read());
    drop(h_page);

    for i in 0..header.max_size() {
        let d_pid = match header.get_directory_page_id(i) {
            Some(pid) => pid,
            None => continue,
        };

        let d_page = ht.bpm.get_read_page(d_pid);
        let directory = HashTableDirectoryPage::deserialize(d_page.read());
        drop(d_page);

        directories.push(directory);
    }

    directories
}

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

    assert_eq!(directories_single_thread.len(), directories_multi_thread.len());
    for d_st in directories_single_thread.iter() {
        for d_mt in directories_multi_thread.iter() {
            assert_eq!(d_st.size(), d_mt.size());
        }
    }
}

fn single_thread(ht: &Arc<DiskExtendibleHashTable<i32, i32>>, size: i32) -> Vec<HashTableDirectoryPage> {
    for i in 0..size {
        let start = i * 250;
        let end = start + 250;

        for i in start..end {
            ht.insert(i, i).unwrap();
        }
    }

    get_directories(ht)
}

fn multi_thread(ht: &Arc<DiskExtendibleHashTable<i32, i32>>, size: i32) -> Vec<HashTableDirectoryPage> {
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
    for i in 0..8 { // insert 8 * 250 = 2000 entries
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