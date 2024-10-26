use std::{env::temp_dir, fs::remove_file, sync::Arc};

use crate::{
    disk::buffer_pool_manager::{BufferPoolManager, DiskRead},
    index::{
        directory_page::HashTableDirectoryPage, header_page::HashTableHeaderPage,
        serial::Deserialize,
    },
};

use super::DiskExtendibleHashTable;

mod concurrent;

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
