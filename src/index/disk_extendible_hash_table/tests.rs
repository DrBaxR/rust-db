use std::{env::temp_dir, fs::remove_file, sync::Arc};

use crate::disk::buffer_pool_manager::BufferPoolManager;

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
