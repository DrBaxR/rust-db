use std::{env::temp_dir, fs::remove_file, sync::Arc, thread};

use super::*;

#[test]
fn default_disk_size() {
    // create temp file
    let db_path = temp_dir().join("default_disk_size.db");
    let db_file_path = db_path.to_str().unwrap().to_string();

    // check size of created db file
    let _ = DiskManager::new(db_file_path.clone());
    let db_file_len = File::open(db_file_path).unwrap().metadata().unwrap().len();

    assert_eq!(
        db_file_len,
        DB_DEFAULT_PAGES_AMOUNT as u64 * DB_PAGE_SIZE as u64
    );

    // remove created db file
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn increase_disk_size() {
    // create temp file
    let db_path = temp_dir().join("increase_disk_size.db");
    let db_file_path = db_path.to_str().unwrap().to_string();

    // check size of created db file
    let mut dm = DiskManager::new(db_file_path.clone());
    dm.increase_disk_size(33); // 33 > 16 * 2, means that size of file should double twice (64 pages)

    let db_file_len = File::open(db_file_path).unwrap().metadata().unwrap().len();
    assert_eq!(
        db_file_len,
        DB_DEFAULT_PAGES_AMOUNT as u64 * 4 * DB_PAGE_SIZE as u64
    );

    // remove created db file
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn multi_thread_read_write() {
    // create temp file
    let db_path = temp_dir().join("multi_thread_read_write.db");
    let db_file_path = db_path.to_str().unwrap().to_string();

    // run multi-threaded test
    let dm = Arc::new(DiskManager::new(db_file_path));
    let mut handles = vec![];

    for i in 0..10 {
        let dm_clone = Arc::clone(&dm);

        let handle = thread::spawn(move || {
            dm_clone.write_page(i, &[i as u8; DB_PAGE_SIZE as usize]);
            let data = dm_clone.read_page(i).unwrap();

            assert_eq!(data, vec![i as u8; DB_PAGE_SIZE as usize]);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // remove created db file
    remove_file(db_path).expect("Couldn't remove test DB file");
}
