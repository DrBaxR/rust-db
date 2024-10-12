use crate::config::DB_PAGE_SIZE;

use super::*;
use std::{env::temp_dir, fs::remove_file, sync::Arc, thread, time::Duration };

#[test]
fn eviction() {
    // init
    let db_path = temp_dir().join("bpm_eviction_test.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = BufferPoolManager::new(db_file_path.clone(), 2, 2);

    let page1_data = [1 as u8; DB_PAGE_SIZE as usize].to_vec();
    let page2_data = [2 as u8; DB_PAGE_SIZE as usize].to_vec();
    let page3_data = [3 as u8; DB_PAGE_SIZE as usize].to_vec();

    // three pages loaded, means one got evicted and written to
    let page_id1 = bpm.new_page();
    let page_id2 = bpm.new_page();

    let mut page2 = bpm.get_write_page(page_id2);
    page2.write(page2_data.clone());
    drop(page2);
    thread::sleep(Duration::new(0, 1000000)); // add 1ms delay to make sure this page access timestamp is different than the next one

    let mut page1 = bpm.get_write_page(page_id1);
    page1.write(page1_data.clone());
    drop(page1);

    let page_id3 = bpm.new_page();
    let mut page3 = bpm.get_write_page(page_id3);
    page3.write(page3_data.clone());
    drop(page3);

    // page 2 should have been evicted and its data flushed
    let dm = DiskManager::new(db_file_path);
    let data = dm.read_page(page_id2).unwrap();
    assert_eq!(data, page2_data);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn delete_memory() {
    // init
    let db_path = temp_dir().join("bpm_delete_memory.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = BufferPoolManager::new(db_file_path.clone(), 2, 2);

    let page1_data = [1 as u8; DB_PAGE_SIZE as usize].to_vec();
    let page2_data = [2 as u8; DB_PAGE_SIZE as usize].to_vec();
    let page3_data = [3 as u8; DB_PAGE_SIZE as usize].to_vec();

    // three pages loaded, means one got evicted and written to
    let page_id1 = bpm.new_page();
    let page_id2 = bpm.new_page();

    let mut page2 = bpm.get_write_page(page_id2);
    page2.write(page2_data.clone());
    drop(page2);

    let mut page1 = bpm.get_write_page(page_id1);
    page1.write(page1_data.clone());
    drop(page1);

    bpm.delete_page(page_id2);

    let page_id3 = bpm.new_page();
    let mut page3 = bpm.get_write_page(page_id3);
    page3.write(page3_data.clone());
    drop(page3);

    // no pages should have been flushed
    let dm = DiskManager::new(db_file_path);
    let data1 = dm.read_page(page_id1).unwrap();
    let data2 = dm.read_page(page_id2).unwrap();
    let data3 = dm.read_page(page_id3).unwrap();

    let empty = [0 as u8; DB_PAGE_SIZE as usize];
    assert_eq!(data1, empty);
    assert_eq!(data2, empty);
    assert_eq!(data3, empty);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn eviction_with_flush_all() {
    // init
    let db_path = temp_dir().join("bpm_eviction_with_flush_all.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = BufferPoolManager::new(db_file_path.clone(), 2, 2);

    let page1_data = [1 as u8; DB_PAGE_SIZE as usize].to_vec();
    let page2_data = [2 as u8; DB_PAGE_SIZE as usize].to_vec();
    let page3_data = [3 as u8; DB_PAGE_SIZE as usize].to_vec();

    // three pages loaded then everything flushed
    let page_id1 = bpm.new_page();
    let page_id2 = bpm.new_page();

    let mut page2 = bpm.get_write_page(page_id2);
    page2.write(page2_data.clone());
    drop(page2);

    let mut page1 = bpm.get_write_page(page_id1);
    page1.write(page1_data.clone());
    drop(page1);

    let page_id3 = bpm.new_page();
    let mut page3 = bpm.get_write_page(page_id3);
    page3.write(page3_data.clone());
    drop(page3);

    bpm.flush_all_pages();

    // data from all pages should be on disk
    let dm = DiskManager::new(db_file_path);
    let disk_page1_data = dm.read_page(page_id1).unwrap();
    let disk_page2_data = dm.read_page(page_id2).unwrap();
    let disk_page3_data = dm.read_page(page_id3).unwrap();

    assert_eq!(page1_data, disk_page1_data);
    assert_eq!(page2_data, disk_page2_data);
    assert_eq!(page3_data, disk_page3_data);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn writes_and_reads() {
    // init
    let db_path = temp_dir().join("bpm_writes_and_reads.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = BufferPoolManager::new(db_file_path.clone(), 2, 2);

    let page1_data = [1 as u8; DB_PAGE_SIZE as usize].to_vec();
    let page2_data = [2 as u8; DB_PAGE_SIZE as usize].to_vec();
    let page3_data = [3 as u8; DB_PAGE_SIZE as usize].to_vec();

    let page_id1 = bpm.new_page();
    let page_id2 = bpm.new_page();

    // execute writes and read written data
    // writes
    let mut page2 = bpm.get_write_page(page_id2);
    page2.write(page2_data.clone());
    drop(page2);

    let mut page1 = bpm.get_write_page(page_id1);
    page1.write(page1_data.clone());
    drop(page1);

    let page_id3 = bpm.new_page();
    let mut page3 = bpm.get_write_page(page_id3);
    page3.write(page3_data.clone());
    drop(page3);

    // reads
    let page2 = bpm.get_read_page(page_id2);
    let read_page2_data = page2.read().clone();
    drop(page2);

    let page3 = bpm.get_read_page(page_id3);
    let read_page3_data = page3.read().clone();
    drop(page3);

    // read data should be the same as write data
    assert_eq!(page2_data, read_page2_data);
    assert_eq!(page3_data, read_page3_data);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn multi_threaded_reads_writes() {
    // init
    let db_path = temp_dir().join("bpm_multi_threaded_reads_writes.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = BufferPoolManager::new(db_file_path.clone(), 2, 2);

    // for a single page, have multiple threads read its content and write it back incremented by 1
    let bpm = Arc::new(bpm);
    let page_id = bpm.new_page();

    let mut handles = vec![];
    for _ in 0..10 {
        let bpm = Arc::clone(&bpm);

        let handle = thread::spawn(move || {
            let read_page = bpm.get_read_page(page_id);
            let _ = read_page.read().clone(); // read only done to increase the chance of concurrency issues
            drop(read_page);

            let mut write_page = bpm.get_write_page(page_id);
            let data = write_page.read();
            let new_data = [data[0] + 1; DB_PAGE_SIZE as usize].to_vec();
            write_page.write(new_data);
            drop(write_page);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    bpm.flush_all_pages();

    // data inside the page should all be bytes of `10`
    let dm = DiskManager::new(db_file_path);

    let actual_data = dm.read_page(page_id).unwrap();
    let expected_data = [10 as u8; DB_PAGE_SIZE as usize];
    assert_eq!(actual_data, expected_data);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}
