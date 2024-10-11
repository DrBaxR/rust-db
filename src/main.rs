use std::{sync::{atomic::AtomicU32, Arc}, thread};

use config::DB_PAGE_SIZE;
use disk::buffer_pool_manager::BufferPoolManager;

mod b_tree;
mod config;
mod disk;

fn main() {
    // for a single page, have multiple threads read its content and write it back incremented by 1
    let bpm = BufferPoolManager::new(String::from("db/test.db"), 2, 2);
    let bpm = Arc::new(bpm);
    let page_id = bpm.new_page();

    let mut handles = vec![];
    for _ in 0..10 {
        let bpm = Arc::clone(&bpm);

        let handle = thread::spawn(move || {
            let read_page = bpm.get_read_page(page_id);
            let data = read_page.read().clone();
            drop(read_page);

            println!("current data: {}", data[0]);

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
}
