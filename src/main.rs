use std::{sync::Arc, thread};

use config::DB_PAGE_SIZE;
use disk::buffer_pool_manager::BufferPoolManager;

mod b_tree;
mod config;
mod disk;

fn main() {
    let bpm = BufferPoolManager::new(String::from("db/test.db"), 2, 2);
    let bpm = Arc::new(bpm);
    let page_id1 = bpm.new_page();
    let page_id2 = bpm.new_page();
    let page_id3 = bpm.new_page();

    let mut handles = vec![];
    for i in 0..30 {
        // 10 threads of each category
        let bpm = Arc::clone(&bpm);

        let handle = thread::spawn(move || {
            let category = i % 3 + 1;
            let page_id = match category {
                1 => page_id1,
                2 => page_id2,
                _ => page_id3,
            };

            let read_page = bpm.get_read_page(page_id);
            let _ = read_page.read().clone();
            drop(read_page);

            let mut write_page = bpm.get_write_page(page_id);
            let data = write_page.read();
            let new_data = [data[0] + category; DB_PAGE_SIZE as usize].to_vec();
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
