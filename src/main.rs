use config::DB_PAGE_SIZE;
use disk::buffer_pool_manager::BufferPoolManager;

mod b_tree;
mod config;
mod disk;

fn main() {
    // TODO: do a bunch of single-threaded test cases
    // TODO: do a bunch of multi-threaded test cases
    let bpm = BufferPoolManager::new("db/test.db".to_string(), 10, 2);

    let page_id = bpm.new_page();

    let mut page = bpm.get_write_page(page_id);
    page.write([69 as u8; DB_PAGE_SIZE as usize].to_vec());
    drop(page);
    
    let page = bpm.get_read_page(page_id);
    dbg!(page.read());
    drop(page);

    bpm.flush_page(page_id);
}
