use config::DB_PAGE_SIZE;
use disk::buffer_pool_manager::BufferPoolManager;

mod b_tree;
mod config;
mod disk;

fn main() {
    // TODO: do a bunch of single-threaded test cases
    // TODO: do a bunch of multi-threaded test cases
    let bpm = BufferPoolManager::new("db/test.db".to_string(), 2, 2);

    let page_id1 = bpm.new_page();
    let page_id2 = bpm.new_page();

    let mut page1 = bpm.get_write_page(page_id1);
    page1.write([1 as u8; DB_PAGE_SIZE as usize].to_vec());
    drop(page1);
    
    let mut page2 = bpm.get_write_page(page_id2);
    page2.write([2 as u8; DB_PAGE_SIZE as usize].to_vec());
    drop(page2);

    let page_id3 = bpm.new_page();
    let mut page3 = bpm.get_write_page(page_id3);
    page3.write([3 as u8; DB_PAGE_SIZE as usize].to_vec());
    drop(page3);

    // page that was evicted doesn't get written to disk, fix!
    bpm.flush_all_pages();
}
