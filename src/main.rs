use config::DB_PAGE_SIZE;
use disk::{buffer_pool_manager::BufferPoolManager, disk_manager::DiskManager};

mod b_tree;
mod config;
mod disk;

fn main() {
    // TODO: do multi-threaded test cases

    // init
    let bpm = BufferPoolManager::new("db/test.db".to_string(), 2, 2);

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

    todo!("assert that nothing was flushed to disk");
}
