use disk::{buffer_pool_manager::{BufferPoolManager, DiskRead, DiskWrite}, disk_manager::DiskManager};
use index::{bucket_page::HashTableBucketPage, serial::{Deserialize, Serialize}};

mod b_tree;
mod config;
mod disk;
mod index;

fn main() {
    let bpm = BufferPoolManager::new(String::from("db/test.db"), 2, 2);
    let page_id = bpm.new_page();

    let mut page = bpm.get_write_page(page_id);
    let mut bucket = HashTableBucketPage::<u32, u8>::new(vec![(1, 2), (2, 3), (3, 4), (3, 5), (4, 5)]);
    bucket.insert(4, 10).unwrap();
    page.write(bucket.serialize());
    drop(page);

    let page = bpm.get_read_page(page_id);
    let bucket = HashTableBucketPage::<u32, u8>::deserialize(page.read());
    println!("{:?}", bucket.lookup(4));
    drop(page);

    bpm.flush_all_pages();
}