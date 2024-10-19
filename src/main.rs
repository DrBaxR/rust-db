use disk::disk_manager::DiskManager;
use index::{bucket_page::HashTableBucketPage, serial::{Deserialize, Serialize}};

mod b_tree;
mod config;
mod disk;
mod index;

fn main() {
    // TODO: demo with buffer pool manager of how pages would be used

    // init
    let dm = DiskManager::new(String::from("db/test.db"));

    // write mock page to disk
    let bucket = HashTableBucketPage::<u32, u8>::new(vec![(1, 2), (2, 3), (3, 4), (3, 5), (4, 5)]);
    dm.write_page(0, &bucket.serialize());

    let bucket = HashTableBucketPage::<u32, u8>::deserialize(&dm.read_page(0).unwrap());
    dbg!(bucket);
}