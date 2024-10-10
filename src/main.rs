use config::DB_PAGE_SIZE;
use disk::{buffer_pool_manager::BufferPoolManager, disk_manager::DiskManager};

mod b_tree;
mod config;
mod disk;

fn main() {
    // TODO: do multi-threaded test cases
    // multiple readers
    // multiple writers
}
