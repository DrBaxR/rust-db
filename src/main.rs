use disk::disk_manager::DiskManager;

const DB_PAGE_SIZE: u32 = 4096;

mod node;
mod tree;
mod disk;

fn main() {
    let dm = DiskManager::new("test.db".to_string());

    let page_id = 1;
    let mut page = dm.read_page(page_id);
    println!("{}", String::from_utf8(page.clone()).unwrap());

    page[4095] = 69 as u8;
    dm.write_page(page_id, &page);
}
